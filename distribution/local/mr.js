const store = require('./store');
const groups = require('./groups');
const comm = require('./comm');
const id = require('../util/id');

// Ignore the SSL certificate
process.env.NODE_TLS_REJECT_UNAUTHORIZED = 0;

/**
 * MapReduce Local Service
 *
 * @param {Object} config
 */
function MapReduceService(config) {
  this.mapFunc = config.map; // map function
  this.reduceFunc = config.reduce; // reduce function
  this.keys = config.keys; // keys for this worker
  this.gid = config.gid; // gid of the group
  this.hashFunc = id[config.hashFuncName]; // hash function used
  this.name = config.serviceName;
  this.memory = config.memory || false;
  this.persistAfterReduce = config.persistAfterReduce || false;

  this.afterMapList = [];
  this.afterCombineMap = {};
  this.preReduceMap = {};
}

/**
 *  Map function
 *
 * @param {Function} cb
 */
MapReduceService.prototype.map = function(cb) {
  if (this.keys.length === 0) {
    cb(null, this.afterMapList);
    return;
  }

  let mapCompletes = 0;
  const doComplete = () => {
    mapCompletes++;

    if (mapCompletes === this.keys.length) {
      if (!this.memory) {
        const afterMapListKey = this.name + '-afterMapList';
        store.put(this.afterMapList, afterMapListKey, (err, res) => {
          cb(null, this.afterMapList);
          this.afterMapList = [];
        });
      } else {
        cb(null, this.afterMapList);
      }
    }
  };

  for (const key of this.keys) {
    store.get(key, (err, val) => {
      if (!err) {
        let mappedVal = this.mapFunc(key, val);

        if (mappedVal instanceof Promise) {
          mappedVal.then((data) => {
            mappedVal = data;

            if (mappedVal) {
              if (Array.isArray(mappedVal)) {
                this.afterMapList.push(...mappedVal);
              } else {
                this.afterMapList.push(mappedVal);
              }
            }

            doComplete();
          });
          return;
        }

        if (mappedVal) {
          if (Array.isArray(mappedVal)) {
            this.afterMapList.push(...mappedVal);
          } else {
            this.afterMapList.push(mappedVal);
          }
        }
      }
      doComplete();
    });
  }
};

/**
 * Shuffle, combine, and sort before reduce
 *
 * @param {Function} cb
 */
MapReduceService.prototype.preReduce = function(cb) {
  const shuffleAndCombine = () => {
    for (const pair of this.afterMapList) {
      const key = Object.keys(pair)[0];
      const val = pair[key];

      if (this.afterCombineMap[key] === undefined) {
        this.afterCombineMap[key] = [];
      }
      this.afterCombineMap[key].push(val);
    }
  };

  // combine
  if (!this.memory) {
    const persisKey = this.name + '-afterMapList';
    store.get(persisKey, (err, res) => {
      this.afterMapList = (err) ? [] : res;
      shuffleAndCombine();
      this.redistributeHelper(cb);
    });
  } else {
    shuffleAndCombine();
    this.redistributeHelper(cb);
  }
};

/**
 * Redistribute the key-value pairs to the right node
 *
 * @param {Function} cb
 */
MapReduceService.prototype.redistributeHelper = function(cb) {
  this.afterMapList = []; // free up space
  groups.get(this.gid, (err, group) => {
    if (err) {
      cb(err, null);
      return;
    }

    const entries = Object.entries(this.afterCombineMap);
    if (entries.length === 0) {
      cb(null, this.preReduceMap);
      return;
    }

    let redistributeComplete = 0;
    for (const entry of entries) {
      const [key, val] = entry;
      const node = id.getProperNode(key, group, this.hashFunc);
      const remote = {service: this.name, method: 'redistribute', node};

      comm.send([{[key]: val}], remote, (err, res) => {
        redistributeComplete++;
        if (redistributeComplete === entries.length) {
          this.afterCombineMap = {};
          cb(null, this.preReduceMap);
        }
      });
    }
  });
};

/**
 * Receive the redistributed intermediate key value pairs
 *
 * @param {Object} obj a key value pair
 * @param {Function} cb
 */
MapReduceService.prototype.redistribute = function(obj, cb) {
  const key = Object.keys(obj)[0];
  const val = obj[key];

  if (this.preReduceMap[key] === undefined) {
    this.preReduceMap[key] = [];
  }
  this.preReduceMap[key].push(...val);
  cb(null, 'redistribute!');
};

/**
 * Persist the preReduce intermediate values into store
 *
 * @param {Function} cb
 */
MapReduceService.prototype.preReducePersist = function(cb) {
  const preReduceMapKey = this.name + '-preReduceMap';
  store.put(this.preReduceMap, preReduceMapKey, (err, res) => {
    cb(null, this.preReduceMap);
    this.preReduceMap = {};
  });
};

/**
 * Reduce phase: use the reduce function
 *
 * @param {*} cb
 */
MapReduceService.prototype.reduce = function(cb) {
  const subReduce = () => {
    const afterReduce = [];
    Object.entries(this.preReduceMap).forEach(([key, vals]) => {
      const pair = this.reduceFunc(key, vals);
      afterReduce.push(pair);
    });

    if (this.persistAfterReduce) {
      let persisted = 0;
      for (const pair of afterReduce) {
        const key = Object.keys(pair)[0];
        const value = pair[key][0];
        global.distribution[this.gid].store.put(value, key, (err, res) => {
          persisted++;
          if (persisted === afterReduce.length) {
            cb(null, afterReduce.map((pair) => Object.keys(pair)[0]));
          }
        });
      }
    } else {
      cb(null, afterReduce);
    }
  };

  if (this.memory) {
    subReduce();
  } else {
    const preReduceMapKey = this.name + '-preReduceMap';
    store.get(preReduceMapKey, (err, res) => {
      this.preReduceMap = (err) ? {} : res;
      subReduce();
    });
  }

  this.preReduceMap = null;
};

/**
 * De-register phase: delete all the intermediate values in local file storage
 *
 * @param {Function} cb
 */
MapReduceService.prototype.deregister = function(cb) {
  if (!this.memory) {
    const afterMapListKey = this.name + '-afterMapList';
    store.del(afterMapListKey, (err, res) => {
      const preReduceMapKey = this.name + '-preReduceMap';
      store.del(preReduceMapKey, (err, res) => {
        cb(null, `Service '${this.name}' is deleted`);
      });
    });
  } else {
    cb(null, `Service '${this.name}' is deleted`);
  }
};

MapReduceService.prototype.hello = function(cb) {
  cb(null, this.preReduceMap);
};


const mr = (config) => new MapReduceService(config);

module.exports = mr;
