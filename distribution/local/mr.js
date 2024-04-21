const store = require('./store');
const groups = require('./groups');
const comm = require('./comm');
const id = require('../util/id');

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
  this.serviceName = config.serviceName; // mr service name
  this.memory = config.memory || false; // whether to use in-mem
  this.coordinator = config.coordinator; // node to notify

  // used for distributed crawler
  if (config.shuffle) { // customized shuffle pahse function
    this.shuffle = config.shuffle;
  }
  if (config.domain) {
    this.domain = config.domain;
  }

  this.afterMapList = [];
  this.afterCombineMap = {};
  this.preReduceMap = {};
}

/**
 * Cooridnator receives a notification; passes it to group.mr.notify to process.
 *
 * @param {Object} notifyInfo
 * @param {Function} cb
 */
MapReduceService.prototype.notify = function(notifyInfo, cb) {
  global.distribution[this.gid].mr.notify(notifyInfo);
  cb(null, 'Cooridnator acks your notification for: ' + notifyInfo.serviceName);
};


/**
 * Worker do notify to the coordinator
 *
 * @param {String} phaseName
 * @param {Object} data
 * @param {Function} cb
 */
MapReduceService.prototype.doNotify = function(phaseName, data, cb) {
  const remote = {
    service: this.serviceName,
    method: 'notify',
    node: this.coordinator,
  };
  const toSend = {phaseName, serviceName: this.serviceName};
  if (phaseName === 'reducePhase') {
    toSend['data'] = data;
  }
  comm.send([toSend], remote, cb);
};


/**
 *  Map function
 *
 * @param {Function} cb send result back to the coordinator
 */
MapReduceService.prototype.map = function(cb) {
  // tell the coordinator that this node can start to map
  cb(null, 'Ready to start map...');

  let mapCompletes = 0;
  const doComplete = () => {
    mapCompletes++;

    if (mapCompletes === this.keys.length) {
      if (!this.memory) {
        const afterMapListKey = this.serviceName + '-afterMapList';
        const metaKey = {key: afterMapListKey, gid: this.gid};
        store.put(this.afterMapList, metaKey, (err, res) => {
          this.doNotify('mapPhase', this.afterMapList, (err, res) => {
            this.afterMapList = [];
            // if (err) {
            //   console.log('map notify fails:', err);
            // } else {
            //   console.log('map notify succeeds:', res);
            // }
          });
        });
      } else {
        this.doNotify('mapPhase', this.afterMapList, (err, res) => {
          // if (err) {
          //   console.log('map notify fails:', err);
          // } else {
          //   console.log('map notify succeeds:', res);
          // }
        });
      }
    }
  };

  if (this.keys.length === 0) {
    mapCompletes = -1;
    doComplete();
    return;
  }

  // Actual map logic
  for (const key of this.keys) {
    const metaKey = {key, gid: this.gid};
    store.get(metaKey, (err, val) => {
      if (!err) {
        let mappedVal = this.mapFunc(key, val);

        if (mappedVal instanceof Promise) {
          mappedVal.then((data) => {
            mappedVal = data;

            if (Array.isArray(mappedVal)) {
              this.afterMapList.push(...mappedVal);
            } else if (Object.values(mappedVal).length > 0) {
              this.afterMapList.push(mappedVal);
            }

            doComplete();
          });
          return;
        }

        if (Array.isArray(mappedVal)) {
          this.afterMapList.push(...mappedVal);
        } else if (Object.values(mappedVal).length > 0) {
          this.afterMapList.push(mappedVal);
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
MapReduceService.prototype.shuffle = function(cb) {
  // tell the coordinator that this node can start to shuffle
  cb(null, 'Ready to start shuffle...');

  const combine = () => {
    for (const pair of this.afterMapList) {
      const key = Object.keys(pair)[0];
      const val = pair[key];

      if (this.afterCombineMap[key] === undefined) {
        this.afterCombineMap[key] = [];
      }

      if (Array.isArray(val)) {
        this.afterCombineMap[key].push(...val);
      } else {
        this.afterCombineMap[key].push(val);
      }
    }
  };

  if (!this.memory) {
    const persisKey = this.serviceName + '-afterMapList';
    const metaKey = {key: persisKey, gid: this.gid};
    store.get(metaKey, (err, res) => {
      this.afterMapList = (err) ? [] : res;
      combine();
      this.shuffleHelper();
    });
  } else {
    combine();
    this.shuffleHelper();
  }
};

/**
 * Redistribute the key-value pairs to the right node
 *
 */
MapReduceService.prototype.shuffleHelper = function() {
  this.afterMapList = []; // free up space
  groups.get(this.gid, (err, group) => {
    const entries = Object.entries(this.afterCombineMap);
    if (entries.length === 0) {
      this.doNotify('shufflePhase', this.preReduceMap, (err, res) => {
        // if (err) {
        //   console.log('shuffleHelper notify fails:', err);
        // } else {
        //   console.log('shuffleHelper notify succeeds:', res);
        // }
      });
      return;
    }

    let redistributeComplete = 0;
    for (const entry of entries) {
      const [key, val] = entry;
      const node = id.getProperNode(key, group, this.hashFunc);
      const remote = {service: this.serviceName, method: 'redistribute', node};

      comm.send([{[key]: val}], remote, (err, res) => {
        redistributeComplete++;
        if (redistributeComplete === entries.length) {
          this.afterCombineMap = {};
          this.doNotify('shufflePhase', this.preReduceMap, (err, res) => {
            // if (err) {
            //   console.log('shuffleHelper notify fails:', err);
            // } else {
            //   console.log('shuffleHelper notify succeeds:', res);
            // }
          });
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
  cb(null, 'Ready to start shuffle...');

  const preReduceMapKey = this.serviceName + '-preReduceMap';
  const metaKey = {key: preReduceMapKey, gid: this.gid};
  store.put(this.preReduceMap, metaKey, (err, res) => {
    this.doNotify('preReducePersistPhase', this.preReduceMap, (err, res) => {
      this.preReduceMap = {};
      // if (err) {
      //   console.log('preReducePersist notify fails:', err);
      // } else {
      //   console.log('preReducePersist notify succeeds:', res);
      // }
    });
  });
};

/**
 * Reduce phase: use the reduce function
 *
 * @param {*} cb
 */
MapReduceService.prototype.reduce = function(cb) {
  cb(null, 'Ready to start reduce...');

  const subReduce = () => {
    const afterReduce = [];
    Object.entries(this.preReduceMap).forEach(([key, vals]) => {
      const pair = this.reduceFunc(key, vals);
      afterReduce.push(pair);
    });

    this.doNotify('reducePhase', afterReduce, (err, res) => {
      this.preReduceMap = null;
      // if (err) {
      //   console.log('reduce notify fails:', err);
      // } else {
      //   console.log('reduce notify succeeds:', res);
      // }
    });
  };

  if (this.memory) {
    subReduce();
  } else {
    const preReduceMapKey = this.serviceName + '-preReduceMap';
    const metaKey = {key: preReduceMapKey, gid: this.gid};
    store.get(metaKey, (err, res) => {
      this.preReduceMap = (err) ? {} : res;
      subReduce();
    });
  }
};

/**
 * De-register phase: delete all the intermediate values in local file storage
 *
 * @param {Function} cb
 */
MapReduceService.prototype.deregister = function(cb) {
  if (!this.memory) {
    const afterMapListKey = this.serviceName + '-afterMapList';
    const metaKey = {key: afterMapListKey, gid: this.gid};
    store.del(metaKey, (err, res) => {
      const preReduceMapKey = this.serviceName + '-preReduceMap';
      const metaKey = {key: preReduceMapKey, gid: this.gid};
      store.del(metaKey, (err, res) => {
        cb(null, `Service '${this.serviceName}' is deleted`);
      });
    });
  } else {
    cb(null, `Service '${this.serviceName}' is deleted`);
  }
};

MapReduceService.prototype.hello = function(cb) {
  cb(null, this.preReduceMap);
};


const mr = (config) => new MapReduceService(config);

module.exports = mr;
