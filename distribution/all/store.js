const distribution = global.distribution;
const id = distribution.util.id;

function DistributedPersistentMemoryService(config) {
  this.context = {};
  this.context.gid = config.gid || 'all';
  this.context.hash = config.hash || id.naiveHash;
}

/**
 * Find the value corresponding to the key in the current group
 *
 * @param {String} key
 * @param {Function} cb
 */
DistributedPersistentMemoryService.prototype.get = function(key, cb) {
  // used for reconf
  if (key != null && typeof key === 'object') {
    const remote = {service: 'store', method: 'get'};
    distribution[this.context.gid].comm.send([key], remote, cb);
    return;
  }

  const metaData = {key: key, gid: this.context.gid};

  // if key is null, get all the keys in all nodes
  if (key === null) {
    const remote = {service: 'store', method: 'get'};
    distribution[this.context.gid].comm.send([metaData], remote, (err, res) => {
      const set = Object.values(res).reduce((acc, curr) => {
        for (let i = 0; i < curr.length; i++) {
          acc.add(curr[i]);
        }
        return acc;
      }, new Set());
      cb(err, [...set]);
    });
    return;
  }

  distribution.local.groups.get(this.context.gid, (err, group) => {
    if (err) {
      cb(err, null);
      return;
    }

    // Retrieve the node
    const node = id.getProperNode(key, group, this.context.hash);
    const remote = {service: 'store', method: 'get', node};

    // Comm to the node to get
    distribution.local.comm.send([metaData], remote, cb);
  });
};

/**
 * Put a key-value pair to the local file system of a node in the group
 *
 * @param {Object} value
 * @param {String} key
 * @param {Function} cb
 */
DistributedPersistentMemoryService.prototype.put = function(value, key, cb) {
  const metaData = {key: key, gid: this.context.gid};

  // if key is null, put the value in all nodes in the group
  if (key === null) {
    const remote = {service: 'store', method: 'put'};
    distribution[this.context.gid].comm.send([value, metaData], remote, cb);
    return;
  }

  // if the key is not null, choose a node to put the key-value pair
  distribution.local.groups.get(this.context.gid, (err, group) => {
    if (err) {
      cb(err, null);
      return;
    }

    // Retrieve the node
    const node = id.getProperNode(key, group, this.context.hash);
    const remote = {service: 'store', method: 'put', node};
    // console.log(key, node);

    distribution.local.comm.send([value, metaData], remote, cb);
  });
};

/**
 * Delete the corresponding key-value pair in the current group
 *
 * @param {String} key
 * @param {Function} cb
 */
DistributedPersistentMemoryService.prototype.del = function(key, cb) {
  const metaData = {key: key, gid: this.context.gid};

  distribution.local.groups.get(this.context.gid, (err, group) => {
    if (err) {
      cb(err, null);
      return;
    }

    // Retrieve the node
    const node = id.getProperNode(key, group, this.context.hash);
    const remote = {service: 'store', method: 'del', node};
    distribution.local.comm.send([metaData], remote, cb);
  });
};

/**
 * It allows reconfiguring the service instance when a group change is detected.
 * It automatically redistributes the set of objects a storage system stores:
 *    1. Go through the list of object keys available in the service instance
 *    2. Relocate each object that has different keys after the new hashing
 *
 * @param {Object} prevGroup previous group nodes
 * @param {*} cb
 */
DistributedPersistentMemoryService.prototype.reconf = function(prevGroup, cb) {
  // get the current group setting
  distribution.local.groups.get(this.context.gid, (e, currGroup) => {
    if (e) {
      cb(e, null);
      return;
    }

    // get all the keys in local persistent storage of this service
    const metaData = {key: null, gid: this.context.gid};
    distribution[this.context.gid].store.get(metaData, (errs, sidToKeys) => {
      if (Object.values(errs).length !== 0) {
        cb(errs, null);
        return;
      }

      const keySet = new Set();
      Object.values(sidToKeys).forEach((subKeys) =>
        subKeys.forEach((subKey) => keySet.add(subKey)));
      const keys = [...keySet];

      // no key-value paired stored
      if (keys.length === 0) {
        cb(null, null);
        return;
      }

      const prevNodes = Object.values(prevGroup);
      const currNodes = Object.values(currGroup);
      let finished = 0;

      // check if the hash changes for each key
      keys.forEach((key) => {
        const oldNode = id.getProperNode(key, prevNodes, this.context.hash);
        const newNode = id.getProperNode(key, currNodes, this.context.hash);

        // if the hash is the same, no need to transfer
        if (id.getID(oldNode) === id.getID(newNode)) {
          finished++;
          if (finished === keys.length) {
            cb(null, null);
          }
        } else {
          // if the hash is not the same, transfer the object
          const remote = {service: 'store', method: 'del', node: oldNode};
          const metaData = {key, gid: this.context.gid};
          distribution.local.comm.send([metaData], remote, (e, v) => {
            distribution[this.context.gid].store.put(v, key, (e, v) => {
              finished++;
              if (finished === keys.length) {
                cb(null, null);
              }
            });
          });
        }
      });
    });
  });
};


const store = (config) => new DistributedPersistentMemoryService(config);
module.exports = store;
