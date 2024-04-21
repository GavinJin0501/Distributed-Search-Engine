const distribution = global.distribution;
const id = distribution.util.id;

/**
 * Service to deal with a certain key-value pair by choosing a node in the group
 *
 * @param {Object} config
 */
function DistributedInMemoryService(config) {
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
DistributedInMemoryService.prototype.get = function(key, cb) {
  const metaData = {key: key, gid: this.context.gid};

  // if key is null, get all the keys in all nodes
  if (key === null) {
    const remote = {service: 'mem', method: 'get'};
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
    const remote = {service: 'mem', method: 'get', node};

    // Comm to the node to get
    distribution.local.comm.send([metaData], remote, cb);
  });
};

/**
 * Put the key-value pair in the current group
 *
 * @param {Object} value
 * @param {String} key
 * @param {Function} cb
 */
DistributedInMemoryService.prototype.put = function(value, key, cb) {
  const metaData = {key: key, gid: this.context.gid};
  // if key is null, put the value in all nodes in the group
  if (key === null) {
    const remote = {service: 'mem', method: 'put'};
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
    const remote = {service: 'mem', method: 'put', node};

    distribution.local.comm.send([value, metaData], remote, cb);
  });
};

/**
 * Delete the corresponding key-value pair in the current group
 *
 * @param {String} key
 * @param {Function} cb
 */
DistributedInMemoryService.prototype.del = function(key, cb) {
  const metaData = {key: key, gid: this.context.gid};

  distribution.local.groups.get(this.context.gid, (err, group) => {
    if (err) {
      cb(err, null);
      return;
    }

    // Retrieve the node
    const node = id.getProperNode(key, group, this.context.hash);
    const remote = {service: 'mem', method: 'del', node};

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
DistributedInMemoryService.prototype.reconf = function(prevGroup, cb) {
  distribution.local.groups.get(this.context.gid, (e, currGroup) => {
    if (e) {
      cb(e, null);
      return;
    }

    // get all the keys in local in-memory storage of this service
    const metaData = {key: null, gid: this.context.gid};
    distribution.local.mem.get(metaData, (e, keys) => {
      if (e) {
        cb(e, null);
        return;
      }

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
          const remote = {service: 'mem', method: 'del', node: oldNode};
          const metaData = {key, gid: this.context.gid};
          distribution.local.comm.send([metaData], remote, (e, v) => {
            distribution[this.context.gid].mem.put(v, key, (e, v) => {
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

DistributedInMemoryService.prototype.append = function(
    value, valueKey, appendKey, cb) {
  const metaKey = {valueKey, gid: this.context.gid};

  // Must specify the valueKey and the appendKey
  if (!valueKey || !appendKey) {
    cb(new Error('valueKey or appendKey is null!'), nul);
  }

  distribution.local.groups.get(this.context.gid, (err, group) => {
    if (err) {
      cb(err, null);
      return;
    }

    // Retrieve the node
    const node = id.getProperNode(valueKey, group, this.context.hash);
    const remote = {service: 'mem', method: 'append', node};

    distribution.local.comm.send([value, metaKey, appendKey], remote, cb);
  });
};


const mem = (config) => new DistributedInMemoryService(config);
module.exports = mem;
