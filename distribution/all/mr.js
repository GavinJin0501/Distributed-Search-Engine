const distribution = global.distribution;
const id = distribution.util.id;
const groups = distribution.local.groups;
const comm = distribution.local.comm;

/**
 * Distributed MapReduce Service
 *
 * @param {Object} config
 */
function DistributedMapReduceService(config) {
  this.context = {};
  this.context.gid = config.gid || 'all';
}

/**
 * Start a map reduce function
 *
 * @param {Object} config
 * @param {Function} callback
 */
DistributedMapReduceService.prototype.exec = function(config, callback) {
  groups.get(this.context.gid, (err, group) => {
    if (err) {
      callback(err, null);
      return;
    }

    // distribute keys to each nodes
    const nodes = Object.values(group);
    const numOfNodes = Object.keys(nodes).length;
    const partialKeys = Array.from({length: numOfNodes}, () => []);
    const hashFunc = distribution[this.context.gid].store.context.hash;
    // const hashFunc = id.consistentHash;
    for (const key of config.keys) {
      const node = id.getProperNode(key, group, hashFunc);
      const idx = nodes.indexOf(node);
      partialKeys[idx].push(key);
    }
    this.setupPhase(config, nodes, partialKeys, callback);
  });
};

/**
 * Setup phase: register the service to all nodes in the group
 *
 * @param {Object} config
 * @param {List} nodes
 * @param {List} partialKeys
 * @param {Function} callback
 */
DistributedMapReduceService.prototype.setupPhase = function(
    config, nodes, partialKeys, callback) {
  const numOfNodes = Object.keys(nodes).length;
  const gid = this.context.gid;
  const hashFuncName = distribution[gid].store.context.hash.name;

  const serviceName = 'mr-' + id.getID(new Date()).substring(0, 10);

  // Send the configuration to the service endpoint
  config.gid = gid;
  config.hashFuncName = hashFuncName;
  config.serviceName = serviceName;

  let setUpComplete = 0;
  for (let i = 0; i < numOfNodes; i++) {
    const remote = {service: 'routes', method: 'put', node: nodes[i]};
    config.keys = partialKeys[i];
    comm.send([config, serviceName], remote, (err, res) => {
      if (err) {
        deregisterIfError(serviceName, err, callback);
        return;
      }
      setUpComplete++;
      if (setUpComplete === numOfNodes) {
        this.mapPhase(config, callback);
      }
    });
  }
};

/**
 * Map phase, includes map the values and do steps behind the scenes:
 * shuffle, combine, and sort
 *
 * @param {Object} config
 * @param {Function} callback
 */
DistributedMapReduceService.prototype.mapPhase = function(config, callback) {
  const remote = {service: config.serviceName, method: 'map'};
  const gid = this.context.gid;

  // map
  distribution[gid].comm.send([], remote, (err, res) => {
    if (Object.keys(err).length !== 0) {
      deregisterIfError(config.serviceName, err, callback);
      return;
    }
    // console.log('all.map res:', res);
    // callback(null, res);

    // shuffle, combine, and sort
    remote.method = 'preReduce';
    distribution[gid].comm.send([], remote, (err, res) => {
      if (Object.keys(err).length !== 0) {
        deregisterIfError(config.serviceName, err, callback);
        return;
      }

      if (config.memory) {
        this.reducePhase(config, callback);
      } else {
        remote.method = 'preReducePersist';
        distribution[gid].comm.send([], remote, (err, res) => {
          this.reducePhase(config, callback);
        });
      }
    });
  });
};

/**
 * Reduce phase
 *
 * @param {Object} config
 * @param {Function} callback
 */
DistributedMapReduceService.prototype.reducePhase = function(config, callback) {
  const remote = {service: config.serviceName, method: 'reduce'};
  const gid = this.context.gid;

  distribution[gid].comm.send([], remote, (err, res) => {
    const errList = [];
    const resList = [];
    errList.push(...Object.values(err));
    Object.values(res).forEach((val) => resList.push(...val));

    // deregister the service after complete
    distribution[gid].routes.del(config.serviceName, (err, res) => {
      callback(errList, resList);
    });
  });
};

/**
 * De-register the service when there are errors
 *
 * @param {String} serviceName
 * @param {Object} err
 * @param {Function} cb
 */
function deregisterIfError(serviceName, err, cb) {
  distribution[gid].routes.del(serviceName, (e, r) => {
    cb(err, null);
  });
}

const mr = (config) => new DistributedMapReduceService(config);
module.exports = mr;
