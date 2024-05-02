const distribution = global.distribution;
const id = distribution.util.id;
const groups = distribution.local.groups;
const comm = distribution.local.comm;

let mrId = 0;

/**
 * Distributed MapReduce Service
 *
 * @param {Object} config
 */
function DistributedMapReduceService(config) {
  // global mr service info, like its gid and nodes
  this.context = {};
  this.context.gid = config.gid || 'all';

  // save the info of each map-reduce service
  // should by {serviceName: {serviceInfo}}
  this.mrServices = {};
  this.notifyCount = {};
}

/**
 * Receive notification from worker node and manage phase transition
 *
 * @param {Object} notifyInfo
 */
DistributedMapReduceService.prototype.notify = function(notifyInfo) {
  const {phaseName, serviceName, data} = notifyInfo;
  const serviceConfig = this.mrServices[serviceName];

  if (!this.notifyCount[serviceName]) {
    this.notifyCount[serviceName] = {};
  }
  const myCount = this.notifyCount[serviceName];
  myCount[phaseName] = myCount[phaseName] ? myCount[phaseName] + 1 : 1;

  if (phaseName === 'reducePhase') {
    // errList.push(...Object.values(err));
    // Object.values(res).forEach((val) => resList.push(...val));
    if (data) {
      data.forEach((pair) => {
        const key = Object.keys(pair)[0];
        const urls = pair[key];

        if (urls.length === 1 && urls[0] === key) {
          serviceConfig.finalErrList.push(key);
        } else {
          serviceConfig.finalResList.push(...data);
        }
      });
    }
  }

  // should move on to the next phase
  if (myCount[phaseName] === this.context.numOfNodes) {
    if (phaseName === 'setUpPhase') {
      // console.log('setUpPhase completes...');
      this.mapPhase(serviceName);
    } else if (phaseName === 'mapPhase') {
      // console.log('mapPhase completes...');
      this.shufflePhase(serviceName);
    } else if (phaseName === 'shufflePhase') {
      // console.log('shufflePhase completes...');
      if (serviceConfig.memory) {
        this.reducePhase(serviceName);
      } else {
        const remote = {
          service: serviceName,
          method: 'preReducePersist',
        };
        distribution[this.context.gid].comm.send([], remote, (err, res) => {
          if (Object.keys(err).length !== 0) {
            console.log('all.preReducePersist deregister', Object.values(err));
            this.deregister(err, serviceName, serviceConfig.finalCb);
            return;
          }

          // console.log('all.preReducePersist is ready:',
          //     Object.values(res).length === this.context.numOfNodes);
        });
      }
    } else if (phaseName === 'preReducePersistPhase') {
      // console.log('preReducePersistPhase completes...');
      this.reducePhase(serviceName);
    } else if (phaseName === 'reducePhase') {
      // console.log('reducePhase completes...');
      this.deregister(null, serviceName, (err, res) => {
        serviceConfig.finalCb(this.finalErrList, serviceConfig.finalResList);
      });
    } else {
      this.deregister(null, serviceName, (err, res) => {
        serviceConfig.finalCb(null, 'phase name invald: ' + phaseName);
      });
    }
  }
};

/**
 * Start a map reduce function
 *
 * @param {Object} serviceConfig
 * @param {Function} callback
 */
DistributedMapReduceService.prototype.exec = function(serviceConfig, callback) {
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
    if (!serviceConfig.keepKeysOrder) {
      for (const key of serviceConfig.keys) {
        const node = id.getProperNode(key, group, hashFunc);
        const idx = nodes.indexOf(node);
        partialKeys[idx].push(key);
      }
    } else {
      // need to check if keys can be splitted equally
      if (serviceConfig.keys.length % numOfNodes !== 0) {
        console.log("hihihi",serviceConfig.keys.length, numOfNodes);
        callback(
            new Error('Under keepKeysOrder, keys should be splitted equally'),
            null);
        return;
      }

      const partialSize = serviceConfig.keys.length / numOfNodes;
      for (let i = 0; i < numOfNodes; i++) {
        partialKeys[i] = serviceConfig.keys.slice(
            partialSize * i,
            partialSize * (i + 1),
        );
      }
    }

    // console.log(partialKeys.map((l) => l.length).join(','));
    // save for later use
    this.context.nodes = nodes;
    this.context.numOfNodes = numOfNodes;

    // Create a new mr service for this map reduce task
    const serviceName = 'mr-' + mrId++;
    if (mrId % 1000 === 0) {
      console.log(mrId);
    }
    this.mrServices[serviceName] = serviceConfig;
    serviceConfig.serviceName = serviceName;
    serviceConfig.gid = this.context.gid; // save the gid for this mr service
    // callback to return out of exec function
    serviceConfig.finalCb = callback || ((err, res) => console.log(err, res));
    serviceConfig.hashFuncName = hashFunc.name; // save the hashFunc needed
    serviceConfig.coordinator = {
      ip: global.nodeConfig.ip,
      port: global.nodeConfig.port,
    }; // save the node info for worker node to notify

    // new setUpPhase using notify
    this.setupPhase(serviceName, partialKeys);
  });
};

/**
 * Setup phase: register the service to all nodes in the group
 *
 * @param {Object} serviceName
 * @param {List} partialKeys
 */
DistributedMapReduceService.prototype.setupPhase = function(
    serviceName, partialKeys) {
  const serviceConfig = this.mrServices[serviceName];
  const numOfNodes = this.context.numOfNodes;

  distribution.local.routes.put(serviceConfig, serviceName, (err, res) => {
    for (let i = 0; i < numOfNodes; i++) {
      const remote = {
        service: 'routes',
        method: 'put',
        node: this.context.nodes[i],
      };
      serviceConfig.keys = partialKeys[i];
      comm.send([serviceConfig, serviceName], remote, (err, res) => {
        if (err) {
          console.log('all.setUpPhase deregister...');
          this.deregister(err, serviceName, serviceConfig.finalCb);
          return;
        }
        console.log("setting up phase")
        this.notify({phaseName: 'setUpPhase', serviceName});
      });
    }
  });
};

/**
 * Map phase, includes map the values and do steps behind the scenes:
 * shuffle, combine, and sort
 *
 * @param {String} serviceName
 */
DistributedMapReduceService.prototype.mapPhase = function(serviceName) {
  const serviceConfig = this.mrServices[serviceName];
  const remote = {service: serviceName, method: 'map'};
  const gid = this.context.gid;

  // map
  distribution[gid].comm.send([], remote, (err, res) => {
    if (Object.keys(err).length !== 0) {
      console.log('all.mapPhase deregister...', Object.values(err));
      this.deregister(err, serviceName, serviceConfig.finalCb);
      return;
    }
    // should receive all worker nodes saying "Ready to start map..."
    // console.log('all.mapPhase is ready:',
    //     Object.values(res).length === this.context.numOfNodes);
  });
};

DistributedMapReduceService.prototype.shufflePhase = function(serviceName) {
  const serviceConfig = this.mrServices[serviceName];

  // shuffle, combine, and sort
  const remote = {service: serviceName, method: 'shuffle'};
  distribution[this.context.gid].comm.send([], remote, (err, res) => {
    if (Object.keys(err).length !== 0) {
      console.log('all.shuffle deregister...');
      this.deregister(err, serviceName, serviceConfig.finalCb);
      return;
    }
    // should receive all worker nodes saying "Ready to start shuffle..."
    // console.log('all.shufflePhase is ready:',
    //     Object.values(res).length === this.context.numOfNodes);
  });
};

/**
 * Reduce phase
 *
 * @param {String} serviceName
 */
DistributedMapReduceService.prototype.reducePhase = function(serviceName) {
  const serviceConfig = this.mrServices[serviceName];
  serviceConfig.finalResList = [];
  serviceConfig.finalErrList = []; // for crawler

  const remote = {service: serviceName, method: 'reduce'};
  const gid = this.context.gid;

  distribution[gid].comm.send([], remote, (err, res) => {
    if (Object.keys(err).length !== 0) {
      console.log('all.reduce deregister...');
      this.deregister(err, serviceName, serviceConfig.finalCb);
      return;
    }
    // should receive all worker nodes saying "Ready to start shuffle..."
    // console.log('all.reducePhase is ready:',
    //     Object.values(res).length === this.context.numOfNodes);
  });
};

/**
 * Deregister the service in normal cases
 *
 * @param {Error} err
 * @param {String} serviceName
 * @param {Function} cb
 */
DistributedMapReduceService.prototype.deregister = function(
    err, serviceName, cb,
) {
  // deregister the service after complete
  if (err && (err instanceof Error || Object.keys(err).length > 0)) {
    console.log('dereigster because of err:', err);
  }
  delete this.mrServices[serviceName];
  delete this.notifyCount[serviceName];
  distribution[this.context.gid].routes.del(serviceName, cb);
};

const mr = (config) => new DistributedMapReduceService(config);
module.exports = mr;
