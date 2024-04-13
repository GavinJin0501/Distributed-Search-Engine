const distribution = global.distribution;

function DistributedStatusService(groupInfo) {
  this.context = {};
  this.context.gid = groupInfo.gid || 'all';
}

/**
 * Get the status of each node in the group
 *
 * @param {String} property property name to get
 * @param {Function} cb
 */
DistributedStatusService.prototype.get = function(property, cb) {
  const remote = {service: 'status', method: 'get'};
  distribution[this.context.gid].comm.send([property], remote, (e, v) => {
    if (['counts', 'heapTotal', 'heapUsed'].includes(property)) {
      let res = 0;
      Object.values(v).forEach((val) => res += val);
      v = res;
    }
    cb(e, v);
  });
};

/**
 * 1. Start a new node with appropriate IP and port information;
 * 2. Add this node to the current group for all nodes
 *
 * @param {Object} nodeToSpawn
 * @param {Function} cb
 */
DistributedStatusService.prototype.spawn = function(nodeToSpawn, cb) {
  distribution.local.status.spawn(nodeToSpawn, (e, v) => {
    if (e) {
      cb(e, null);
    } else {
      distribution[this.context.gid].groups.add(
          this.context.gid, nodeToSpawn, (e, v) => {
            if (Object.keys(e).length > 0) {
              cb(e, null);
            } else {
              cb(null, nodeToSpawn);
            }
          });
    }
  });
};

DistributedStatusService.prototype.stop = function(cb) {
  const remote = {service: 'status', method: 'stop'};
  distribution[this.context.gid].comm.send([], remote, cb);
};

const status = (groupInfo) => new DistributedStatusService(groupInfo);
module.exports = status;
