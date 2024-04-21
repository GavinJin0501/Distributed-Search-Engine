const distribution = global.distribution;

/**
 * Distributed routes service
 *
 * @param {*} groupInfo config
 */
function DistributedRoutesService(groupInfo) {
  this.context = {};
  this.context.gid = groupInfo.gid || 'all';
}

/**
 * Put a service to all nodes in the group
 *
 * @param {*} service service
 * @param {*} serviceName name of the service
 * @param {*} cb callback function
 */
DistributedRoutesService.prototype.put = function(service, serviceName, cb) {
  distribution.local.routes.put(service, serviceName, (err, res) => {
    if (err) {
      cb(err, null);
      return;
    }
    const remote = {service: 'routes', method: 'put'};
    const data = [service, serviceName];
    distribution[this.context.gid].comm.send(data, remote, cb);
  });
};

/**
 * Del a service to all nodes in the group
 *
 * @param {*} serviceName name of the service
 * @param {*} cb callback function
 */
DistributedRoutesService.prototype.del = function(serviceName, cb) {
  distribution.local.routes.del(serviceName, (err, res) => {
    const remote = {service: 'routes', method: 'del'};
    distribution[this.context.gid].comm.send([serviceName], remote, cb);
  });
};

const routes = (groupInfo) => new DistributedRoutesService(groupInfo);
module.exports = routes;
