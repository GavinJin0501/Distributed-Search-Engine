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
  const remote = {service: 'routes', method: 'put'};
  distribution[this.context.gid].comm.send([service, serviceName], remote, cb);
};

/**
 * Del a service to all nodes in the group
 *
 * @param {*} serviceName name of the service
 * @param {*} cb callback function
 */
DistributedRoutesService.prototype.del = function(serviceName, cb) {
  const remote = {service: 'routes', method: 'del'};
  distribution[this.context.gid].comm.send([serviceName], remote, cb);
};

const routes = (groupInfo) => new DistributedRoutesService(groupInfo);
module.exports = routes;
