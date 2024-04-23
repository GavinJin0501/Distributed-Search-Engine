let distribution = global.distribution;

function DistributedGroupsService(groupInfo) {
  this.context = {};
  this.context.gid = groupInfo.gid || 'all';
}

/**
 * Get the gid group from all nodes in the current group
 *
 * @param {String} gid group name
 * @param {Function} cb
 */
DistributedGroupsService.prototype.get = function(gid, cb) {
  const remote = {service: 'groups', method: 'get'};
  distribution[this.context.gid].comm.send([gid], remote, (e, v) => {
    const [nodeErrs, nodeRes] = [e, v];
    const currSid = distribution.util.id.getSID(global.nodeConfig);
    distribution.local.groups.get(gid, (e, v) => {
      if (e) {
        nodeErrs[currSid] = e;
      } else {
        nodeRes[currSid] = v;
      }
      cb(nodeErrs, nodeRes);
    });
  });
};

/**
 * Put a group to all nodes
 *
 * @param {Object} group group info for the nodes
 * @param {Object} nodes information of each node
 * @param {Function} cb
 */
DistributedGroupsService.prototype.put = function(group, nodes, cb) {
  distribution = global.distribution;
  distribution.local.groups.put(group, nodes, (err, val) => {
    const remote = {service: 'groups', method: 'put'};
    distribution[this.context.gid].comm.send([group, nodes], remote, (e, v) => {
      const currSid = distribution.util.id.getSID(global.nodeConfig);
      if (err) {
        e[currSid] = err;
      } else {
        v[currSid] = val;
      }
      cb(e, v);
    });
  });
};

/**
 * Delete a group for all nodes
 *
 * @param {String} gid
 * @param {Function} cb
 */
DistributedGroupsService.prototype.del = function(gid, cb) {
  const remote = {service: 'groups', method: 'del'};
  distribution[this.context.gid].comm.send([gid], remote, (err, res) => {
    const currSid = distribution.util.id.getSID(global.nodeConfig);
    distribution.local.groups.del(gid, (e, v) => {
      if (e) {
        err[currSid] = e;
      } else {
        res[currSid] = v;
      }
      cb(err, res);
    });
  });
};

/**
 * Add a node to a group for all nodes
 *
 * @param {String} gid
 * @param {Object} node
 * @param {Function} cb
 */
DistributedGroupsService.prototype.add = function(gid, node, cb) {
  const remote = {service: 'groups', method: 'add'};
  distribution[this.context.gid].comm.send([gid, node], remote, (e, v) => {
    const [nodeErrs, nodeRes] = [e, v];
    const currSid = distribution.util.id.getSID(global.nodeConfig);
    distribution.local.groups.add(gid, node, (e, v) => {
      if (e) {
        nodeErrs[currSid] = e;
      } else {
        nodeRes[currSid] = v;
      }
      cb(nodeErrs, nodeRes);
    });
  });
};

/**
 * Remove a node in a group for all nodes
 *
 * @param {String} gid
 * @param {Object} node
 * @param {Function} cb
 */
DistributedGroupsService.prototype.rem = function(gid, node, cb) {
  distribution.local.groups.rem(gid, node, (e, v) => {
    const remote = {service: 'groups', method: 'rem'};
    distribution[this.context.gid].comm.send([gid, node], remote, cb);
  });
};

const groups = (groupInfo) => new DistributedGroupsService(groupInfo);
module.exports = groups;
