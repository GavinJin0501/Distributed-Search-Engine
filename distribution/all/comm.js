const distribution = global.distribution;

function DistributedCommService(groupInfo) {
  this.context = {};
  this.context.gid = groupInfo.gid || 'all';
}

DistributedCommService.prototype.send = function(message, remote, cb) {
  distribution.local.groups.get(this.context.gid, (e, nodes) => {
    if (e) {
      cb({e}, {});
    } else {
      const nodeToRes = {};
      const nodeToErr = {};
      let count = 0;
      Object.keys(nodes).forEach((sid) => {
        remote.node = nodes[sid];
        distribution.local.comm.send(message, remote, (e, v) => {
          count++;
          if (e) {
            nodeToErr[sid] = e;
          } else {
            nodeToRes[sid] = v;
          }

          if (count == Object.keys(nodes).length) {
            cb(nodeToErr, nodeToRes);
          }
        });
      });
    }
  });
};

const comm = (groupInfo) => new DistributedCommService(groupInfo);
module.exports = comm;
