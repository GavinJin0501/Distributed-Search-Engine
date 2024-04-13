const distribution = global.distribution;
const currNode = global.nodeConfig;

function DistributedGossipService(groupInfo) {
  this.context = {};
  this.context.gid = groupInfo.gid || 'all';
  this.context.subset = groupInfo.subset || ((lst) => 10);

  this.periodicMethods = new Map();
}

/**
 * Gossip protocol send
 *
 * @param {String} message message to propagate
 * @param {Object} remote message info
 * @param {Function} cb
 */
DistributedGossipService.prototype.send = function(message, remote, cb) {
  distribution.local.groups.get(this.context.gid, (e, v) => {
    if (e) {
      cb(e, null);
      return;
    }

    // process message metadata
    const subsetSize = this.context.subset();
    const messageId = distribution.util.id.getID(
        {msg: message, timestamp: new Date()},
    );
    const msgMetadata = {
      id: messageId, gid: this.context.gid, groupSize: subsetSize,
    };

    // prepare to send the message
    const gossipMessage = [message, remote, msgMetadata];
    remote.node = {ip: currNode.ip, port: currNode.port};

    distribution.local.gossip.recv(...gossipMessage, cb);
  });
};


DistributedGossipService.prototype.at = function(period, func, cb) {
  distribution.local.groups.get(this.context.gid, (e, v) => {
    if (e) {
      cb(e, null);
      return;
    }

    const toExecute = () => {
      const subsetSize = this.context.subset();
      const message = [func];
      const remote = {type: 'periodicFunc'};
      remote.node = {ip: currNode.ip, port: currNode.port};

      const messageId = distribution.util.id.getID(
          {msg: message, timestamp: new Date()},
      );
      const msgMetadata = {
        id: messageId, gid: this.context.gid, groupSize: subsetSize,
      };

      // prepare to send the message
      const gossipMessage = [message, remote, msgMetadata];
      distribution.local.gossip.recv(...gossipMessage, cb);
    };
    toExecute();

    const interval = setInterval(toExecute, period);
    const intervalId = distribution.util.id.getID(interval);

    this.periodicMethods.set(intervalId, interval);
    cb(null, intervalId);
  });
};

DistributedGossipService.prototype.del = function(intervalId, cb) {
  const interval = this.periodicMethods.get(intervalId);
  if (interval) {
    this.periodicMethods.delete(intervalId);
    clearInterval(interval);
  }
  cb();
};


const gossip = (groupInfo) => new DistributedGossipService(groupInfo);
module.exports = gossip;
