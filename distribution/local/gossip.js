const comm = require('./comm');
const groups = require('./groups');

/**
 * Check if obj has the same value for all keys in toCheck
 *
 * @param {Object} obj
 * @param {Object} toCheck
 * @return {Boolean}
 */
function hasSameValues(obj, toCheck) {
  const keysToCheck = Object.keys(toCheck);
  return keysToCheck.every((key) => obj[key] === toCheck[key]);
}

/**
 * Randomly select some number of elements without a certain element
 *
 * @param {Array} data
 * @param {Integer} size
 * @param {Object} toRemove
 * @return {Array}
 */
function randomSelectWithout(data, size, toRemove) {
  let idx = data.length - 1;
  for (let i = 0; i < data.length; i++) {
    if (hasSameValues(data[i], toRemove)) {
      idx = i;
      break;
    }
  }
  [data[idx], data[data.length - 1]] = [data[data.length - 1], data[idx]];

  // Randomly shuffle the array
  data = data.slice(0, data.length - 1);
  for (let i = 0; i < data.length; i++) {
    const j = Math.floor(Math.random() * (data.length - i));
    [data[i], data[j]] = [data[j], data[i]];
  }
  return data.slice(0, size);
}

function GossipService() {
  // deduplicate logic: {messageId}
  this.messageSet = new Set();
}

/**
 * Gossip recv function
 *
 * @param  {...any} args
 */
GossipService.prototype.recv = function(...args) {
  // args must have: message & callback
  const cb = args.pop() || console.log;
  if (args.length != 3) {
    const error = new Error(`gossip.recv() does not have enough arguments`);
    cb(error, null);
    return;
  }

  const message = args[0]; // message
  const remote = args[1]; // operation
  const msgMetadata = args[2]; // message metadata
  const remoteNode = remote.node; // node that received from

  // Discard the message if the message is duplicated:
  //    messageId exists in the map && messageClock is before record
  if (this.messageSet.has(msgMetadata.id)) {
    cb(new Error('Your message is duplicated'), null);
    return;
  }
  this.messageSet.add(msgMetadata.id);


  // If the message is not duplicated:
  // 1. Execute the logic of the message
  remote.node = global.nodeConfig;
  const gossipRemote = {service: 'gossip', method: 'recv'};
  const gossipMessage = [message, remote, msgMetadata];

  if (remote.type === 'periodicFunc') {
    const func = message[0];
    func((e, v) => {
      propagation(e, v, cb, msgMetadata, remote,
          remoteNode, gossipRemote, gossipMessage);
    });
  } else { // normal messages
    comm.send(message, remote, (e, v) => {
      propagation(e, v, cb, msgMetadata, remote,
          remoteNode, gossipRemote, gossipMessage);
    });
  }
};

function propagation(e, v, cb, msgMetadata, remote,
    remoteNode, gossipRemote, gossipMessage) {
  // If the local operation fails, no need to propogate
  if (e) {
    cb(e, null);
    return;
  }

  // 2. Propogate the message in the current group
  groups.get(msgMetadata.gid, (e, v) => {
    // if not such group, no need to propagate
    if (e) {
      cb(new Error(`Group '${msgMetadata.gid}' does not exists`), null);
      return;
    }

    const nodes = Object.values(v);
    const selectedNodes = randomSelectWithout(
        nodes, msgMetadata.groupSize, remoteNode,
    );


    let sent = 0;
    selectedNodes.forEach((node) => {
      gossipRemote.node = node;
      comm.send(gossipMessage, gossipRemote, (e, v) => {
        // TODO: need to change
        sent++;

        if (sent === selectedNodes.length) {
          // Respond the data to the sender
          cb(null, `Node ${remote.node.ip}:${remote.node.port} gossips...`);
        }
      });
    });
  });
}


const gossip = new GossipService();
module.exports = gossip;
