const id = require('../util/id');

const node = global.nodeConfig;
const nodeName = id.getSID(node);

function defaultCallback(error, value) {
  if (error) {
    console.log(error);
  } else {
    console.log(value);
  }
}

/**
 * Groups Service
 */
function GroupsService() {
  // {groupName: {nodeName1: node1, nodeName2: node2...}}}
  this.groupToNodes = {all: {[nodeName]: node}, local: {[nodeName]: node}};
}

/**
 * Get the group of nodes with its group name
 *
 * @param {String} groupName
 * @param {Function} cb
 */
GroupsService.prototype.get = function(groupName, cb=defaultCallback) {
  if (this.groupToNodes[groupName] === undefined) {
    cb(new Error(`No such group name '${groupName}'`), null);
  } else {
    cb(null, this.groupToNodes[groupName]);
  }
};

/**
 * Put a group of nodes into the mapping
 *
 * @param {String} groupInfo
 * @param {Object} group
 * @param {Function} cb
 */
GroupsService.prototype.put = function(groupInfo, group, cb=defaultCallback) {
  const gid = typeof groupInfo === 'string' ? groupInfo : groupInfo.gid;
  const hash = groupInfo.hash;

  this.groupToNodes[gid] = group;

  // Add the group to the current node's distribution space
  // After this, the group can be referenced as distribution.groupName
  const distribution = global.distribution;
  distribution[gid] = {};
  distribution[gid].comm = require('../all/comm')({gid});
  distribution[gid].gossip = require('../all/gossip')({gid});
  distribution[gid].groups = require('../all/groups')({gid});
  distribution[gid].routes = require('../all/routes')({gid});
  distribution[gid].status = require('../all/status')({gid});
  distribution[gid].mr = require('../all/mr')({gid});

  // storage
  distribution[gid].mem = require('../all/mem')({gid, hash});
  distribution[gid].store = require('../all/store')({gid, hash});

  // Update the all group
  Object.keys(group).forEach((key) => {
    this.groupToNodes.all[key] = group[key];
  });
  cb(null, this.groupToNodes[gid]);
};

/**
 * Add a node to an existing group
 *
 * @param {String} groupName
 * @param {Object} newNode
 * @param {Function} cb
 */
GroupsService.prototype.add = function(groupName, newNode, cb=defaultCallback) {
  if (this.groupToNodes[groupName] === undefined) {
    cb(new Error(`No such group name '${groupName}'`), null);
  } else {
    const newNodeName = id.getSID(newNode);
    this.groupToNodes[groupName][newNodeName] = newNode;
    // Update the all group
    this.groupToNodes.all[newNodeName] = newNode;
    cb(null, this.groupToNodes[groupName]);
  }
};

/**
 * Remove a node from an exising group
 *
 * @param {String} groupName
 * @param {Object} nodeName
 * @param {Function} cb
 */
GroupsService.prototype.rem = function(
    groupName, nodeName, cb=defaultCallback) {
  if (this.groupToNodes[groupName] === undefined) {
    cb(new Error(`No such group name '${groupName}'`), null);
  } else {
    delete this.groupToNodes[groupName][nodeName];
    // Update the all group
    delete this.groupToNodes.all[nodeName];
    cb(null, this.groupToNodes[groupName]);
  }
};

/**
 * Delete a group of nodes
 *
 * @param {String} groupName
 * @param {Function} cb
 */
GroupsService.prototype.del = function(groupName, cb=defaultCallback) {
  if (this.groupToNodes[groupName]) {
    const group = this.groupToNodes[groupName];
    delete this.groupToNodes[groupName];
    // Update the all group
    Object.keys(group).forEach((key) => {
      delete this.groupToNodes.all[key];
    });
    cb(null, group);
  } else {
    cb(new Error(`No such group '${groupName}'`), null);
  }
};


const groups = new GroupsService();

module.exports = groups;
