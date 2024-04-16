var crypto = require('crypto');

// The ID is the SHA256 hash of the JSON representation of the object
function getID(obj) {
  const hash = crypto.createHash('sha256');
  hash.update(JSON.stringify(obj));
  return hash.digest('hex');
}

// The NID is the SHA256 hash of the JSON representation of the node
function getNID(node) {
  node = {ip: node.ip, port: node.port};
  return getID(node);
}

// The SID is the first 5 characters of the NID
function getSID(node) {
  return getNID(node).substring(0, 5);
}

/**
 * Convert a sha256 hash (hex string) to the corresponding interger
 *
 * @param {String} id a sha256 hash
 * @return {Integer}
 */
function idToNum(id) {
  return parseInt(id, 16);
}

/**
 * Use native hash to find a proper node for further operation
 *
 * @param {Integer} kid key identifier -> sha256(primary key)
 * @param {Array} nids an array of node id to choose from
 * @return {Integer} nid
 */
function naiveHash(kid, nids) {
  nids.sort();
  return nids[idToNum(kid) % nids.length];
}

/**
 * Use consistent hash to find a proper node for further operation
 *    1. Place all nodes of a group on a ring
 *    2. Place the objct ID on the same ring
 *    3. Pick the node ID immediately following the object ID
 *
 * @param {Integer} kid key identifier -> sha256(primary key)
 * @param {Array} nids an array of node id to choose from
 * @return {Integer} nid
 */
function consistentHash(kid, nids) {
  // Place all nodes(index) of a group on a ring list
  const ringList = [];
  for (let i = 0; i < nids.length; i++) {
    ringList.push(i);
  }
  ringList.push(nids.length);
  // const kidInteger = idToNum(kid);
  const kidInteger = parseInt(kid, 16);

  // sort the list
  ringList.sort(function(a, b) {
    // const aId = a === nids.length ? kidInteger : idToNum(nids[a]);
    // const bId = b === nids.length ? kidInteger : idToNum(nids[b]);
    const aId = a === nids.length ? kidInteger : parseInt(nids[a], 16);
    const bId = b === nids.length ? kidInteger : parseInt(nids[b], 16);
    return aId - bId;
  });

  // pick the element right after the kidInteger
  let idx = ringList.indexOf(nids.length);
  idx = (idx + 1) % ringList.length;

  // convert resId back to hex
  return nids[ringList[idx]];
}

/**
 * Use rendezvousHash hash to find a proper node for further operation
 *    1. Construct combined values for all combindations of the kid and nids
 *    2. Hash all of those combindation values
 *    3. Pick the highest such value
 *
 * @param {Integer} kid key identifier -> sha256(primary key)
 * @param {Array} nids an array of node id to choose from
 * @return {Integer} nid
 */
function rendezvousHash(kid, nids) {
  const combList = [];
  nids.forEach((nid) => {
    combList.push(kid+nid);
  });

  let maxIdx = -1;
  let maxVal = -1;

  for (let i = 0; i < nids.length; i++) {
    const num = idToNum(getID(combList[i]));
    if (num > maxVal) {
      maxIdx = i;
      maxVal = num;
    }
  }

  return maxIdx === -1 ? null : nids[maxIdx];
}

/**
 * Get the proper node to process the data
 *
 * @param {String} key key of the key-value pair
 * @param {Object} group group of nodes to decide
 * @param {Function} hashFunc hash function to find the node
 * @return {Object} the node that is chosen to process the data
 */
function getProperNode(key, group, hashFunc) {
  // Get all the nodes
  const nodes = Object.values(group);
  // Get all nid (hash of the nodes)
  const nids = nodes.map((node) => getNID(node));
  const kid = getID(key);
  // Hash of the chosen node
  const nidHash = hashFunc(kid, nids);

  // Find the corresponding node of the hash
  for (const node of nodes) {
    if (getNID(node) === nidHash) {
      return node;
    }
  }
  return nodes[0];
}

function convertToAlphanumericOnlyString(s) {
  return s.replace(/[^a-zA-Z0-9]/g, '');
}

module.exports = {
  getNID: getNID,
  getSID: getSID,
  getID: getID,
  idToNum: idToNum,
  naiveHash: naiveHash,
  consistentHash: consistentHash,
  rendezvousHash: rendezvousHash,
  getProperNode: getProperNode,
  convertToAlphanumericOnlyString: convertToAlphanumericOnlyString,
};
