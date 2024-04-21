const id = require('../util/id');

/**
 * Mem Service - In-memory Storage
 */
function InMemoryService() {
  this.dataMap = new Map();
  // {gid: Set(keys)}: used to identify data stored by distributed services
  this.groupKeysMap = new Map();
}

/**
 * Put a key-value pair to the data map
 * If the key is an object (from all.mem), besides saving the key-value pair,
 * we also save this key to the groupKeyMap
 *
 * @param {Object} value
 * @param {String} key could be string, null, or {key, gid}
 * @param {Function} cb
 */
InMemoryService.prototype.put = function(value, key, cb) {
  if (key === null || typeof key === 'string') {
    key = key ? key : id.getID(value);
  } else {
    const gid = key.gid;
    key = key.key;
    key = key ? key : id.getID(value);

    // add this key to this group
    if (!this.groupKeysMap.has(gid)) {
      this.groupKeysMap.set(gid, new Set());
    }
    this.groupKeysMap.get(gid).add(key);
  }

  // save the actual key-value pair into the memory
  this.dataMap.set(key, value);
  cb(null, value);
};

/**
 * Get the value using the key
 *
 * @param {String} key could be string, null, or {key, gid}
 * @param {Function} cb
 */
InMemoryService.prototype.get = function(key, cb) {
  // return all keys in the dataMap
  if (key === null) {
    cb(null, Array.from(this.dataMap.keys()));
    return;
  }

  // local get request
  if (typeof key == 'string') {
    if (this.dataMap.has(key)) {
      cb(null, this.dataMap.get(key));
    } else {
      cb(new Error(`Key '${key}' does not exist.`), null);
    }
    return;
  }

  // distributed get request
  const gid = key.gid;
  key = key.key;

  if (key === null) {
    let keyArr = [];
    if (this.groupKeysMap.has(gid)) {
      keyArr = Array.from(this.groupKeysMap.get(gid));
    }
    cb(null, keyArr);
    return;
  }

  if (!this.groupKeysMap.has(gid) || !this.groupKeysMap.get(gid).has(key)) {
    cb(new Error(`Key '${key}' does not exist.`), null);
  } else {
    cb(null, this.dataMap.get(key));
  }
};

/**
 * Delete a value using the key
 *
 * @param {String} key
 * @param {Function} cb
 */
InMemoryService.prototype.del = function(key, cb) {
  if (!key) {
    cb(new Error(`Key '${key}' does not exist.`), null);
    return;
  }

  let gid = null;

  // check local del request
  if (typeof key === 'string') {
    if (!this.dataMap.has(key)) {
      cb(new Error(`Key '${key}' does not exist.`), null);
      return;
    }
  }

  // check the distributed del request
  if (typeof key === 'object') {
    gid = key.gid;
    key = key.key;
    if (!this.groupKeysMap.has(gid) || !this.groupKeysMap.get(gid).has(key)) {
      cb(new Error(`Key '${key}' does not exist.`), null);
      return;
    }
    this.groupKeysMap.get(gid).delete(key);
  }

  // delete the key-value pair in the memory
  const value = this.dataMap.get(key);
  this.dataMap.delete(key);
  cb(null, value);
};


InMemoryService.prototype.append = function(value, valueKey, appendKey, cb) {
  // Must specify the valueKey and the appendKey
  if (valueKey === null || appendKey === null) {
    cb(new Error('valueKey or appendKey is null!'), nul);
    return;
  }

  if (typeof valueKey === 'object') {
    const gid = valueKey.gid;

    // add this key to this group
    if (!this.groupKeysMap.has(gid)) {
      this.groupKeysMap.set(gid, new Set());
    }
    this.groupKeysMap.get(gid).add(appendKey);
  }

  if (!this.dataMap.has(appendKey)) {
    this.dataMap.set(appendKey, []);
  }
  const list = this.dataMap.get(appendKey);

  if (Array.isArray(value)) {
    list.push(...value);
  } else {
    list.push(value);
  }

  cb(null, value);
};

const mem = new InMemoryService();
module.exports = mem;
