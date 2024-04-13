const path = require('path');
const fs = require('fs');
const id = require('../util/id');
const serialization = require('../util/serialization');

const SID = id.getSID(global.nodeConfig);

/**
 * Store Service - Persistent Storage
 */
function PersistentMemoryService() {
  this.location = path.resolve(__dirname, `../../store/s-${SID}`);
  this.groupKeysMap = new Map();

  if (!fs.existsSync(this.location)) {
    fs.mkdirSync(this.location, {recursive: true});
  }
}

/**
 * Save a key-value pair to local
 *
 * @param {Object} value
 * @param {String} key
 * @param {Function} cb
 */
PersistentMemoryService.prototype.put = function(value, key, cb) {
  let gid = null;
  if (key !== null && typeof key === 'object') {
    gid = key.gid;
    key = key.key;
  }

  key = key ? key : id.getID(value);
  // key = id.convertToAlphanumericOnlyString(key);
  const filePath = path.resolve(this.location, key);
  const fileContent = serialization.serialize(value);

  fs.writeFile(filePath, fileContent, (err) => {
    if (err) {
      cb(new Error(err.message), null);
      return;
    }

    // if it is distributed put, add key info to the groupKeysMap
    if (gid !== null) {
      if (!this.groupKeysMap.has(gid)) {
        this.groupKeysMap.set(gid, new Set());
      }
      this.groupKeysMap.get(gid).add(key);
    }
    cb(null, value);
  });
};

/**
 *  Get the value of the key
 *
 * @param {String} key
 * @param {Function} cb
 */
PersistentMemoryService.prototype.get = function(key, cb) {
  // if key is null, return all keys
  if (key === null) {
    fs.readdir(this.location, (err, fileList) => {
      if (err) {
        cb(new Error(err.message), null);
      } else {
        cb(null, fileList);
      }
    });
    return;
  }

  // local get request
  if (typeof key === 'string') {
    // key = id.convertToAlphanumericOnlyString(key);
    const filePath = path.resolve(this.location, key);

    fs.readFile(filePath, (err, data) => {
      if (err) {
        cb(new Error(err.message), null);
      } else {
        cb(null, serialization.deserialize(data));
      }
    });
    return;
  }

  // distribtued get request
  // console.log(`I am ${global.nodeConfig.port}: ` + key);
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

  // key = id.convertToAlphanumericOnlyString(key);
  const filePath = path.resolve(this.location, key);
  if (!this.groupKeysMap.has(gid) || !this.groupKeysMap.get(gid).has(key)) {
    cb(new Error(`Key '${key}' does not exist.`), null);
  } else {
    fs.readFile(filePath, (err, data) => {
      if (err) {
        cb(new Error(err.message), null);
      } else {
        cb(null, serialization.deserialize(data));
      }
    });
  }
};


/**
 * Delete the key-value pair
 *
 * @param {String} key
 * @param {Function} cb
 */
PersistentMemoryService.prototype.del = function(key, cb) {
  if (!key) {
    cb(new Error(`Key '${key}' does not exist.`), null);
    return;
  }

  let gid = null;
  if (typeof key === 'object') {
    gid = key.gid;
    key = key.key;
    if (!this.groupKeysMap.has(gid) || !this.groupKeysMap.get(gid).has(key)) {
      cb(new Error(`Key '${key}' does not exist.`), null);
      return;
    }
  }

  // key = id.convertToAlphanumericOnlyString(key);
  const filePath = path.resolve(this.location, key);

  fs.readFile(filePath, (err, data) => {
    if (err) {
      cb(new Error(err.message), null);
    } else {
      fs.unlink(filePath, (err) => {
        if (err) {
          cb(new Error(err.message), null);
        } else {
          if (gid) {
            this.groupKeysMap.get(gid).delete(key);
          }
          cb(null, serialization.deserialize(data));
        }
      });
    }
  });
};


const store = new PersistentMemoryService();
module.exports = store;
