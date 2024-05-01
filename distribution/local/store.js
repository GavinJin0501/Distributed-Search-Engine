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

  if (!fs.existsSync(this.location)) {
    fs.mkdirSync(this.location, {recursive: true});
  }
}

/**
 * Save a key-value pair to local
 * key is null / string => invoked locally, path: this.location/key
 * key is object => invoked distributedly, path: this.location/gid/key
 *
 * @param {Object} value
 * @param {String} key
 * @param {Function} cb
 */
PersistentMemoryService.prototype.put = function(value, key, cb) {
  let gid = null;
  let folderPath = this.location;

  // invoked distributedly
  if (key !== null && typeof key === 'object') {
    gid = key.gid;
    key = key.key;
    folderPath = path.resolve(this.location, gid);

    if (!fs.existsSync(folderPath)) {
      fs.mkdirSync(folderPath, {recursive: true});
    }
  }

  key = key ? key : id.getID(value);
  const filePath = path.resolve(folderPath, key);
  const fileContent = serialization.serialize(value);

  fs.writeFile(filePath, fileContent, (err) => {
    if (err) {
      cb(new Error(err.message), null);
      return;
    }

    cb(null, value);
  });
};

/**
 *  Get the value of the key
 *  key is null / string => invoked locally, path: this.location/key
 *  key is object => invoked distributedly, path: this.location/gid/key
 *
 * @param {String} key
 * @param {Function} cb
 */
PersistentMemoryService.prototype.get = function(key, cb) {
  let gid = null;
  let folderPath = this.location;

  // invoked distributedly
  if (key !== null && typeof key === 'object') {
    gid = key.gid;
    key = key.key;
    folderPath = path.resolve(this.location, gid);

    if (!fs.existsSync(folderPath)) {
      cb(new Error(`Key '${key}' does not exist in group '${gid}'`), null);
      return;
    }
  }

  // if key is null, return all keys in the folder
  if (key === null) {
    fs.readdir(folderPath, (err, fileList) => {
      if (err) {
        cb(new Error(err.message), null);
      } else {
        fileList = fileList.filter((fileName) => {
          const entryPath = path.resolve(folderPath, fileName);
          return fs.statSync(entryPath).isFile();
        });
        cb(null, fileList);
      }
    });
  } else {
    const filePath = path.resolve(folderPath, key);
    fs.readFile(filePath, (err, data) => {
      if (err) {
        cb(new Error(err.message), null);
      } else {
        if(filePath.endsWith('.txt')){
          cb(null,data.toString());
        }else{
          cb(null, serialization.deserialize(data));
        }
       
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
  let folderPath = this.location;
  if (typeof key === 'object') {
    gid = key.gid;
    key = key.key;
    folderPath = path.resolve(this.location, gid);
  }

  const filePath = path.resolve(folderPath, key);

  fs.readFile(filePath, (err, data) => {
    if (err) {
      cb(new Error(err.message), null);
      return;
    }
    fs.unlink(filePath, (err) => {
      if (err) {
        cb(new Error(err.message), null);
      } else {
        cb(null, serialization.deserialize(data));
      }
    });
  });
};

/**
 * Append a value to its corresponding key locally
 * Mainly used in crawler and invoked distributedly
 *
 * @param {Object} value
 * @param {String} valueKey
 * @param {String} appendKey
 * @param {Function} cb
 */
PersistentMemoryService.prototype.append = function(
    value, valueKey, appendKey, cb) {
  let gid = null;
  let folderPath = this.location;

  // Must specify the valueKey and the appendKey
  if (appendKey === null) {
    cb(new Error('valueKey or appendKey is null!'), nul);
  }

  // invoked distributedly
  if (valueKey != null && typeof valueKey === 'object') {
    gid = valueKey.gid;
    folderPath = path.resolve(this.location, gid);

    if (!fs.existsSync(folderPath)) {
      fs.mkdirSync(folderPath, {recursive: true});
    }
  }

  const filePath = path.resolve(folderPath, appendKey);

  this.get({gid, key: appendKey}, (err, res) => {
    let fileContent = (res) ? res : [];
    if (Array.isArray(value)) {
      fileContent.push(...value);
    } else {
      fileContent.push(value);
    }

    fileContent = serialization.serialize(fileContent);

    fs.writeFile(filePath, fileContent, (err) => {
      if (err) {
        cb(new Error(err.message), null);
        return;
      }

      cb(null, value);
    });
  });
};


const store = new PersistentMemoryService();
module.exports = store;
