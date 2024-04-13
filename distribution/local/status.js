const {id, wire, serialize} = require('../util/util');
const http = require('http');
const {fork} = require('child_process');
const path = require('path');

const node = global.nodeConfig;
const distPath = path.resolve(__dirname, '../../distribution.js');
const pathToNode = '/usr/bin/node';

/**
 * Status Service
 */
function StatusService() {
  // All node status that can get
  // {statusName: (node) => statusName of the node}
  this.getMap = {
    'nid': (node) => id.getNID(node),
    // 'nid': (node) => ({'ip': node.ip, 'port': node.port}),
    'sid': id.getSID,
    'ip': (node) => node.ip,
    'port': (node) => node.port,
    'counts': (node) => node.msgCount,
    'heapTotal': (node) => process.memoryUsage().heapTotal,
    'heapUsed': (node) => process.memoryUsage().heapUsed,
  };
}

function defaultCallback(error, value) {
  if (error) {
    console.log(error);
  } else {
    console.log(value);
  }
}


/**
 * Get method for StatusService
 *
 * @param {String} key
 * @param {Function} cb
 */
StatusService.prototype.get = function(key, cb=defaultCallback) {
  if (this.getMap[key] === undefined) {
    const error = new Error('No such get method \'${key}\' in status');
    cb(error, null);
  } else {
    const data = this.getMap[key](node);
    cb(null, data);
  }
};

/**
 * Method to stop the node server
 *
 * @param {Function} cb
 */
StatusService.prototype.stop = function(cb=defaultCallback) {
  if (global.nodeServer instanceof http.Server) { // service on a node server
    // this.server.close();
    cb(null, node);
    global.nodeServer.close(() => {
      // console.log('server shutdown:', node);
      process.exit();
    });
  } else {
    const error = new Error(`No node server is needed to close`);
    cb(error, null);
  }
};


/**
 * Launching a new node with appropriate configuration information
 *  using child_process standard library for forking and executing
 *  a new distribution.js process.
 *
 * @param {Object} conf configuration object
 * @param {Function} cb callback function after the server starts
 */
StatusService.prototype.spawn = function(conf, cb) {
  // const asyncCb = wire.toAsync(cb);
  const asyncCb = (server) => {
    cb(null, conf);
  };
  const cbRpc = wire.createRPC(asyncCb);
  if (conf.onStart === undefined) {
    conf.onStart = cbRpc;
  } else {
    const prevOnStart = conf.onStart;
    const newFunctionStr = `(${prevOnStart.toString()})(server);
    (${cbRpc.toString()}(...args))`;
    const newOnStart = new Function('server', '...args', newFunctionStr);
    conf.onStart = newOnStart;
  }

  fork(distPath, ['--config', serialize(conf)], {execPath: pathToNode});
  // spawn('node', [distPath, '--config', serialize(conf)]);
};

const status = new StatusService();

module.exports = status;
