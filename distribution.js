#!/usr/bin/env node

const util = require('./distribution/util/util.js');
const args = require('yargs').argv;
const {JSDOM} = require('jsdom');
const {URL} = require('url');
const {convert} = require('html-to-text');
const cheerio = require('cheerio');
const Bottleneck = require('bottleneck');
const {spawnSync} = require('child_process');

global.JSDOM = JSDOM;
global.URL = URL;
global.cheerio = cheerio;
// for usenix
global.limiter = new Bottleneck({
  maxConcurrent: 25,
  minTime: 30,
});
global.spawnSync = spawnSync;

// // for openbooks
// global.limiter = new Bottleneck({
//   maxConcurrent: 1000,
//   minTime: 1000,
// });
global.convert = convert;

// log output to a file
const fs = require('fs');
const path = require('path');

const logFilePath = path.join(__dirname, 'log.txt');
if (fs.existsSync(logFilePath)) {
  fs.truncateSync(logFilePath, 0);
}
const logStream = fs.createWriteStream(logFilePath, {flags: 'a'});

console.log = function() {
  const message = Array.from(arguments).join(' ');
  logStream.write(message + '\n');
};

// Default configuration
global.nodeConfig = global.nodeConfig || {
  ip: '127.0.0.1',
  port: 8080,
  onStart: () => {
    console.log('Node started!');
  },
};

// Ignore the SSL certificate
process.env.NODE_TLS_REJECT_UNAUTHORIZED = 0;

/*
    As a debugging tool, you can pass ip and port arguments directly.
    This is just to allow for you to easily startup nodes from the terminal.

    Usage:
    ./distribution.js --ip '127.0.0.1' --port 1234
  */
if (args.ip) {
  global.nodeConfig.ip = args.ip;
}

if (args.port) {
  global.nodeConfig.port = parseInt(args.port);
}

if (args.config) {
  let nodeConfig = util.deserialize(args.config);
  global.nodeConfig.ip = nodeConfig.ip ? nodeConfig.ip : global.nodeConfig.ip;
  global.nodeConfig.port = nodeConfig.port ?
        nodeConfig.port : global.nodeConfig.port;
  global.nodeConfig.onStart = nodeConfig.onStart ?
        nodeConfig.onStart : global.nodeConfig.onStart;
}

const distribution = {
  util: require('./distribution/util/util.js'),
  local: require('./distribution/local/local.js'),
  node: require('./distribution/local/node.js'),
};

global.distribution = distribution;

distribution['all'] = {};
distribution['all'].status =
    require('./distribution/all/status')({gid: 'all'});
distribution['all'].comm =
    require('./distribution/all/comm')({gid: 'all'});
distribution['all'].gossip =
    require('./distribution/all/gossip')({gid: 'all'});
distribution['all'].groups =
    require('./distribution/all/groups')({gid: 'all'});
distribution['all'].routes =
    require('./distribution/all/routes')({gid: 'all'});
distribution['all'].mem =
    require('./distribution/all/mem')({gid: 'all'});
distribution['all'].store =
    require('./distribution/all/store')({gid: 'all'});

module.exports = global.distribution;

/* The following code is run when distribution.js is run directly */
if (require.main === module) {
  distribution.node.start(global.nodeConfig.onStart);
}
