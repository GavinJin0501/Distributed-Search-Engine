const http = require('http');
const serialization = require('../util/serialization.js');

/**
 * Comm Service
 */
function CommService() {
  this.getMap = {};
}

function defaultCallback(error, value) {
  if (error) {
    console.log(error);
  } else {
    console.log(value);
  }
}

/**
   * Send method for Comm Service
   *
   * @param {Array} message array of arguments
   * @param {Object} remote contain the remote node and service to invoke
   * @param {Function} cb
   */
CommService.prototype.send = function(message, remote, cb=defaultCallback) {
  const remoteNode = remote.node;
  const options = {
    hostname: remoteNode.ip,
    port: remoteNode.port,
    path: `/${remote.service}/${remote.method}`,
    method: 'PUT',
    headers: {
      'Content-Type': 'application/json',
    },
  };
  const request = http.request(options, (response) => {
    let data = '';
    response.on('data', (chunk) => {
      data += chunk;
    });

    response.on('end', () => {
      data = serialization.deserialize(data);
      cb(data[0], data[1]);
    });
  });

  request.on('error', (error) => {
    // console.log(message, remote, cb);
    cb(new Error(error.message), null);
  });

  const putData = serialization.serialize({message});
  request.write(putData);
  request.end();
};

const comm = new CommService();

module.exports = comm;
