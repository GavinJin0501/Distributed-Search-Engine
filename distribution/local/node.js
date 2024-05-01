const http = require('http');
const {routes} = require('./local');
const util = require('../util/util');
const node = global.nodeConfig;

const start = function(started) {
  const server = http.createServer((req, res) => {
    res.setHeader('Access-Control-Allow-Origin', '*'); // This allows all domains
    res.setHeader('Access-Control-Allow-Methods', 'OPTIONS, GET, POST, PUT, DELETE');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
    // Callback function to send back response
    const serviceCallback = (e, v) => {
      res.end(util.serialize([e, v]));
    };

    let data = '';
    req.on('data', (chunk) => {
      data += chunk;
    });
    req.on('end', () => {
      const {method, url} = req;
      // Handle preflight requests
      if (req.method === 'OPTIONS') {
        // Stop here for preflight requests
        res.writeHead(204); // No Content
        res.end();
        return;
      }
      if (method !== 'PUT') {
        const error = new Error('Http method must be PUT!');
        serviceCallback(error, null);
      }
      const message = util.deserialize(data).message;

      /*
        The path of the http request will determine the service to be used.
        The url will have the form: http://node_ip:node_port/service/method
      */
      const parts = url.split('/').filter(Boolean);
      if (parts.length != 2) {
        const error = new Error(`Url ${url} should be: http://node_ip:node_port/service/method`);
        serviceCallback(error, null);
      }
      const [serviceName, methodName] = parts;

      // Route the payload through the appropriate service
      routes.get(serviceName, (e, v) => {
        if (e) {
          // the requested service does not exist
          serviceCallback(e, v);
        } else if (v[methodName] === undefined) {
          // the requested method does not exist
          e = new Error(`No such '${methodName}' method in '${serviceName}'.`);
          serviceCallback(e, null);
        } else {
          node.msgCount++;
          v[methodName](...message, serviceCallback);
        }
      });
    });
  });

  server.listen(node.port, node.ip, () => {
    global.nodeServer = server;
    if (node.port === 8000) {
      // console.log('node:', started.toString());
    }
    console.log(`Node started at ${node.ip}:${node.port}`);
    started(server);
  });
};

module.exports = {
  start: start,
};
