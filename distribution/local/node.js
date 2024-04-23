const http = require('http');
const {routes} = require('./local');
const util = require('../util/util');
const node = global.nodeConfig;
const groupsTemplate = require('../all/groups');
const start = function(started) {
  const server = http.createServer((req, res) => {
    // Callback function to send back response
    const serviceCallback = (e, v) => {
      res.end(util.serialize([e, v]));
    };

    let data = '';
    req.on('data', (chunk) => data += chunk);

    req.on('end', () => {
      const {method, url} = req;
      const message = util.deserialize(data).message;

      if (method !== 'PUT') {
        const error = new Error('Http method must be PUT!');
        serviceCallback(error, null);
      }

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
  const crawlerSetup = function (cb) {

    // WE NEED TO CHANGE THIS TO MATCH FOR AWS INSTANCES. WE ALSO HAVE TO DEPLOY THIS NODE LAST OR HAVE THIS NODE SPAWN THE OTHERS
    if (node.port === 8080) {
      const crwalerGroup = {};
      const n1 = {ip: '127.0.0.1', port: 7110};
      const n2 = {ip: '127.0.0.1', port: 7111};
      const n3 = {ip: '127.0.0.1', port: 7112};
      crwalerGroup[global.distribution.util.id.getSID(n1)] = n1;
      crwalerGroup[global.distribution.util.id.getSID(n2)] = n2;
      crwalerGroup[global.distribution.util.id.getSID(n3)] = n3;
      groupsTemplate({gid: 'crawler'}).put({gid: 'crawler'}, crwalerGroup, (e, v) => {
        cb();
      });
    }
    else {
      cb()
    }
  }
  server.listen(node.port, node.ip, () => {
    global.nodeServer = server;
    crawlerSetup(()=> {
      started(server);
    })
  });
};

module.exports = {
  start: start,
};
