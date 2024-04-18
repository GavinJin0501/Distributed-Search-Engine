if (!global.nodeConfig) {
  global.nodeConfig = {
    ip: '127.0.0.1',
    port: 8080,
  };
}
global.nodeConfig.msgCount = 0;

/*

Service  Description                                Methods
status   Status and control of the current node     get, spawn, stop
comm     A message communication interface          send
groups   A mapping from group names to nodes        get, put, add, rem, del
gossip   The receiver part of the gossip protocol   recv
routes   A mapping from names to functions          get, put

*/

/* Status Service */

const status = require('./status');

/* Groups Service */

const groups = require('./groups');

/* Comm Service */

const comm = require('./comm');

/* Rpc Service */

const rpc = require('./rpc');

/* Gossip Service */

const gossip = require('./gossip');

/* Storage Service */

// in-memory
const mem = require('./mem');

// persistent
const store = require('./store');

const query = require('./query');

/* Routes Service */

const routes = require('./routes');
routes.put(status, 'status', () => {});
routes.put(groups, 'groups', () => {});
routes.put(comm, 'comm', () => {});
routes.put(rpc, 'rpc', () => {});
routes.put(gossip, 'gossip', () => {});
routes.put(mem, 'mem', () => {});
routes.put(store, 'store', () => {});
routes.put(query, 'query', () => {});

module.exports = {
  status: status,
  routes: routes,
  comm: comm,
  groups: groups,
  gossip: gossip,
  rpc: rpc,
  mem: mem,
  store: store,
};
