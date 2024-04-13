const {getID} = require('./id');

function createRPC(func) {
  const remotePtr = getID(func.toString());
  global.rpcFunctions[remotePtr] = func;

  return new Function('...args', `{
    const cb = args.pop() || ((e, v) => console.log(e, v));
    const remote = {
      node: {ip: '${global.nodeConfig.ip}', port: ${global.nodeConfig.port}},
      service: 'rpc',
      method: 'get',
    };
    args.push('${remotePtr}');
    distribution.local.comm.send(args, remote, cb);
  };`);
}

/*
    The toAsync function converts a synchronous function that returns a value
    to one that takes a callback as its last argument and returns the value
    to the callback.
*/
function toAsync(func) {
  return function(...args) {
    const callback = args.pop() || function() {};
    try {
      const result = func(...args);
      callback(null, result);
    } catch (error) {
      callback(error);
    }
  };
}

module.exports = {
  createRPC: createRPC,
  toAsync: toAsync,
};
