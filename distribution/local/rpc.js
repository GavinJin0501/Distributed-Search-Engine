/**
 * Rpc Service
 */
function RPC() {
  global.rpcFunctions = {};
}

/**
   * Rpc get remote function
   *
   * @param  {...any} args args[:-1] are arguments; args[-1] is the remotePtr
   */
RPC.prototype.get = function(...args) {
  // args must have: remotePtr & callback
  const cb = args.pop() || console.log;
  if (args.length < 1) {
    const error = new Error(`RPC.get() does not have enough arguments`);
    cb(error, null);
    return;
  }

  const remotePtr = args.pop();
  const argvList = args;

  if (global.rpcFunctions[remotePtr]) {
    const func = global.rpcFunctions[remotePtr];
    // console.log(func.toString());
    func(...argvList, cb);
  } else {
    const error = new Error(`RPC does not have remotePtr: ${remotePtr}`);
    cb(error, null);
  }
};

const rpc = new RPC();
module.exports = rpc;
