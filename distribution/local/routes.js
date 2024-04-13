const mr = require('./mr');

/**
 * Routes Service
 */
function RoutesService() {
  // {serviceName: serviceObj}
  this.getMap = {
    'routes': this,
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
   * Get method for Routes Service
   *
   * @param {String} key
   * @param {Function} cb
   */
RoutesService.prototype.get = function(key, cb=defaultCallback) {
  if (this.getMap[key] !== undefined) {
    const obj = this.getMap[key];
    cb(null, obj);
  } else {
    const error = new Error(`No such get service \'${key}\' in routes`);
    cb(error, null);
  }
};

/**
   * Put method for RoutesService
   * Add a new service into the routes
   *
   * @param {Object} service
   * @param {String} key
   * @param {Function} cb
   */
RoutesService.prototype.put = function(service, key, cb=defaultCallback) {
  if (this.getMap[key] !== undefined) {
    const error = new Error('Service \'${key}\' already exists in routes');
    cb(error, null);
  } else if (key.startsWith('mr-')) {
    // map reduce functions
    this.getMap[key] = mr(service);
    cb(null, key);
  } else {
    this.getMap[key] = service;
    cb(null, key);
  }
};

/**
 * Delete method for RoutesService
 * Delete a service in the routes
 *
 * @param {*} key
 * @param {*} cb
 */
RoutesService.prototype.del = function(key, cb=defaultCallback) {
  if (this.getMap[key] === undefined) {
    const error = new Error('Service \'${key}\' does not exist in routes');
    cb(error, null);
  } else if (key.startsWith('mr-')) {
    const service = this.getMap[key];
    delete this.getMap[key];
    service.deregister(cb);
  } else {
    delete this.getMap[key];
    cb(null, 'Service \'${key}\' is deleted');
  }
};

const routes = new RoutesService();

module.exports = routes;
