const queryWorkflow = require('../workflow/query');
const groups = require('./groups');
function QueryService() {
  // {serviceName: serviceObj}
  this.hashed = {
  };
}

function defaultCallback(error, value) {
  if (error) {
    console.log(error);
  } else {
    console.log(value);
  }
}

QueryService.prototype.get = function(queryInput, cb=defaultCallback) {
  global.distribution['crawler'].mem.put(key, (e, v) => {
    groups.get('crawler', (e, v) => {
      const numberOfNodes = Object.keys(v).length;
      const queryConfig = {
        keys: Array(numberOfNodes).fill('index.txt'),
        gid: 'crawler',
      };
      const queryService = queryWorkflow(queryConfig);
      global.distribution['crawler'].mr.exec(queryService, (e, v) => {
        cb(e, v);
      });
    });
  });
};

const query = new QueryService();
module.exports = query;
