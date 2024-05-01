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
  console.log("query input received:",queryInput);
  global.distribution['crawler'].mem.put(queryInput,{key: 'query', gid: 'crawler'}, (e, v) => { // key here or queryinput?
    groups.get('crawler', (e, v) => {
      const numberOfNodes = Object.keys(v).length;
      const queryConfig = {
        keys: Array(numberOfNodes).fill('index.txt'),
        gid: 'crawler',
      };
      const queryService = queryWorkflow(queryConfig);
      global.distribution['crawler'].mr.exec(queryService, (e, v) => {
        console.log("error happened in query",e);
        console.log("query value received:",v);
        cb(e, v);
      });
    });
  });
};

const query = new QueryService();
module.exports = query;
