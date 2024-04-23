const queryWorkflow = require('../workflow/query');
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
    // global.distribution["crawler"].mem.put(key, (e, v) => {
    // global.distribution["crawler"].store.get(null, (e, v)=> {
    //     const filteredArray = array.filter(element => element.startsWith("index-"));
    //     queryConfig = {
    //         gid : 'crawler',
    //         keys : filtered_list,
    //     }
    //     queryService = queryWorkflow(queryConfig);
    //     global.distribution["crawler"].mr.exec(queryService, (e, v) => {
    //         cb(e, v);
    //     })
    //     });
    //     });
    cb(null, "Further implementation required");
    }

    const query = new QueryService();
module.exports = query;
