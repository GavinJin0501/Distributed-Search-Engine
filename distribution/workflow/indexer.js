
function IndexerWorkflow(config) {
  this.gid = config.gid || 'all';
  this.keys = config.urls || ['placeholder', 'placeholder', 'placeholder'];
  this.keepKeysOrder = config.keepKeysOrder || true;
  // this.keys = config.urls || ['page-d52c05b38ba5d8bf0bb1cfbefd22950f323c22b4064b73f723a9c768947af708'];
  this.memory = config.memory || true;
}


IndexerWorkflow.prototype.map = function(key='key', value='value') {
  const SID = global.distribution.util.id.getSID(global.nodeConfig);
  // console.log('key:'+key);
  // return {'mapped': 'yess'};
  // console.log('path is : ' + global.dirname);
  console.error('Inside indexer map phase');
  const scriptPath = global.path.join(global.dirname, './distribution/workflow/indexer_helper/index.sh');
  const indexFilePath = global.path.join(global.dirname, './distribution/workflow/indexer_helper');
  const indexHelperPath = global.path.join(global.dirname, `/store/s-${SID}/${this.gid}/index.txt`);
  // console.log('indexHelper path is : ' + indexHelperPath);
  return new Promise((resolve, reject) => {
    global.distribution.local.store.get({key: null, gid: this.gid}, (e, v) => {
      // console.log('process started' + v.length);
      if (v && v.length !== 0) {
        const filteredList = v.filter((element) => element.startsWith('page-'));
        const pageCount = filteredList.length;
        const processedPages = [];
        if (pageCount !==0) {
          filteredList.forEach((ele) => {
            global.distribution.local.store.get(
                {key: ele, gid: this.gid}, (e, v)=>{
                  console.log('file name is : ' + ele);
                  // console.log('file contents are : '+v);
                  // console.log('path is : ' + global.dirname);
                  const result = global.spawnSync('sh',
                      [scriptPath, v[1], v[0], indexHelperPath, indexFilePath]);
                  if (result.status === 0) {
                    console.log('Script executed successfully.');
                    // console.log('Output:', result.stdout.toString());
                    // const obj = {};
                    // obj[ele] = result.stdout.toString();
                    // processedPages.push(obj);

                    const scriptResult = result.stdout.toString();
                    const scriptResultEntries = scriptResult.split('\n');
                    scriptResultEntries.forEach((ele)=>{
                      processedPages.push({[ele.split('|')[0].trim()]: ele});
                    });

                    // resolve({'indexed': 'yes'});
                  } else {
                    console.error('Error executing script:');
                    console.error(result.stderr.toString());
                    processedPages.push({ele: new Error(result.stderr.toString())});
                    // reject(new Error(result.stderr.toString()));
                  }
                });

            const intervalId = setInterval(() => {
              // console.log('Checking condition...');
              if (processedPages.length >= pageCount) {
                console.log('Condition met. Stopping timer.');
                // console.log('processed Index keys are : ' + JSON.stringify(processedPages));
                clearInterval(intervalId);
                resolve(processedPages);
              }
            }, 1000);
          });
        } else {
          resolve({'indexed': 'no'});
        }
      } else {
        resolve({'indexed': 'no'});
      }
    });
  });
};

/**
 *
 * @param {String} key url
 * @param {String} value web content
 * @return {Object} {url: url-html-content}
 */

IndexerWorkflow.prototype.preReduce = function() {

};

IndexerWorkflow.prototype.reduce = function(key='indexed', value='yes') {
  // console.log(JSON.stringify({[key]: value}));
  const SID = global.distribution.util.id.getSID(global.nodeConfig);
  const processedPages = [];
  let processed = false;
  if (Array.isArray(value)) {
    // console.log('it is a array');
    value = value.join('\n');
  }
  const scriptPath = global.path.join(global.dirname, './distribution/workflow/indexer_helper/indexReducer.sh');
  const indexHelperPath = global.path.join(global.dirname, './distribution/workflow/indexer_helper');
  const indexFilePath = global.path.join(global.dirname, `/store/s-${SID}/${this.gid}/index.txt`);
  // console.log('indexHelper path is : ' + indexHelperPath);
  return new Promise((resolve, reject) => {
    const result = global.spawnSync('sh',
        [scriptPath, value, indexHelperPath, indexFilePath]);
    if (result.status === 0) {
      processed = true;
      console.log('Index Reducer Script executed successfully');
      // console.log('Output:', result.stdout.toString());
      processedPages.push({[key]: value});
      // resolve({'indexed': 'yes'});
    } else {
      processed = true;
      console.error('Error executing script:');
      console.error(result.stderr.toString());
      processedPages.push({ele: new Error(result.stderr.toString())});
      // reject(new Error(result.stderr.toString()));
    }


    const intervalId = setInterval(() => {
      // console.log('Checking condition...');
      if (processed) {
        console.log('Condition met. Stopping timer for reducer');
        clearInterval(intervalId);
        resolve(processedPages);
      }
    }, 1000);
  });

  // return {[key]: value};
};

const indexerWorkflow = (config) => new IndexerWorkflow(config);
module.exports = indexerWorkflow;
