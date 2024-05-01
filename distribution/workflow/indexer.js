
function IndexerWorkflow(config) {
  this.gid = config.gid || 'all';
  this.keys = config.urls || ['placeholder', 'placeholder', 'placeholder'];
  this.keepKeysOrder = config.keepKeysOrder || true;
  // this.keys = config.urls || ['page-d52c05b38ba5d8bf0bb1cfbefd22950f323c22b4064b73f723a9c768947af708'];
  this.memory = config.memory || true;
}


IndexerWorkflow.prototype.map = function(key='key', value='value') {
  const SID = global.distribution.util.id.getSID(global.nodeConfig);
  console.log('key:'+key);
  // return {'mapped': 'yess'};
  console.log('path is : ' + global.dirname);
  console.error('Inside indexer map');
  const scriptPath = global.path.join(global.dirname, './distribution/workflow/indexer_helper/index.sh');
  const indexFilePath = global.path.join(global.dirname, './distribution/workflow/indexer_helper');
  const indexHelperPath = global.path.join(global.dirname, `/store/s-${SID}/${this.gid}/index.txt`);
  console.log('indexHelper path is : ' + indexHelperPath);
  return new Promise((resolve, reject) => {
    global.distribution.local.store.get({key: null, gid: this.gid}, (e, v) => {
      console.log('process started' + v.length);
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
                  console.log('path is : ' + global.dirname);
                  const result = global.spawnSync('sh',
                      [scriptPath, v[1], v[0], indexHelperPath, indexFilePath]);
                  if (result.status === 0) {
                    console.log('Script executed successfully.');
                    console.log('Output:', result.stdout.toString());
                    const obj = {};
                    obj[ele] = 'yes';
                    processedPages.push(obj);
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
  const Obj = {[key]: value};
  console.log(Obj);
  return Obj;
};

const indexerWorkflow = (config) => new IndexerWorkflow(config);
module.exports = indexerWorkflow;
