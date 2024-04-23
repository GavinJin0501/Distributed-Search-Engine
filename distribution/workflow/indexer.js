
function IndexerWorkflow(config) {
  this.gid = config.gid || 'all';
  this.keys = config.urls || ['page-8e411dcde3fc6885d58c2ada8e84d3974f515ae07f63b27a506fb346975f856a', 'page-a1d8ea1b54ef02ca9c45fd344673e8af2869f617946d96fb21896016a2a5dfc4', 'placeholder'];
  this.keepKeysOrder = config.keepKeysOrder || true;
  // this.keys = config.urls || ['page-d52c05b38ba5d8bf0bb1cfbefd22950f323c22b4064b73f723a9c768947af708'];
  this.memory = config.memory || true;
}


IndexerWorkflow.prototype.map = function(key='key', value='value') {
  const SID = global.distribution.util.id.getSID(global.nodeConfig);
  console.log('key:'+key);
  // return {'mapped': 'yess'};

  console.error('Inside indexer map');
  const scriptPath = './indexer_helper/index.sh';

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
                  console.log('file contents are : '+v);
                  const result = global.spawnSync('sh',
                      [scriptPath, v[1], v[0], `../../store/s-${SID}/${this.gid}/index-${SID}_${this.gid}.txt`]);
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
              console.log('Checking condition...');
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
  return {[key]: value};
};

const indexerWorkflow = (config) => new IndexerWorkflow(config);
module.exports = indexerWorkflow;
