global.nodeConfig = {ip: '127.0.0.1', port: 7070};
const distribution = require('../distribution');
const id = distribution.util.id;
const comm = distribution.local.comm;
const crawlerWorkflow = require('../distribution/workflow/crawler');
const indexerWorkflow = require('../distribution/workflow/indexer');
const queryWorkflow =  require ('../distribution/workflow/query');
const disCrawlerWorkflow =
      require('../distribution/workflow/distributed_crawler');

const groupsTemplate = require('../distribution/all/groups');
const crwalerGroup = {};

jest.setTimeout(200000);

const fs = require('fs');
const {url} = require('inspector');
beforeAll((done) => {
  /* Stop the nodes if they are running */
  const n1 = {ip: '127.0.0.1', port: 7110};
  const n2 = {ip: '127.0.0.1', port: 7111};
  const n3 = {ip: '127.0.0.1', port: 7112};
  // const n4 = {ip: '127.0.0.1', port: 7113};
  // const n5 = {ip: '127.0.0.1', port: 7114};
  // const n6 = {ip: '127.0.0.1', port: 7115};
  // const n7 = {ip: '127.0.0.1', port: 7116};
  // const n8 = {ip: '127.0.0.1', port: 7117};
  // const n9 = {ip: '127.0.0.1', port: 7118};
  crwalerGroup[id.getSID(n1)] = n1;
  crwalerGroup[id.getSID(n2)] = n2;
  crwalerGroup[id.getSID(n3)] = n3;
  // crwalerGroup[id.getSID(n4)] = n4;
  // crwalerGroup[id.getSID(n5)] = n5;
  // crwalerGroup[id.getSID(n6)] = n6;
  // crwalerGroup[id.getSID(n7)] = n7;
  // crwalerGroup[id.getSID(n8)] = n8;
  // crwalerGroup[id.getSID(n9)] = n9;
  distribution.node.start((server) => {
    localServer = server;
    const crawlConfig = {gid: 'crawler'};
    // const crawlConfig = {gid: 'crawler', hash: id.consistentHash};
    groupsTemplate(crawlConfig).put(crawlConfig, crwalerGroup, (e, v) => {
      console.log('Group created:', e, v);
      done();
    });
  });
});
afterAll((done) => {
  localServer.close();
  done();
});

test('crawler usenix', (done) => {
  const BASE_URL = 'https://www.usenix.org/publications/proceedings?page=';
  const dirVisited = new Set();
  const txtVisited = new Set();
  let round = 1;

  const doCrawl = (keys, cb) => {
    // convert the keys to key-value pairs
    const dataset = keys.map((url) => {
      const res = {};
      res['url-' + id.getID(url)] = url;
      return res;
    });

    let saveCompleted = 0;
    const newUrls = new Set();

    // Save all the dataset urls across the nodes
    dataset.forEach((o) => {
      let key = Object.keys(o)[0]; // url-urlHash
      let value = o[key]; // url

      distribution.crawler.store.put(value, key, (e, v) => {
        saveCompleted++;
        if (saveCompleted === dataset.length) {
          // start map reduce
          doMapReduce(dataset, (err, res) => {
            // res: {url: [newUrls]]}
            res.forEach((pair) => {
              const key = Object.keys(pair)[0];

              // update the visited urls
              if (key.endsWith('txt')) {
                txtVisited.add(key);
              } else {
                dirVisited.add(key);
              }

              // update the new urls
              const urls = pair[key];
              urls.forEach((url) => {
                if (typeof url === 'string' &&
                    !dirVisited.has(url) &&
                    !txtVisited.has(url)) {
                  newUrls.add(url);
                }
              });

              if (Array.isArray(err)) {
                err.forEach((url) => newUrls.push(url));
              }
            });
            // Current round completes
            console.log(`Round ${round}:
    dirsVisited: ${dirVisited.size}
    txtsVisited: ${txtVisited.size}
    newUrls: ${newUrls.size}
    currUrls: ${dataset.length} Time: ${Date.now()}`);

            if (newUrls.size === 0) {
              cb(null, [dirVisited, txtVisited]);
              return;
            } else {
              doCrawl([BASE_URL + round++, BASE_URL + round++, ...newUrls], cb);
            }
          });
        }
      });
    });
  };

  const doMapReduce = (dataset, cb) => {
    const config = {
      gid: 'crawler',
      urls: dataset.map((pair) => Object.keys(pair)[0]),
    };
    const crawler = crawlerWorkflow(config);
    distribution.crawler.mr.exec(crawler, cb);
  };

  doCrawl([BASE_URL + round++, BASE_URL + round++], (err, res) => {
    const [dirVisited, txtVisited] = res;
    const totalUrls = dirVisited.size + txtVisited.size;
    console.log('final visited web pages:', totalUrls);
    console.log('final downloaded books:', txtVisited.size);
    fs.writeFile('./bak/visited/dirVisited.txt',
        JSON.stringify(Array.from(dirVisited)), (err) => {
          fs.writeFile('./bak/visited/txtVisited.txt',
              JSON.stringify(Array.from(txtVisited)), (err) => {
                done();
              });
        });
  });
}, 1000000);


// test indexer


// test('indexer', (done) => {
//   console.log('starting test');
//   const config = {
//     gid: 'crawler',
//   };
//   const indexer = indexerWorkflow(config);
//   distribution.crawler.mr.exec(indexer, (e, v)=>{
//     if (v) {
//       done();
//     }
//   });
// }, 1000000);

test('indexer', (done) => {
  console.log('starting test');
  const config = {
    gid: 'crawler',
    urls: Object.keys(crwalerGroup).map((node) => 'placeholder'),
  };
  const indexer = indexerWorkflow(config);
  distribution.crawler.mr.exec(indexer, (e, v)=>{
    if (v) {
      done();
    }
  });
}, 20000000);

// test query
// test('query',(done)=>{
// const n = 1000; // 1, 10, .....

// for (let i = 0; i < n; i++) {
//   global.distribution['crawler'].mem.put('network',{key: 'query', gid: 'crawler'}, (e, v) => { // key here or queryinput?
//     groups.get('crawler', (e, v) => {
//       const numberOfNodes = Object.keys(v).length;
//       const queryConfig = {
//         keys: Array(numberOfNodes).fill('index.txt'),
//         gid: 'crawler',
//       };
//       const queryService = queryWorkflow(queryConfig);
//       global.distribution['crawler'].mr.exec(queryService, (e, v) => {
//         console.log("error happened in query",e);
//         console.log("query value received:",v);
//       });
//     });
//   });
// }
// done()
// })
test('query', (done) => {
  const n = 100; // 1, 10, .....
  let totalTime = 0;
  let completed = 0;

  for (let i = 0; i < n; i++) {
      const start = performance.now();
      global.distribution['crawler'].mem.put('network', { key: 'query', gid: 'crawler' }, (e, v) => {
          const numberOfNodes = Object.keys(v).length;
          console.log("number of nodes",numberOfNodes);
          const queryConfig = {
              keys:Object.keys(crwalerGroup).map((node) => 'index.txt'),
              gid: 'crawler',
          };
          console.log("queryConfig",queryConfig);
          const queryService = queryWorkflow(queryConfig);
          global.distribution['crawler'].mr.exec(queryService, (e, v) => {
              const end = performance.now();
              totalTime += end - start;
              completed++;
              // console.log('error happened in query', e);
              // console.log('query value received:', v);

              if (completed === n) {
                 console.log('n',n);
                  console.log('Avg time is:', totalTime / n, 'ms');
                  done();
              }
          });
      });
  }

},50000);