global.nodeConfig = {ip: '127.0.0.1', port: 7070};
const distribution = require('../distribution');
const id = distribution.util.id;
const comm = distribution.local.comm;
const crawlerWorkflow = require('../distribution/workflow/crawler');
const disCrawlerWorkflow =
      require('../distribution/workflow/distributed_crawler');

const groupsTemplate = require('../distribution/all/groups');
const crwalerGroup = {};
jest.setTimeout(20000);

const fs = require('fs');
const {url} = require('inspector');

/*
   This hack is necessary since we can not
   gracefully stop the local listening node.
   The process that node is
   running in is the actual jest process
*/
let localServer = null;

/*
    The local node will be the orchestrator.
*/

const n1 = {ip: '127.0.0.1', port: 7110};
const n2 = {ip: '127.0.0.1', port: 7111};
const n3 = {ip: '127.0.0.1', port: 7112};

beforeAll((done) => {
  /* Stop the nodes if they are running */

  crwalerGroup[id.getSID(n1)] = n1;
  crwalerGroup[id.getSID(n2)] = n2;
  crwalerGroup[id.getSID(n3)] = n3;

  const startNodes = (cb) => {
    distribution.local.status.spawn(n1, (e, v) => {
      distribution.local.status.spawn(n2, (e, v) => {
        distribution.local.status.spawn(n3, (e, v) => {
          cb();
        });
      });
    });
  };

  distribution.node.start((server) => {
    localServer = server;

    startNodes(() => {
      const crawlConfig = {gid: 'crawler'};
      // const crawlConfig = {gid: 'crawler', hash: id.consistentHash};
      groupsTemplate(crawlConfig).put(crawlConfig, crwalerGroup, (e, v) => {
        done();
      });
    });
  });
});

afterAll((done) => {
  let remote = {service: 'status', method: 'stop'};
  remote.node = n1;
  distribution.local.comm.send([], remote, (e, v) => {
    remote.node = n2;
    distribution.local.comm.send([], remote, (e, v) => {
      remote.node = n3;
      distribution.local.comm.send([], remote, (e, v) => {
        localServer.close();
        done();
      });
    });
  });
});

// test('crawler real dataset', (done) => {
//   const baseUrls = [
//     // 'https://atlas.cs.brown.edu/data/gutenberg/',
//     // 'https://atlas.cs.brown.edu/data/gutenberg/1/2/3/',
//   ];
//   const BASE_URL = baseUrls[0];
//   const dirVisited = new Set();
//   const txtVisited = new Set();
//   const BATCH_SIZE = 250;
//   let round = 0;

//   const doCrwal = (keys, cb) => {
//     // add key to the urls
//     const dataset = keys.map((url) => {
//       const res = {};
//       res['url-' + id.getID(url)] = url;
//       return res;
//     });

//     let cntr = 0;
//     // We send the dataset to the cluster
//     dataset.forEach((o) => {
//       let key = Object.keys(o)[0]; // url-urlHash
//       let value = o[key]; // url
//       distribution.crawler.store.put(value, key, (e, v) => {
//         cntr++;

//         // Once all urls have been saved, run the map reduce
//         if (cntr === dataset.length) {
//           // If dataset is too large, run multiple map-reduce
//           // at the same time to split the workload
//           // const totalBatch = Math.ceil(dataset.length / BATCH_SIZE);
//           const totalBatch = 1;
//           let batchCompleted = 0;
//           let totalNewUrls = new Set();
//           for (let batch = 0; batch < totalBatch; batch++) {
//             const startIdx = batch * BATCH_SIZE;
//             const endIdx = startIdx + BATCH_SIZE;
//             const currBatchDataset = dataset.slice(startIdx, endIdx);

//             // A single batch of Map-Reduce
//             doMapReduce(dataset, (err, res) => {
//               // aggregate the new urls
//               const batchNewUrls = new Set();
//               if (Array.isArray(res)) {
//                 res.forEach((pair) => {
//                   const key = Object.keys(pair)[0];
//                   const urls = pair[key];
//                   urls.forEach((url) => {
//                     if (!dirVisited.has(url) &&
//                       !txtVisited.has(url) &&
//                       url.startsWith(BASE_URL)) {
//                       batchNewUrls.add(url);
//                     }
//                   });
//                 });

//                 err = new Set(err);
//                 // update the visited urls: suppose all of those will be queried
//                 Object.values(dataset).forEach((pair) => {
//                   const url = Object.values(pair)[0];
//                   if (err.has(url)) {
//                     return;
//                   }

//                   if (url.endsWith('txt')) {
//                     txtVisited.add(url);
//                   } else {
//                     dirVisited.add(url);
//                   }
//                 });
//               }
//               totalNewUrls = new Set([...totalNewUrls, ...batchNewUrls]);

//               batchCompleted++;
//               // All batches completes => this round completes
//               if (batchCompleted === totalBatch) {
//                 round++;
//                 console.log(`Round ${round}:
//     dirsVisited: ${dirVisited.size}
//     txtsVisited: ${txtVisited.size}
//     newUrls: ${totalNewUrls.size}
//     currUrls: ${dataset.length}
//     batchSize: ${totalBatch}`);

//                 if (totalNewUrls.size === 0 || round === 5) {
//                   cb(null, [dirVisited, txtVisited]);
//                   return;
//                 }
//                 // Go to the next recursion
//                 doCrwal([...totalNewUrls], cb);
//               }
//             });
//           }
//         }
//       });
//     });
//   };

//   const doMapReduce = (dataset, cb) => {
//     const config = {
//       gid: 'crawler',
//       urls: dataset.map((pair) => Object.keys(pair)[0]),
//     };
//     const crawler = crawlerWorkflow(config);
//     distribution.crawler.mr.exec(crawler, cb);
//   };

//   doCrwal(baseUrls, (err, res) => {
//     const [dirVisited, txtVisited] = res;
//     const totalUrls = dirVisited.size + txtVisited.size;
//     console.log('final visited web pages:', totalUrls);
//     console.log('final downloaded books:', txtVisited.size);
//     fs.writeFile('./bak/visited/dirVisited.txt',
//         JSON.stringify(Array.from(dirVisited)), (err) => {
//           fs.writeFile('./bak/visited/txtVisited.txt',
//               JSON.stringify(Array.from(txtVisited)), (err) => {
//                 done();
//               });
//         });
//   });
// }, 500000);


// test('crawler sandbox', (done) => {
//   const baseUrls = [
//     'https://cs.brown.edu/courses/csci1380/sandbox/1/',
//     'https://cs.brown.edu/courses/csci1380/sandbox/2/',
//     'https://cs.brown.edu/courses/csci1380/sandbox/3/',
//     'https://cs.brown.edu/courses/csci1380/sandbox/4/',
//   ];
//   const dirVisited = new Set();
//   const txtVisited = new Set();
//   const BATCH_SIZE = 256;
//   let round = 0;

//   const doCrwal = (keys, cb) => {
//     // add key to the urls
//     const dataset = keys.map((url) => {
//       const res = {};
//       res['url-' + id.getID(url)] = url;
//       return res;
//     });

//     let cntr = 0;
//     // We send the dataset to the cluster
//     dataset.forEach((o) => {
//       let key = Object.keys(o)[0]; // url-urlHash
//       let value = o[key]; // url
//       distribution.crawler.store.put(value, key, (e, v) => {
//         cntr++;

//         // Once all urls have been saved, run the map reduce
//         if (cntr === dataset.length) {
//           // update the visited urls: suppose all of those will be queried
//           Object.values(dataset).forEach((pair) => {
//             const url = Object.values(pair)[0];
//             if (url.endsWith('txt')) {
//               txtVisited.add(url);
//             } else {
//               dirVisited.add(url);
//             }
//           });

//           // If dataset is too large, run multiple map-reduce
//           // at the same time to split the workload
//           const totalBatch = Math.ceil(dataset.length / BATCH_SIZE);
//           let batchCompleted = 0;
//           let totalNewUrls = new Set();
//           for (let batch = 0; batch < totalBatch; batch++) {
//             const startIdx = batch * BATCH_SIZE;
//             const endIdx = startIdx + BATCH_SIZE;
//             const currBatchDataset = dataset.slice(startIdx, endIdx);

//             // A single batch of Map-Reduce
//             doMapReduce(currBatchDataset, (err, res) => {
//               if (err) {
//                 console.log('crawler.test.doMapReduce completes:', err);
//               }

//               // aggregate the new urls
//               const batchNewUrls = new Set();
//               if (res) {
//                 res.forEach((pair) => {
//                   const urls = Object.values(pair)[0] || [];
//                   urls.forEach((url) => {
//                     if (!dirVisited.has(url) &&
//                       !txtVisited.has(url)) {
//                       batchNewUrls.add(url);
//                     }
//                   });
//                 });
//               }
//               totalNewUrls = new Set([...totalNewUrls, ...batchNewUrls]);

//               batchCompleted++;
//               // All batches completes => this round completes
//               if (batchCompleted === totalBatch) {
//                 round++;
//                 console.log(`Round ${round}:
//     dirsVisited: ${dirVisited.size}
//     txtsVisited: ${txtVisited.size}
//     newUrls: ${totalNewUrls.size}
//     currUrls: ${dataset.length}
//     batchSize: ${totalBatch}`);

//                 if (totalNewUrls.size === 0) {
//                   cb(null, [dirVisited, txtVisited]);
//                   return;
//                 }
//                 // Go to the next recursion
//                 doCrwal([...totalNewUrls], cb);
//               }
//             });
//           }
//         }
//       });
//     });
//   };

//   const doMapReduce = (dataset, cb) => {
//     const config = {
//       gid: 'crawler',
//       urls: dataset.map((pair) => Object.keys(pair)[0]),
//     };
//     const crawler = crawlerWorkflow(config);
//     distribution.crawler.mr.exec(crawler, cb);
//   };

//   doCrwal(baseUrls, (err, res) => {
//     const [dirVisited, txtVisited] = res;
//     const totalUrls = dirVisited.size + txtVisited.size;
//     console.log('final visited web pages:', totalUrls);
//     console.log('final downloaded books:', txtVisited.size);
//     fs.writeFile('./bak/visited/dirVisited.txt',
//         JSON.stringify(Array.from(dirVisited)), (err) => {
//           fs.writeFile('./bak/visited/txtVisited.txt',
//               JSON.stringify(Array.from(txtVisited)), (err) => {
//                 done();
//               });
//         });
//   });
// }, 50000);


// test('distributed crawler real dataset', (done) => {
//   const baseUrls = [];
//   for (let i = 1; i <= 10; i++) {
//     baseUrls.push('https://www.usenix.org/publications/proceedings?page='+i);
//   }
//   const toCrawlKeys = Object.values(crwalerGroup).map((pair) => 'urlsToCrawl');

//   // first put the urlsToCrawl file to each node
//   const setUpUrlFiles = () => {
//     let initFilesSetup = 0;
//     Object.values(crwalerGroup).forEach((node) => {
//       const remote = {service: 'store', method: 'put', node};
//       const metaData = {gid: 'crawler', key: 'urlsToCrawl'};
//       comm.send([[], metaData], remote, (err, res) => {
//         if (err) {
//           throw err;
//         }
//         metaData.key = 'urlsCrawled';
//         comm.send([[], metaData], remote, (err, (res) => {
//           if (err) {
//             throw err;
//           }

//           initFilesSetup++;
//           if (initFilesSetup === Object.values(crwalerGroup).length) {
//             console.log('all urlsToCrawl and urlsCrawled are stored');
//             storeBaseUrls();
//           }
//         }));
//       });
//     });
//   };

//   const storeBaseUrls = () => {
//     if (baseUrls.length === 0) {
//       done();
//       return;
//     }

//     const nodes = Object.values(crwalerGroup);
//     const numOfNodes = Object.keys(nodes).length;
//     const partialKeys = Array.from({length: numOfNodes}, () => []);
//     const hashFunc = distribution.crawler.store.context.hash;
//     for (const key of baseUrls) {
//       const node = id.getProperNode(key, crwalerGroup, hashFunc);
//       const idx = nodes.indexOf(node);
//       partialKeys[idx].push(key);
//     }

//     let urlsToCrawlSaved = 0;
//     for (let i = 0; i < numOfNodes; i++) {
//       const remote = {service: 'store', method: 'append', node: nodes[i]};
//       const valueKey = {gid: 'crawler', key: null};
//       const appendKey = 'urlsToCrawl';
//       comm.send([partialKeys[i], valueKey, appendKey], remote, (err, res) => {
//         if (err) {
//           console.log('inital save err:', err.message);
//         }
//         urlsToCrawlSaved++;

//         if (urlsToCrawlSaved === numOfNodes) {
//           doCrawl((res) => {
//             console.log('finally, crawledUrlNum:', res);
//             done();
//           });
//         }
//       });
//     }
//   };

//   let crawledUrlNum = 0;
//   let round = 0;
//   const doCrawl = (cb) => {
//     // cb(10);
//     doMapReduce((err, res) => {
//       // console.log('res:', res);
//       let newUrlsToCrawl = 0;
//       res.forEach((pair) => {
//         const info = Object.values(pair)[0];
//         crawledUrlNum += info[0];
//         newUrlsToCrawl += info[1];
//       });

//       round++;
//       console.log(`Round ${round} =>
//     newUrls: ${newUrlsToCrawl}
//     urlsCrawled: ${crawledUrlNum}`);
//       if (newUrlsToCrawl === 0) {
//         cb(crawledUrlNum);
//       } else {
//         doCrawl(cb);
//       }
//     });
//   };

//   const doMapReduce = (cb) => {
//     const config = {
//       gid: 'crawler',
//       urls: toCrawlKeys,
//       // domain: 'https://atlas.cs.brown.edu/data/gutenberg',
//     };

//     const distributedCrawler = disCrawlerWorkflow(config);
//     distribution.crawler.mr.exec(distributedCrawler, cb);
//   };

//   setUpUrlFiles();
// }, 500000);


// test('crawler usenix', (done) => {
//   const BASE_URL = 'https://www.usenix.org/publications/proceedings?page=';
//   const dirVisited = new Set();
//   const txtVisited = new Set();
//   let round = 1;

//   const doCrawl = (keys, cb) => {
//     // convert the keys to key-value pairs
//     const dataset = keys.map((url) => {
//       const res = {};
//       res['url-' + id.getID(url)] = url;
//       return res;
//     });

//     let saveCompleted = 0;
//     const newUrls = new Set();

//     // Save all the dataset urls across the nodes
//     dataset.forEach((o) => {
//       let key = Object.keys(o)[0]; // url-urlHash
//       let value = o[key]; // url

//       distribution.crawler.store.put(value, key, (e, v) => {
//         saveCompleted++;
//         if (saveCompleted === dataset.length) {
//           // start map reduce
//           doMapReduce(dataset, (err, res) => {
//             // res: {url: [newUrls]]}
//             res.forEach((pair) => {
//               const key = Object.keys(pair)[0];

//               // update the visited urls
//               if (key.endsWith('txt')) {
//                 txtVisited.add(key);
//               } else {
//                 dirVisited.add(key);
//               }

//               // update the new urls
//               const urls = pair[key];
//               urls.forEach((url) => {
//                 if (typeof url === 'string' &&
//                     !dirVisited.has(url) &&
//                     !txtVisited.has(url)) {
//                   newUrls.add(url);
//                 }
//               });

//               if (Array.isArray(err)) {
//                 err.forEach((url) => newUrls.push(url));
//               }
//             });
//             // Current round completes
//             console.log(`Round ${round}:
//     dirsVisited: ${dirVisited.size}
//     txtsVisited: ${txtVisited.size}
//     newUrls: ${newUrls.size}
//     currUrls: ${dataset.length}`);

//             if (newUrls.size === 0) {
//               cb(null, [dirVisited, txtVisited]);
//               return;
//             } else {
//               doCrawl([BASE_URL + round++, BASE_URL + round++, ...newUrls], cb);
//             }
//           });
//         }
//       });
//     });
//   };

//   const doMapReduce = (dataset, cb) => {
//     const config = {
//       gid: 'crawler',
//       urls: dataset.map((pair) => Object.keys(pair)[0]),
//     };
//     const crawler = crawlerWorkflow(config);
//     distribution.crawler.mr.exec(crawler, cb);
//   };

//   doCrawl([BASE_URL + round++, BASE_URL + round++], (err, res) => {
//     const [dirVisited, txtVisited] = res;
//     const totalUrls = dirVisited.size + txtVisited.size;
//     console.log('final visited web pages:', totalUrls);
//     console.log('final downloaded books:', txtVisited.size);
//     fs.writeFile('./bak/visited/dirVisited.txt',
//         JSON.stringify(Array.from(dirVisited)), (err) => {
//           fs.writeFile('./bak/visited/txtVisited.txt',
//               JSON.stringify(Array.from(txtVisited)), (err) => {
//                 done();
//               });
//         });
//   });
// }, 1000000);
