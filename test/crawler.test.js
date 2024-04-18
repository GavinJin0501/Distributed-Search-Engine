global.nodeConfig = {ip: '127.0.0.1', port: 7070};
const distribution = require('../distribution');
const id = distribution.util.id;
const crawlerWorkflow = require('../distribution/workflow/crawler');

const groupsTemplate = require('../distribution/all/groups');
const crwalerGroup = {};

const fs = require('fs');

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

test('crawler real dataset', (done) => {
  const baseUrls = [
    'https://atlas.cs.brown.edu/data/gutenberg/',
    // 'https://atlas.cs.brown.edu/data/gutenberg/1/2/3/',
    // 'https://www.gutenberg.org/browse/scores/top/',
  ];
  const BASE_URL = baseUrls[0];
  const dirVisited = new Set();
  const txtVisited = new Set();
  const BATCH_SIZE = 250;
  let round = 0;

  const doCrwal = (keys, cb) => {
    // add key to the urls
    const dataset = keys.map((url) => {
      const res = {};
      res['url-' + id.getID(url)] = url;
      return res;
    });

    let cntr = 0;
    // We send the dataset to the cluster
    dataset.forEach((o) => {
      let key = Object.keys(o)[0]; // url-urlHash
      let value = o[key]; // url
      distribution.crawler.store.put(value, key, (e, v) => {
        cntr++;

        // Once all urls have been saved, run the map reduce
        if (cntr === dataset.length) {
          // update the visited urls: suppose all of those will be queried
          Object.values(dataset).forEach((pair) => {
            const url = Object.values(pair)[0];
            if (url.endsWith('txt')) {
              txtVisited.add(url);
            } else {
              dirVisited.add(url);
            }
          });

          // If dataset is too large, run multiple map-reduce
          // at the same time to split the workload
          const totalBatch = Math.ceil(dataset.length / BATCH_SIZE);
          let batchCompleted = 0;
          let totalNewUrls = new Set();
          for (let batch = 0; batch < totalBatch; batch++) {
            const startIdx = batch * BATCH_SIZE;
            const endIdx = startIdx + BATCH_SIZE;
            const currBatchDataset = dataset.slice(startIdx, endIdx);

            // A single batch of Map-Reduce
            doMapReduce(currBatchDataset, (err, res) => {
              if (err.length > 0) {
                console.log('crawler.test.doMapReduce completes:', err);
              }

              // aggregate the new urls
              const batchNewUrls = new Set();
              if (res) {
                res.forEach((pair) => {
                  const urls = Object.values(pair)[0] || [];
                  urls.forEach((url) => {
                    if (!dirVisited.has(url) &&
                      !txtVisited.has(url) &&
                      url.startsWith(BASE_URL)) {
                      batchNewUrls.add(url);
                    }
                  });
                });
              }
              totalNewUrls = new Set([...totalNewUrls, ...batchNewUrls]);

              batchCompleted++;
              // All batches completes => this round completes
              if (batchCompleted === totalBatch) {
                round++;
                console.log(`Round ${round}:
    dirsVisited: ${dirVisited.size}
    txtsVisited: ${txtVisited.size}
    newUrls: ${totalNewUrls.size}`);

                if (totalNewUrls.size === 0 || round === 5) {
                  cb(null, [dirVisited, txtVisited]);
                  return;
                }
                // Go to the next recursion
                doCrwal([...totalNewUrls], cb);
              }
            });
          }
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

  doCrwal(baseUrls, (err, res) => {
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
}, 500000);


// test('crawler sandbox', (done) => {
//   const baseUrls = [
//     'https://cs.brown.edu/courses/csci1380/sandbox/4/',
//   ];
//   const dirVisited = new Set();
//   const txtVisited = new Set();
//   const BATCH_SIZE = 100;
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
//               if (err.length > 0) {
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
//     newUrls: ${totalNewUrls.size}`);

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
// }, 30000);

// test('get all', (done) => {
//   distribution.crawler.store.get(null, (err, res) => {
//     const allErr = Object.keys(err).length === Object.keys(crwalerGroup).length;
//     const haveResult = res.length > 0;
//     expect(allErr || haveResult).toEqual(true);
//     console.log(res.length);
//     done();
//   });
// });

// test('deserialize web page', (done) => {
//   const key = '';
//   let baseURL = atob(key.split('page-')[1]);
//   console.log(baseURL);
//   if (baseURL.endsWith('.html')) {
//     baseURL += '/../';
//   } else {
//     baseURL += '/';
//   }

//   distribution.local.store.get(key, (e, html) => {
//     try {
//       expect(e).toBeFalsy();
//       const dom = new JSDOM(html);
//       const aTags = dom.window.document.getElementsByTagName('a');
//       const set = new Set();
//       for (let i = 0; i < aTags.length; i++) {
//         const currPath = aTags[i].href;
//         const url = new URL(currPath, baseURL);

//         if (!set.has(url.href)) {
//           set.add(url.href);
//         }
//       }
//       console.log(set);
//       done();
//     } catch (error) {
//       done(error);
//     }
//   });
// });
