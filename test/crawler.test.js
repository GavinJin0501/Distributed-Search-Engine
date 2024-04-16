global.nodeConfig = {ip: '127.0.0.1', port: 7070};
const distribution = require('../distribution');
const id = distribution.util.id;
const crawlerWorkflow = require('../distribution/workflow/crawler');

const groupsTemplate = require('../distribution/all/groups');
const crwalerGroup = {};

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

test('crawler', (done) => {
  const visited = new Set();
  const doCrwal = (keys, cb) => {
    const dataset = keys.filter((url) => !visited.has(url))
        .map((url) => {
          const res = {};
          res[id.getID(url)] = url;
          return res;
        });
    if (Object.values(dataset).length === 0) {
      cb(null, visited);
      return;
    }

    // We send the dataset to the cluster
    let cntr = 0;
    dataset.forEach((o) => {
      let key = Object.keys(o)[0];
      let value = o[key];
      distribution.crawler.store.put(value, key, (e, v) => {
        cntr++;
        // Once we are done, run the map reduce
        if (cntr === dataset.length) {
          doMapReduce((err, res) => {
            // update the visited urls
            Object.values(dataset)
                .forEach((pair) => visited.add(Object.values(pair)[0]));

            // add the new urls
            const newUrls = new Set();
            res.forEach((pair) => {
              const urls = Object.values(pair)[0] || [];
              urls.forEach((url) => newUrls.add(url));
            });

            // console.log('visited:', visited);
            // console.log('newUrls:', newUrls);
            doCrwal([...newUrls], cb);
            // cb(null, null);
          });
        }
      });
    });
  };

  const doMapReduce = (cb) => {
    distribution.crawler.store.get(null, (e, v) => {
      const config = {
        gid: 'crawler',
        urls: v,
      };
      const crawler = crawlerWorkflow(config);
      distribution.crawler.mr.exec(crawler, cb);
    });
  };

  doCrwal([
    'https://cs.brown.edu/courses/csci1380/sandbox/1/',
  ], (err, res) => {
    // console.log('visited urls:', res);
    console.log('final visited web pages: ', res.size);
    done();
  });
});


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
