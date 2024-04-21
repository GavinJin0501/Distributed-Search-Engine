global.nodeConfig = {ip: '127.0.0.1', port: 7070};
const distribution = require('../distribution');
const id = distribution.util.id;
const crawlerWorkflow = require('../distribution/workflow/crawler');

const groupsTemplate = require('../distribution/all/groups');

const ncdcGroup = {};
const dlibGroup = {};
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

  ncdcGroup[id.getSID(n1)] = n1;
  ncdcGroup[id.getSID(n2)] = n2;
  ncdcGroup[id.getSID(n3)] = n3;

  dlibGroup[id.getSID(n1)] = n1;
  dlibGroup[id.getSID(n2)] = n2;
  dlibGroup[id.getSID(n3)] = n3;

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

    const ncdcConfig = {gid: 'ncdc'};
    startNodes(() => {
      groupsTemplate(ncdcConfig).put(ncdcConfig, ncdcGroup, (e, v) => {
        const dlibConfig = {gid: 'dlib'};
        groupsTemplate(dlibConfig).put(dlibConfig, dlibGroup, (e, v) => {
          const crawlConfig = {gid: 'crawler'};
          groupsTemplate(crawlConfig).put(crawlConfig, crwalerGroup, (e, v) => {
            done();
          });
        });
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

function sanityCheck(mapper, reducer, dataset, expected, done) {
  let mapped = dataset.map((o) =>
    mapper(Object.keys(o)[0], o[Object.keys(o)[0]]));
  /* Flatten the array. */
  mapped = mapped.flat();
  let shuffled = mapped.reduce((a, b) => {
    let key = Object.keys(b)[0];
    if (a[key] === undefined) a[key] = [];
    a[key].push(b[key]);
    return a;
  }, {});
  let reduced = Object.keys(shuffled).map((k) => reducer(k, shuffled[k]));

  try {
    expect(reduced).toEqual(expect.arrayContaining(expected));
  } catch (e) {
    done(e);
  }
}

// test('all.mr:ncdc -> memory = true', (done) => {
//   let m1 = (key, value) => {
//     let words = value.split(/(\s+)/).filter((e) => e !== ' ');
//     // console.log(words);
//     let out = {};
//     out[words[1]] = parseInt(words[3]);
//     return out;
//   };

//   let r1 = (key, values) => {
//     let out = {};
//     out[key] = values.reduce((a, b) => Math.max(a, b), -Infinity);
//     return out;
//   };

//   let dataset = [
//     {'000': '006701199099999 1950 0515070049999999N9 +0000 1+9999'},
//     {'106': '004301199099999 1950 0515120049999999N9 +0022 1+9999'},
//     {'212': '004301199099999 1950 0515180049999999N9 -0011 1+9999'},
//     {'318': '004301265099999 1949 0324120040500001N9 +0111 1+9999'},
//     {'424': '004301265099999 1949 0324180040500001N9 +0078 1+9999'},
//   ];

//   let expected = [{'1950': 22}, {'1949': 111}];

//   /* Sanity check: map and reduce locally */
//   sanityCheck(m1, r1, dataset, expected, done);

//   /* Now we do the same thing but on the cluster */
//   const doMapReduce = (cb) => {
//     distribution.ncdc.store.get(null, (e, v) => {
//       try {
//         expect(v.length).toBe(dataset.length);
//       } catch (e) {
//         done(e);
//       }


//       distribution.ncdc.mr.exec({keys: v, map: m1, reduce: r1, memory: true},
//           (e, v) => {
//             try {
//               expect(v).toEqual(expect.arrayContaining(expected));
//               done();
//             } catch (e) {
//               done(e);
//             }
//           });
//     });
//   };

//   let cntr = 0;

//   // We send the dataset to the cluster
//   dataset.forEach((o) => {
//     let key = Object.keys(o)[0];
//     let value = o[key];
//     distribution.ncdc.store.put(value, key, (e, v) => {
//       cntr++;
//       // Once we are done, run the map reduce
//       if (cntr === dataset.length) {
//         doMapReduce();
//       }
//     });
//   });
// });

// test('all.mr:dlib -> memory = true', (done) => {
//   let m2 = (key, value) => {
//     // map each word to a key-value pair like {word: 1}
//     let words = value.split(/(\s+)/).filter((e) => e !== ' ');
//     let out = [];
//     words.forEach((w) => {
//       let o = {};
//       o[w] = 1;
//       out.push(o);
//     });
//     return out;
//   };

//   let r2 = (key, values) => {
//     let out = {};
//     out[key] = values.length;
//     return out;
//   };

//   let dataset = [
//     {'b1-l1': 'It was the best of times, it was the worst of times,'},
//     {'b1-l2': 'it was the age of wisdom, it was the age of foolishness,'},
//     {'b1-l3': 'it was the epoch of belief, it was the epoch of incredulity,'},
//     {'b1-l4': 'it was the season of Light, it was the season of Darkness,'},
//     {'b1-l5': 'it was the spring of hope, it was the winter of despair,'},
//   ];

//   let expected = [
//     {It: 1}, {was: 10},
//     {the: 10}, {best: 1},
//     {of: 10}, {'times,': 2},
//     {it: 9}, {worst: 1},
//     {age: 2}, {'wisdom,': 1},
//     {'foolishness,': 1}, {epoch: 2},
//     {'belief,': 1}, {'incredulity,': 1},
//     {season: 2}, {'Light,': 1},
//     {'Darkness,': 1}, {spring: 1},
//     {'hope,': 1}, {winter: 1},
//     {'despair,': 1},
//   ];

//   /* Sanity check: map and reduce locally */
//   sanityCheck(m2, r2, dataset, expected, done);

//   /* Now we do the same thing but on the cluster */
//   const doMapReduce = (cb) => {
//     distribution.dlib.store.get(null, (e, v) => {
//       try {
//         expect(v.length).toBe(dataset.length);
//       } catch (e) {
//         done(e);
//       }

//       distribution.dlib.mr.exec({keys: v, map: m2, reduce: r2, memory: true},
//           (e, v) => {
//             try {
//               expect(v).toEqual(expect.arrayContaining(expected));
//               done();
//             } catch (e) {
//               done(e);
//             }
//           });
//     });
//   };

//   let cntr = 0;

//   // We send the dataset to the cluster
//   dataset.forEach((o) => {
//     let key = Object.keys(o)[0];
//     let value = o[key];
//     distribution.dlib.store.put(value, key, (e, v) => {
//       cntr++;
//       // Once we are done, run the map reduce
//       if (cntr === dataset.length) {
//         doMapReduce();
//       }
//     });
//   });
// });

// test('crawler', (done) => {
//   const dataset = [
//     {'html-1': 'https://cs.brown.edu/courses/csci1380/sandbox/1/'},
//     {'html-2': 'https://cs.brown.edu/courses/csci1380/sandbox/1/level_1a/index.html'},
//     {'html-3': 'https://cs.brown.edu/courses/csci1380/sandbox/1/level_1b/index.html'},
//     {'html-4': 'https://cs.brown.edu/courses/csci1380/sandbox/1/level_1c/index.html'},
//     {'html-5': 'https://cs.brown.edu/courses/csci1380/sandbox/1/level_1c/fact_5/index.html'},
//   ];

//   const doMapReduce = (cb) => {
//     distribution.crawler.store.get(null, (e, v) => {
//       try {
//         expect(v.length).toBe(dataset.length);
//       } catch (e) {
//         done(e);
//       }

//       const config = {
//         gid: 'crawler',
//         urls: v,
//       };
//       const crawler = crawlerWorkflow(config);

//       distribution.crawler.mr.exec(crawler, (e, v) => {
//         try {
//           // const expected = dataset.map((pair) => Object.values(pair)[0]);
//           // expect(v).toEqual(expect.arrayContaining(expected));
//           done();
//         } catch (e) {
//           done(e);
//         }
//       });
//     });
//   };

//   // We send the dataset to the cluster
//   let cntr = 0;
//   dataset.forEach((o) => {
//     let key = Object.keys(o)[0];
//     let value = o[key];
//     distribution.crawler.store.put(value, key, (e, v) => {
//       cntr++;
//       // Once we are done, run the map reduce
//       if (cntr === dataset.length) {
//         doMapReduce();
//       }
//     });
//   });
// });

test('hang up by execution time', (done) => {
  let m1 = (key, value) => {
    let words = value.split(/(\s+)/).filter((e) => e !== ' ');
    let out = {};
    for (let i = 0; i < 15000000; i++) {
      out[words[1]] = parseInt(words[3]);
    }
    return out;
  };

  let r1 = (key, values) => {
    let out = {};
    for (let i = 0; i < 15000000; i++) {
      out[key] = values.reduce((a, b) => Math.max(a, b), -Infinity);
    }
    return out;
  };

  let dataset = [
    {'000': '006701199099999 1950 0515070049999999N9 +0000 1+9999'},
    {'106': '004301199099999 1950 0515120049999999N9 +0022 1+9999'},
    {'212': '004301199099999 1950 0515180049999999N9 -0011 1+9999'},
    {'318': '004301265099999 1949 0324120040500001N9 +0111 1+9999'},
    {'424': '004301265099999 1949 0324180040500001N9 +0078 1+9999'},
  ];

  let expected = [{'1950': 22}, {'1949': 111}];

  /* Sanity check: map and reduce locally */
  sanityCheck(m1, r1, dataset, expected, done);

  /* Now we do the same thing but on the cluster */
  const doMapReduce = (cb) => {
    distribution.ncdc.store.get(null, (e, v) => {
      try {
        expect(v.length).toBe(dataset.length);
      } catch (e) {
        done(e);
      }


      distribution.ncdc.mr.exec({keys: v, map: m1, reduce: r1, memory: true},
          (e, v) => {
            try {
              expect(v).toEqual(expect.arrayContaining(expected));
              done();
            } catch (e) {
              done(e);
            }
          });
    });
  };

  let cntr = 0;

  // We send the dataset to the cluster
  dataset.forEach((o) => {
    let key = Object.keys(o)[0];
    let value = o[key];
    distribution.ncdc.store.put(value, key, (e, v) => {
      cntr++;
      // Once we are done, run the map reduce
      if (cntr === dataset.length) {
        doMapReduce();
      }
    });
  });
}, 60000);


// test('hang up by sending too many data', (done) => {
//   let m1 = (key, value) => {
//     let words = value.split(/(\s+)/).filter((e) => e !== ' ');
//     let out = {};
//     out[words[1]] = parseInt(words[3]);
//     return out;
//   };

//   let r1 = (key, values) => {
//     let out = {};
//     out[key] = values.reduce((a, b) => Math.max(a, b), -Infinity);
//     return out;
//   };

//   let oneDataset = [
//     {'000': '006701199099999 1950 0515070049999999N9 +0000 1+9999'},
//     {'106': '004301199099999 1950 0515120049999999N9 +0022 1+9999'},
//     {'212': '004301199099999 1950 0515180049999999N9 -0011 1+9999'},
//     {'318': '004301265099999 1949 0324120040500001N9 +0111 1+9999'},
//     {'424': '004301265099999 1949 0324180040500001N9 +0078 1+9999'},
//   ];

//   const dataset = [];
//   for (let i = 0; i < 3000; i++) {
//     dataset.push(...oneDataset);
//   }

//   let expected = [{'1950': 22}, {'1949': 111}];

//   /* Sanity check: map and reduce locally */
//   sanityCheck(m1, r1, dataset, expected, done);

//   /* Now we do the same thing but on the cluster */
//   const doMapReduce = (cb) => {
//     distribution.ncdc.store.get(null, (e, v) => {
//       try {
//         expect(v.length).toBe(oneDataset.length);
//       } catch (e) {
//         done(e);
//       }

//       const keys = dataset.map((pair) => Object.keys(pair)[0]);
//       distribution.ncdc.mr.exec({keys: keys, map: m1, reduce: r1, memory: true},
//           (e, v) => {
//             try {
//               expect(v).toEqual(expect.arrayContaining(expected));
//               done();
//             } catch (e) {
//               done(e);
//             }
//           });
//     });
//   };

//   let cntr = 0;

//   // We send the dataset to the cluster
//   dataset.forEach((o) => {
//     let key = Object.keys(o)[0];
//     let value = o[key];
//     distribution.ncdc.store.put(value, key, (e, v) => {
//       cntr++;
//       // Once we are done, run the map reduce
//       if (cntr === dataset.length) {
//         doMapReduce();
//       }
//     });
//   });
// }, 60000);
