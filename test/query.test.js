
global.nodeConfig = {ip: '127.0.0.1', port: 7070};
const distribution = require('../distribution');
const id = distribution.util.id;
const queryWorkflow = require('../distribution/workflow/query');

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


test('crawler', (done) => {
  const index1 =
      'fungi insect firefli | https://cs.brown.edu/courses/csci1380/sandbox/1/level_1c/fact_6/index.html 1 https://cs.brown.edu/courses/csci1380/sandbox/1/level_1c/fact_6/index.html 1\n' +
      'fungi insect | https://cs.brown.edu/courses/csci1380/sandbox/1/level_1c/fact_6/index.html 1 https://cs.brown.edu/courses/csci1380/sandbox/1/level_1c/fact_6/index.html 1\n' +
      'fungi speci reason | https://cs.brown.edu/courses/csci1380/sandbox/1/level_1c/fact_6/index.html 1 https://cs.brown.edu/courses/csci1380/sandbox/1/level_1c/fact_6/index.html 1\n' +
      'fungi speci | https://cs.brown.edu/courses/csci1380/sandbox/1/level_1c/fact_6/index.html 1 https://cs.brown.edu/courses/csci1380/sandbox/1/level_1c/fact_6/index.html 1';
  const index2 =
      'geographi cultur travel | https://cs.brown.edu/courses/csci1380/sandbox/1/level_1b/fact_4/index.html 1 https://cs.brown.edu/courses/csci1380/sandbox/1/level_1b/fact_4/index.html 1\n' +
      'geographi cultur | https://cs.brown.edu/courses/csci1380/sandbox/1/level_1b/fact_4/index.html 1 https://cs.brown.edu/courses/csci1380/sandbox/1/level_1b/fact_4/index.html 1\n' +
      'geographi | https://cs.brown.edu/courses/csci1380/sandbox/1/level_1b/fact_4/index.html 1 https://cs.brown.edu/courses/csci1380/sandbox/1/level_1b/fact_4/index.html 1\n'+
      'fungi | https://cs.brown.edu/courses/csci1380/sandbox/1/level_1c/fact_6/index.html 2 https://cs.brown.edu/courses/csci1380/sandbox/1/level_1c/fact_6/index.html 2';
  const index3 =
      'global knowledg histori | https://cs.brown.edu/courses/csci1380/sandbox/1/level_1c/fact_5/index.html 1 https://cs.brown.edu/courses/csci1380/sandbox/1/level_1c/fact_5/index.html 1\n' +
      'global knowledg | https://cs.brown.edu/courses/csci1380/sandbox/1/level_1c/fact_5/index.html 1 https://cs.brown.edu/courses/csci1380/sandbox/1/level_1c/fact_5/index.html 1\n' +
      'global | https://cs.brown.edu/courses/csci1380/sandbox/1/level_1c/fact_5/index.html 1 https://cs.brown.edu/courses/csci1380/sandbox/1/level_1c/fact_5/index.html 1';

  const index4 =
      'glow wave beach | https://cs.brown.edu/courses/csci1380/sandbox/1/level_1c/fact_6/index.html 1 https://cs.brown.edu/courses/csci1380/sandbox/1/level_1c/fact_6/index.html 1\n' +
      'glow wave | https://cs.brown.edu/courses/csci1380/sandbox/1/level_1c/fact_6/index.html 1 https://cs.brown.edu/courses/csci1380/sandbox/1/level_1c/fact_6/index.html 1\n' +
      'glow | https://cs.brown.edu/courses/csci1380/sandbox/1/level_1c/fact_6/index.html 1 https://cs.brown.edu/courses/csci1380/sandbox/1/level_1c/fact_6/index.html 1';

  const index5 =
      'gogh claud monet | https://cs.brown.edu/courses/csci1380/sandbox/1/level_1b/fact_3/index.html 1 https://cs.brown.edu/courses/csci1380/sandbox/1/level_1b/fact_3/index.html 1\n' +
      'gogh claud | https://cs.brown.edu/courses/csci1380/sandbox/1/level_1b/fact_3/index.html 1 https://cs.brown.edu/courses/csci1380/sandbox/1/level_1b/fact_3/index.html 1';
  const dataset = [
    {'index1': index1},
    {'index2': index2},
    {'index3': index3},
    {'index4': index4},
    {'index5': index5},
  ];

  const doMapReduce = (cb) => {
    distribution.crawler.store.get(null, (e, v) => {
      try {
        expect(v.length).toBe(dataset.length);
      } catch (e) {
        done(e);
      }

      const config = {
        gid: 'crawler',
        indexes: v,
      };
      const crawler = queryWorkflow(config);
      distribution.crawler.mem.put('fungi', 'query', (e, v) => {
        distribution.crawler.mr.exec(crawler, (e, v) => {
          try {
            console.log('Output from exec:', v);
            const expectedArray = [
              'fungi | https://cs.brown.edu/courses/csci1380/sandbox/1/level_1c/fact_6/index.html 2 https://cs.brown.edu/courses/csci1380/sandbox/1/level_1c/fact_6/index.html 2',
              'fungi insect firefli | https://cs.brown.edu/courses/csci1380/sandbox/1/level_1c/fact_6/index.html 1 https://cs.brown.edu/courses/csci1380/sandbox/1/level_1c/fact_6/index.html 1',
              'fungi insect | https://cs.brown.edu/courses/csci1380/sandbox/1/level_1c/fact_6/index.html 1 https://cs.brown.edu/courses/csci1380/sandbox/1/level_1c/fact_6/index.html 1',
              'fungi speci reason | https://cs.brown.edu/courses/csci1380/sandbox/1/level_1c/fact_6/index.html 1 https://cs.brown.edu/courses/csci1380/sandbox/1/level_1c/fact_6/index.html 1',
              'fungi speci | https://cs.brown.edu/courses/csci1380/sandbox/1/level_1c/fact_6/index.html 1 https://cs.brown.edu/courses/csci1380/sandbox/1/level_1c/fact_6/index.html 1',
            ];
            // v will only have one value.
            const result = v[0]['fungi'];
            expect(result).toEqual(expect.arrayContaining(expectedArray));
            done();
          } catch (e) {
            done(e);
          }
        });
      });
    });
  };


  // We send the dataset to the cluster
  let cntr = 0;
  dataset.forEach((o) => {
    let key = Object.keys(o)[0];
    let value = o[key];
    distribution.crawler.store.put(value, key, (e, v) => {
      cntr++;
      // Once we are done, run the map reduce
      if (cntr === dataset.length) {
        doMapReduce();
      }
    });
  });
});

