#!/usr/bin/env node

global.nodeConfig = {ip: '127.0.0.1', port: 7070};
const distribution = require('../../distribution');
const id = distribution.util.id;
const groupsTemplate = require('../all/groups');
const indexerWorkflow = require('./indexer');
const crawlerWorkflow = require('./crawler');
if (require.main === module) {
  console.log('started');
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

  const beforeAll = function(callback) {
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
              callback(null, 'done');
            });
          });
        });
      });
    });
  };


  const afterAll = function(callback) {
    let remote = {service: 'status', method: 'stop'};
    remote.node = n1;
    distribution.local.comm.send([], remote, (e, v) => {
      remote.node = n2;
      distribution.local.comm.send([], remote, (e, v) => {
        remote.node = n3;
        distribution.local.comm.send([], remote, (e, v) => {
          localServer.close();
          callback(null, 'done');
        });
      });
    });
  };

  beforeAll((e, v)=>{
    if (v) {
      const config = {
        gid: 'crawler',
      };
      const indexer = indexerWorkflow(config);
      distribution.crawler.mr.exec(indexer, (e, v)=>{
        if (v) {
          afterAll((e, v)=>{
            console.log('DONE');
          });
        }
      });
    }
  });
}

