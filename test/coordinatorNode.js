global.nodeConfig = {ip: '127.0.0.1', port: 7070};
const distribution = require('../distribution');
const id = distribution.util.id;
const comm = distribution.local.comm;
const crawlerWorkflow = require('../distribution/workflow/crawler');
const indexerWorkflow = require('../distribution/workflow/indexer');
const disCrawlerWorkflow =
      require('../distribution/workflow/distributed_crawler');

const groupsTemplate = require('../distribution/all/groups');
const crwalerGroup = {};
const n1 = {ip: '127.0.0.1', port: 7110};
  const n2 = {ip: '127.0.0.1', port: 7111};
  const n3 = {ip: '127.0.0.1', port: 7112};
  crwalerGroup[id.getSID(n1)] = n1;
  crwalerGroup[id.getSID(n2)] = n2;
  crwalerGroup[id.getSID(n3)] = n3;

  distribution.node.start((server) => {
    localServer = server;
    const crawlConfig = {gid: 'crawler'};
    // const crawlConfig = {gid: 'crawler', hash: id.consistentHash};
    groupsTemplate(crawlConfig).put(crawlConfig, crwalerGroup, (e, v) => {
      console.log('Group created:', e, v);
      
    });
  });