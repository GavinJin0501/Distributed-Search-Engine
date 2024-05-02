#!/usr/bin/env node
global.nodeConfig = {ip: '127.0.0.1', port: process.env.PORT || 7110 };
const distribution = require('../distribution');
var localServer;
const gid = 'crawler';
distribution.local.store.put('', {key: 'placeholder', gid: 'crawler'}, (err, res) => {
  distribution.node.start((server) => {
        localServer = server;
        console.log('Node started successfully:', server);
        distribution.local.store.put("",{gid: gid, key: 'placeholder'}, (e, v) => {
            console.log('Put:', e, v);
        });
});
});