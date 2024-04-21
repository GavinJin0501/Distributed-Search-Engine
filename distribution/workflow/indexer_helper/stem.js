#!/usr/bin/env node

// use Porter Stemmer to stem individual terms in a streaming fashion

var readline = require('readline');
var natural = require('natural');

var stemmedOutput = '';
const emptyLineRegex = /^\s*$/;
var rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
  terminal: false,
});

rl.on('line', function(line) {
  // TODO some code
  // console.log(natural.PorterStemmer.stem(line));
  if (!emptyLineRegex.test(line)) {
    if (stemmedOutput==='') {
      stemmedOutput=natural.PorterStemmer.stem(line);
    } else {
      stemmedOutput=stemmedOutput + '\n' + natural.PorterStemmer.stem(line);
    }
  }
});

rl.on('close', () => {
  console.log(stemmedOutput);
});

