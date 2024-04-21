#!/usr/bin/env node

// merge two files---the incoming 1-page index and the global index (on disk)
// the details of the global index can be seen in the test cases.

const fs = require('fs');
const readline = require('readline');

const rl = readline.createInterface({
  input: process.stdin,
});

function readFileAsync(filePath) {
  return new Promise((resolve, reject) => {
    fs.readFile(filePath, 'ascii', (err, data) => {
      if (err) {
        reject(err);
        return;
      }
      resolve(data);
    });
  });
}

// TODO some code here
var input = [];
rl.on('line', (line) => {
  // TODO some code here
  input.push(line);
});

rl.on('close', () => {
  mergeIndices();
});

const mergeIndices = () => {
  // TODO some code here
  // "check | 3 | https://cs.brown.edu/courses/csci1380/sandbox/1 \n check | 2 | https://cs.brown.edu/courses/csci1380/sandbox/1/level_1a/index.html";

  // process.stdout.write("input is : "+ input +"\n");
  // process.stdout.write("file name read from input is : "+process.argv[2]);
  /* fs.readFile(process.argv[2], 'ascii', (err, data) => {
    if (err) {
      console.error('Error reading the file:', err);
      return;
    }
    if(data.trim().length !== 0){
      const lines = data.split('\n');
      input.push(...lines);
      process.stdout.write("File read data is being used now : "+lines);

    }
  });*/

  // fs.close();

  // Initialize an empty map
  const resultMap = {};

  // Iterate over each line
  input.forEach((line) => {
    if (line!==undefined && line.trim()!=='') {
      // Split each line by the pipe character (|) and trim whitespace
      const parts = line.split('|').map((part) => part.trim());
      // process.stdout.write("input parts are 1>"+parts[0]+"2>"+parts[1]+"\n");
      const key = parts[0];
      const value = `${parts[2]} ${parts[1]}`;
      // process.stdout.write("values from input are : {"+key+","+value+"}\n");
      if (!resultMap[key]) {
        resultMap[key] = [];
      }
      resultMap[key].push(value);
    }
  });


  readFileAsync(process.argv[2])
      .then((data) => {
        if (data !== undefined && data.trim().length !== 0) {
          const fileData = data.split('\n');
          // console.log("File read data is being used now:", lines);
          fileData.forEach((line) => {
            // Split each line by the pipe character (|) and trim whitespace
            const parts = line.split('|').map((part) => part.trim());
            // process.stdout.write("parts are 1>"+parts[0]+"2>"+parts[1]+"\n");
            const key = parts[0];
            // eslint-disable-next-line max-len
            // process.stdout.write("values from file are : {"+key+","+parts[1]+"}\n");
            if (parts[0]!==undefined && parts[1]!==undefined) {
              // eslint-disable-next-line max-len
              const pairs = parts[1].trim().split(/\s+/).reduce((acc, curr, index, array) => {
                if (index % 2 === 0) {
                  // eslint-disable-next-line max-len
                  // process.stdout.write("cur is : "+curr + "and index is : "+index+ "array is : "+array+"\n");
                  const pair = curr + ' ' + array[index + 1];
                  acc.push(pair);
                }
                return acc;
              }, []);

              pairs.forEach((ele) =>{
                const eleParts = ele.split(' ').map((part) => part.trim());

                if (!resultMap[key]) {
                  resultMap[key] = [];
                }
                const val = `${eleParts[0]} ${eleParts[1]}`;
                resultMap[key].push(val);
                // eslint-disable-next-line max-len
                // process.stdout.write("Pairs_1 from file are : {"+eleParts[0]+"\n");
                // eslint-disable-next-line max-len
                // process.stdout.write("Pairs_2 from file are : {"+eleParts[1]+"\n");
              });
            }


            // const value = `${parts[1]}`;
          });
        }
      })
      .catch((err) => {
        console.error('Error reading the file:', err);
      }).finally(() => {
        // eslint-disable-next-line max-len
        // Sort the values based on the value of `${parts[1]}` in descending order
        for (const key in resultMap) {
          // eslint-disable-next-line max-len
          // process.stdout.write("resultmap values are is "+ resultMap[key]+ "\n");
          if (resultMap.hasOwnProperty(key)) {
            resultMap[key].sort((a, b) => {
              const numA = parseInt(a.trim().split(' ').slice(-1)[0]);
              const numB = parseInt(b.trim().split(' ').slice(-1)[0]);
              // process.stdout.write("numA : "+numA + "numB : "+numB);
              return numB - numA;
            });
          }
        }
        for (const [key, values] of Object.entries(resultMap)) {
          const output = `${key} | ${values.join(' ')}`;
          process.stdout.write(output+'\n');
        }
      });
};

