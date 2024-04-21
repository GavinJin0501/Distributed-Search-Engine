/**
 * Crawler Workflow
 *
 * @param {Object} config
 */
function DistributedCrawlerWorkflow(config) {
  this.keys = config.urls || [];
  this.memory = config.memory || true;
  this.keepKeysOrder = config.keepKeysOrder || true;
  this.domain = config.domain || '';
}

/**
 * Customized shuffle function for distributed crawler
 * After its map, mr.afterMapList should only has
 * one key-value pair: {sid: [visitedUrls, [failed urls]]}
 *
 * @param {Function} cb
 */
DistributedCrawlerWorkflow.prototype.shuffle = function(cb) {
  // tell the coordinator that this node can start to shuffle
  cb(null, 'Ready to start shuffle...');
  const distribution = global.distribution;
  const local = distribution.local;
  const info = Object.values(this.afterMapList[0])[0];
  const sid = distribution.util.id.getSID(global.nodeConfig);

  // {sid: [visitedUrls, newUrls]}
  this.preReduceMap = {[sid]: [info[0]]};
  const failedUrls = info[1];
  this.afterMapList = []; // free up space

  // check the in-mem newUrls locally and use urlsCrawled to remove dup
  local.mem.get({gid: this.gid, key: 'newUrls'}, (err, res) => {
    const newUrls = (err) ? [] : res;
    local.store.get({gid: this.gid, key: 'urlsCrawled'}, (err, res) => {
      const crawledUrls = new Set(res);
      const finalNewUrls = [...failedUrls, ...newUrls]
          .filter((url) => !crawledUrls.has(url));

      // set the number of new urls
      this.preReduceMap[sid].push(finalNewUrls.length);
      // use the final new urls to replace urlsToCrawl
      const metaKey = {gid: this.gid, key: 'urlsToCrawl'};
      local.store.put(finalNewUrls, metaKey, (err, res) => {
        local.mem.del({gid: this.gid, key: 'newUrls'}, (err, res) => {
          this.doNotify('shufflePhase', null, (err, res) => {});
        });
      });
    });
  });
};

/**
 * Map function for crawler
 *
 * @param {String} fileName
 * @param {String} urlsToCrawl url
 * @return {Object} {nodeSID: [visitedUrlsNum, [failed urls]]}
 */
DistributedCrawlerWorkflow.prototype.map = function(fileName, urlsToCrawl) {
  const urlsCrawled = [];
  const urlsFailed = [];
  const sid = global.distribution.util.id.getSID(global.nodeConfig);

  const extractUrls = (baseURL, html) => {
    const $ = global.cheerio.load(html);
    const links = $('a');
    const set = new Set(
        links.map((index, element) => {
          const currPath = $(element).attr('href');
          if (!currPath || currPath.startsWith('?')) {
            return baseURL;
          }
          const finalUrl = new URL(currPath, baseURL).href;
          if (this.domain && !finalUrl.startsWith(this.domain)) {
            return baseURL;
          }
          return finalUrl;
        }).get(),
    );
    set.delete(baseURL);
    return set;
  };

  const doComplete = (success, url, resolve) => {
    if (success) {
      urlsCrawled.push(url);
    } else {
      urlsFailed.push(url);
    }

    // all urls have been visited once
    if (urlsFailed.length + urlsCrawled.length === urlsToCrawl.length) {
      if (urlsCrawled.length === 0) {
        resolve({[sid]: [0, urlsFailed]});
        return;
      }

      // Persist the urlsCrawled
      global.distribution.local.store.append(
          urlsCrawled, {gid: this.gid}, 'urlsCrawled', (err, res) => {
            if (err) {
              resolve({[sid]: [0, urlsFailed]});
            } else {
              resolve({[sid]: [urlsCrawled.length, urlsFailed]});
            }
          });
    }
  };

  return global.limiter.schedule(() => new Promise((resolve, reject) => {
    // each worker node crwal the urls it needs to
    if (urlsToCrawl.length === 0) {
      resolve({[sid]: [0, []]});
      return;
    }

    for (const url of urlsToCrawl) {
      let baseURL = url;
      if (baseURL.endsWith('/')) {
        baseURL = baseURL.slice(0, -1);
      }
      if (baseURL.endsWith('.html') || baseURL.endsWith('.txt')) {
        baseURL += '/../';
      } else {
        baseURL += '/';
      }


      fetch(url)
          .then((response) => {
            if (!response.ok) {
              throw new Error('Network response was not ok:', response);
            }
            return response.text();
          })
          .then((html) => {
            let newUrls = url.includes('page=') ?
                          extractUrls(baseURL, html) :[];
            newUrls = [...newUrls].filter((url) =>
              url.startsWith('https://www.usenix.org/conference/') &&
              url.split('/').length > 5,
            );

            const urlHash = global.distribution.util.id.getID(url);
            const key = 'page-' + urlHash;
            const data = [url, global.convert(html.trim()).trim()];
            const metaKey = {gid: this.gid, key};

            global.distribution.local.store.put(data, metaKey, (err, res) => {
              if (newUrls.length === 0) {
                doComplete(true, url, resolve);
              } else {
                let newUrlsAppended = 0;
                newUrls.forEach((newUrl) => {
                  const id = global.distribution.util.id.getID(newUrl);
                  global.distribution[this.gid].mem.append(newUrl,
                      'url-' + id, 'newUrls', (err, res) => {
                        newUrlsAppended++;
                        if (newUrlsAppended === newUrls.length) {
                          doComplete(true, url, resolve);
                        }
                      });
                });
              }
            });
          })
          .catch((error) => {
            console.log('error inside fetch:', error.message, url);
            doComplete(false, url, resolve);
          });
    }
  }));
};

/**
 *
 * @param {String} key url
 * @param {String} value list of new urls
 * @return {Object} {url: [new urls]}
 */
DistributedCrawlerWorkflow.prototype.reduce = function(key, value) {
  return {[key]: value};
};

const disCrawlerWorkflow = (config) => new DistributedCrawlerWorkflow(config);
module.exports = disCrawlerWorkflow;
