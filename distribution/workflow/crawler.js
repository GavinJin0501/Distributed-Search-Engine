/**
 * Crawler Workflow
 *
 * @param {Object} config
 */
function CrawlerWorkflow(config) {
  this.gid = config.gid || 'all';
  this.keys = config.urls || [];
  this.memory = config.memory || true;
}

/**
 * Map function for crawler
 *
 * @param {String} urlHash
 * @param {String} url url
 * @return {Object} {url: [new urls extracted from url]}
 */
CrawlerWorkflow.prototype.map = function(urlHash, url) {
  let baseURL = url;
  if (baseURL.endsWith('/')) {
    baseURL = baseURL.slice(0, -1);
  }
  if (baseURL.endsWith('.html') || baseURL.endsWith('.txt')) {
    baseURL += '/../';
  } else {
    baseURL += '/';
  }

  const extractUrls = (html) => {
    const $ = global.cheerio.load(html);
    const links = $('a');
    const set = new Set(
        links.map((index, element) => {
          const currPath = $(element).attr('href');
          if (!currPath || currPath.startsWith('?')) {
            return baseURL;
          }
          return new URL(currPath, baseURL).href;
        }).get(),
    );
    set.delete(baseURL);
    return set;
  };

  return fetch(url)
      .then((response) => {
        if (!response.ok) {
          throw new Error('Network response was not ok:', response);
        }
        return response.text();
      })
      .then((html) => {
        return new Promise((resolve, reject) => {
          const newUrls = extractUrls(html);
          const key = 'page-' + urlHash.slice(4);
          const data = [url, html];

          // // for sanbox 3
          // global.distribution[this.gid].store.put(data, key, (err, res) => {
          //   resolve({[url]: [...newUrls]});
          // });

          if (url.endsWith('.txt')) {
            global.distribution[this.gid].store.put(data, key, (err, res) => {
              resolve({[url]: [...newUrls]});
            });
          } else {
            resolve({[url]: [...newUrls]});
          }
        });
      })
      .catch((error) => {
        console.log(`crawler fetch ${url}:`, error);
        return {};
      });
};

/**
 *
 * @param {String} key url
 * @param {String} value list of new urls
 * @return {Object} {url: [new urls]}
 */
CrawlerWorkflow.prototype.reduce = function(key, value) {
  return {[key]: value};
};

const crawlerWorkflow = (config) => new CrawlerWorkflow(config);
module.exports = crawlerWorkflow;
