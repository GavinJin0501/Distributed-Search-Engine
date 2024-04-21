/**
 * Crawler Workflow
 *
 * @param {Object} config
 */
function CrawlerWorkflow(config) {
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
    set.delete(url);
    return set;
  };

  const fetchPage = () => fetch(url)
      .then((response) => {
        if (!response.ok) {
          console.log('Network response was not ok:', response);
          resolve({});
          return;
        }
        return response.text();
      })
      .then((html) => {
        return new Promise((resolve, reject) => {
          let newUrls = url.includes('page=') ? extractUrls(html) : [];
          // special logic for usenix
          newUrls = [...newUrls].filter((url) =>
            url.startsWith('https://www.usenix.org/conference/') &&
            url.split('/').length > 5,
          );
          const key = 'page-' + urlHash.slice(4);
          const data = [url, global.convert(html.trim()).trim()];
          const metaKey = {gid: this.gid, key};

          // for sanbox 3
          global.distribution.local.store.put(data, metaKey, (err, res) => {
            if (newUrls.length === 0) {
              resolve({[url]: [{}]});
            } else {
              resolve({[url]: [...newUrls]});
            }
          });
        });
      })
      .catch((error) => {
        console.log(`crawler fails to fetch ${url}:`, error.message);
        return {url: [url]};
      });

  return global.limiter.schedule(fetchPage);
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
