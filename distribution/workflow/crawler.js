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
 * @param {String} key
 * @param {String} value url
 * @return {Object} {url: [new urls extracted from url]}
 */
CrawlerWorkflow.prototype.map = function(key, value) {
  let baseURL = value;
  if (baseURL.endsWith('.html')) {
    baseURL += '/../';
  } else {
    baseURL += '/';
  }

  return fetch(value)
      .then((response) => {
        if (!response.ok) {
          throw new Error('Network response was not ok');
        }
        return response.text();
      })
      .then((html) => {
        return new Promise((resolve, reject) => {
          const key = 'page-' + btoa(value);
          global.distribution[this.gid].store.put(html, key, (err, res) => {
            const dom = new global.JSDOM(html);
            const aTags = dom.window.document.getElementsByTagName('a');
            const set = new Set();
            for (let i = 0; i < aTags.length; i++) {
              const currPath = aTags[i].href;
              const url = new global.URL(currPath, baseURL);

              if (!set.has(url.href)) {
                set.add(url.href);
              }
            }
            resolve({[value]: [...set]});
          });
        });
      })
      .catch((error) => {
        // console.log('hahaha:', error);
        return {};
      });
};

/**
 *
 * @param {String} key url
 * @param {String} value web content
 * @return {Object} {url: url-html-content}
 */
CrawlerWorkflow.prototype.reduce = function(key, value) {
  return {[key]: value};
};

const crawlerWorkflow = (config) => new CrawlerWorkflow(config);
module.exports = crawlerWorkflow;
