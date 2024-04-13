/**
 * Crawler Workflow
 *
 * @param {Object} config
 */
function CrawlerWorkflow(config) {
  this.gid = config.gid || 'all';
  this.keys = config.urls || [];
  this.memory = config.memory || true;
  this.persistAfterReduce = config.persistAfterReduce || true;
}

/**
 * Map function for crawler
 *
 * @param {String} key
 * @param {String} value url
 * @return {Object} {url: url-html-content}
 */
CrawlerWorkflow.prototype.map = function(key, value) {
  return fetch(value)
      .then((response) => {
        if (!response.ok) {
          throw new Error('Network response was not ok');
        }
        return response.text();
      })
      .then((data) => {
        return {[value]: data};
      })
      .catch((error) => {
        console.log(error);
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
  return {['page-' + btoa(key)]: value};
};

const crawlerWorkflow = (config) => new CrawlerWorkflow(config);
module.exports = crawlerWorkflow;
