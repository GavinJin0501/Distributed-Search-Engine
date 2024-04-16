/**
 * Query Workflow
 *
 * @param {Object} config
 */
function QueryWorkflow(config) {
  this.gid = config.gid || 'all';
  this.keys = config.indexes || [];
  this.memory = config.memory || true;
  this.persistAfterReduce = config.persistAfterReduce || true;
}
/**
 * Map function for querying
 *
 * @param {String} key
 * @param {String} value url
 * @return {Object} {url: url-html-content}
 */
QueryWorkflow.prototype.map = function(key, value) {
  return new Promise((resolve, reject) => {
    // eslint-disable-next-line max-len
    // Remember to keep the group id the same. Dynamic group id will require additional work
    global.distribution.crawler.mem.get('query', (error, result) => {
      if (error) {
        reject(error);
      } else {
        const out = {};
        const lines = value.split('\n');
        const searchRegex = new RegExp(`\\b${result}\\b`, 'gi');
        const matchingLines = lines.filter((line) => line.match(searchRegex));
        out[result] = matchingLines.join('\n');
        resolve(out);
      }
    });
  });
};

/**
 *
 * @param {String} key url
 * @param {String} value web content
 * @return {Object} {url: url-html-content}
 */
QueryWorkflow.prototype.reduce = function(key, value) {
  return {[key]: value.filter(Boolean).join('\n').split('\n')};
};

const queryWorkflow = (config) => new QueryWorkflow(config);
module.exports = queryWorkflow;
