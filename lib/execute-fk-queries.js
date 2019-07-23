var _ = require('lodash');
var bluebird = require('bluebird');
var util = require('./util');
var knex;

/**
 * Iterate the dataSpec config finding all of the foreign key queries and
 * executing them.
 *
 * For performance, the queries are de-duplicated
 */
module.exports = function resolveQueryObjects(config, knexInst) {
  var pendingQueries = [];
  knex = knexInst;

  const pMap = {};

  _.forEach(config, function handleTable(entries) {
    util.asArray(entries).forEach(function (entry) {
      var promises = _.map(entry, function (colValue, colName) {
        if (!isQueryObject(colValue)) return null;
        const key = JSON.stringify(colValue);
        function handleResult(result) {
          if (result.length > 1) {
            throw new Error(key + ' matches >1 possible FK!');
          } else {
            entry[colName] = result[0][colValue.column || 'id'];
          }
        }
        const p = pMap[key];
        if (p) {
          return p.then(handleResult);
        }
        const newP = knex(colValue.from).where(colValue.where).then(r => r);
        pMap[key] = newP;
        return newP.then(handleResult);
      });
      pendingQueries = pendingQueries.concat(promises);
    });
  });
  return pendingQueries;
};

function isQueryObject(value) {
  return _(value).has('from', 'where');
}
