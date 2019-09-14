/**
 * Iterate the dataSpec config finding all of the foreign key queries and
 * executing them.
 *
 * For performance, the queries are de-duplicated
 */
module.exports = function resolveQueryObjects(config, knexInst) {
  const pendingQueries = [];
  const knex = knexInst;

  const pMap = {};

  Object.values(config).forEach(entries => {
    Array.from(entries).forEach(entry => {
      var promises = Object.entries(entry).map(([colName, colValue]) => {
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
      pendingQueries.push.apply(pendingQueries, promises);
    });
  });
  return pendingQueries;
};

function isQueryObject(value) {
  return value && !!value.from && !!value.where;
}
