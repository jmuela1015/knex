const _debugQuery = require('debug')('knex:query');
const debugBindings = require('debug')('knex:bindings');
const debugQuery = (sql, txId) => _debugQuery(sql.replace(/%/g, '%%'), txId);
const { isString } = require('../../util/is');

function formatQuery(sql, bindings, timeZone, client) {
  bindings = bindings == null ? [] : [].concat(bindings);
  let index = 0;
  return sql.replace(/\\?\?/g, (match) => {
    if (match === '\\?') {
      return '?';
    }
    if (index === bindings.length) {
      return match;
    }
    const value = bindings[index++];
    return client._escapeBinding(value, { timeZone });
  });
}

const debug2 = require('debug')('metrik'); // MTRK
const dumpObject = (prefix, obj, indent) => {
  const indentStr = new Array(indent + 1).join(' ');
  if (!obj) {
    debug2(prefix + ' ' + indentStr + 'NULL/UNDEFINED');
    return;
  }
  Object.keys(obj).forEach(function (key) {
    const value = obj[key];

    if (typeof value === 'object') {
      debug2(prefix + ' ' + indentStr + key);
      dumpObject(prefix, value, indent + 2);
    } else {
      debug2(prefix + ' ' + indentStr + key + '=' + value);
    }
  });
}

function enrichQueryObject(connection, queryParam, client) {
  // dumpObject('MTRK303', queryParam, 0);
  // if (queryParam[1] && str(queryParam[1].includes(','))) dumpObject('MTRK304', queryParam, 0);
  const queryObject = isString(queryParam) ? { sql: queryParam } : queryParam;

  queryObject.bindings = client.prepBindings(queryObject.bindings);
  queryObject.sql = client.positionBindings(queryObject.sql);

  const { __knexUid, __knexTxId } = connection;

  client.emit('query', Object.assign({ __knexUid, __knexTxId }, queryObject));
  debugQuery(queryObject.sql, __knexTxId);
  debugBindings('MTRK300');
  debugBindings('MTRK301 ' + queryObject.sql);
  debugBindings('MTRK302 ' + queryObject.bindings);
  debugBindings(queryObject.bindings, __knexTxId);

  return queryObject;
}

function executeQuery(connection, queryObject, client) {
  return client._query(connection, queryObject).catch((err) => {
    err.message =
      formatQuery(queryObject.sql, queryObject.bindings, undefined, client) +
      ' - ' +
      err.message;
    client.emit(
      'query-error',
      err,
      Object.assign(
        { __knexUid: connection.__knexUid, __knexTxId: connection.__knexUid },
        queryObject
      )
    );
    throw err;
  });
}

module.exports = {
  enrichQueryObject,
  executeQuery,
  formatQuery,
};
