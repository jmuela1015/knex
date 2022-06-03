function generateCombinedName(logger, postfix, name, subNames) {
  const crypto = require('crypto');
  if (!Array.isArray(subNames)) subNames = subNames ? [subNames] : [];
  const table = name.replace(/\.|-/g, '_');
  const subNamesPart = subNames.join('_');
  let result = `${table}_${
    subNamesPart.length ? subNamesPart + '_' : ''
  }${postfix}`.toLowerCase();
  return result;
}

function createAutoIncrementTrigger(logger, tableName) {
  const triggerName = generateCombinedName(
    logger,
    'autoinc_trg',
    tableName
  );
  const sequenceName = generateCombinedName(logger, 'seq', tableName);
  return {
    sequence: sequenceName,
    sql: `CREATE SEQUENCE ${sequenceName} AS INT START WITH 1 INCREMENT BY 1;`
  };
};

// helper function for pushAdditional in increments() and bigincrements()
function createAutoIncrementTriggerAndSequence(columnCompiler) {
  let sequenceName = '';
  // TODO Add warning that sequence etc is created
  columnCompiler.pushAdditional(function () {
    const tableName = this.tableCompiler.tableNameRaw;
    const sequence = createAutoIncrementTrigger(
      this.client.logger,
      tableName
    );
    const createTriggerSQL = sequence.sql;
    sequenceName = sequence.sequence;
    this.pushQuery(createTriggerSQL);
  });
  return sequenceName;
}

module.exports = {
  createAutoIncrementTriggerAndSequence,
};
