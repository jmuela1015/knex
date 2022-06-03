// MSSQL Column Compiler
// -------
const debug = require('debug')('metrik'); // MTRK

const ColumnCompiler = require('../../../schema/columncompiler');
const {
  createAutoIncrementTriggerAndSequence,
} = require('./internal/incrementUtils');
const { toNumber } = require('../../../util/helpers');
const { formatDefault } = require('../../../formatter/formatterUtils');

class ColumnCompiler_MSSQL extends ColumnCompiler {
  constructor(client, tableCompiler, columnBuilder) {
    super(client, tableCompiler, columnBuilder);
    this.modifiers = ['nullable', 'defaultTo', 'first', 'after', 'comment'];
  }

  // Types
  // ------

  increments({ primaryKey = true } = {}) {
    // MTRK
    // if (this.columnBuilder._method === 'alter') {
      const sequenceName = createAutoIncrementTriggerAndSequence(this);
      const constraintName =
        `${this.tableCompiler.tableNameRaw}_${this.getColumnName()}_default_seq`.toLowerCase();
      const seq = `ALTER TABLE ${this.tableCompiler.tableName()} ADD CONSTRAINT ${this.formatter.wrap(constraintName)} DEFAULT (NEXT VALUE FOR ${sequenceName}) FOR ${this.formatter.wrap(this.getColumnName())}`; // MTRK
      // debug('MTRK SEQ=' + seq);

      this.pushAdditional(function () {
        this.pushQuery(
          `ALTER TABLE ${this.tableCompiler.tableName()} ADD CONSTRAINT ${this.formatter.wrap(
            constraintName
          )} DEFAULT (NEXT VALUE FOR ${sequenceName}) FOR ${this.formatter.wrap(
            this.getColumnName()
          )}`
        );
      });
    // }
    // return 'int identity(1,1) not null' + (primaryKey ? ' primary key' : '');
    return 'int not null' + (primaryKey ? ' primary key' : '');
  }

  bigincrements({ primaryKey = true } = {}) {
    // MTRK
    // if (this.columnBuilder._method === 'alter') {
      const sequenceName = createAutoIncrementTriggerAndSequence(this);

      const constraintName =
        `${this.tableCompiler.tableNameRaw}_${this.getColumnName()}_default_seq`.toLowerCase();
      this.pushAdditional(function () {
        this.pushQuery(
          `ALTER TABLE ${this.tableCompiler.tableName()} ADD CONSTRAINT ${this.formatter.wrap(
            constraintName
          )} DEFAULT (NEXT VALUE FOR ${sequenceName}) FOR ${this.formatter.wrap(
            this.getColumnName()
          )}`
        );
      });
    // }
    // return 'int identity(1,1) not null' + (primaryKey ? ' primary key' : '');
    return 'bigint not null' + (primaryKey ? ' primary key' : '');
  }

  double(precision, scale) {
    return 'float';
  }

  floating(precision, scale) {
    // ignore precicion / scale which is mysql specific stuff
    return `float`;
  }

  integer() {
    // mssql does not support length
    return 'int';
  }

  tinyint() {
    // mssql does not support length
    return 'tinyint';
  }

  varchar(length) {
    return `nvarchar(${toNumber(length, 255)})`;
  }

  timestamp({ useTz = false } = {}) {
    return useTz ? 'datetimeoffset' : 'datetime2';
  }

  datetime() {
    const old_column_name = this.getColumnName();
    const new_column_name = old_column_name + '_tmp';

    // MTRK
    // if (this.columnBuilder._method === 'alter') {
      this.pushAdditional(function () {
        this.pushQuery(
          `ALTER TABLE ${this.tableCompiler.tableName()} ADD ${new_column_name} datetime2`
        );
      });
      this.pushAdditional(function () {
        this.pushQuery(
          `UPDATE ${this.tableCompiler.tableName()} SET ${new_column_name} = DATEADD(MILLISECOND, CAST(${old_column_name} as BIGINT) % 1000, DATEADD(SECOND, CAST(${old_column_name} as BIGINT) / 1000, '19700101'))`
        );
      });
      this.pushAdditional(function () {
        this.pushQuery(
          `ALTER TABLE ${this.tableCompiler.tableName()} DROP COLUMN ${old_column_name}`
        );
      });
      this.pushAdditional(function () {
        this.pushQuery(
          `EXEC sp_rename '${this.tableCompiler.tableName()}.${new_column_name}', '${old_column_name}', 'COLUMN'`
        );
      });
    // }

    return 'varchar(25)'; // MTRK should be datetime2
    // return 'datetime2';
  }

  bit(length) {
    if (length > 1) {
      this.client.logger.warn('Bit field is exactly 1 bit length for MSSQL');
    }
    return 'bit';
  }

  binary(length) {
    return length ? `varbinary(${toNumber(length)})` : 'varbinary(max)';
  }

  // Modifiers
  // ------

  first() {
    this.client.logger.warn('Column first modifier not available for MSSQL');
    return '';
  }

  after(column) {
    this.client.logger.warn('Column after modifier not available for MSSQL');
    return '';
  }

  defaultTo(value, { constraintName } = {}) {
    const formattedValue = formatDefault(value, this.type, this.client);
    constraintName =
      typeof constraintName !== 'undefined'
        ? constraintName
        : `${
            this.tableCompiler.tableNameRaw
          }_${this.getColumnName()}_default`.toLowerCase();
    if (this.columnBuilder._method === 'alter') {
      this.pushAdditional(function () {
        this.pushQuery(
          `ALTER TABLE ${this.tableCompiler.tableName()} ADD CONSTRAINT ${this.formatter.wrap(
            constraintName
          )} DEFAULT ${formattedValue} FOR ${this.formatter.wrap(
            this.getColumnName()
          )}`
        );
      });
      return '';
    }
    if (!constraintName) {
      return `DEFAULT ${formattedValue}`;
    }
    return `CONSTRAINT ${this.formatter.wrap(
      constraintName
    )} DEFAULT ${formattedValue}`;
  }

  comment(comment) {
    // XXX: This is a byte limit, not character, so we cannot definitively say they'll exceed the limit without database collation info.
    // (Yes, even if the column has its own collation, the sqlvariant still uses the database collation.)
    // I'm not sure we even need to raise a warning, as MSSQL will return an error when the limit is exceeded itself.
    if (comment && comment.length > 7500 / 2) {
      this.client.logger.warn(
        'Your comment might be longer than the max comment length for MSSQL of 7,500 bytes.'
      );
    }
    return '';
  }
}

ColumnCompiler_MSSQL.prototype.bigint = 'bigint';
ColumnCompiler_MSSQL.prototype.mediumint = 'int';
ColumnCompiler_MSSQL.prototype.smallint = 'smallint';
ColumnCompiler_MSSQL.prototype.text = 'nvarchar(max)';
ColumnCompiler_MSSQL.prototype.mediumtext = 'nvarchar(max)';
ColumnCompiler_MSSQL.prototype.longtext = 'nvarchar(max)';
ColumnCompiler_MSSQL.prototype.json = 'nvarchar(max)';

// TODO: mssql supports check constraints as of SQL Server 2008
// so make enu here more like postgres
ColumnCompiler_MSSQL.prototype.enu = 'nvarchar(100)';
ColumnCompiler_MSSQL.prototype.uuid = 'uniqueidentifier';
// ColumnCompiler_MSSQL.prototype.datetime = 'datetime2';
ColumnCompiler_MSSQL.prototype.bool = 'bit';

module.exports = ColumnCompiler_MSSQL;
