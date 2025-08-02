/**
 * DB SQL Generator
 * Generates CREATE TABLE SQL statements from TableSchema objects
 */

import { DatabaseSchema, TableSchema, ColumnDefinition, ForeignKey } from './db-schema-types.js';

export class DbSqlGeneratorError extends Error {
  constructor(
    message: string,
    public cause?: Error
  ) {
    super(message);
    this.name = 'DbSqlGeneratorError';
  }
}

export class DbSqlGenerator {
  /**
   * Generate CREATE TABLE statements for all tables in the schema
   * Returns SQL statements in dependency order (foreign key constraints)
   */
  static generateCreateTableStatements(schema: DatabaseSchema): string[] {
    const statements: string[] = [];
    const tableOrder = schema.getTablesInDependencyOrder();

    for (const tableName of tableOrder) {
      const table = schema.getTable(tableName);
      if (!table) {
        throw new DbSqlGeneratorError(`Table not found: ${tableName}`);
      }

      const createTableSql = this.generateCreateTableStatement(table);
      statements.push(createTableSql);
    }

    return statements;
  }

  /**
   * Generate a single CREATE TABLE statement
   */
  static generateCreateTableStatement(table: TableSchema): string {
    const columnDefinitions: string[] = [];
    const constraints: string[] = [];

    // Generate column definitions
    for (const column of table.columns) {
      const columnSql = this.generateColumnDefinition(column, table.name);
      columnDefinitions.push(columnSql);
    }

    // Generate primary key constraint
    const primaryKeyColumns = table.columns
      .filter(col => col.primaryKey)
      .map(col => `"${col.name}"`);

    if (primaryKeyColumns.length > 0) {
      constraints.push(`PRIMARY KEY (${primaryKeyColumns.join(', ')})`);
    }

    // Generate unique constraints
    const uniqueColumns = table.columns
      .filter(col => col.unique && !col.primaryKey)
      .map(col => `"${col.name}"`);

    for (const column of uniqueColumns) {
      constraints.push(`UNIQUE (${column})`);
    }

    // Generate foreign key constraints
    for (const fk of table.foreignKeys) {
      const fkSql = this.generateForeignKeyConstraint(fk);
      constraints.push(fkSql);
    }

    // Combine all parts
    const allDefinitions = [...columnDefinitions, ...constraints];

    return `CREATE TABLE IF NOT EXISTS "${table.name}" (\n  ${allDefinitions.join(',\n  ')}\n);`;
  }

  /**
   * Generate column definition SQL
   */
  private static generateColumnDefinition(column: ColumnDefinition, _tableName: string): string {
    let sql = `"${column.name}"`;

    // Handle autoIncrement columns - ALL should be SERIAL in PostgreSQL (Prisma mapping)
    if (column.autoIncrement && column.type === 'INTEGER') {
      sql += ' SERIAL';
      if (!column.nullable) {
        sql += ' NOT NULL';
      }
      return sql;
    }

    // Handle non-autoIncrement columns
    sql += ` ${column.type}`;

    // Add NOT NULL constraint
    if (!column.nullable) {
      sql += ' NOT NULL';
    }

    // Add DEFAULT value
    if (column.defaultValue !== undefined) {
      if (column.defaultValue === 'CURRENT_TIMESTAMP') {
        sql += ' DEFAULT CURRENT_TIMESTAMP';
      } else if (typeof column.defaultValue === 'string') {
        sql += ` DEFAULT '${column.defaultValue}'`;
      } else {
        sql += ` DEFAULT ${column.defaultValue}`;
      }
    }

    return sql;
  }

  /**
   * Generate foreign key constraint SQL
   */
  private static generateForeignKeyConstraint(fk: ForeignKey): string {
    let sql = `FOREIGN KEY ("${fk.fromColumn}") REFERENCES "${fk.toTable}" ("${fk.toColumn}")`;

    if (fk.onDelete) {
      sql += ` ON DELETE ${fk.onDelete}`;
    }

    if (fk.onUpdate) {
      sql += ` ON UPDATE ${fk.onUpdate}`;
    }

    return sql;
  }

  /**
   * Generate DROP TABLE statements in reverse dependency order
   */
  static generateDropTableStatements(schema: DatabaseSchema): string[] {
    const statements: string[] = [];
    const tableOrder = schema.getTablesInDependencyOrder().reverse();

    for (const tableName of tableOrder) {
      statements.push(`DROP TABLE IF EXISTS "${tableName}" CASCADE;`);
    }

    return statements;
  }

  /**
   * Generate CREATE INDEX statements for all indexes in the schema
   */
  static generateCreateIndexStatements(schema: DatabaseSchema): string[] {
    const statements: string[] = [];

    for (const tableName of schema.getTableNames()) {
      const table = schema.getTable(tableName);
      if (!table) continue;

      for (const index of table.indexes) {
        const indexSql = this.generateCreateIndexStatement(tableName, index);
        statements.push(indexSql);
      }
    }

    return statements;
  }

  /**
   * Generate a single CREATE INDEX statement
   */
  private static generateCreateIndexStatement(
    tableName: string,
    index: { name: string; columns: string[]; unique: boolean; type?: string }
  ): string {
    const uniqueClause = index.unique ? 'UNIQUE ' : '';
    const columnList = index.columns.map(col => `"${col}"`).join(', ');
    const usingClause = index.type ? ` USING ${index.type.toUpperCase()}` : '';

    return `CREATE ${uniqueClause}INDEX IF NOT EXISTS "${index.name}" ON "${tableName}"${usingClause} (${columnList});`;
  }

  /**
   * Generate complete schema setup SQL (tables + indexes)
   */
  static generateCompleteSchema(schema: DatabaseSchema): string[] {
    const statements: string[] = [];

    // Create tables (SERIAL columns automatically create sequences)
    statements.push(...this.generateCreateTableStatements(schema));

    // Create indexes after tables
    statements.push(...this.generateCreateIndexStatements(schema));

    return statements;
  }
}
