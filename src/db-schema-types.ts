/**
 * Type definitions for database schema representation
 * Used to parse Prisma schemas into an intermediate format for raw SQL generation
 */

export interface ColumnDefinition {
  name: string;
  type: string; // PostgreSQL type (VARCHAR, INTEGER, etc.)
  nullable: boolean;
  primaryKey: boolean;
  unique: boolean;
  autoIncrement: boolean;
  defaultValue?: string;
}

export interface ForeignKey {
  fromColumn: string;
  toTable: string;
  toColumn: string;
  onDelete?: 'CASCADE' | 'SET NULL' | 'RESTRICT';
  onUpdate?: 'CASCADE' | 'SET NULL' | 'RESTRICT';
}

export interface Index {
  name: string;
  columns: string[];
  unique: boolean;
}

export interface TableSchema {
  name: string;
  columns: ColumnDefinition[];
  foreignKeys: ForeignKey[];
  indexes: Index[];
}

export interface DatabaseSchema {
  tables: Map<string, TableSchema>;

  // Helper methods
  getTableNames(): string[];
  getTable(name: string): TableSchema | undefined;
  getTablesInDependencyOrder(): string[]; // Topological sort for foreign key dependencies
}

/**
 * Mapping from Prisma field types to PostgreSQL types
 */
export const PRISMA_TO_POSTGRES: Record<string, string> = {
  String: 'VARCHAR(255)',
  Int: 'INTEGER',
  BigInt: 'BIGINT',
  Boolean: 'BOOLEAN',
  DateTime: 'TIMESTAMP',
  Decimal: 'DECIMAL',
  Float: 'REAL',
  Double: 'DOUBLE PRECISION',
  Json: 'JSONB',
  Bytes: 'BYTEA',
};

/**
 * Prisma field attributes and their SQL equivalents
 */
export interface PrismaFieldAttribute {
  id?: boolean;
  unique?: boolean;
  autoIncrement?: boolean;
  default?: string | number | boolean;
  map?: string; // Custom column name
  updatedAt?: boolean;
}

/**
 * Prisma model attribute for table-level configurations
 */
export interface PrismaModelAttribute {
  map?: string; // Custom table name
  id?: string[]; // Composite primary key
  unique?: string[]; // Composite unique constraint
  index?: string[]; // Composite index
}
