/**
 * Database Schema Data Structures
 *
 * This module contains data structures for representing database schema information
 * including tables, fields, constraints, indexes, and other database objects.
 *
 * These types are used across the migration system for schema introspection,
 * data loading, and database operations.
 */

/**
 * Represents a table schema with fields and relationships.
 */
export interface TableSchema {
  name: string
  fields: Record<string, string> // field_name -> field_type
  dependencies: string[] // tables this depends on (foreign keys)
  sequenceField?: string // auto-increment field name
}

/**
 * Information about a foreign key constraint.
 */
export interface ForeignKeyInfo {
  constraintName: string
  tableName: string
  columnNames: string[]
  referredTableName: string
  referredColumnNames: string[]
  onUpdate?: string
  onDelete?: string
}

/**
 * Information about a database sequence.
 */
export interface SequenceInfo {
  sequenceName: string
  tableName: string
  columnName: string
  dataType: string
  startValue: number
  incrementBy: number
  minValue?: number
  maxValue?: number
  isCyclic: boolean
}

/**
 * Information about a database index.
 */
export interface IndexInfo {
  indexName: string
  tableName: string
  columnNames: string[]
  isUnique: boolean
  isPrimary: boolean
  indexType: string
}

/**
 * Information about custom enum types.
 */
export interface EnumInfo {
  enumName: string
  schemaName: string
  enumValues: string[]
}

/**
 * Information about table swapping operations.
 */
export interface TableSwapInfo {
  sourceTable: string
  targetTable: string
  backupTable: string
  swapTimestamp: Date
  isCompleted: boolean
  rollbackSql?: string
}

/**
 * Database connection configuration.
 */
export interface DatabaseConfig {
  host: string
  port: number
  database: string
  user: string
  password: string
  ssl?: boolean
  schema?: string
}

/**
 * Result of data loading operations.
 */
export interface LoadResult {
  success: boolean
  tablesLoaded: string[]
  recordCounts: Record<string, number>
  errors: string[]
  duration: number
}

/**
 * Test data counts for verification.
 */
export interface DataCounts {
  [tableName: string]: number
}
