/**
 * Database Test Fixture Loader
 * Loads test data from JSON fixtures using raw SQL, with proper foreign key dependency handling
 */

import fs from "fs"
import { Client } from "pg"
import { DatabaseSchema } from "./db-schema-types.js"

export class FixtureLoadError extends Error {
  constructor(message: string, public cause?: Error) {
    super(message)
    this.name = "FixtureLoadError"
  }
}

export interface FixtureData {
  [tableName: string]: Record<string, any>[]
}

export class DbTestFixtureLoader {
  private client: Client
  private schema: DatabaseSchema

  constructor(client: Client, schema: DatabaseSchema) {
    this.client = client
    this.schema = schema
  }

  /**
   * Load test data from a JSON fixture file
   */
  async loadFixtureFile(fixturePath: string): Promise<void> {
    if (!fs.existsSync(fixturePath)) {
      throw new FixtureLoadError(`Fixture file not found: ${fixturePath}`)
    }

    try {
      const content = fs.readFileSync(fixturePath, "utf-8")
      const fixtureData: FixtureData = JSON.parse(content)
      await this.loadFixtureData(fixtureData)
    } catch (error) {
      throw new FixtureLoadError(
        `Failed to load fixture file: ${fixturePath}`,
        error as Error
      )
    }
  }

  /**
   * Load test data from fixture data object
   */
  async loadFixtureData(fixtureData: FixtureData): Promise<void> {
    try {
      // Get tables in dependency order to handle foreign keys
      const tableOrder = this.schema.getTablesInDependencyOrder()

      for (const tableName of tableOrder) {
        const records = fixtureData[tableName]
        if (records && records.length > 0) {
          await this.loadTableData(tableName, records)
        }
      }

      // Update sequences for auto-increment columns
      await this.updateSequences(fixtureData)
    } catch (error) {
      throw new FixtureLoadError("Failed to load fixture data", error as Error)
    }
  }

  /**
   * Load data for a single table
   */
  private async loadTableData(
    tableName: string,
    records: Record<string, any>[]
  ): Promise<void> {
    const table = this.schema.getTable(tableName)
    if (!table) {
      throw new FixtureLoadError(`Table not found in schema: ${tableName}`)
    }

    const columnNames = table.columns.map((col) => col.name)

    for (const record of records) {
      await this.insertRecord(tableName, record, columnNames)
    }

    console.log(`✅ Loaded ${records.length} records into ${tableName}`)
  }

  /**
   * Insert a single record into a table
   */
  private async insertRecord(
    tableName: string,
    record: Record<string, any>,
    columnNames: string[]
  ): Promise<void> {
    // Filter record to only include columns that exist in the table
    const filteredRecord: Record<string, any> = {}
    for (const columnName of columnNames) {
      if (record[columnName] !== undefined) {
        filteredRecord[columnName] = record[columnName]
      }
    }

    if (Object.keys(filteredRecord).length === 0) {
      console.warn(
        `No valid columns found for record in table ${tableName}:`,
        record
      )
      return
    }

    const columns = Object.keys(filteredRecord)
    const values = Object.values(filteredRecord)
    const placeholders = values.map((_, index) => `$${index + 1}`)

    const columnList = columns.map((col) => `"${col}"`).join(", ")
    const placeholderList = placeholders.join(", ")

    const sql = `INSERT INTO "${tableName}" (${columnList}) VALUES (${placeholderList})`

    try {
      await this.client.query(sql, values)
    } catch (error) {
      throw new FixtureLoadError(
        `Failed to insert record into ${tableName}: ${JSON.stringify(
          filteredRecord
        )}`,
        error as Error
      )
    }
  }

  /**
   * Update PostgreSQL sequences for auto-increment columns after data loading
   */
  private async updateSequences(fixtureData: FixtureData): Promise<void> {
    for (const tableName of Object.keys(fixtureData)) {
      const table = this.schema.getTable(tableName)
      if (!table) continue

      const autoIncrementColumns = table.columns.filter(
        (col) => col.autoIncrement
      )

      for (const column of autoIncrementColumns) {
        await this.updateSequenceForColumn(tableName, column.name)
      }
    }
  }

  /**
   * Update a sequence to the maximum value + 1 for an auto-increment column
   */
  private async updateSequenceForColumn(
    tableName: string,
    columnName: string
  ): Promise<void> {
    try {
      // Get the maximum value in the column
      const maxQuery = `SELECT COALESCE(MAX("${columnName}"), 0) as max_val FROM "${tableName}"`
      const maxResult = await this.client.query(maxQuery)
      const maxValue = parseInt(maxResult.rows[0].max_val)

      // Update the sequence to max + 1
      const sequenceName = `${tableName}_${columnName}_seq`
      const updateSequenceQuery = `SELECT setval('"${sequenceName}"', $1)`
      await this.client.query(updateSequenceQuery, [maxValue + 1])

      console.log(`🔄 Updated sequence ${sequenceName} to ${maxValue + 1}`)
    } catch (error) {
      // Sequence might not exist, which is okay for some setups
      console.warn(
        `⚠️ Could not update sequence for ${tableName}.${columnName}:`,
        (error as Error).message
      )
    }
  }

  /**
   * Clear all data from tables in reverse dependency order
   */
  async clearAllData(): Promise<void> {
    try {
      const tableOrder = this.schema.getTablesInDependencyOrder().reverse()

      for (const tableName of tableOrder) {
        await this.client.query(`DELETE FROM "${tableName}"`)
        console.log(`🗑️ Cleared table: ${tableName}`)
      }

      console.log("✅ All test data cleared")
    } catch (error) {
      throw new FixtureLoadError("Failed to clear test data", error as Error)
    }
  }

  /**
   * Get record counts for all tables
   */
  async getTableCounts(): Promise<Record<string, number>> {
    const counts: Record<string, number> = {}

    try {
      for (const tableName of this.schema.getTableNames()) {
        const result = await this.client.query(
          `SELECT COUNT(*) as count FROM "${tableName}"`
        )
        counts[tableName] = parseInt(result.rows[0].count)
      }

      return counts
    } catch (error) {
      throw new FixtureLoadError("Failed to get table counts", error as Error)
    }
  }

  /**
   * Check if tables have expected record counts
   */
  async verifyDataCounts(
    expectedCounts: Record<string, number>
  ): Promise<boolean> {
    const actualCounts = await this.getTableCounts()

    for (const [tableName, expectedCount] of Object.entries(expectedCounts)) {
      const actualCount = actualCounts[tableName] || 0
      if (actualCount !== expectedCount) {
        console.error(
          `❌ Table ${tableName}: expected ${expectedCount} records, found ${actualCount}`
        )
        return false
      }
    }

    console.log("✅ All table counts match expected values")
    return true
  }
}
