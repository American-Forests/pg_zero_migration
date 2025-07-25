#!/usr/bin/env node
/* eslint-disable no-console */
/* eslint-disable turbo/no-undeclared-env-vars */

import { Client } from "pg"
import { parseArgs } from "node:util"
import {
  DatabaseMigrator,
  parseDatabaseUrl,
  DatabaseConfig,
} from "./migration-core.js"
import { fileURLToPath } from "url"
interface BackupInfo {
  timestamp: number
  schemaName: string
  created: Date
  tableCount: number
  size: string
}

class MigrationManager {
  private client: Client
  private dryRun: boolean

  constructor(config: DatabaseConfig, dryRun: boolean = false) {
    this.client = new Client(config)
    this.dryRun = dryRun
  }

  private log(message: string): void {
    console.log(message)
  }

  private async connect(): Promise<void> {
    await this.client.connect()
  }

  private async disconnect(): Promise<void> {
    await this.client.end()
  }

  async listBackups(json: boolean = false): Promise<void> {
    await this.connect()
    try {
      const backups = await this.getAvailableBackups()

      if (json) {
        console.log(
          JSON.stringify(
            backups.map((b) => ({
              timestamp: b.timestamp,
              created: b.created.toISOString(),
              tableCount: b.tableCount,
              size: b.size,
            })),
            null,
            2
          )
        )
      } else {
        this.printBackupsTable(backups)
      }
    } finally {
      await this.disconnect()
    }
  }

  private async getAvailableBackups(): Promise<BackupInfo[]> {
    const query = `
      SELECT 
        nspname as schema_name,
        CASE 
          WHEN nspname ~ '^backup_([0-9]+)$' 
          THEN CAST(substring(nspname from '^backup_([0-9]+)$') AS BIGINT)
          ELSE 0
        END as timestamp
      FROM pg_namespace 
      WHERE nspname LIKE 'backup_%'
        AND nspname ~ '^backup_[0-9]+$'
      ORDER BY timestamp DESC;
    `

    const result = await this.client.query(query)
    const backups: BackupInfo[] = []

    for (const row of result.rows) {
      const timestamp = row.timestamp
      const schemaName = row.schema_name
      const created = new Date(timestamp)

      // Get table count for this backup schema
      const tableCountQuery = `
        SELECT COUNT(*) as count 
        FROM information_schema.tables 
        WHERE table_schema = $1;
      `
      const tableCountResult = await this.client.query(tableCountQuery, [
        schemaName,
      ])
      const tableCount = parseInt(tableCountResult.rows[0].count)

      // Get approximate size
      const sizeQuery = `
        SELECT 
          COALESCE(
            pg_size_pretty(
              SUM(pg_total_relation_size(schemaname||'.'||tablename))
            ), 
            '0 bytes'
          ) as size
        FROM pg_tables 
        WHERE schemaname = $1;
      `
      const sizeResult = await this.client.query(sizeQuery, [schemaName])
      const size = sizeResult.rows[0]?.size || "0 bytes"

      backups.push({
        timestamp,
        schemaName,
        created,
        tableCount,
        size,
      })
    }

    return backups
  }

  private printBackupsTable(backups: BackupInfo[]): void {
    if (backups.length === 0) {
      console.log("No backup schemas found.")
      return
    }

    console.log("┌─────────────────┬─────────────────────┬────────┬──────────┐")
    console.log("│ Timestamp       │ Created             │ Tables │ Size     │")
    console.log("├─────────────────┼─────────────────────┼────────┼──────────┤")

    for (const backup of backups) {
      const timestamp = backup.timestamp.toString().padEnd(15)
      const created = backup.created
        .toISOString()
        .replace("T", " ")
        .substring(0, 19)
        .padEnd(19)
      const tables = backup.tableCount.toString().padEnd(6)
      const size = backup.size.padEnd(8)
      console.log(`│ ${timestamp} │ ${created} │ ${tables} │ ${size} │`)
    }

    console.log("└─────────────────┴─────────────────────┴────────┴──────────┘")
  }

  async rollback(
    timestamp: number | "latest",
    keepTables: string[] = []
  ): Promise<void> {
    await this.connect()
    try {
      const backups = await this.getAvailableBackups()

      if (backups.length === 0) {
        throw new Error("No backup schemas found")
      }

      const targetBackup =
        timestamp === "latest"
          ? backups[0]
          : backups.find((b) => b.timestamp === timestamp)

      if (!targetBackup) {
        throw new Error(`Backup not found: ${timestamp}`)
      }

      await this.performRollback(targetBackup, keepTables)
    } finally {
      await this.disconnect()
    }
  }

  private async performRollback(
    backup: BackupInfo,
    keepTables: string[]
  ): Promise<void> {
    const rollbackTimestamp = Date.now()

    this.log("\n⚠️  DESTRUCTIVE ROLLBACK - NO UNDO AVAILABLE")
    this.log(`• Current 'public' schema → renamed to 'shadow' (temporary)`)
    this.log(`• Backup '${backup.schemaName}' → renamed to 'public' (restored)`)
    this.log(`• Existing 'shadow' schema will be DELETED if present`)

    if (keepTables.length > 0) {
      this.log(`\nCURRENT DATA TO BE KEPT:`)
      this.log(
        `• Tables to copy from shadow to public: ${keepTables.join(", ")}`
      )
    }

    this.log(`\nBACKUP CONSUMED:`)
    this.log(
      `• Backup '${backup.schemaName}' will be consumed (no longer available)`
    )

    if (this.dryRun) {
      this.log("\n🔍 DRY RUN - No changes made")
      return
    }

    this.log("\n✅ Proceeding with destructive rollback...")

    try {
      // Disable foreign key constraints for rollback operations
      this.log("🔓 Disabling foreign key constraints for rollback...")
      await this.client.query("SET session_replication_role = replica;")

      // Step 1: Clear existing shadow schema
      await this.client.query("DROP SCHEMA IF EXISTS shadow CASCADE;")
      this.log("• Cleared existing shadow schema")

      // Step 2: Rename current public to shadow
      await this.client.query("ALTER SCHEMA public RENAME TO shadow;")
      this.log("• Renamed current public schema to shadow")

      // Step 3: Rename backup to public
      await this.client.query(
        `ALTER SCHEMA ${backup.schemaName} RENAME TO public;`
      )
      this.log(`• Renamed backup schema to public`)

      // Step 4: Handle keep-tables if specified
      if (keepTables.length > 0) {
        await this.copyKeepTables(keepTables)
      }

      // Step 5: Re-enable foreign key constraints
      this.log("🔒 Re-enabling foreign key constraints...")
      await this.client.query("SET session_replication_role = origin;")

      // Step 6: Cleanup shadow schema
      await this.client.query("DROP SCHEMA shadow CASCADE;")
      this.log("• Cleaned up shadow schema")

      this.log("\n✅ Rollback completed successfully!")
      this.log(`📦 Backup '${backup.schemaName}' has been consumed`)
    } catch (error) {
      this.log("\n❌ Rollback failed, attempting to restore original state...")

      // Ensure foreign key constraints are re-enabled even on failure
      try {
        this.log(
          "🔒 Re-enabling foreign key constraints after rollback error..."
        )
        await this.client.query("SET session_replication_role = origin;")
      } catch (fkError) {
        this.log(
          `⚠️  Warning: Could not re-enable foreign key constraints: ${fkError}`
        )
      }

      await this.recoverFromFailedRollback()
      throw error
    }
  }

  private async copyKeepTables(keepTables: string[]): Promise<void> {
    this.log("\n🔄 Copying preserved tables from shadow to public...")

    for (const tableName of keepTables) {
      try {
        // Check if table exists in shadow
        const checkQuery = `
          SELECT EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_schema = 'shadow' AND table_name = $1
          );
        `
        const checkResult = await this.client.query(checkQuery, [tableName])

        if (!checkResult.rows[0].exists) {
          this.log(
            `⚠️  Table '${tableName}' not found in shadow schema, skipping`
          )
          continue
        }

        // Drop table in public if it exists
        await this.client.query(
          `DROP TABLE IF EXISTS public."${tableName}" CASCADE;`
        )

        // Create table structure from shadow
        await this.client.query(
          `CREATE TABLE public."${tableName}" (LIKE shadow."${tableName}" INCLUDING ALL);`
        )

        // Copy data
        await this.client.query(
          `INSERT INTO public.${tableName} SELECT * FROM shadow.${tableName};`
        )

        this.log(`  ✓ Copied table '${tableName}' from shadow to public`)
      } catch (error) {
        this.log(`  ❌ Failed to copy table '${tableName}': ${error}`)
        throw error
      }
    }
  }

  private async recoverFromFailedRollback(): Promise<void> {
    try {
      // Check if shadow schema exists
      const shadowCheck = await this.client.query(`
        SELECT EXISTS (
          SELECT 1 FROM information_schema.schemata 
          WHERE schema_name = 'shadow'
        );
      `)

      if (shadowCheck.rows[0].exists) {
        // Restore shadow to public
        await this.client.query("DROP SCHEMA IF EXISTS public CASCADE;")
        await this.client.query("ALTER SCHEMA shadow RENAME TO public;")
        this.log("✅ Original state restored from shadow")
      } else {
        this.log("❌ Cannot recover: shadow schema not found")
      }
    } catch (recoverError) {
      this.log(`❌ Recovery failed: ${recoverError}`)
    }
  }

  async cleanup(beforeDate: string): Promise<void> {
    await this.connect()
    try {
      const cutoffTimestamp = this.parseDateString(beforeDate)
      const backups = await this.getAvailableBackups()
      const toDelete = backups.filter((b) => b.timestamp < cutoffTimestamp)

      if (toDelete.length === 0) {
        this.log("No backup schemas found before the specified date.")
        return
      }

      if (this.dryRun) {
        this.log(`Would delete ${toDelete.length} backup schema(s):`)
        for (const backup of toDelete) {
          this.log(
            `• ${backup.schemaName} (${backup.created
              .toISOString()
              .replace("T", " ")
              .substring(0, 19)})`
          )
        }
        return
      }

      this.log(`Deleting ${toDelete.length} backup schema(s)...`)

      for (const backup of toDelete) {
        await this.client.query(`DROP SCHEMA ${backup.schemaName} CASCADE;`)
        this.log(`• Deleted ${backup.schemaName}`)
      }

      this.log("✅ Cleanup completed successfully!")
    } finally {
      await this.disconnect()
    }
  }

  private parseDateString(dateStr: string): number {
    // Support ISO dates: "2025-07-15" or "2025-07-15 10:30"
    let date: Date

    if (dateStr.match(/^\d{4}-\d{2}-\d{2}$/)) {
      // ISO date only
      date = new Date(dateStr + "T00:00:00.000Z")
    } else if (dateStr.match(/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}$/)) {
      // ISO date + time
      date = new Date(dateStr.replace(" ", "T") + ":00.000Z")
    } else if (dateStr.match(/^\d+$/)) {
      // Timestamp
      return parseInt(dateStr)
    } else {
      throw new Error(
        `Invalid date format: ${dateStr}. Use ISO format (2025-07-15) or timestamp.`
      )
    }

    if (isNaN(date.getTime())) {
      throw new Error(`Invalid date: ${dateStr}`)
    }

    return date.getTime()
  }

  async verify(timestamp: number): Promise<void> {
    await this.connect()
    try {
      const backups = await this.getAvailableBackups()
      const backup = backups.find((b) => b.timestamp === timestamp)

      if (!backup) {
        throw new Error(`Backup not found: ${timestamp}`)
      }

      this.log(`🔍 Verifying backup: ${backup.schemaName}`)
      this.log(
        `Created: ${backup.created
          .toISOString()
          .replace("T", " ")
          .substring(0, 19)}`
      )

      // Advanced verification
      await this.performAdvancedVerification(backup)

      this.log("✅ Backup verification completed successfully!")
    } finally {
      await this.disconnect()
    }
  }

  private async performAdvancedVerification(backup: BackupInfo): Promise<void> {
    // 1. Schema exists check
    const schemaCheck = await this.client.query(
      `
      SELECT EXISTS (
        SELECT 1 FROM information_schema.schemata 
        WHERE schema_name = $1
      );
    `,
      [backup.schemaName]
    )

    if (!schemaCheck.rows[0].exists) {
      throw new Error(`Schema ${backup.schemaName} does not exist`)
    }
    this.log("  ✓ Schema exists")

    // 2. Get all tables in backup schema
    const tablesQuery = await this.client.query(
      `
      SELECT table_name 
      FROM information_schema.tables 
      WHERE table_schema = $1 
      ORDER BY table_name;
    `,
      [backup.schemaName]
    )

    const tables = tablesQuery.rows.map((row) => row.table_name)
    this.log(`  ✓ Found ${tables.length} tables`)

    // 3. Row count verification for each table
    for (const tableName of tables) {
      const rowCountQuery = await this.client.query(`
        SELECT COUNT(*) as count 
        FROM ${backup.schemaName}.${tableName};
      `)
      const rowCount = parseInt(rowCountQuery.rows[0].count)
      this.log(`    • ${tableName}: ${rowCount} rows`)
    }

    // 4. Sample data integrity check (check first 5 rows of each table have data)
    for (const tableName of tables) {
      const sampleQuery = await this.client.query(`
        SELECT COUNT(*) as count 
        FROM ${backup.schemaName}.${tableName} 
        LIMIT 5;
      `)
      const sampleCount = parseInt(sampleQuery.rows[0].count)

      if (sampleCount > 0) {
        // Verify we can actually read the data
        await this.client.query(
          `SELECT * FROM ${backup.schemaName}.${tableName} LIMIT 1;`
        )
      }
    }
    this.log("  ✓ Sample data integrity verified")
  }
}

function printUsage(): void {
  console.log(`
Usage: npm run migration <command> [options]

Commands:
  list                                    List all available backup schemas
  start --source <url> --dest <url>       Start database migration
  rollback --latest                       Rollback to most recent backup
  rollback --timestamp <ts>               Rollback to specific backup timestamp
  cleanup --before <date>                 Delete backups before specified date
  verify --timestamp <ts>                 Verify backup integrity

Options:
  --source <url>                         Source database URL (for start command)
  --dest <url>                           Destination database URL (for start command)
  --preserved-tables <table1,table2>     Tables to preserve during migration (for start command)
  --keep-tables <table1,table2>          Tables to preserve during rollback
  --json                                 Output as JSON (list command only)
  --dry-run                              Preview changes without executing
  --help                                 Show this help message

Examples:
  npm run migration -- list
  npm run migration -- list --json
  npm run migration -- start --source postgres://user:pass@host:port/db --dest postgres://user:pass@host:port/db
  npm run migration -- start --source postgres://... --dest postgres://... --preserved-tables users,sessions --dry-run
  npm run migration -- rollback --latest
  npm run migration -- rollback --timestamp 1753207951602
  npm run migration -- rollback --latest --keep-tables users,sessions
  npm run migration -- rollback --latest --dry-run
  npm run migration cleanup --before "2025-07-15"
  npm run migration cleanup --before "2025-07-15 10:30" --dry-run
  npm run migration verify --timestamp 1753207951602

Date Formats:
  ISO Date: 2025-07-15
  Date + Time: 2025-07-15 10:30
  Timestamp: 1753207951602
`)
}

async function main(): Promise<void> {
  try {
    const { values, positionals } = parseArgs({
      args: process.argv.slice(2),
      options: {
        latest: { type: "boolean" },
        timestamp: { type: "string" },
        before: { type: "string" },
        source: { type: "string" },
        dest: { type: "string" },
        "preserved-tables": { type: "string" },
        "keep-tables": { type: "string" },
        json: { type: "boolean" },
        "dry-run": { type: "boolean" },
        help: { type: "boolean" },
      },
      allowPositionals: true,
    })

    if (values.help || positionals.length === 0) {
      printUsage()
      return
    }

    const command = positionals[0]
    const dryRun = values["dry-run"] || false

    switch (command) {
      case "start":
        await handleStartCommand(values, dryRun)
        break

      case "list":
      case "rollback":
      case "cleanup":
      case "verify":
        await handleBackupCommand(command, values, dryRun)
        break

      default:
        console.error(`Unknown command: ${command}`)
        printUsage()
        process.exit(1)
    }
  } catch (error) {
    console.error("\n❌ Error:", error instanceof Error ? error.message : error)
    process.exit(1)
  }
}

async function handleStartCommand(values: any, dryRun: boolean): Promise<void> {
  const sourceUrl = values.source || process.env.SOURCE_DATABASE_URL
  const destUrl =
    values.dest || process.env.DEST_DATABASE_URL || process.env.DATABASE_URL
  const preservedTablesEnv =
    values["preserved-tables"] || process.env.PRESERVED_TABLES || ""

  if (!sourceUrl || !destUrl) {
    console.error("❌ Start command requires:")
    console.error("   --source <url> - Source database connection string")
    console.error("   --dest <url> - Destination database connection string")
    console.error(
      "   Optional: --preserved-tables <table1,table2> - Tables to preserve"
    )
    console.error("   Optional: --dry-run - Preview mode")
    process.exit(1)
  }

  const preservedTables = preservedTablesEnv
    .split(",")
    .map((table: string) => table.trim())
    .filter((table: string) => table.length > 0)

  const sourceConfig = parseDatabaseUrl(sourceUrl)
  const destConfig = parseDatabaseUrl(destUrl)

  console.log("🚀 Database Migration Tool")
  console.log(
    `📍 Source: ${sourceConfig.host}:${sourceConfig.port}/${sourceConfig.database}`
  )
  console.log(
    `📍 Destination: ${destConfig.host}:${destConfig.port}/${destConfig.database}`
  )
  console.log(`🔒 Preserved tables: ${preservedTables.join(", ") || "none"}`)
  console.log("")

  const migrator = new DatabaseMigrator(
    sourceConfig,
    destConfig,
    preservedTables,
    dryRun
  )

  try {
    await migrator.migrate()

    console.log("\n✅ Migration completed successfully!")
    console.log("📦 Schema backup retained for rollback purposes")
    process.exit(0)
  } catch (error) {
    console.error("\n❌ Migration failed:", error)
    process.exit(1)
  }
}

async function handleBackupCommand(
  command: string,
  values: any,
  dryRun: boolean
): Promise<void> {
  // Database configuration for single-database backup operations
  const config: DatabaseConfig = {
    host: process.env.DB_HOST || "localhost",
    port: parseInt(process.env.DB_PORT || "5432"),
    database: process.env.DB_NAME || "postgres",
    user: process.env.DB_USER || "postgres",
    password: process.env.DB_PASSWORD || "",
  }

  const manager = new MigrationManager(config, dryRun)

  switch (command) {
    case "list":
      await manager.listBackups(values.json || false)
      break

    case "rollback":
      let target: number | "latest"
      if (values.latest) {
        target = "latest"
      } else if (values.timestamp) {
        target = parseInt(values.timestamp)
        if (isNaN(target)) {
          throw new Error("Invalid timestamp format")
        }
      } else {
        throw new Error("Rollback requires --latest or --timestamp option")
      }

      const keepTables = values["keep-tables"]
        ? values["keep-tables"].split(",").map((t: string) => t.trim())
        : []

      await manager.rollback(target, keepTables)
      break

    case "cleanup":
      if (!values.before) {
        throw new Error("Cleanup requires --before option")
      }
      await manager.cleanup(values.before)
      break

    case "verify":
      if (!values.timestamp) {
        throw new Error("Verify requires --timestamp option")
      }
      const verifyTimestamp = parseInt(values.timestamp)
      if (isNaN(verifyTimestamp)) {
        throw new Error("Invalid timestamp format")
      }
      await manager.verify(verifyTimestamp)
      break

    default:
      throw new Error(`Unknown backup command: ${command}`)
  }
}

// Execute if called directly
if (process.argv[1] === fileURLToPath(import.meta.url)) {
  main()
}

export { MigrationManager }
