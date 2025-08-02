#!/usr/bin/env node

import { Client } from 'pg';
import { parseArgs } from 'node:util';
import { writeFileSync } from 'fs';
import { join } from 'path';
import {
  DatabaseMigrator,
  parseDatabaseUrl,
  DatabaseConfig,
  MigrationResult,
  PreparationResult,
} from './migration-core.js';
import { DatabaseRollback } from './rollback.js';
import { fileURLToPath } from 'url';
interface BackupInfo {
  timestamp: number;
  schemaName: string;
  created: Date;
  tableCount: number;
  size: string;
}

interface ParsedArgs {
  latest?: boolean;
  timestamp?: string;
  before?: string;
  source?: string;
  dest?: string;
  'preserved-tables'?: string;
  'keep-tables'?: string;
  json?: boolean;
  'dry-run'?: boolean;
  help?: boolean;
}

class MigrationManager {
  private client: Client;
  private dryRun: boolean;
  private rollbackManager: DatabaseRollback;

  constructor(config: DatabaseConfig, dryRun: boolean = false) {
    this.client = new Client(config);
    this.dryRun = dryRun;
    this.rollbackManager = new DatabaseRollback(config);
  }

  private log(message: string): void {
    console.log(message);
  }

  private async connect(): Promise<void> {
    await this.client.connect();
  }

  private async disconnect(): Promise<void> {
    await this.client.end();
  }

  async listBackups(json: boolean = false): Promise<void> {
    await this.connect();
    try {
      const backups = await this.getAvailableBackups();

      if (json) {
        console.log(
          JSON.stringify(
            backups.map(b => ({
              timestamp: b.timestamp,
              created: b.created.toISOString(),
              tableCount: b.tableCount,
              size: b.size,
            })),
            null,
            2
          )
        );
      } else {
        this.printBackupsTable(backups);
      }
    } finally {
      await this.disconnect();
    }
  }

  private async getAvailableBackups(): Promise<BackupInfo[]> {
    const rollbackBackups = await this.rollbackManager.getAvailableBackups();

    // Convert DatabaseRollback.BackupInfo to MigrationManager.BackupInfo
    return rollbackBackups.map(backup => ({
      timestamp: parseInt(backup.timestamp),
      schemaName: backup.schemaName,
      created: backup.createdAt,
      tableCount: backup.tableCount,
      size: backup.totalSize,
    }));
  }

  private printBackupsTable(backups: BackupInfo[]): void {
    if (backups.length === 0) {
      console.log('No backup schemas found.');
      return;
    }

    console.log('┌─────────────────┬─────────────────────┬────────┬──────────┐');
    console.log('│ Timestamp       │ Created             │ Tables │ Size     │');
    console.log('├─────────────────┼─────────────────────┼────────┼──────────┤');

    for (const backup of backups) {
      const timestamp = backup.timestamp.toString().padEnd(15);
      const created = backup.created.toISOString().replace('T', ' ').substring(0, 19).padEnd(19);
      const tables = backup.tableCount.toString().padEnd(6);
      const size = backup.size.padEnd(8);
      console.log(`│ ${timestamp} │ ${created} │ ${tables} │ ${size} │`);
    }

    console.log('└─────────────────┴─────────────────────┴────────┴──────────┘');
  }

  async rollback(timestamp: number | 'latest', keepTables: string[] = []): Promise<void> {
    const backups = await this.getAvailableBackups();

    if (backups.length === 0) {
      throw new Error('No backup schemas found');
    }

    const targetBackup =
      timestamp === 'latest' ? backups[0] : backups.find(b => b.timestamp === timestamp);

    if (!targetBackup) {
      throw new Error(`Backup not found: ${timestamp}`);
    }

    // Use DatabaseRollback for the actual rollback operation
    await this.rollbackManager.rollback(targetBackup.timestamp.toString(), keepTables);
  }

  async cleanup(beforeDate: string): Promise<void> {
    await this.connect();
    try {
      const cutoffTimestamp = this.parseDateString(beforeDate);
      const backups = await this.getAvailableBackups();
      const toDelete = backups.filter(b => b.timestamp < cutoffTimestamp);

      if (toDelete.length === 0) {
        this.log('No backup schemas found before the specified date.');
        return;
      }

      if (this.dryRun) {
        this.log(`Would delete ${toDelete.length} backup schema(s):`);
        for (const backup of toDelete) {
          this.log(
            `• ${backup.schemaName} (${backup.created
              .toISOString()
              .replace('T', ' ')
              .substring(0, 19)})`
          );
        }
        return;
      }

      this.log(`Deleting ${toDelete.length} backup schema(s)...`);

      for (const backup of toDelete) {
        await this.client.query(`DROP SCHEMA ${backup.schemaName} CASCADE;`);
        this.log(`• Deleted ${backup.schemaName}`);
      }

      this.log('✅ Cleanup completed successfully!');
    } finally {
      await this.disconnect();
    }
  }

  private parseDateString(dateStr: string): number {
    // Support ISO dates: "2025-07-15" or "2025-07-15 10:30"
    let date: Date;

    if (dateStr.match(/^\d{4}-\d{2}-\d{2}$/)) {
      // ISO date only
      date = new Date(dateStr + 'T00:00:00.000Z');
    } else if (dateStr.match(/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}$/)) {
      // ISO date + time
      date = new Date(dateStr.replace(' ', 'T') + ':00.000Z');
    } else if (dateStr.match(/^\d+$/)) {
      // Timestamp
      return parseInt(dateStr);
    } else {
      throw new Error(`Invalid date format: ${dateStr}. Use ISO format (2025-07-15) or timestamp.`);
    }

    if (isNaN(date.getTime())) {
      throw new Error(`Invalid date: ${dateStr}`);
    }

    return date.getTime();
  }

  async verify(timestamp: number): Promise<void> {
    await this.connect();
    try {
      const backups = await this.getAvailableBackups();
      const backup = backups.find(b => b.timestamp === timestamp);

      if (!backup) {
        throw new Error(`Backup not found: ${timestamp}`);
      }

      this.log(`🔍 Verifying backup: ${backup.schemaName}`);
      this.log(`Created: ${backup.created.toISOString().replace('T', ' ').substring(0, 19)}`);

      // Advanced verification
      await this.performAdvancedVerification(backup);

      this.log('✅ Backup verification completed successfully!');
    } finally {
      await this.disconnect();
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
    );

    if (!schemaCheck.rows[0].exists) {
      throw new Error(`Schema ${backup.schemaName} does not exist`);
    }
    this.log('  ✓ Schema exists');

    // 2. Get all tables in backup schema
    const tablesQuery = await this.client.query(
      `
      SELECT table_name 
      FROM information_schema.tables 
      WHERE table_schema = $1 
      ORDER BY table_name;
    `,
      [backup.schemaName]
    );

    const tables = tablesQuery.rows.map(row => row.table_name);
    this.log(`  ✓ Found ${tables.length} tables`);

    // 3. Row count verification for each table
    for (const tableName of tables) {
      const rowCountQuery = await this.client.query(`
        SELECT COUNT(*) as count 
        FROM ${backup.schemaName}.${tableName};
      `);
      const rowCount = parseInt(rowCountQuery.rows[0].count);
      this.log(`    • ${tableName}: ${rowCount} rows`);
    }

    // 4. Sample data integrity check (check first 5 rows of each table have data)
    for (const tableName of tables) {
      const sampleQuery = await this.client.query(`
        SELECT COUNT(*) as count 
        FROM ${backup.schemaName}.${tableName} 
        LIMIT 5;
      `);
      const sampleCount = parseInt(sampleQuery.rows[0].count);

      if (sampleCount > 0) {
        // Verify we can actually read the data
        await this.client.query(`SELECT * FROM ${backup.schemaName}.${tableName} LIMIT 1;`);
      }
    }
    this.log('  ✓ Sample data integrity verified');
  }
}

function printUsage(): void {
  console.log(`
Usage: npm run migration <command> [options]

Commands:
  list                                    List all available backup schemas
  start --source <url> --dest <url>       Start complete database migration (prepare + swap)
  prepare --source <url> --dest <url>     Prepare migration (create dump, setup shadow schema)
  swap --dest <url> [--timestamp <ts>]    Complete migration (atomic schema swap)
  status --dest <url>                     Show current migration status
  rollback --latest                       Rollback to most recent backup
  rollback --timestamp <ts>               Rollback to specific backup timestamp
  cleanup --before <date>                 Delete backups before specified date
  verify --timestamp <ts>                 Verify backup integrity

Options:
  --source <url>                         Source database URL (for start/prepare commands)
  --dest <url>                           Destination database URL (for all commands)
  --preserved-tables <table1,table2>     Tables to preserve during migration (for start/prepare commands)
  --timestamp <ts>                       Specific migration timestamp (for swap command)
  --keep-tables <table1,table2>          Tables to preserve during rollback
  --json                                 Output as JSON (list/status commands)
  --dry-run                              Preview changes without executing
  --help                                 Show this help message

Examples:
  npm run migration -- list
  npm run migration -- list --json
  npm run migration -- start --source postgres://user:pass@host:port/db --dest postgres://user:pass@host:port/db
  npm run migration -- prepare --source postgres://... --dest postgres://... --preserved-tables users,sessions
  npm run migration -- status --dest postgres://...
  npm run migration -- swap --dest postgres://...
  npm run migration -- swap --dest postgres://... --timestamp 1753207951602
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

Two-Phase Migration Workflow:
  1. Run 'prepare' to create shadow schema and setup sync triggers
  2. Monitor and validate the shadow schema (preserved tables stay synced)
  3. Run 'swap' when ready to complete the migration (zero downtime)
`);
}

/**
 * Write migration log to disk file
 */
function writeLogFile(
  result: MigrationResult,
  sourceConfig: DatabaseConfig,
  destConfig: DatabaseConfig
): void {
  try {
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
    const sourceDb = sourceConfig.database.replace(/[^a-zA-Z0-9]/g, '_');
    const destDb = destConfig.database.replace(/[^a-zA-Z0-9]/g, '_');
    const logFilePath = join(process.cwd(), `migration_${timestamp}_${sourceDb}_to_${destDb}.log`);

    const duration = result.stats.endTime
      ? (result.stats.endTime.getTime() - result.stats.startTime.getTime()) / 1000
      : 0;

    const outcome = result.success ? 'SUCCESS' : 'FAILED';

    // Create log file header
    const header = [
      '='.repeat(80),
      'DATABASE MIGRATION LOG',
      '='.repeat(80),
      `Migration Outcome: ${outcome}`,
      `Start Time: ${result.stats.startTime.toISOString()}`,
      `End Time: ${result.stats.endTime?.toISOString() || 'N/A'}`,
      `Duration: ${duration}s`,
      '',
      'Source Database:',
      `  Host: ${sourceConfig.host}:${sourceConfig.port}`,
      `  Database: ${sourceConfig.database}`,
      `  User: ${sourceConfig.user}`,
      '',
      'Destination Database:',
      `  Host: ${destConfig.host}:${destConfig.port}`,
      `  Database: ${destConfig.database}`,
      `  User: ${destConfig.user}`,
      '',
      'Migration Statistics:',
      `  Tables Processed: ${result.stats.tablesProcessed}`,
      `  Records Migrated: ${result.stats.recordsMigrated}`,
      `  Warnings: ${result.stats.warnings.length}`,
      `  Errors: ${result.stats.errors.length}`,
      '',
      '='.repeat(80),
      'MIGRATION LOG DETAILS',
      '='.repeat(80),
      '',
    ];

    // Create log file footer
    const footer = ['', '='.repeat(80), 'END OF MIGRATION LOG', '='.repeat(80)];

    // Combine header, log buffer, and footer
    const logContent = [...header, ...result.logs, ...footer].join('\n');

    // Write to file
    writeFileSync(logFilePath, logContent, 'utf8');

    console.log(`📄 Migration log written to: ${logFilePath}`);
  } catch (error) {
    console.error(`Warning: Failed to write log file: ${error}`);
  }
}

async function main(): Promise<void> {
  try {
    const { values, positionals } = parseArgs({
      args: process.argv.slice(2),
      options: {
        latest: { type: 'boolean' },
        timestamp: { type: 'string' },
        before: { type: 'string' },
        source: { type: 'string' },
        dest: { type: 'string' },
        'preserved-tables': { type: 'string' },
        'keep-tables': { type: 'string' },
        json: { type: 'boolean' },
        'dry-run': { type: 'boolean' },
        help: { type: 'boolean' },
      },
      allowPositionals: true,
    });

    if (values.help || positionals.length === 0) {
      printUsage();
      return;
    }

    const command = positionals[0];
    const dryRun = values['dry-run'] || false;

    switch (command) {
      case 'start':
        await handleStartCommand(values, dryRun);
        break;

      case 'prepare':
        await handlePrepareCommand(values, dryRun);
        break;

      case 'swap':
        await handleSwapCommand(values, dryRun);
        break;

      case 'status':
        await handleStatusCommand(values);
        break;

      case 'list':
      case 'rollback':
      case 'cleanup':
      case 'verify':
        await handleBackupCommand(command, values, dryRun);
        break;

      default:
        console.error(`Unknown command: ${command}`);
        printUsage();
        process.exit(1);
    }
  } catch (error) {
    console.error('\n❌ Error:', error instanceof Error ? error.message : error);
    process.exit(1);
  }
}

async function handlePrepareCommand(values: ParsedArgs, dryRun: boolean): Promise<void> {
  const sourceUrl = values.source || process.env.SOURCE_DATABASE_URL;
  const destUrl = values.dest || process.env.DEST_DATABASE_URL || process.env.DATABASE_URL;
  const preservedTablesEnv = values['preserved-tables'] || process.env.PRESERVED_TABLES || '';

  if (!sourceUrl || !destUrl) {
    console.error('❌ Prepare command requires:');
    console.error('   --source <url> - Source database connection string');
    console.error('   --dest <url> - Destination database connection string');
    console.error('   Optional: --preserved-tables <table1,table2> - Tables to preserve');
    console.error('   Optional: --dry-run - Preview mode');
    process.exit(1);
  }

  const preservedTables = preservedTablesEnv
    .split(',')
    .map((table: string) => table.trim())
    .filter((table: string) => table.length > 0);

  const sourceConfig = parseDatabaseUrl(sourceUrl);
  const destConfig = parseDatabaseUrl(destUrl);

  console.log('🚀 Database Migration Tool - Preparation Phase');
  console.log(`📍 Source: ${sourceConfig.host}:${sourceConfig.port}/${sourceConfig.database}`);
  console.log(`📍 Destination: ${destConfig.host}:${destConfig.port}/${destConfig.database}`);
  console.log(`🔒 Preserved tables: ${preservedTables.join(', ') || 'none'}`);
  console.log('');

  const migrator = new DatabaseMigrator(sourceConfig, destConfig, preservedTables, dryRun);

  try {
    const result: PreparationResult = await migrator.prepareMigration();

    if (result.success) {
      if (dryRun) {
        console.log('\n✅ Dry run preparation completed successfully!');
        console.log('💡 Review the analysis above and run without --dry-run when ready');
      } else {
        console.log('\n✅ Migration preparation completed successfully!');
        console.log(`📄 Migration ID: ${result.migrationId}`);
        console.log(`🔢 Timestamp: ${result.timestamp}`);
        console.log(`🔄 Active sync triggers: ${result.activeTriggers.length}`);
        console.log('📦 Shadow schema ready for swap');

        // Generate the exact swap command to run next
        let swapCommand = `npm run migration -- swap --dest "${destUrl}"`;
        if (preservedTables.length > 0) {
          swapCommand += ` --preserved-tables "${preservedTables.join(',')}"`;
        }
        swapCommand += ` --timestamp ${result.timestamp}`;

        console.log('');
        console.log('� Next step: Run the swap command to complete the migration:');
        console.log(`   ${swapCommand}`);
      }
      process.exit(0);
    } else {
      console.error('\n❌ Migration preparation failed:', result.error);
      process.exit(1);
    }
  } catch (error) {
    console.error('\n❌ Migration preparation failed with unexpected error:', error);
    process.exit(1);
  }
}

async function handleSwapCommand(values: ParsedArgs, dryRun: boolean): Promise<void> {
  const destUrl = values.dest || process.env.DEST_DATABASE_URL || process.env.DATABASE_URL;
  const preservedTablesEnv = values['preserved-tables'] || process.env.PRESERVED_TABLES || '';

  if (!destUrl) {
    console.error('❌ Swap command requires:');
    console.error('   --dest <url> - Destination database connection string');
    console.error(
      '   Optional: --preserved-tables <table1,table2> - Tables that should have sync triggers'
    );
    console.error('   Optional: --dry-run - Preview mode (not recommended for swap)');
    process.exit(1);
  }

  const destConfig = parseDatabaseUrl(destUrl);
  const preservedTables = preservedTablesEnv
    .split(',')
    .map((table: string) => table.trim())
    .filter((table: string) => table.length > 0);

  console.log('🔄 Database Migration Tool - Swap Phase');
  console.log(`📍 Destination: ${destConfig.host}:${destConfig.port}/${destConfig.database}`);
  console.log(`� Expected preserved tables: ${preservedTables.join(', ') || 'none'}`);
  console.log('');

  const migrator = new DatabaseMigrator({} as DatabaseConfig, destConfig, preservedTables, dryRun);

  try {
    const result: MigrationResult = await migrator.completeMigration(preservedTables);

    if (result.success) {
      console.log('\n✅ Migration swap completed successfully!');
      console.log('📦 Schema backup retained for rollback purposes');
      process.exit(0);
    } else {
      console.error('\n❌ Migration swap failed:', result.error);
      process.exit(1);
    }
  } catch (error) {
    console.error('\n❌ Migration swap failed with unexpected error:', error);
    process.exit(1);
  }
}

async function handleStatusCommand(values: ParsedArgs): Promise<void> {
  const destUrl = values.dest || process.env.DEST_DATABASE_URL || process.env.DATABASE_URL;
  const json = values.json || false;

  if (!destUrl) {
    console.error('❌ Status command requires:');
    console.error('   --dest <url> - Destination database connection string');
    console.error('   Optional: --json - Output as JSON');
    process.exit(1);
  }

  const destConfig = parseDatabaseUrl(destUrl);

  if (!json) {
    console.log('🔍 Database Migration Status');
    console.log(`📍 Destination: ${destConfig.host}:${destConfig.port}/${destConfig.database}`);
    console.log('');
  }

  // Create direct connection for status checking
  const { Pool } = await import('pg');
  const pool = new Pool({
    ...destConfig,
    ssl: destConfig.ssl !== false ? { rejectUnauthorized: false } : false,
  });

  try {
    const client = await pool.connect();

    try {
      // Check shadow schema
      const shadowExists = await client.query(`
        SELECT EXISTS (
          SELECT 1 FROM information_schema.schemata 
          WHERE schema_name = 'shadow'
        )
      `);

      // Count shadow tables
      const shadowTables = await client.query(`
        SELECT COUNT(*) as count
        FROM information_schema.tables 
        WHERE table_schema = 'shadow'
      `);

      // Count sync triggers
      const syncTriggers = await client.query(`
        SELECT COUNT(*) as count
        FROM information_schema.triggers 
        WHERE trigger_name LIKE 'sync_%_to_shadow_trigger'
      `);

      // Find backup schemas
      const backupSchemas = await client.query(`
        SELECT schema_name 
        FROM information_schema.schemata 
        WHERE schema_name LIKE 'backup_%'
        ORDER BY schema_name DESC
      `);

      const shadowSchemaExists = shadowExists.rows[0].exists;
      const shadowTableCount = parseInt(shadowTables.rows[0].count);
      const syncTriggerCount = parseInt(syncTriggers.rows[0].count);
      const backupCount = backupSchemas.rows.length;

      if (json) {
        console.log(
          JSON.stringify(
            {
              shadowSchemaExists,
              shadowTableCount,
              syncTriggerCount,
              backupCount,
              backupSchemas: backupSchemas.rows.map((r: { schema_name: string }) => r.schema_name),
              readyForSwap: shadowSchemaExists && shadowTableCount > 0,
            },
            null,
            2
          )
        );
        return;
      }

      // Human-readable status
      console.log(`📦 Shadow schema exists: ${shadowSchemaExists ? '✅' : '❌'}`);
      console.log(`📊 Shadow tables: ${shadowTableCount}`);
      console.log(`🔄 Active sync triggers: ${syncTriggerCount}`);
      console.log(`�️  Backup schemas: ${backupCount}`);

      if (backupCount > 0) {
        console.log(
          `   Latest backups: ${backupSchemas.rows
            .slice(0, 3)
            .map((r: { schema_name: string }) => r.schema_name)
            .join(', ')}`
        );
      }

      console.log('');

      if (shadowSchemaExists && shadowTableCount > 0) {
        console.log('🚀 Status: READY FOR SWAP');
        console.log('💡 Run the swap command to complete the migration');
      } else if (shadowSchemaExists && shadowTableCount === 0) {
        console.log('⚠️  Status: SHADOW SCHEMA EMPTY');
        console.log('💡 Run the prepare command to populate the shadow schema');
      } else {
        console.log('� Status: NO MIGRATION PREPARED');
        console.log('💡 Run the prepare command to start a new migration');
      }
    } finally {
      client.release();
    }

    process.exit(0);
  } catch (error) {
    console.error('\n❌ Failed to get migration status:', error);
    process.exit(1);
  } finally {
    await pool.end();
  }
}

async function handleStartCommand(values: ParsedArgs, dryRun: boolean): Promise<void> {
  const sourceUrl = values.source || process.env.SOURCE_DATABASE_URL;
  const destUrl = values.dest || process.env.DEST_DATABASE_URL || process.env.DATABASE_URL;
  const preservedTablesEnv = values['preserved-tables'] || process.env.PRESERVED_TABLES || '';

  if (!sourceUrl || !destUrl) {
    console.error('❌ Start command requires:');
    console.error('   --source <url> - Source database connection string');
    console.error('   --dest <url> - Destination database connection string');
    console.error('   Optional: --preserved-tables <table1,table2> - Tables to preserve');
    console.error('   Optional: --dry-run - Preview mode');
    process.exit(1);
  }

  const preservedTables = preservedTablesEnv
    .split(',')
    .map((table: string) => table.trim())
    .filter((table: string) => table.length > 0);

  const sourceConfig = parseDatabaseUrl(sourceUrl);
  const destConfig = parseDatabaseUrl(destUrl);

  console.log('🚀 Database Migration Tool');
  console.log(`📍 Source: ${sourceConfig.host}:${sourceConfig.port}/${sourceConfig.database}`);
  console.log(`📍 Destination: ${destConfig.host}:${destConfig.port}/${destConfig.database}`);
  console.log(`🔒 Preserved tables: ${preservedTables.join(', ') || 'none'}`);
  console.log('');

  const migrator = new DatabaseMigrator(sourceConfig, destConfig, preservedTables, dryRun);

  try {
    const result = await migrator.migrate();

    // Write log file to disk
    writeLogFile(result, sourceConfig, destConfig);

    if (result.success) {
      if (dryRun) {
        console.log('\n✅ Dry run completed successfully!');
        console.log('💡 Review the analysis above and run without --dry-run when ready');
      } else {
        console.log('\n✅ Migration completed successfully!');
        console.log('📦 Schema backup retained for rollback purposes');
      }
      process.exit(0);
    } else {
      console.error('\n❌ Migration failed:', result.error);
      process.exit(1);
    }
  } catch (error) {
    console.error('\n❌ Migration failed with unexpected error:', error);
    process.exit(1);
  }
}

async function handleBackupCommand(
  command: string,
  values: ParsedArgs,
  dryRun: boolean
): Promise<void> {
  // Database configuration for single-database backup operations
  const config: DatabaseConfig = {
    host: process.env.DB_HOST || 'localhost',
    port: parseInt(process.env.DB_PORT || '5432'),
    database: process.env.DB_NAME || 'postgres',
    user: process.env.DB_USER || 'postgres',
    password: process.env.DB_PASSWORD || '',
  };

  const manager = new MigrationManager(config, dryRun);

  switch (command) {
    case 'list':
      await manager.listBackups(values.json || false);
      break;

    case 'rollback': {
      let target: number | 'latest';
      if (values.latest) {
        target = 'latest';
      } else if (values.timestamp) {
        target = parseInt(values.timestamp);
        if (isNaN(target)) {
          throw new Error('Invalid timestamp format');
        }
      } else {
        throw new Error('Rollback requires --latest or --timestamp option');
      }

      const keepTables = values['keep-tables']
        ? values['keep-tables'].split(',').map((t: string) => t.trim())
        : [];

      await manager.rollback(target, keepTables);
      break;
    }

    case 'cleanup':
      if (!values.before) {
        throw new Error('Cleanup requires --before option');
      }
      await manager.cleanup(values.before);
      break;

    case 'verify': {
      if (!values.timestamp) {
        throw new Error('Verify requires --timestamp option');
      }
      const verifyTimestamp = parseInt(values.timestamp);
      if (isNaN(verifyTimestamp)) {
        throw new Error('Invalid timestamp format');
      }
      await manager.verify(verifyTimestamp);
      break;
    }

    default:
      throw new Error(`Unknown backup command: ${command}`);
  }
}

// Execute if called directly
if (process.argv[1] === fileURLToPath(import.meta.url)) {
  main();
}

export { MigrationManager };
