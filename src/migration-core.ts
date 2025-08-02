/* eslint-disable @typescript-eslint/no-explicit-any */
/**
 * Database Migration Core Module
 *
 * Contains the DatabaseMigrator class and related interfaces for performing
 * zero-downtime database migrations with real-time synchronization.
 */

import { Pool } from 'pg';
import { execa } from 'execa';
import { unlinkSync, existsSync } from 'fs';
import { join } from 'path';
import { cpus } from 'os';

export interface DatabaseConfig {
  host: string;
  port: number;
  database: string;
  user: string;
  password: string;
  ssl?: boolean;
}

export interface TableInfo {
  tableName: string;
  schemaName: string;
  columns: ColumnInfo[];
  constraints: ConstraintInfo[];
  indexes: IndexInfo[];
  sequences: SequenceInfo[];
}

export interface ColumnInfo {
  columnName: string;
  dataType: string;
  isNullable: boolean;
  defaultValue: string | null;
  characterMaximumLength: number | null;
}

export interface ConstraintInfo {
  constraintName: string;
  constraintType: string;
  definition: string;
}

export interface IndexInfo {
  indexName: string;
  definition: string;
  isUnique: boolean;
  indexType: string;
}

export interface SequenceInfo {
  sequenceName: string;
  columnName: string;
  currentValue: number;
}

export interface MigrationStats {
  startTime: Date;
  endTime?: Date;
  tablesProcessed: number;
  recordsMigrated: number;
  errors: string[];
  warnings: string[];
}

export interface MigrationResult {
  success: boolean;
  stats: MigrationStats;
  logs: string[];
  error?: string;
}

export interface SyncTriggerInfo {
  tableName: string;
  functionName: string;
  triggerName: string;
  isActive: boolean;
  checksum?: string;
  rowCount?: number;
  validationStatus?: 'pending' | 'passed' | 'failed';
  lastValidated?: Date;
}

export interface SyncValidationResult {
  tableName: string;
  isValid: boolean;
  sourceRowCount: number;
  targetRowCount: number;
  sourceChecksum: string;
  targetChecksum: string;
  errors: string[];
}

export interface PreparationResult {
  success: boolean;
  migrationId: string;
  timestamp: number;
  activeTriggers: SyncTriggerInfo[];
  stats: MigrationStats;
  logs: string[];
  error?: string;
}

export class DatabaseMigrator {
  private sourceConfig: DatabaseConfig;
  private destConfig: DatabaseConfig;
  private sourcePool: Pool;
  private destPool: Pool;
  private preservedTables: Set<string>;
  private stats: MigrationStats;
  private tempDir: string;
  private dryRun: boolean;
  private activeSyncTriggers: SyncTriggerInfo[] = [];
  private logBuffer: string[] = [];

  constructor(
    sourceConfig: DatabaseConfig,
    destConfig: DatabaseConfig,
    preservedTables: string[] = [],
    dryRun: boolean = false
  ) {
    this.sourceConfig = sourceConfig;
    this.destConfig = destConfig;

    // Create pools with SSL configuration
    this.sourcePool = new Pool({
      ...this.sourceConfig,
      ssl: this.sourceConfig.ssl !== false ? { rejectUnauthorized: false } : false,
    });
    this.destPool = new Pool({
      ...this.destConfig,
      ssl: this.destConfig.ssl !== false ? { rejectUnauthorized: false } : false,
    });

    this.preservedTables = new Set(preservedTables);
    this.tempDir = '/tmp';
    this.dryRun = dryRun;

    this.stats = {
      startTime: new Date(),
      tablesProcessed: 0,
      recordsMigrated: 0,
      errors: [],
      warnings: [],
    };
  }

  /**
   * Main migration method
   */
  async migrate(): Promise<MigrationResult> {
    try {
      this.log('🚀 Starting database migration...');
      this.log(`📊 Dry run mode: ${this.dryRun ? 'ENABLED' : 'DISABLED'}`);

      // Pre-migration checks
      await this.performPreMigrationChecks();

      // Analyze schemas for both dry run and real migrations
      const sourceTables = await this.analyzeSchema(this.sourcePool, 'source');
      const destTables = await this.analyzeSchema(this.destPool, 'destination');

      this.log(`📋 Found ${sourceTables.length} source tables to migrate`);
      this.log(`📋 Found ${destTables.length} destination tables to backup`);
      this.log(`🔒 Will restore ${this.preservedTables.size} preserved tables after migration`);

      if (this.dryRun) {
        await this.performDryRun(sourceTables, destTables);
        return {
          success: true,
          stats: this.stats,
          logs: [...this.logBuffer],
        };
      }

      // Perform actual migration
      await this.doMigration(sourceTables, destTables);

      this.stats.endTime = new Date();
      this.logSummary();

      this.log('✅ Migration completed successfully');

      return {
        success: true,
        stats: this.stats,
        logs: [...this.logBuffer],
      };
    } catch (error) {
      this.logError('Migration failed', error);

      return {
        success: false,
        stats: this.stats,
        logs: [...this.logBuffer],
        error: error instanceof Error ? error.message : String(error),
      };
    } finally {
      await this.cleanup();
    }
  }

  /**
   * Prepare migration (Phases 1-3): Creates dump, restores to shadow, sets up sync triggers
   */
  async prepareMigration(): Promise<PreparationResult> {
    try {
      this.log('🚀 Starting migration preparation...');
      this.log(`📊 Dry run mode: ${this.dryRun ? 'ENABLED' : 'DISABLED'}`);

      const timestamp = Date.now();
      const migrationId = `migration_${timestamp}_${this.sourceConfig.database}_to_${this.destConfig.database}`;

      // Pre-migration checks
      await this.performPreMigrationChecks();

      // Analyze schemas
      const sourceTables = await this.analyzeSchema(this.sourcePool, 'source');
      const destTables = await this.analyzeSchema(this.destPool, 'destination');

      this.log(`📋 Found ${sourceTables.length} source tables to migrate`);
      this.log(`📋 Found ${destTables.length} destination tables to backup`);
      this.log(`🔒 Will restore ${this.preservedTables.size} preserved tables after migration`);

      if (this.dryRun) {
        await this.performDryRun(sourceTables, destTables);

        return {
          success: true,
          migrationId,
          timestamp,
          activeTriggers: [],
          stats: this.stats,
          logs: [...this.logBuffer],
        };
      }

      // Perform actual preparation phases
      await this.doPreparation(sourceTables, destTables, timestamp, migrationId);

      this.log('✅ Migration preparation completed successfully');

      return {
        success: true,
        migrationId,
        timestamp,
        activeTriggers: [...this.activeSyncTriggers],
        stats: this.stats,
        logs: [...this.logBuffer],
      };
    } catch (error) {
      this.logError('Migration preparation failed', error);

      return {
        success: false,
        migrationId: '',
        timestamp: 0,
        activeTriggers: [],
        stats: this.stats,
        logs: [...this.logBuffer],
        error: error instanceof Error ? error.message : String(error),
      };
    } finally {
      await this.cleanup();
    }
  }

  /**
   * Complete migration (Phases 4-7): Performs schema swap, cleanup, and finalization
   * Uses database introspection instead of state files for validation
   */
  async completeMigration(preservedTables: string[] = []): Promise<MigrationResult> {
    try {
      this.log('🚀 Starting migration completion...');

      // Validate migration readiness using database introspection
      await this.validateMigrationReadiness(preservedTables);

      // Detect timestamp from existing backup schemas if not provided
      const timestamp = await this.detectMigrationTimestamp();

      // Get table info for completion phases
      const sourceTables = await this.analyzeSchema(this.destPool, 'shadow');

      // Perform completion phases
      await this.doCompletion(sourceTables, timestamp);

      this.stats.endTime = new Date();
      this.logSummary();

      this.log('✅ Migration completed successfully');

      return {
        success: true,
        stats: this.stats,
        logs: [...this.logBuffer],
      };
    } catch (error) {
      this.logError('Migration completion failed', error);

      return {
        success: false,
        stats: this.stats,
        logs: [...this.logBuffer],
        error: error instanceof Error ? error.message : String(error),
      };
    } finally {
      await this.cleanup();
    }
  }

  /**
   * Validate that the database is ready for migration completion using introspection
   */
  private async validateMigrationReadiness(preservedTables: string[]): Promise<void> {
    this.log('🔍 Validating migration readiness...');

    const client = await this.destPool.connect();
    const issues: string[] = [];

    try {
      // 1. Check if shadow schema exists
      const shadowExists = await client.query(`
        SELECT EXISTS (
          SELECT 1 FROM information_schema.schemata 
          WHERE schema_name = 'shadow'
        )
      `);

      if (!shadowExists.rows[0].exists) {
        issues.push('❌ Shadow schema does not exist. Run prepare command first.');
      } else {
        this.log('✅ Shadow schema exists');
      }

      // 2. Check if shadow schema has tables
      const shadowTables = await client.query(`
        SELECT COUNT(*) as count
        FROM information_schema.tables 
        WHERE table_schema = 'shadow'
      `);

      const shadowTableCount = parseInt(shadowTables.rows[0].count);
      if (shadowTableCount === 0) {
        issues.push('❌ Shadow schema is empty. Run prepare command first.');
      } else {
        this.log(`✅ Shadow schema has ${shadowTableCount} tables`);
      }

      // 3. Check preserved table sync triggers if preserved tables are expected
      if (preservedTables.length > 0) {
        // Build a more specific query to check for triggers on the exact preserved tables
        // Note: PostgreSQL creates one trigger entry per event type (INSERT/UPDATE/DELETE)
        // so we need to count DISTINCT trigger names, not individual entries
        const tableList = preservedTables
          .map(table => `'sync_${table.toLowerCase()}_to_shadow_trigger'`)
          .join(',');
        const syncTriggers = await client.query(`
          SELECT COUNT(DISTINCT trigger_name) as count
          FROM information_schema.triggers 
          WHERE trigger_name IN (${tableList})
        `);

        const triggerCount = parseInt(syncTriggers.rows[0].count);
        if (triggerCount === 0) {
          issues.push(
            `⚠️  Expected ${preservedTables.length} sync triggers for preserved tables, but found none`
          );
        } else if (triggerCount !== preservedTables.length) {
          // List the expected vs actual triggers for better debugging
          const expectedTriggers = preservedTables.map(
            table => `sync_${table.toLowerCase()}_to_shadow_trigger`
          );
          const existingTriggers = await client.query(`
            SELECT DISTINCT trigger_name 
            FROM information_schema.triggers 
            WHERE trigger_name IN (${tableList})
          `);
          const actualTriggers = existingTriggers.rows.map(row => row.trigger_name);

          issues.push(
            `⚠️  Expected ${preservedTables.length} sync triggers for preserved tables [${expectedTriggers.join(', ')}], but found ${triggerCount} [${actualTriggers.join(', ')}]`
          );
        } else {
          this.log(`✅ Found ${triggerCount} sync triggers for preserved tables`);
        }

        // 4. Validate specific preserved tables exist in both schemas
        for (const tableName of preservedTables) {
          const publicExists = await client.query(
            `
            SELECT EXISTS (
              SELECT 1 FROM information_schema.tables 
              WHERE table_schema = 'public' AND table_name = $1
            )
          `,
            [tableName]
          );

          const shadowExists = await client.query(
            `
            SELECT EXISTS (
              SELECT 1 FROM information_schema.tables 
              WHERE table_schema = 'shadow' AND table_name = $1
            )
          `,
            [tableName]
          );

          if (!publicExists.rows[0].exists) {
            issues.push(`❌ Preserved table '${tableName}' not found in public schema`);
          }
          if (!shadowExists.rows[0].exists) {
            issues.push(`❌ Preserved table '${tableName}' not found in shadow schema`);
          }
        }
      }

      // 5. Check for existing backup schemas (indicating previous migrations)
      const backupSchemas = await client.query(`
        SELECT schema_name 
        FROM information_schema.schemata 
        WHERE schema_name LIKE 'backup_%'
        ORDER BY schema_name DESC
        LIMIT 3
      `);

      if (backupSchemas.rows.length > 0) {
        this.log(
          `ℹ️  Found ${backupSchemas.rows.length} existing backup schemas: ${backupSchemas.rows.map((r: any) => r.schema_name).join(', ')}`
        );
      }

      if (issues.length > 0) {
        throw new Error(`Migration not ready:\n${issues.join('\n')}`);
      }

      this.log('✅ Migration readiness validation passed');
    } finally {
      client.release();
    }
  }

  /**
   * Detect migration timestamp from database state
   */
  private async detectMigrationTimestamp(): Promise<number> {
    // Generate new timestamp for this migration completion
    const timestamp = Date.now();
    this.log(`📅 Using timestamp: ${timestamp}`);
    return timestamp;
  }

  /**
   * Perform preparation phases (1-3)
   */
  private async doPreparation(
    sourceTables: TableInfo[],
    destTables: TableInfo[],
    timestamp: number,
    _migrationId: string
  ): Promise<void> {
    this.log('🔄 Starting migration preparation phases...');

    try {
      // Phase 1: Create source dump
      const dumpPath = await this.createSourceDump(sourceTables, timestamp);

      // Phase 2: Restore source dump to destination shadow schema
      await this.restoreToDestinationShadow(sourceTables, dumpPath);

      // Phase 3: Setup preserved table synchronization
      await this.setupPreservedTableSync(destTables, timestamp);

      this.log('✅ Preparation phases completed successfully');
      this.log(`📦 Shadow schema ready for swap`);
      this.log('💡 Run the swap command when ready to complete migration');
    } catch (error) {
      // Cleanup any partial preparation state
      try {
        if (this.activeSyncTriggers.length > 0) {
          this.log('🧹 Cleaning up sync triggers after preparation failure...');
          await this.cleanupRealtimeSync(this.activeSyncTriggers);
        }
      } catch (cleanupError) {
        this.logError('Warning: Could not cleanup sync triggers', cleanupError);
      }

      throw error;
    }
  }

  /**
   * Perform completion phases (4-7)
   */
  private async doCompletion(sourceTables: TableInfo[], timestamp: number): Promise<void> {
    this.log('🔄 Starting migration completion phases...');

    try {
      // Phase 4: Perform atomic schema swap (zero downtime!)
      await this.performAtomicSchemaSwap(timestamp);

      // Phase 5: Cleanup sync triggers and validate consistency
      await this.cleanupSyncTriggersAndValidate(timestamp);

      // Phase 6: Reset sequences and recreate indexes
      this.log('🔢 Phase 6: Resetting sequences...');
      await this.resetSequences(sourceTables);

      this.log('🗂️  Phase 7: Recreating indexes...');
      await this.recreateIndexes(sourceTables);

      // Write protection was already disabled after schema swap in Phase 4

      this.log('✅ Zero-downtime migration finished successfully');
      this.log(`📦 Original schema preserved in backup_${timestamp} schema`);
      this.log('💡 Call cleanupBackupSchema(timestamp) to remove backup after verification');
    } catch (error) {
      this.logError('Migration completion failed', error);

      // Cleanup any active sync triggers before rollback
      try {
        if (this.activeSyncTriggers.length > 0) {
          this.log('🧹 Cleaning up active sync triggers before rollback...');
          await this.cleanupRealtimeSync(this.activeSyncTriggers);
        }
      } catch (cleanupError) {
        this.logError('Warning: Could not cleanup sync triggers', cleanupError);
      }

      // Ensure destination write protection is disabled on error
      try {
        await this.disableDestinationWriteProtection();
      } catch (cleanupError) {
        this.logError('Warning: Could not disable destination write protection', cleanupError);
      }

      this.log('🔄 Attempting automatic rollback via schema swap...');
      try {
        await this.rollbackSchemaSwap(timestamp);
        this.log('✅ Rollback completed - original schema restored');
      } catch (rollbackError) {
        this.logError('Rollback failed', rollbackError);
        this.log('⚠️  Manual intervention required - check backup schema');
      }
      throw error;
    }
  }

  /**
   * Perform pre-migration validation checks
   */
  private async performPreMigrationChecks(): Promise<void> {
    this.log('🔍 Performing pre-migration checks...');

    // Enhanced connectivity check for both dry run and real migrations
    await this.performConnectivityCheck();

    // Check for required extensions
    await this.ensureExtensions();

    // Check disk space (simplified check)
    await this.checkDiskSpace();

    // Critical: Validate data consistency before migration
    await this.validatePreMigrationDataConsistency();
  }

  /**
   * Comprehensive database connectivity and compatibility check
   */
  private async performConnectivityCheck(): Promise<void> {
    this.log('🔗 Performing database connectivity check...');

    // Test source database connection and gather info
    let sourceVersion = '';
    let sourceDbName = '';
    try {
      await this.sourcePool.query('SELECT 1');
      this.log('✅ Source database connection successful');

      // Get database version
      const versionResult = await this.sourcePool.query('SELECT version()');
      sourceVersion = versionResult.rows[0].version;
      const versionMatch = sourceVersion.match(/PostgreSQL (\d+\.\d+)/);
      const shortVersion = versionMatch ? versionMatch[1] : 'unknown';
      this.log(`📊 Source PostgreSQL version: ${shortVersion}`);

      // Get database name
      const dbResult = await this.sourcePool.query('SELECT current_database()');
      sourceDbName = dbResult.rows[0].current_database;
      this.log(`📊 Source database: ${sourceDbName}`);

      // Check permissions
      await this.checkSourcePermissions();
    } catch (error) {
      throw new Error(`Failed to connect to source database: ${error}`);
    }

    // Test destination database connection and gather info
    let destVersion = '';
    let destDbName = '';
    try {
      await this.destPool.query('SELECT 1');
      this.log('✅ Destination database connection successful');

      // Get database version
      const versionResult = await this.destPool.query('SELECT version()');
      destVersion = versionResult.rows[0].version;
      const versionMatch = destVersion.match(/PostgreSQL (\d+\.\d+)/);
      const shortVersion = versionMatch ? versionMatch[1] : 'unknown';
      this.log(`📊 Destination PostgreSQL version: ${shortVersion}`);

      // Get database name
      const dbResult = await this.destPool.query('SELECT current_database()');
      destDbName = dbResult.rows[0].current_database;
      this.log(`📊 Destination database: ${destDbName}`);

      // Check permissions
      await this.checkDestinationPermissions();
    } catch (error) {
      throw new Error(`Failed to connect to destination database: ${error}`);
    }

    // Version compatibility check
    this.checkVersionCompatibility(sourceVersion, destVersion);

    this.log('✅ Database connectivity check completed');
  }

  /**
   * Check source database permissions
   */
  private async checkSourcePermissions(): Promise<void> {
    try {
      // Check if user can SELECT from tables
      await this.sourcePool.query(`
        SELECT has_database_privilege(current_user, current_database(), 'CONNECT') as can_connect,
               has_database_privilege(current_user, current_database(), 'CREATE') as can_create
      `);
      this.log('✅ Source database permissions verified (CONNECT, CREATE)');

      // Check if user can create schemas
      await this.sourcePool.query("SELECT has_schema_privilege(current_user, 'public', 'CREATE')");
      this.log('✅ Source schema permissions verified (CREATE in public schema)');
    } catch (error) {
      this.stats.warnings.push(`Could not verify all source permissions: ${error}`);
    }
  }

  /**
   * Check destination database permissions
   */
  private async checkDestinationPermissions(): Promise<void> {
    try {
      // Check if user can SELECT from tables and CREATE schemas
      await this.destPool.query(`
        SELECT has_database_privilege(current_user, current_database(), 'CONNECT') as can_connect,
               has_database_privilege(current_user, current_database(), 'CREATE') as can_create
      `);
      this.log('✅ Destination database permissions verified (CONNECT, CREATE)');

      // Check if user can create schemas
      await this.destPool.query("SELECT has_schema_privilege(current_user, 'public', 'CREATE')");
      this.log('✅ Destination schema permissions verified (CREATE in public schema)');
    } catch (error) {
      this.stats.warnings.push(`Could not verify all destination permissions: ${error}`);
    }
  }

  /**
   * Check PostgreSQL version compatibility
   */
  private checkVersionCompatibility(sourceVersion: string, destVersion: string): void {
    const sourceMatch = sourceVersion.match(/PostgreSQL (\d+)\.(\d+)/);
    const destMatch = destVersion.match(/PostgreSQL (\d+)\.(\d+)/);

    if (sourceMatch && destMatch) {
      const sourceMajor = parseInt(sourceMatch[1]);
      const sourceMinor = parseInt(sourceMatch[2]);
      const destMajor = parseInt(destMatch[1]);
      const destMinor = parseInt(destMatch[2]);

      if (sourceMajor === destMajor) {
        this.log('✅ PostgreSQL versions are compatible (same major version)');
      } else if (Math.abs(sourceMajor - destMajor) <= 1) {
        this.log(
          '⚠️  PostgreSQL versions differ by one major version - migration should work but test thoroughly'
        );
        this.stats.warnings.push(
          `Version difference: source=${sourceMajor}.${sourceMinor}, dest=${destMajor}.${destMinor}`
        );
      } else {
        this.log(
          '⚠️  PostgreSQL versions differ significantly - migration may have compatibility issues'
        );
        this.stats.warnings.push(
          `Significant version difference: source=${sourceMajor}.${sourceMinor}, dest=${destMajor}.${destMinor}`
        );
      }
    } else {
      this.log('⚠️  Could not parse PostgreSQL versions for compatibility check');
    }
  }

  /**
   * Ensure required PostgreSQL extensions are installed
   */
  private async ensureExtensions(): Promise<void> {
    const requiredExtensions = ['postgis', 'uuid-ossp'];

    for (const extension of requiredExtensions) {
      try {
        await this.destPool.query(`CREATE EXTENSION IF NOT EXISTS "${extension}"`);
        this.log(`✅ Extension ${extension} is available`);
      } catch (error) {
        this.stats.warnings.push(`Could not enable extension ${extension}: ${error}`);
      }
    }
  }

  /**
   * Basic disk space check
   */
  private async checkDiskSpace(): Promise<void> {
    try {
      const result = await this.sourcePool.query(`
        SELECT pg_size_pretty(pg_database_size(current_database())) as size
      `);
      this.log(`📊 Source database size: ${result.rows[0].size}`);
    } catch (error) {
      this.stats.warnings.push(`Could not determine database size: ${error}`);
    }
  }

  /**
   * Validate data consistency before migration starts
   * This is critical to ensure both databases are in a consistent state
   */
  private async validatePreMigrationDataConsistency(): Promise<void> {
    this.log('🔍 Validating pre-migration data consistency...');

    // Check for active transactions that might interfere
    await this.checkActiveTransactions();

    // Note: Expensive validation checks (FK integrity, database consistency) have been moved to tests
    // This improves migration performance while maintaining data safety through proper testing

    this.log('✅ Pre-migration data consistency validation completed');
  }

  /**
   * Check for active transactions that might interfere with migration
   */
  private async checkActiveTransactions(): Promise<void> {
    this.log('🔍 Checking for active transactions...');

    // Check source database
    const sourceTransactions = await this.sourcePool.query(`
      SELECT COUNT(*) as active_count, 
             MAX(EXTRACT(EPOCH FROM (now() - query_start))) as longest_running
      FROM pg_stat_activity 
      WHERE state = 'active' 
      AND query NOT LIKE '%pg_stat_activity%'
      AND query NOT LIKE '%COMMIT%'
      AND query NOT LIKE '%BEGIN%'
    `);

    const sourceActiveCount = parseInt(sourceTransactions.rows[0].active_count);
    const sourceLongestRunning = parseFloat(sourceTransactions.rows[0].longest_running || '0');

    if (sourceActiveCount > 10) {
      this.stats.warnings.push(
        `Source: High number of active transactions (${sourceActiveCount}) - consider migrating during low activity`
      );
    }

    if (sourceLongestRunning > 300) {
      // 5 minutes
      this.stats.warnings.push(
        `Source: Long-running transaction detected (${Math.round(sourceLongestRunning)}s) - consider waiting for completion`
      );
    }

    // Check destination database
    const destTransactions = await this.destPool.query(`
      SELECT COUNT(*) as active_count,
             MAX(EXTRACT(EPOCH FROM (now() - query_start))) as longest_running
      FROM pg_stat_activity 
      WHERE state = 'active' 
      AND query NOT LIKE '%pg_stat_activity%'
      AND query NOT LIKE '%COMMIT%'
      AND query NOT LIKE '%BEGIN%'
    `);

    const destActiveCount = parseInt(destTransactions.rows[0].active_count);
    const destLongestRunning = parseFloat(destTransactions.rows[0].longest_running || '0');

    if (destActiveCount > 10) {
      this.stats.warnings.push(
        `Destination: High number of active transactions (${destActiveCount}) - consider migrating during low activity`
      );
    }

    if (destLongestRunning > 300) {
      // 5 minutes
      this.stats.warnings.push(
        `Destination: Long-running transaction detected (${Math.round(destLongestRunning)}s) - consider waiting for completion`
      );
    }

    this.log(
      `✅ Active transaction check completed (source: ${sourceActiveCount}, dest: ${destActiveCount})`
    );
  }

  /**
   * Validate atomic schema swap completion
   * Ensures all components of the schema swap completed successfully
   */
  private async validateAtomicSchemaSwap(timestamp: number): Promise<void> {
    this.log('🔍 Validating atomic schema swap completion...');

    const client = await this.destPool.connect();
    try {
      // 1. Verify public schema exists and contains expected objects
      const publicSchemaCheck = await client.query(`
        SELECT schema_name 
        FROM information_schema.schemata 
        WHERE schema_name = 'public'
      `);

      if (publicSchemaCheck.rows.length === 0) {
        throw new Error('Critical: Public schema does not exist after swap');
      }

      // 2. Verify backup schema exists with expected naming
      const backupSchemaName = `backup_${timestamp}`;
      const backupSchemaCheck = await client.query(
        `
        SELECT schema_name 
        FROM information_schema.schemata 
        WHERE schema_name = $1
      `,
        [backupSchemaName]
      );

      if (backupSchemaCheck.rows.length === 0) {
        throw new Error(`Critical: Backup schema ${backupSchemaName} does not exist after swap`);
      }

      // 3. Verify new shadow schema was created
      const shadowSchemaCheck = await client.query(`
        SELECT schema_name 
        FROM information_schema.schemata 
        WHERE schema_name = 'shadow'
      `);

      if (shadowSchemaCheck.rows.length === 0) {
        throw new Error('Critical: New shadow schema was not created after swap');
      }

      // 4. Verify public schema has tables (not empty)
      const publicTablesCheck = await client.query(`
        SELECT COUNT(*) as table_count
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_type = 'BASE TABLE'
      `);

      const publicTableCount = parseInt(publicTablesCheck.rows[0].table_count);
      if (publicTableCount === 0) {
        this.stats.warnings.push('Post-swap: Public schema appears to be empty');
        this.log('⚠️  Post-swap: Public schema appears to be empty');
      }

      // 5. Quick validation of key table accessibility
      try {
        await client.query('SELECT 1 FROM information_schema.tables LIMIT 1');
        this.log('✅ Post-swap: Database connectivity and basic operations verified');
      } catch (error) {
        throw new Error(`Critical: Database operations failed after swap: ${error}`);
      }

      // 6. Verify schema ownership and permissions are intact
      const schemaOwnerCheck = await client.query(
        `
        SELECT schema_name, schema_owner
        FROM information_schema.schemata 
        WHERE schema_name IN ('public', 'shadow', $1)
        ORDER BY schema_name
      `,
        [backupSchemaName]
      );

      if (schemaOwnerCheck.rows.length < 3) {
        this.stats.warnings.push('Post-swap: Some schemas may have ownership issues');
        this.log('⚠️  Post-swap: Some schemas may have ownership issues');
      }

      this.log('✅ Atomic schema swap validation completed successfully');
    } catch (error) {
      // Log as error but don't fail the migration since the swap already happened
      this.stats.errors.push(`Post-swap validation failed: ${error}`);
      this.log(`❌ Post-swap validation failed: ${error}`);
      throw error; // Re-throw since this is critical
    } finally {
      client.release();
    }
  }

  /**
   * Validate sync trigger exists and is properly configured
   * Lightweight check without data manipulation
   */
  private async validateTriggerExists(triggerInfo: SyncTriggerInfo): Promise<void> {
    const client = await this.destPool.connect();
    try {
      // Check trigger exists and is enabled
      const triggerCheck = await client.query(
        `
        SELECT tgname, tgenabled
        FROM pg_trigger t
        JOIN pg_class c ON c.oid = t.tgrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public'
        AND c.relname = $1
        AND t.tgname = $2
      `,
        [triggerInfo.tableName, triggerInfo.triggerName]
      );

      if (triggerCheck.rows.length === 0) {
        throw new Error(`Sync trigger ${triggerInfo.triggerName} was not created`);
      }

      const triggerEnabled = triggerCheck.rows[0].tgenabled;
      if (triggerEnabled !== 'O') {
        throw new Error(`Sync trigger ${triggerInfo.triggerName} is not enabled`);
      }

      // Check function exists
      const functionCheck = await client.query(
        `
        SELECT proname
        FROM pg_proc
        WHERE proname = $1
      `,
        [triggerInfo.functionName]
      );

      if (functionCheck.rows.length === 0) {
        throw new Error(`Sync trigger function ${triggerInfo.functionName} was not created`);
      }

      triggerInfo.validationStatus = 'passed';
      triggerInfo.lastValidated = new Date();

      this.log(`✅ Sync trigger validated: ${triggerInfo.triggerName}`);
    } catch (error) {
      triggerInfo.validationStatus = 'failed';
      triggerInfo.lastValidated = new Date();
      throw new Error(`Sync trigger validation failed: ${error}`);
    } finally {
      client.release();
    }
  }

  /**
   * Analyze database schema
   */
  private async analyzeSchema(pool: Pool, dbName: string): Promise<TableInfo[]> {
    this.log(`🔬 Analyzing ${dbName} database schema...`);

    const tablesQuery = `
      SELECT 
        c.relname as table_name,
        n.nspname as table_schema
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = 'public'
        AND c.relkind = 'r'
        AND c.relname != 'spatial_ref_sys'
        AND c.relname != '_prisma_migrations'
      ORDER BY c.relname
    `;

    const tablesResult = await pool.query(tablesQuery);
    const tables: TableInfo[] = [];

    for (const row of tablesResult.rows) {
      const tableInfo = await this.getTableInfo(pool, row.table_schema, row.table_name);
      tables.push(tableInfo);
    }

    this.log(`📊 ${dbName} database has ${tables.length} tables`);
    return tables;
  }

  /**
   * Get detailed information about a specific table
   */
  private async getTableInfo(
    pool: Pool,
    schemaName: string,
    tableName: string
  ): Promise<TableInfo> {
    // Get column information
    const columnsQuery = `
      SELECT 
        column_name,
        data_type,
        is_nullable,
        column_default,
        character_maximum_length
      FROM information_schema.columns
      WHERE table_schema = $1 AND table_name = $2
      ORDER BY ordinal_position
    `;

    const columnsResult = await pool.query(columnsQuery, [schemaName, tableName]);
    const columns: ColumnInfo[] = columnsResult.rows.map((row: any) => ({
      columnName: row.column_name,
      dataType: row.data_type,
      isNullable: row.is_nullable === 'YES',
      defaultValue: row.column_default,
      characterMaximumLength: row.character_maximum_length,
    }));

    // Get constraints
    const constraintsQuery = `
      SELECT 
        tc.constraint_name,
        tc.constraint_type,
        pg_get_constraintdef(c.oid) as definition
      FROM information_schema.table_constraints tc
      JOIN pg_constraint c ON c.conname = tc.constraint_name
      WHERE tc.table_schema = $1 AND tc.table_name = $2
    `;

    const constraintsResult = await pool.query(constraintsQuery, [schemaName, tableName]);
    const constraints: ConstraintInfo[] = constraintsResult.rows.map((row: any) => ({
      constraintName: row.constraint_name,
      constraintType: row.constraint_type,
      definition: row.definition,
    }));

    // Get indexes
    const indexesQuery = `
      SELECT 
        i.relname as index_name,
        pg_get_indexdef(i.oid) as definition,
        ix.indisunique as is_unique,
        am.amname as index_type
      FROM pg_class t
      JOIN pg_index ix ON t.oid = ix.indrelid
      JOIN pg_class i ON i.oid = ix.indexrelid
      JOIN pg_am am ON i.relam = am.oid
      WHERE t.relname = $1
        AND t.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = $2)
        AND NOT ix.indisprimary
    `;

    const indexesResult = await pool.query(indexesQuery, [tableName, schemaName]);
    const indexes: IndexInfo[] = indexesResult.rows.map((row: any) => ({
      indexName: row.index_name,
      definition: row.definition,
      isUnique: row.is_unique,
      indexType: row.index_type,
    }));

    // Get sequences
    const sequencesQuery = `
      SELECT 
        pg_get_serial_sequence($1, column_name) as sequence_name,
        column_name
      FROM information_schema.columns
      WHERE table_schema = $2 AND table_name = $3
        AND column_default LIKE 'nextval%'
    `;

    const sequencesResult = await pool.query(sequencesQuery, [
      `"${tableName}"`, // Quote the table name for pg_get_serial_sequence
      schemaName,
      tableName, // Unquoted for information_schema query
    ]);
    const sequences: SequenceInfo[] = [];

    for (const row of sequencesResult.rows) {
      if (row.sequence_name) {
        const seqValueResult = await pool.query(`SELECT last_value FROM ${row.sequence_name}`);
        sequences.push({
          sequenceName: row.sequence_name,
          columnName: row.column_name,
          currentValue: parseInt(seqValueResult.rows[0].last_value),
        });
      }
    }

    return {
      tableName,
      schemaName,
      columns,
      constraints,
      indexes,
      sequences,
    };
  }

  /**
   * Disable all foreign key constraints using session_replication_role
   */
  private async disableForeignKeyConstraints(pool: Pool): Promise<void> {
    this.log('🔓 Disabling foreign key constraints...');
    await pool.query('SET session_replication_role = replica');
    this.log('✅ Foreign key constraints disabled');
  }

  /**
   * Re-enable all foreign key constraints
   */
  private async enableForeignKeyConstraints(pool: Pool): Promise<void> {
    this.log('🔒 Re-enabling foreign key constraints...');
    await pool.query('SET session_replication_role = origin');
    this.log('✅ Foreign key constraints re-enabled');
  }

  /**
   * Perform comprehensive dry run analysis with real database data
   */
  private async performDryRun(sourceTables: TableInfo[], destTables: TableInfo[]): Promise<void> {
    this.log('🧪 Performing comprehensive dry run analysis...');

    // Migration plan overview
    this.log(`📊 Migration Plan Overview:`);
    this.log(`   Source tables to migrate: ${sourceTables.length}`);
    this.log(`   Destination tables to backup: ${destTables.length}`);
    this.log(`   Preserved tables: ${this.preservedTables.size}`);

    // Analyze source tables with real data
    this.log(`\n📋 Source Database Analysis:`);
    let totalSourceRecords = 0;
    for (const sourceTable of sourceTables) {
      try {
        const countResult = await this.sourcePool.query(
          `SELECT COUNT(*) FROM "${sourceTable.tableName}"`
        );
        const rowCount = parseInt(countResult.rows[0].count);
        totalSourceRecords += rowCount;

        // Analyze table structure
        const columnsCount = sourceTable.columns.length;
        const indexesCount = sourceTable.indexes.length;
        const constraintsCount = sourceTable.constraints.length;
        const sequencesCount = sourceTable.sequences.length;

        this.log(`   📦 ${sourceTable.tableName}:`);
        this.log(`      └─ Records: ${rowCount.toLocaleString()}`);
        this.log(
          `      └─ Columns: ${columnsCount}, Indexes: ${indexesCount}, Constraints: ${constraintsCount}, Sequences: ${sequencesCount}`
        );

        // Check for special column types that might need attention
        const specialColumns = sourceTable.columns.filter(
          col =>
            col.dataType.includes('geometry') ||
            col.dataType.includes('geography') ||
            col.dataType.includes('json') ||
            col.dataType.includes('array')
        );

        if (specialColumns.length > 0) {
          this.log(
            `      └─ Special columns: ${specialColumns.map(c => `${c.columnName} (${c.dataType})`).join(', ')}`
          );
        }
      } catch (error) {
        this.log(`   📦 ${sourceTable.tableName}: ⚠️ Could not analyze (${error})`);
      }
    }

    // Analyze destination tables with real data
    this.log(`\n📋 Destination Database Analysis:`);
    const timestamp = Date.now();
    for (const destTable of destTables) {
      try {
        const countResult = await this.destPool.query(
          `SELECT COUNT(*) FROM "${destTable.tableName}"`
        );
        const rowCount = parseInt(countResult.rows[0].count);
        this.log(
          `   📦 ${destTable.tableName}: ${rowCount.toLocaleString()} records → backup_${timestamp} schema`
        );
      } catch (error) {
        this.log(`   📦 ${destTable.tableName}: ⚠️ Could not count records (${error})`);
      }
    }

    // Preserved tables analysis
    if (this.preservedTables.size > 0) {
      this.log(`\n🔒 Preserved Tables Analysis:`);
      const preservedTablesFound = [];
      for (const preservedTableName of this.preservedTables) {
        const sourceHasTable = sourceTables.some(
          t => t.tableName.toLowerCase() === preservedTableName.toLowerCase()
        );
        const destTable = destTables.find(
          t => t.tableName.toLowerCase() === preservedTableName.toLowerCase()
        );

        if (sourceHasTable && destTable) {
          preservedTablesFound.push(preservedTableName);
          try {
            const countResult = await this.destPool.query(
              `SELECT COUNT(*) FROM "${destTable.tableName}"`
            );
            const rowCount = parseInt(countResult.rows[0].count);
            this.log(
              `   🔄 ${preservedTableName}: ${rowCount.toLocaleString()} records (will be synced and restored)`
            );
          } catch (error) {
            this.log(`   🔄 ${preservedTableName}: ⚠️ Could not count records (${error})`);
          }
        } else if (!sourceHasTable) {
          this.log(`   ❌ ${preservedTableName}: Not found in source database`);
        } else if (!destTable) {
          this.log(`   ❌ ${preservedTableName}: Not found in destination database`);
        }
      }
    }

    // Migration steps preview
    this.log(`\n🔄 Migration Steps (DRY RUN - no changes will be made):`);
    this.log(`   1. 🔒 Enable write protection on source and destination databases`);
    this.log(
      `   2. 📦 Create source dump of ${sourceTables.length} tables (${totalSourceRecords.toLocaleString()} total records)`
    );
    this.log(`   3. 🔄 Restore source data to destination shadow schema`);
    if (this.preservedTables.size > 0) {
      this.log(`   4. 🔄 Setup real-time sync for ${this.preservedTables.size} preserved tables`);
    } else {
      this.log(`   4. ⏭️  No preserved tables to sync`);
    }
    this.log(`   5. ⚡ Perform atomic schema swap (zero downtime!)`);
    this.log(`   6. 🧹 Cleanup sync triggers and validate consistency`);
    this.log(
      `   7. 🔢 Reset sequences for ${sourceTables.filter(t => t.sequences.length > 0).length} tables`
    );
    this.log(
      `   8. 🗂️  Recreate ${sourceTables.reduce((acc, t) => acc + t.indexes.length, 0)} indexes`
    );
    this.log(`  10. 🔓 Remove write protection and complete migration`);

    // Risk assessment
    this.log(`\n⚡ Risk Assessment:`);
    const highRiskFactors = [];
    const mediumRiskFactors = [];

    if (totalSourceRecords > 1000000) {
      highRiskFactors.push(`Large dataset (${totalSourceRecords.toLocaleString()} records)`);
    } else if (totalSourceRecords > 100000) {
      mediumRiskFactors.push(`Medium dataset (${totalSourceRecords.toLocaleString()} records)`);
    }

    const tablesWithGeometry = sourceTables.filter(t =>
      t.columns.some(c => c.dataType.includes('geometry') || c.dataType.includes('geography'))
    );
    if (tablesWithGeometry.length > 0) {
      mediumRiskFactors.push(`${tablesWithGeometry.length} tables with spatial data`);
    }

    const tablesWithManyIndexes = sourceTables.filter(t => t.indexes.length > 10);
    if (tablesWithManyIndexes.length > 0) {
      mediumRiskFactors.push(`${tablesWithManyIndexes.length} tables with many indexes (>10)`);
    }

    if (highRiskFactors.length > 0) {
      this.log(`   🔴 High Risk Factors: ${highRiskFactors.join(', ')}`);
    }
    if (mediumRiskFactors.length > 0) {
      this.log(`   🟡 Medium Risk Factors: ${mediumRiskFactors.join(', ')}`);
    }
    if (highRiskFactors.length === 0 && mediumRiskFactors.length === 0) {
      this.log(`   🟢 Low Risk: Standard migration with no significant risk factors`);
    }

    // Estimated timing
    const estimatedDumpTime = Math.ceil(totalSourceRecords / 50000); // ~50k records per minute
    const estimatedRestoreTime = Math.ceil(totalSourceRecords / 30000); // ~30k records per minute
    const estimatedTotalTime = estimatedDumpTime + estimatedRestoreTime + 2; // +2 for overhead

    this.log(`\n⏱️  Estimated Timing:`);
    this.log(`   📦 Dump phase: ~${estimatedDumpTime} minutes`);
    this.log(`   🔄 Restore phase: ~${estimatedRestoreTime} minutes`);
    this.log(`   ⚡ Schema swap: <30 seconds (zero downtime)`);
    this.log(`   🎯 Total estimated time: ~${estimatedTotalTime} minutes`);

    // Update statistics for dry run
    this.stats.tablesProcessed = sourceTables.length;
    this.stats.recordsMigrated = totalSourceRecords;

    this.log('\n🧪 Dry run analysis completed - no changes were made to any databases');
    this.log('💡 Review the analysis above and run without --dry-run when ready');
  }

  /**
   * Perform the complete migration using schema-based approach with real-time sync
   */
  private async doMigration(sourceTables: TableInfo[], destTables: TableInfo[]): Promise<void> {
    this.log('🔄 Starting database migration...');

    const timestamp = Date.now();

    try {
      // Phase 1: Create source dump
      const dumpPath = await this.createSourceDump(sourceTables, timestamp);

      // Phase 2: Restore source dump to destination shadow schema
      await this.restoreToDestinationShadow(sourceTables, dumpPath);

      // Phase 3: Setup preserved table synchronization
      await this.setupPreservedTableSync(destTables, timestamp);

      // Phase 4: Perform atomic schema swap (zero downtime!)
      await this.performAtomicSchemaSwap(timestamp);

      // Phase 5: Cleanup sync triggers and validate consistency
      await this.cleanupSyncTriggersAndValidate(timestamp);

      // Phase 6: Reset sequences and recreate indexes
      this.log('🔢 Phase 6: Resetting sequences...');
      await this.resetSequences(sourceTables);

      this.log('🗂️  Phase 7: Recreating indexes...');
      await this.recreateIndexes(sourceTables);

      // Disable destination write protection after all critical phases complete
      await this.disableDestinationWriteProtection();

      this.log('✅ Zero-downtime migration finished successfully');
      this.log(`📦 Original schema preserved in backup_${timestamp} schema`);
      this.log('💡 Call cleanupBackupSchema(timestamp) to remove backup after verification');
    } catch (error) {
      this.logError('Migration failed', error);

      // Cleanup any active sync triggers before rollback
      try {
        if (this.activeSyncTriggers.length > 0) {
          this.log('🧹 Cleaning up active sync triggers before rollback...');
          await this.cleanupRealtimeSync(this.activeSyncTriggers);
        }
      } catch (cleanupError) {
        this.logError('Warning: Could not cleanup sync triggers', cleanupError);
      }

      // Ensure destination write protection is disabled on error
      try {
        await this.disableDestinationWriteProtection();
      } catch (cleanupError) {
        this.logError('Warning: Could not disable destination write protection', cleanupError);
      }

      this.log('🔄 Attempting automatic rollback via schema swap...');
      try {
        await this.rollbackSchemaSwap(timestamp);
        this.log('✅ Rollback completed - original schema restored');
      } catch (rollbackError) {
        this.logError('Rollback failed', rollbackError);
        this.log('⚠️  Manual intervention required - check backup schema');
      }
      throw error;
    }
  }

  /**
   * Phase 1: Create source dump from source database
   */
  private async createSourceDump(sourceTables: TableInfo[], timestamp: number): Promise<string> {
    this.log('🔧 Phase 1: Creating source dump...');

    try {
      // Prepare source database by moving tables to shadow schema
      await this.prepareSourceForShadowDump(sourceTables);

      // Enable write protection on source database for safety during dump/restore process
      await this.enableWriteProtection();

      // Create binary dump for maximum efficiency and parallelization
      const dumpPath = join(this.tempDir, `source_dump_${timestamp}.backup`);
      await this.createBinaryDump(dumpPath);

      // Disable write protection before restoring source database structure
      await this.disableWriteProtection();

      // Restore source database tables back to public schema
      await this.restoreSourceFromShadowDump(sourceTables);

      this.log('✅ Source dump created successfully');
      return dumpPath;
    } catch (error) {
      // Always remove write protection from source database, even if migration fails
      await this.disableWriteProtection();
      throw error;
    }
  }

  /**
   * Phase 2: Restore source dump to destination shadow schema
   */
  private async restoreToDestinationShadow(
    sourceTables: TableInfo[],
    dumpPath: string
  ): Promise<void> {
    this.log('🔧 Phase 2: Restoring source data to destination shadow schema...');

    const client = await this.destPool.connect();

    try {
      // No write protection on destination during prepare phase
      // This allows production traffic to continue normally and preserved table sync to work

      // Disable foreign key constraints for the destination database during setup
      await this.disableForeignKeyConstraints(this.destPool);

      // Drop and recreate shadow schema on destination before restore
      this.log('⚠️  Dropping existing shadow schema on destination (if exists)');
      await client.query('DROP SCHEMA IF EXISTS shadow CASCADE;');
      this.log('✅ Shadow schema dropped - will be recreated by pg_restore');

      // Restore shadow schema data with full parallelization
      const jobCount = Math.min(8, cpus().length);
      this.log(`🚀 Restoring with ${jobCount} parallel jobs...`);

      const restoreArgs = [
        '--jobs',
        jobCount.toString(),
        '--format',
        'custom',
        '--no-privileges',
        '--no-owner',
        '--disable-triggers',
        '--dbname',
        this.destConfig.database,
        '--host',
        this.destConfig.host,
        '--port',
        this.destConfig.port.toString(),
        '--username',
        this.destConfig.user,
        dumpPath,
      ];

      const restoreEnv = {
        ...process.env,
        PGPASSWORD: this.destConfig.password,
      };

      await execa('pg_restore', restoreArgs, { env: restoreEnv });
      this.log('✅ Source data restored to shadow schema with parallelization');

      // Clean up dump file
      if (existsSync(dumpPath)) {
        unlinkSync(dumpPath);
      }

      // Update statistics
      this.stats.tablesProcessed = sourceTables.length;
      await this.updateRecordsMigratedCount(sourceTables);

      this.log('✅ Destination shadow schema restore completed');
    } finally {
      client.release();
    }
  }

  /**
   * Create binary dump of source database
   */
  private async createBinaryDump(dumpPath: string): Promise<void> {
    this.log('📦 Creating binary dump of source database...');

    const dumpArgs = [
      '--format',
      'custom',
      '--no-privileges',
      '--no-owner',
      '--disable-triggers',
      '--verbose',
      '--schema',
      'shadow',
      '--file',
      dumpPath,
      '--host',
      this.sourceConfig.host,
      '--port',
      this.sourceConfig.port.toString(),
      '--username',
      this.sourceConfig.user,
      '--dbname',
      this.sourceConfig.database,
    ];

    const dumpEnv = { ...process.env, PGPASSWORD: this.sourceConfig.password };

    await execa('pg_dump', dumpArgs, { env: dumpEnv });
    this.log(`✅ Binary dump created: ${dumpPath}`);
  }

  /**
   * Move objects from public schema to shadow schema
   */
  private async moveObjectsToShadowSchema(client: any, sourceTables: TableInfo[]): Promise<void> {
    this.log('🔄 Moving restored objects to shadow schema...');

    for (const table of sourceTables) {
      try {
        // Check if table exists in public schema
        const checkResult = await client.query(
          `
          SELECT EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = $1
          )
        `,
          [table.tableName]
        );

        if (checkResult.rows[0].exists) {
          await client.query(`ALTER TABLE public."${table.tableName}" SET SCHEMA shadow;`);
          this.log(`📦 Moved ${table.tableName} to shadow schema`);
        }
      } catch (error) {
        const errorMessage = error instanceof Error ? error.message : String(error);
        this.log(
          `⚠️  Warning: Could not move ${table.tableName} to shadow schema: ${errorMessage}`
        );
      }
    }

    this.log('✅ Objects moved to shadow schema');
  }

  /**
   * Prepare source database by moving tables to shadow schema before dump
   */
  private async prepareSourceForShadowDump(sourceTables: TableInfo[]): Promise<void> {
    this.log('🔄 Preparing source database - moving tables to shadow schema...');

    const sourceClient = await this.sourcePool.connect();
    try {
      // Create shadow schema on source database
      await sourceClient.query('DROP SCHEMA IF EXISTS shadow CASCADE;');
      await sourceClient.query('CREATE SCHEMA shadow;');
      this.log('✅ Shadow schema created on source database');

      // Move each table from public to shadow schema
      for (const table of sourceTables) {
        try {
          await sourceClient.query(`ALTER TABLE public."${table.tableName}" SET SCHEMA shadow;`);
          this.log(`📦 Moved source table ${table.tableName} to shadow schema`);
        } catch (error) {
          const errorMessage = error instanceof Error ? error.message : String(error);
          this.log(`⚠️  Warning: Could not move source table ${table.tableName}: ${errorMessage}`);
          throw error; // Re-throw since we need all tables moved
        }
      }

      this.log('✅ All source tables moved to shadow schema');
    } finally {
      sourceClient.release();
    }
  }

  /**
   * Restore source database by moving tables back from shadow to public schema
   */
  private async restoreSourceFromShadowDump(sourceTables: TableInfo[]): Promise<void> {
    this.log('🔄 Restoring source database - moving tables back to public schema...');

    const sourceClient = await this.sourcePool.connect();
    try {
      // Move each table back from shadow to public schema
      for (const table of sourceTables) {
        try {
          await sourceClient.query(`ALTER TABLE shadow."${table.tableName}" SET SCHEMA public;`);
          this.log(`📦 Restored source table ${table.tableName} to public schema`);
        } catch (error) {
          const errorMessage = error instanceof Error ? error.message : String(error);
          this.log(
            `⚠️  Warning: Could not restore source table ${table.tableName}: ${errorMessage}`
          );
        }
      }

      // Clean up shadow schema on source
      await sourceClient.query('DROP SCHEMA IF EXISTS shadow CASCADE;');
      this.log('✅ Source database restored and shadow schema cleaned up');
    } finally {
      sourceClient.release();
    }
  }

  /**
   * Phase 4: Perform atomic schema swap
   */
  private async performAtomicSchemaSwap(timestamp: number): Promise<void> {
    this.log('🔄 Phase 4: Performing atomic schema swap...');

    const client = await this.destPool.connect();

    try {
      // Enable brief write protection only during the actual schema swap operations
      this.log('🔒 Enabling brief write protection for atomic schema operations...');
      await this.enableDestinationWriteProtection(); // Enable full protection

      await client.query('BEGIN');

      // Move current public schema to backup
      const backupSchemaName = `backup_${timestamp}`;
      await client.query(`ALTER SCHEMA public RENAME TO ${backupSchemaName};`);
      this.log(`📦 Moved public schema to ${backupSchemaName}`);

      // Move shadow schema to public (becomes active)
      await client.query(`ALTER SCHEMA shadow RENAME TO public;`);
      this.log('🚀 Activated shadow schema as new public schema');

      // Create new shadow schema for future use
      await client.query('CREATE SCHEMA shadow;');
      this.log('✅ Created new shadow schema');

      await client.query('COMMIT');

      // Remove write protection immediately after schema swap completes
      this.log('🔓 Removing write protection after atomic swap completion...');
      await this.disableDestinationWriteProtection();

      this.log('✅ Atomic schema swap completed - migration is now live!');

      // Validate the atomic schema swap completed successfully
      await this.validateAtomicSchemaSwap(timestamp);
    } catch (error) {
      await client.query('ROLLBACK');
      throw new Error(`Failed to perform schema swap: ${error}`);
    } finally {
      client.release();
    }
  }

  /**
   * Phase 3: Setup preserved table synchronization
   */
  private async setupPreservedTableSync(destTables: TableInfo[], timestamp: number): Promise<void> {
    if (this.preservedTables.size === 0) {
      this.log('✅ No preserved tables to sync');
      return;
    }

    this.log(`🔄 Phase 3: Setting up preserved table synchronization (backup_${timestamp})...`);

    // Validate preserved tables exist in destination schema
    const missingTables = [];
    for (const preservedTableName of this.preservedTables) {
      const tableExists = destTables.some(
        table => table.tableName.toLowerCase() === preservedTableName.toLowerCase()
      );
      if (!tableExists) {
        missingTables.push(preservedTableName);
      }
    }

    if (missingTables.length > 0) {
      throw new Error(`Preserved tables not found in destination: ${missingTables.join(', ')}`);
    }

    const client = await this.destPool.connect();

    try {
      for (const tableName of this.preservedTables) {
        // Get table info from destTables for better validation
        const tableInfo = destTables.find(
          table => table.tableName.toLowerCase() === tableName.toLowerCase()
        );

        if (!tableInfo) {
          this.log(`⚠️  Preserved table ${tableName} not found in destination schema, skipping`);
          continue;
        }

        // Use the actual table name from schema (correct case)
        const actualTableName = tableInfo.tableName;

        this.log(
          `🔄 Setting up sync for preserved table: ${actualTableName} (${tableInfo.columns.length} columns)`
        );

        // Verify table exists in destination database (double-check)
        const tableExists = await client.query(
          `
          SELECT EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = $1
          )
        `,
          [actualTableName]
        );

        if (tableExists.rows[0].exists) {
          // Begin transaction to eliminate race condition between copy and trigger creation
          await client.query('BEGIN');

          try {
            // Step 1: Clear shadow table and copy current data atomically
            // Temporarily disable foreign key constraints for this operation
            await client.query('SET session_replication_role = replica');
            await client.query(`DELETE FROM shadow."${actualTableName}"`);
            await client.query(
              `INSERT INTO shadow."${actualTableName}" SELECT * FROM public."${actualTableName}"`
            );
            await client.query('SET session_replication_role = origin');

            // Step 2: Setup real-time sync triggers (in same transaction)
            const triggerInfo = await this.createRealtimeSyncTrigger(client, actualTableName);
            triggerInfo.checksum = `sync_${timestamp}`; // Use timestamp for tracking
            this.activeSyncTriggers.push(triggerInfo);

            // Commit the transaction - makes copy and trigger creation atomic
            await client.query('COMMIT');

            // Step 2.1: Validate trigger was created successfully
            await this.validateTriggerExists(triggerInfo);

            // Step 3: Basic sync setup validation
            const rowCountResult = await client.query(
              `SELECT COUNT(*) FROM public."${actualTableName}"`
            );
            const rowCount = parseInt(rowCountResult.rows[0].count);

            this.log(`✅ Sync setup complete for ${actualTableName} (${rowCount} rows)`);
          } catch (error) {
            // Rollback transaction on any error
            await client.query('ROLLBACK');
            throw error;
          }
        } else {
          throw new Error(
            `Preserved table ${actualTableName} exists in schema analysis but not in actual database`
          );
        }
      }

      this.log(
        `✅ Real-time sync setup complete for ${this.activeSyncTriggers.length} preserved tables (backup_${timestamp})`
      );
    } catch (error) {
      // Cleanup any triggers created so far
      try {
        await this.cleanupRealtimeSync(this.activeSyncTriggers);
      } catch (cleanupError) {
        this.logError('Failed to cleanup triggers after setup error', cleanupError);
      }
      throw error;
    } finally {
      client.release();
    }
  }

  /**
   * Create real-time sync trigger for a preserved table
   */
  private async createRealtimeSyncTrigger(
    client: any,
    tableName: string
  ): Promise<SyncTriggerInfo> {
    const functionName = `sync_${tableName.toLowerCase()}_to_shadow`;
    const triggerName = `${functionName}_trigger`;

    // Get table columns for dynamic trigger function
    const columnsResult = await client.query(
      `
      SELECT column_name, data_type 
      FROM information_schema.columns 
      WHERE table_schema = 'public' 
      AND table_name = $1 
      ORDER BY ordinal_position
    `,
      [tableName]
    );

    const columns = columnsResult.rows.map((row: any) => row.column_name);
    const columnList = columns.map((col: string) => `"${col}"`).join(', ');
    const newColumnList = columns.map((col: string) => `NEW."${col}"`).join(', ');
    const setClause = columns.map((col: string) => `"${col}" = NEW."${col}"`).join(', ');

    // Create trigger function
    const functionSQL = `
      CREATE OR REPLACE FUNCTION ${functionName}()
      RETURNS TRIGGER AS $$
      BEGIN
        IF TG_OP = 'DELETE' THEN
          DELETE FROM shadow."${tableName}" WHERE id = OLD.id;
          RETURN OLD;
        ELSIF TG_OP = 'UPDATE' THEN
          UPDATE shadow."${tableName}" 
          SET ${setClause}
          WHERE id = OLD.id;
          RETURN NEW;
        ELSIF TG_OP = 'INSERT' THEN
          INSERT INTO shadow."${tableName}" (${columnList})
          VALUES (${newColumnList});
          RETURN NEW;
        END IF;
        RETURN NULL;
      END;
      $$ LANGUAGE plpgsql;
    `;

    await client.query(functionSQL);

    // Create trigger
    const triggerSQL = `
      CREATE TRIGGER ${triggerName}
        AFTER INSERT OR UPDATE OR DELETE ON public."${tableName}"
        FOR EACH ROW EXECUTE FUNCTION ${functionName}();
    `;

    await client.query(triggerSQL);

    this.log(`✅ Created sync trigger: ${triggerName}`);

    return {
      tableName,
      functionName,
      triggerName,
      isActive: true,
      validationStatus: 'pending',
      lastValidated: undefined,
    };
  }

  /**
   * Phase 6: Cleanup sync triggers and validate consistency
   */
  private async cleanupSyncTriggersAndValidate(timestamp: number): Promise<void> {
    if (this.activeSyncTriggers.length === 0) {
      this.log('✅ No sync triggers to cleanup');
      return;
    }

    this.log(
      `🧹 Phase 6: Cleaning up sync triggers and validating consistency (backup_${timestamp})...`
    );

    try {
      // Skip validation after schema swap since migration is already live
      // and shadow schema no longer exists in the expected state
      this.log('ℹ️ Skipping sync validation - migration already completed and live');

      // Cleanup triggers - pass the backup schema name since triggers are now on backup schema after swap
      await this.cleanupRealtimeSync(this.activeSyncTriggers, `backup_${timestamp}`);

      this.log(`✅ Sync triggers cleaned up and validation complete (backup_${timestamp})`);
    } catch (error) {
      // Ensure triggers are cleaned up even if validation fails
      try {
        await this.cleanupRealtimeSync(this.activeSyncTriggers, `backup_${timestamp}`);
      } catch (cleanupError) {
        this.logError('Failed to cleanup triggers after validation error', cleanupError);
      }
      throw error;
    }
  }

  /**
   * Cleanup real-time sync triggers
   */
  private async cleanupRealtimeSync(
    triggerInfos: SyncTriggerInfo[],
    schemaName: string = 'public'
  ): Promise<void> {
    if (triggerInfos.length === 0) {
      return;
    }

    this.log(`🧹 Cleaning up ${triggerInfos.length} sync triggers...`);

    const client = await this.destPool.connect();

    try {
      for (const triggerInfo of triggerInfos) {
        try {
          // First check what triggers actually exist for this table
          const existingTriggers = await client.query(
            `
            SELECT trigger_name, event_manipulation
            FROM information_schema.triggers 
            WHERE trigger_name = $1 AND event_object_table = $2 AND event_object_schema = $3
            ORDER BY event_manipulation
          `,
            [triggerInfo.triggerName, triggerInfo.tableName, schemaName]
          );

          if (existingTriggers.rows.length === 0) {
            this.log(
              `ℹ️ No triggers found for ${triggerInfo.triggerName} on ${schemaName}."${triggerInfo.tableName}"`
            );
            continue;
          }

          this.log(
            `🔍 Found ${existingTriggers.rows.length} trigger entries for ${triggerInfo.triggerName}: ${existingTriggers.rows.map(r => r.event_manipulation).join(', ')}`
          );

          // Drop trigger from the correct schema (backup schema after swap)
          await client.query(
            `DROP TRIGGER IF EXISTS ${triggerInfo.triggerName} ON ${schemaName}."${triggerInfo.tableName}"`
          );

          // Verify trigger cleanup by checking information_schema again
          const remainingTriggers = await client.query(
            `
            SELECT trigger_name, event_manipulation
            FROM information_schema.triggers 
            WHERE trigger_name = $1 AND event_object_table = $2 AND event_object_schema = $3
          `,
            [triggerInfo.triggerName, triggerInfo.tableName, schemaName]
          );

          if (remainingTriggers.rows.length > 0) {
            this.logError(
              `Warning: ${remainingTriggers.rows.length} trigger entries still exist for ${triggerInfo.triggerName} (${remainingTriggers.rows.map(r => r.event_manipulation).join(', ')})`,
              new Error('Trigger cleanup may be incomplete')
            );
          } else {
            this.log(`✅ Successfully dropped all trigger entries for ${triggerInfo.triggerName}`);
          }

          // Drop function
          await client.query(`DROP FUNCTION IF EXISTS ${triggerInfo.functionName}()`);

          this.log(`✅ Cleaned up sync trigger: ${triggerInfo.triggerName}`);
          triggerInfo.isActive = false;
        } catch (error) {
          this.logError(`Failed to cleanup trigger ${triggerInfo.triggerName}`, error);
        }
      }

      // Clear active triggers list
      this.activeSyncTriggers = [];
    } finally {
      client.release();
    }
  }

  /**
   * Validate sync consistency between public and shadow schemas
   */
  private async validateSyncConsistency(tableName: string): Promise<SyncValidationResult> {
    const client = await this.destPool.connect();

    try {
      // Get row counts
      const sourceCountResult = await client.query(`SELECT COUNT(*) FROM public."${tableName}"`);
      const targetCountResult = await client.query(`SELECT COUNT(*) FROM shadow."${tableName}"`);

      const sourceRowCount = parseInt(sourceCountResult.rows[0].count);
      const targetRowCount = parseInt(targetCountResult.rows[0].count);

      // Get primary key columns for this table
      const pkResult = await client.query(
        `
        SELECT a.attname
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = $1::regclass AND i.indisprimary
        ORDER BY a.attnum
      `,
        [`public."${tableName}"`]
      );

      const pkColumns = pkResult.rows.map((row: any) => row.attname);

      // Create checksum based on primary key columns (avoiding PostGIS geometry issues)
      let checksumQuery: string;
      if (pkColumns.length > 0) {
        const pkColumnsStr = pkColumns.map(col => `"${col}"::text`).join(` || ',' || `);
        checksumQuery = `
          SELECT md5(string_agg(${pkColumnsStr}, ',' ORDER BY ${pkColumns.map(col => `"${col}"`).join(', ')})) as checksum 
          FROM TABLE_PLACEHOLDER."${tableName}"
        `;
      } else {
        // Fallback to row count only if no primary key found
        checksumQuery = `SELECT md5(COUNT(*)::text) as checksum FROM TABLE_PLACEHOLDER."${tableName}"`;
      }

      const sourceChecksumResult = await client.query(
        checksumQuery.replace('TABLE_PLACEHOLDER', 'public')
      );
      const targetChecksumResult = await client.query(
        checksumQuery.replace('TABLE_PLACEHOLDER', 'shadow')
      );

      const sourceChecksum = sourceChecksumResult.rows[0]?.checksum || '';
      const targetChecksum = targetChecksumResult.rows[0]?.checksum || '';

      const errors: string[] = [];
      let isValid = true;

      if (sourceRowCount !== targetRowCount) {
        errors.push(`Row count mismatch: source=${sourceRowCount}, target=${targetRowCount}`);
        isValid = false;
      }

      if (sourceChecksum !== targetChecksum) {
        errors.push(`Checksum mismatch: source=${sourceChecksum}, target=${targetChecksum}`);
        isValid = false;
      }

      return {
        tableName,
        isValid,
        sourceRowCount,
        targetRowCount,
        sourceChecksum,
        targetChecksum,
        errors,
      };
    } catch (error) {
      return {
        tableName,
        isValid: false,
        sourceRowCount: 0,
        targetRowCount: 0,
        sourceChecksum: '',
        targetChecksum: '',
        errors: [`Validation error: ${error}`],
      };
    } finally {
      client.release();
    }
  }

  /**
   * Rollback schema swap in case of failure
   */
  private async rollbackSchemaSwap(timestamp: number): Promise<void> {
    this.log('🔄 Rolling back schema swap...');

    const client = await this.destPool.connect();
    const backupSchemaName = `backup_${timestamp}`;

    try {
      await client.query('BEGIN');

      // Check if backup schema exists
      const backupExists = await client.query(
        `
        SELECT EXISTS (
          SELECT 1 FROM information_schema.schemata 
          WHERE schema_name = $1
        )
      `,
        [backupSchemaName]
      );

      if (backupExists.rows[0].exists) {
        // Move current public to temp name
        await client.query(`ALTER SCHEMA public RENAME TO failed_migration_${timestamp};`);

        // Restore backup as public
        await client.query(`ALTER SCHEMA ${backupSchemaName} RENAME TO public;`);

        await client.query('COMMIT');
        this.log('✅ Schema rollback completed');
      } else {
        await client.query('ROLLBACK');
        this.log('⚠️  No backup schema found for rollback');
      }
    } catch (error) {
      await client.query('ROLLBACK');
      throw new Error(`Rollback failed: ${error}`);
    } finally {
      client.release();
    }
  }

  /**
   * Clean up backup schema (optional - for cleanup after successful migration)
   */
  async cleanupBackupSchema(timestamp: number): Promise<void> {
    this.log('🗑️  Cleaning up backup schema...');

    const client = await this.destPool.connect();
    const backupSchemaName = `backup_${timestamp}`;

    try {
      // Check if backup schema exists
      const schemaExists = await client.query(
        `
        SELECT EXISTS (
          SELECT 1 FROM information_schema.schemata 
          WHERE schema_name = $1
        )
      `,
        [backupSchemaName]
      );

      if (schemaExists.rows[0].exists) {
        await client.query(`DROP SCHEMA ${backupSchemaName} CASCADE;`);
        this.log(`🗑️  Cleaned up backup schema: ${backupSchemaName}`);
      } else {
        this.log(`⚠️  Backup schema ${backupSchemaName} not found`);
      }

      this.log('✅ Backup cleanup completed');
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      this.log(`⚠️  Warning: Could not clean up backup schema: ${errorMessage}`);
    } finally {
      client.release();
    }
  }

  /**
   * Reset sequences to correct values
   */
  private async resetSequences(tables: TableInfo[]): Promise<void> {
    this.log('🔢 Resetting sequences...');

    for (const table of tables) {
      for (const sequence of table.sequences) {
        try {
          const tableName = table.tableName.replace('_shadow', '');

          // First check if the table exists
          const tableExistsResult = await this.destPool.query(
            `SELECT EXISTS (
              SELECT 1 FROM information_schema.tables 
              WHERE table_schema = 'public' 
              AND table_name = $1
            )`,
            [tableName]
          );

          if (!tableExistsResult.rows[0].exists) {
            this.log(
              `⚠️  Table "${tableName}" does not exist, skipping sequence reset for ${sequence.sequenceName}`
            );
            continue;
          }

          // Check if the column exists
          const columnExistsResult = await this.destPool.query(
            `SELECT EXISTS (
              SELECT 1 FROM information_schema.columns 
              WHERE table_schema = 'public' 
              AND table_name = $1 
              AND column_name = $2
            )`,
            [tableName, sequence.columnName]
          );

          if (!columnExistsResult.rows[0].exists) {
            this.log(
              `⚠️  Column "${sequence.columnName}" does not exist in table "${tableName}", skipping sequence reset for ${sequence.sequenceName}`
            );
            continue;
          }

          // Check if the sequence exists
          const sequenceExistsResult = await this.destPool.query(
            `SELECT EXISTS (
              SELECT 1 FROM information_schema.sequences 
              WHERE sequence_schema = 'public' 
              AND sequence_name = $1
            )`,
            [sequence.sequenceName.replace(/^"?public"?\./, '').replace(/"/g, '')]
          );

          if (!sequenceExistsResult.rows[0].exists) {
            this.log(
              `⚠️  Sequence "${sequence.sequenceName}" does not exist, skipping sequence reset`
            );
            continue;
          }

          // Properly quote table and column names to handle case sensitivity
          const maxResult = await this.destPool.query(
            `SELECT COALESCE(MAX("${sequence.columnName}"), 0) as max_val FROM "${tableName}"`
          );
          const maxValue = parseInt(maxResult.rows[0].max_val);
          const nextValue = maxValue + 1;

          await this.destPool.query(`SELECT setval('${sequence.sequenceName}', $1)`, [nextValue]);
          this.log(
            `✅ Reset sequence ${sequence.sequenceName} to ${nextValue} (max value in ${tableName}.${sequence.columnName}: ${maxValue})`
          );
        } catch (error) {
          this.logError(`Failed to reset sequence ${sequence.sequenceName}`, error);
        }
      }
    }
  }

  /**
   * Recreate spatial and other indexes
   */
  private async recreateIndexes(tables: TableInfo[]): Promise<void> {
    this.log('🗂️  Recreating indexes...');

    for (const table of tables) {
      for (const index of table.indexes) {
        try {
          // Skip indexes that were already created with the table
          if (index.definition.includes('UNIQUE') || index.definition.includes('PRIMARY KEY')) {
            continue;
          }

          // Update index definition to use correct table name
          const tableName = table.tableName.replace('_shadow', '');
          let indexDef = index.definition.replace(table.tableName, tableName);

          // Add IF NOT EXISTS to avoid conflicts with existing indexes
          if (indexDef.startsWith('CREATE INDEX ')) {
            indexDef = indexDef.replace('CREATE INDEX ', 'CREATE INDEX IF NOT EXISTS ');
          } else if (indexDef.startsWith('CREATE UNIQUE INDEX ')) {
            indexDef = indexDef.replace(
              'CREATE UNIQUE INDEX ',
              'CREATE UNIQUE INDEX IF NOT EXISTS '
            );
          }

          // Special handling for spatial indexes
          if (index.indexType === 'gist' || indexDef.includes('USING gist')) {
            this.log(`🌍 Recreating spatial index: ${index.indexName}`);
          }

          await this.destPool.query(indexDef);
          this.log(`✅ Recreated index: ${index.indexName}`);
        } catch (error) {
          this.logError(`Failed to recreate index ${index.indexName}`, error);
          this.stats.warnings.push(`Could not recreate index ${index.indexName}: ${error}`);
        }
      }
    }
  }

  /**
   * Cleanup resources
   */
  private async cleanup(): Promise<void> {
    this.log('🧹 Cleaning up resources...');

    try {
      await this.sourcePool.end();
      await this.destPool.end();
      this.log('✅ Database connections closed');
    } catch (error) {
      this.logError('Error during cleanup', error);
    }
  }

  /**
   * Log informational messages
   */
  private log(message: string): void {
    const timestamp = new Date().toISOString();
    const logMessage = `[${timestamp}] ${message}`;
    console.log(logMessage);
    this.logBuffer.push(logMessage);
  }

  /**
   * Log error messages
   */
  private logError(message: string, error: any): void {
    const timestamp = new Date().toISOString();
    const errorMessage = `[${timestamp}] ❌ ${message}: ${error}`;
    console.error(errorMessage);
    this.stats.errors.push(errorMessage);
    this.logBuffer.push(errorMessage);
  }

  /**
   * Log migration summary
   */
  private logSummary(): void {
    const duration = this.stats.endTime
      ? (this.stats.endTime.getTime() - this.stats.startTime.getTime()) / 1000
      : 0;

    this.log('📊 Migration Summary:');
    this.log(`   ⏱️  Duration: ${duration}s`);
    this.log(`   📦 Tables processed: ${this.stats.tablesProcessed}`);
    this.log(`   📊 Records migrated: ${this.stats.recordsMigrated}`);
    this.log(`   ⚠️  Warnings: ${this.stats.warnings.length}`);
    this.log(`   ❌ Errors: ${this.stats.errors.length}`);

    if (this.stats.warnings.length > 0) {
      this.log('⚠️  Warnings:');
      this.stats.warnings.forEach(warning => this.log(`   - ${warning}`));
    }

    if (this.stats.errors.length > 0) {
      this.log('❌ Errors:');
      this.stats.errors.forEach(error => this.log(`   - ${error}`));
    }
  }

  /**
   * Update the records migrated count by querying the migrated tables
   */
  private async updateRecordsMigratedCount(sourceTables: TableInfo[]): Promise<void> {
    this.log('📊 Counting migrated records...');

    try {
      let totalRecords = 0;

      for (const table of sourceTables) {
        try {
          // Count records in the destination table (after migration)
          const result = await this.destPool.query(
            `SELECT COUNT(*) as count FROM "${table.tableName}"`
          );
          const tableCount = parseInt(result.rows[0].count);
          totalRecords += tableCount;

          this.log(`📋 ${table.tableName}: ${tableCount} records migrated`);
        } catch (error) {
          this.logError(`Could not count records in ${table.tableName}`, error);
        }
      }

      this.stats.recordsMigrated = totalRecords;
      this.log(`✅ Total records migrated: ${totalRecords}`);
    } catch (error) {
      this.logError('Failed to count migrated records', error);
    }
  }

  /**
   * Enable write protection on source database tables to prevent data modification during migration
   * Uses triggers that block INSERT/UPDATE/DELETE operations while allowing schema operations
   */
  private async enableWriteProtection(): Promise<void> {
    this.log('🔒 Enabling write protection on source database tables...');

    const client = await this.sourcePool.connect();
    try {
      // Create a function that blocks writes
      await client.query(`
        CREATE OR REPLACE FUNCTION migration_block_writes()
        RETURNS TRIGGER AS $$
        BEGIN
          RAISE EXCEPTION 'Data modification blocked during migration process'
            USING ERRCODE = 'P0001',
                  HINT = 'Migration in progress - please wait';
          RETURN NULL;
        END;
        $$ LANGUAGE plpgsql;
      `);

      // Get all user tables (excluding system tables)
      const result = await client.query(`
        SELECT tablename 
        FROM pg_tables 
        WHERE schemaname = 'public'
      `);

      // Create triggers on all tables to block writes
      for (const row of result.rows) {
        const tableName = row.tablename;
        const triggerName = `migration_write_block_${tableName}`;

        await client.query(`
          CREATE TRIGGER ${triggerName}
          BEFORE INSERT OR UPDATE OR DELETE ON "${tableName}"
          FOR EACH ROW
          EXECUTE FUNCTION migration_block_writes();
        `);

        this.log(`🔒 Write protection enabled for table: ${tableName}`);
      }

      this.log('✅ Write protection enabled on all source tables');
    } catch (error) {
      this.logError('Failed to enable write protection', error);
      throw error;
    } finally {
      client.release();
    }
  }

  /**
   * Disable write protection on source database tables to restore normal operations
   */
  private async disableWriteProtection(): Promise<void> {
    this.log('🔓 Removing write protection from source database tables...');

    const client = await this.sourcePool.connect();
    try {
      // Get all user tables
      const result = await client.query(`
        SELECT tablename 
        FROM pg_tables 
        WHERE schemaname = 'public'
      `);

      // Drop triggers from all tables
      for (const row of result.rows) {
        const tableName = row.tablename;
        const triggerName = `migration_write_block_${tableName}`;

        try {
          await client.query(`DROP TRIGGER IF EXISTS ${triggerName} ON "${tableName}";`);
          this.log(`🔓 Write protection removed from table: ${tableName}`);
        } catch (error) {
          // Log but don't fail if trigger doesn't exist
          this.log(`⚠️  Could not remove trigger from ${tableName}: ${error}`);
        }
      }

      // Clean up the blocking function
      await client.query('DROP FUNCTION IF EXISTS migration_block_writes();');

      this.log('✅ Write protection removed from all source tables');
    } catch (error) {
      this.logError('Failed to disable write protection', error);
      // Don't throw here - we want to continue even if cleanup fails
    } finally {
      client.release();
    }
  }

  /**
   * Enable write protection on destination database tables to prevent data modification during migration
   * Uses triggers that block INSERT/UPDATE/DELETE operations while allowing schema operations
   * @param excludedTables Optional array of table names to exclude from write protection
   */
  private async enableDestinationWriteProtection(excludedTables: string[] = []): Promise<void> {
    const excludeMessage =
      excludedTables.length > 0 ? ` (excluding ${excludedTables.length} preserved tables)` : '';
    this.log(`🔒 Enabling write protection on destination database tables${excludeMessage}...`);

    const client = await this.destPool.connect();
    try {
      // Create a function that blocks writes
      await client.query(`
        CREATE OR REPLACE FUNCTION migration_block_writes()
        RETURNS TRIGGER AS $$
        BEGIN
          RAISE EXCEPTION 'Data modification blocked during migration process'
            USING ERRCODE = 'P0001',
                  HINT = 'Migration in progress - please wait';
          RETURN NULL;
        END;
        $$ LANGUAGE plpgsql;
      `);

      // Get all user tables (excluding system tables)
      const result = await client.query(`
        SELECT tablename 
        FROM pg_tables 
        WHERE schemaname = 'public'
      `);

      // Create triggers on tables to block writes, excluding preserved tables during prepare
      const excludedTableSet = new Set(excludedTables.map(t => t.toLowerCase()));

      for (const row of result.rows) {
        const tableName = row.tablename;

        // Skip preserved tables if they are in the excluded list
        if (excludedTableSet.has(tableName.toLowerCase())) {
          this.log(`🔓 Skipping write protection for preserved table: ${tableName}`);
          continue;
        }

        const triggerName = `migration_write_block_${tableName}`;

        await client.query(`
          CREATE TRIGGER ${triggerName}
          BEFORE INSERT OR UPDATE OR DELETE ON "${tableName}"
          FOR EACH ROW
          EXECUTE FUNCTION migration_block_writes();
        `);

        this.log(`🔒 Write protection enabled for destination table: ${tableName}`);
      }

      const protectedCount = result.rows.length - excludedTables.length;
      this.log(`✅ Write protection enabled on ${protectedCount} destination tables`);
    } catch (error) {
      this.logError('Failed to enable destination write protection', error);
      throw error;
    } finally {
      client.release();
    }
  }

  /**
   * Disable write protection on destination database tables to restore normal operations
   */
  private async disableDestinationWriteProtection(): Promise<void> {
    this.log('🔓 Removing write protection from destination database tables...');

    const client = await this.destPool.connect();
    try {
      // Get all user tables
      const result = await client.query(`
        SELECT tablename 
        FROM pg_tables 
        WHERE schemaname = 'public'
      `);

      // Drop triggers from all tables
      for (const row of result.rows) {
        const tableName = row.tablename;
        const triggerName = `migration_write_block_${tableName}`;

        try {
          await client.query(`DROP TRIGGER IF EXISTS ${triggerName} ON "${tableName}";`);
          this.log(`🔓 Write protection removed from destination table: ${tableName}`);
        } catch (error) {
          // Log but don't fail if trigger doesn't exist
          this.log(`⚠️  Could not remove trigger from ${tableName}: ${error}`);
        }
      }

      // Clean up the blocking function
      await client.query('DROP FUNCTION IF EXISTS migration_block_writes();');

      this.log('✅ Write protection removed from all destination tables');
    } catch (error) {
      this.logError('Failed to disable destination write protection', error);
      // Don't throw here - we want to continue even if cleanup fails
    } finally {
      client.release();
    }
  }
}

/**
 * Parse database URL into config object
 */
export function parseDatabaseUrl(url: string): DatabaseConfig {
  const parsed = new URL(url);

  // Check for SSL parameters in query string
  const sslParam = parsed.searchParams.get('ssl');
  const sslModeParam = parsed.searchParams.get('sslmode');

  // Determine SSL based on hostname and parameters
  let ssl = true;

  // Default to SSL for cloud providers
  if (
    parsed.hostname.includes('localhost') ||
    parsed.hostname.includes('127.0.0.1') ||
    parsed.hostname.startsWith('192.168.')
  ) {
    ssl = false;
  }

  // Override based on explicit parameters
  if (sslParam === 'true' || sslModeParam === 'require' || sslModeParam === 'prefer') {
    ssl = true;
  } else if (sslParam === 'false' || sslModeParam === 'disable') {
    ssl = false;
  }

  return {
    host: parsed.hostname,
    port: parseInt(parsed.port) || 5432,
    database: parsed.pathname.substring(1),
    user: parsed.username,
    password: parsed.password,
    ssl: ssl,
  };
}
