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

export interface SyncTriggerInfo {
  tableName: string;
  functionName: string;
  triggerName: string;
  isActive: boolean;
  checksum?: string;
  rowCount?: number;
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

  constructor(
    sourceConfig: DatabaseConfig,
    destConfig: DatabaseConfig,
    preservedTables: string[] = [],
    dryRun: boolean = false
  ) {
    this.sourceConfig = sourceConfig;
    this.destConfig = destConfig;
    this.sourcePool = new Pool(this.sourceConfig);
    this.destPool = new Pool(this.destConfig);
    this.preservedTables = new Set(preservedTables.map(t => t.toLowerCase()));
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
  async migrate(): Promise<void> {
    try {
      this.log('🚀 Starting database migration...');
      this.log(`📊 Dry run mode: ${this.dryRun ? 'ENABLED' : 'DISABLED'}`);

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
        return;
      }

      // Perform actual migration
      await this.performCompleteMigration(sourceTables, destTables);

      this.stats.endTime = new Date();
      this.logSummary();
    } catch (error) {
      this.logError('Migration failed', error);
      throw error;
    } finally {
      await this.cleanup();
    }
  }

  /**
   * Perform pre-migration validation checks
   */
  private async performPreMigrationChecks(): Promise<void> {
    this.log('🔍 Performing pre-migration checks...');

    // Test database connections
    try {
      await this.sourcePool.query('SELECT 1');
      this.log('✅ Source database connection successful');
    } catch (error) {
      throw new Error(`Failed to connect to source database: ${error}`);
    }

    try {
      await this.destPool.query('SELECT 1');
      this.log('✅ Destination database connection successful');
    } catch (error) {
      throw new Error(`Failed to connect to destination database: ${error}`);
    }

    // Check for required extensions
    await this.ensureExtensions();

    // Check disk space (simplified check)
    await this.checkDiskSpace();
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
   * Perform dry run analysis
   */
  private async performDryRun(sourceTables: TableInfo[], destTables: TableInfo[]): Promise<void> {
    this.log('🧪 Performing dry run analysis...');

    this.log(`📊 Migration Plan:`);
    this.log(`   Source tables to migrate: ${sourceTables.length}`);

    for (const sourceTable of sourceTables) {
      try {
        const countResult = await this.sourcePool.query(
          `SELECT COUNT(*) FROM ${sourceTable.tableName}`
        );
        const rowCount = parseInt(countResult.rows[0].count);
        this.log(
          `   - ${sourceTable.tableName}: ${rowCount} rows → ${sourceTable.tableName}_shadow`
        );
      } catch (error) {
        this.log(`   - ${sourceTable.tableName}: Could not count rows (${error})`);
      }
    }

    this.log(`   Destination schema to backup: ${destTables.length} tables`);
    const timestamp = Date.now();
    this.log(`   → All tables will be preserved in backup_${timestamp} schema`);

    for (const destTable of destTables) {
      try {
        const countResult = await this.destPool.query(
          `SELECT COUNT(*) FROM ${destTable.tableName}`
        );
        const rowCount = parseInt(countResult.rows[0].count);
        this.log(`   - ${destTable.tableName}: ${rowCount} rows`);
      } catch (error) {
        this.log(`   - ${destTable.tableName}: Could not count rows (${error})`);
      }
    }

    // Check for preserved table restoration
    const preservedTablesFound = [];
    for (const preservedTableName of this.preservedTables) {
      const sourceHasTable = sourceTables.some(
        t => t.tableName.toLowerCase() === preservedTableName
      );
      const destHasTable = destTables.some(t => t.tableName.toLowerCase() === preservedTableName);

      if (sourceHasTable && destHasTable) {
        preservedTablesFound.push(preservedTableName);
      }
    }

    if (preservedTablesFound.length > 0) {
      this.log(`   Preserved tables to restore: ${preservedTablesFound.length}`);
      for (const tableName of preservedTablesFound) {
        this.log(`   - ${tableName}: Will clear and restore from backup`);
      }
    }

    this.log('🧪 Dry run completed - no changes made');
  }

  /**
   * Perform the complete migration using schema-based approach with real-time sync
   */
  private async performCompleteMigration(
    sourceTables: TableInfo[],
    destTables: TableInfo[]
  ): Promise<void> {
    this.log('🔄 Starting database migration...');

    const timestamp = Date.now();

    try {
      // Phase 1: Create shadow schema and restore source data with parallelization
      await this.createShadowSchemaAndRestore(sourceTables, timestamp);

      // Phase 2A: Copy preserved tables to shadow and setup real-time sync
      await this.setupPreservedTableSync(destTables, timestamp);

      // Phase 2B: Backup preserved table data (for rollback safety)
      await this.backupPreservedTableData(destTables, timestamp);

      // Phase 3: Perform atomic schema swap (zero downtime!)
      await this.performAtomicSchemaSwap(timestamp);

      // Phase 4: Cleanup sync triggers and validate consistency
      await this.cleanupSyncTriggersAndValidate(timestamp);

      // Phase 5: Reset sequences and recreate indexes
      this.log('🔢 Phase 5: Resetting sequences...');
      await this.resetSequences(sourceTables);

      this.log('🗂️  Phase 6: Recreating indexes...');
      await this.recreateIndexes(sourceTables);

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
   * Phase 1: Create shadow schema and restore source data with full parallelization
   */
  private async createShadowSchemaAndRestore(
    sourceTables: TableInfo[],
    timestamp: number
  ): Promise<void> {
    this.log('🔧 Phase 1: Creating shadow schema and restoring source data...');

    const client = await this.destPool.connect();

    try {
      // Disable foreign key constraints for the destination database during setup
      await this.disableForeignKeyConstraints(this.destPool);

      // Prepare source database by moving tables to shadow schema
      await this.prepareSourceForShadowDump(sourceTables);

      // Create binary dump for maximum efficiency and parallelization
      const dumpPath = join(this.tempDir, `source_dump_${timestamp}.backup`);
      await this.createBinaryDump(dumpPath);

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

      // Restore source database tables back to public schema
      await this.restoreSourceFromShadowDump(sourceTables);

      // Clean up dump file
      if (existsSync(dumpPath)) {
        unlinkSync(dumpPath);
      }

      // Update statistics
      this.stats.tablesProcessed = sourceTables.length;
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
   * Phase 2: Backup preserved table data before schema swap
   */
  private async backupPreservedTableData(
    destTables: TableInfo[],
    timestamp: number
  ): Promise<void> {
    if (this.preservedTables.size === 0) {
      this.log('✅ No preserved tables to backup');
      return;
    }

    this.log('💾 Phase 2: Backing up preserved table data...');

    const client = await this.destPool.connect();

    try {
      await client.query('BEGIN');

      for (const tableName of this.preservedTables) {
        const backupTableName = `${tableName}_backup_${timestamp}`;

        try {
          // Check if table exists
          const tableExists = await client.query(
            `
            SELECT EXISTS (
              SELECT 1 FROM information_schema.tables 
              WHERE table_schema = 'public' 
              AND table_name = $1
            )
          `,
            [tableName]
          );

          if (tableExists.rows[0].exists) {
            this.log(`💾 Backing up preserved table: ${tableName} → ${backupTableName}`);

            // Create backup table with same structure
            await client.query(`CREATE TABLE "${backupTableName}" AS SELECT * FROM "${tableName}"`);

            const countResult = await client.query(`SELECT COUNT(*) FROM "${backupTableName}"`);
            const rowCount = parseInt(countResult.rows[0].count);
            this.log(`📊 Backed up ${rowCount} rows from ${tableName}`);
          } else {
            this.log(`⚠️  Table ${tableName} not found, skipping backup`);
          }
        } catch (error) {
          const errorMessage = error instanceof Error ? error.message : String(error);
          this.log(`⚠️  Warning: Could not backup ${tableName}: ${errorMessage}`);
        }
      }

      await client.query('COMMIT');
      this.log('✅ Preserved table data backed up');
    } catch (error) {
      await client.query('ROLLBACK');
      throw new Error(`Failed to backup preserved table data: ${error}`);
    } finally {
      client.release();
    }
  }

  /**
   * Phase 3: Perform atomic schema swap
   */
  private async performAtomicSchemaSwap(timestamp: number): Promise<void> {
    this.log('🔄 Phase 3: Performing atomic schema swap...');

    const client = await this.destPool.connect();

    try {
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
      this.log('✅ Atomic schema swap completed - migration is now live!');
    } catch (error) {
      await client.query('ROLLBACK');
      throw new Error(`Failed to perform schema swap: ${error}`);
    } finally {
      client.release();
    }
  }

  /**
   * Phase 2A: Setup preserved table synchronization
   */
  private async setupPreservedTableSync(destTables: TableInfo[], timestamp: number): Promise<void> {
    if (this.preservedTables.size === 0) {
      this.log('✅ No preserved tables to sync');
      return;
    }

    this.log(`🔄 Phase 2A: Setting up preserved table synchronization (backup_${timestamp})...`);

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

        this.log(
          `🔄 Setting up sync for preserved table: ${tableName} (${tableInfo.columns.length} columns)`
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
          [tableName]
        );

        if (tableExists.rows[0].exists) {
          // Step 1: Clear shadow table and copy current data
          await client.query(`DELETE FROM shadow."${tableName}"`);
          await client.query(
            `INSERT INTO shadow."${tableName}" SELECT * FROM public."${tableName}"`
          );

          // Step 2: Setup real-time sync triggers
          const triggerInfo = await this.createRealtimeSyncTrigger(client, tableName);
          triggerInfo.checksum = `sync_${timestamp}`; // Use timestamp for tracking
          this.activeSyncTriggers.push(triggerInfo);

          // Step 3: Validate initial sync
          const validation = await this.validateSyncConsistency(tableName);
          if (!validation.isValid) {
            throw new Error(
              `Initial sync validation failed for ${tableName}: ${validation.errors.join(', ')}`
            );
          }

          this.log(`✅ Sync setup complete for ${tableName} (${validation.sourceRowCount} rows)`);
        } else {
          throw new Error(
            `Preserved table ${tableName} exists in schema analysis but not in actual database`
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
    const functionName = `sync_${tableName}_to_shadow`;
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
          DELETE FROM shadow.${tableName} WHERE id = OLD.id;
          RETURN OLD;
        ELSIF TG_OP = 'UPDATE' THEN
          UPDATE shadow.${tableName} 
          SET ${setClause}
          WHERE id = OLD.id;
          RETURN NEW;
        ELSIF TG_OP = 'INSERT' THEN
          INSERT INTO shadow.${tableName} (${columnList})
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
        AFTER INSERT OR UPDATE OR DELETE ON public.${tableName}
        FOR EACH ROW EXECUTE FUNCTION ${functionName}();
    `;

    await client.query(triggerSQL);

    this.log(`✅ Created sync trigger: ${triggerName}`);

    return {
      tableName,
      functionName,
      triggerName,
      isActive: true,
    };
  }

  /**
   * Phase 4: Cleanup sync triggers and validate consistency
   */
  private async cleanupSyncTriggersAndValidate(timestamp: number): Promise<void> {
    if (this.activeSyncTriggers.length === 0) {
      this.log('✅ No sync triggers to cleanup');
      return;
    }

    this.log(
      `🧹 Phase 4: Cleaning up sync triggers and validating consistency (backup_${timestamp})...`
    );

    try {
      // Validate sync consistency before cleanup
      const validationResults = await Promise.all(
        this.activeSyncTriggers.map(trigger => this.validateSyncConsistency(trigger.tableName))
      );

      let allValid = true;
      for (const result of validationResults) {
        if (!result.isValid) {
          allValid = false;
          this.logError(
            `Sync validation failed for ${result.tableName}`,
            new Error(result.errors.join(', '))
          );
          this.log(`   Source: ${result.sourceRowCount} rows, checksum: ${result.sourceChecksum}`);
          this.log(`   Target: ${result.targetRowCount} rows, checksum: ${result.targetChecksum}`);
        } else {
          this.log(
            `✅ Sync validation passed for ${result.tableName} (${result.sourceRowCount} rows)`
          );
        }
      }

      if (!allValid) {
        throw new Error('Sync validation failed for one or more preserved tables');
      }

      // Cleanup triggers
      await this.cleanupRealtimeSync(this.activeSyncTriggers);

      this.log(`✅ Sync triggers cleaned up and validation complete (backup_${timestamp})`);
    } catch (error) {
      // Ensure triggers are cleaned up even if validation fails
      try {
        await this.cleanupRealtimeSync(this.activeSyncTriggers);
      } catch (cleanupError) {
        this.logError('Failed to cleanup triggers after validation error', cleanupError);
      }
      throw error;
    }
  }

  /**
   * Cleanup real-time sync triggers
   */
  private async cleanupRealtimeSync(triggerInfos: SyncTriggerInfo[]): Promise<void> {
    if (triggerInfos.length === 0) {
      return;
    }

    this.log(`🧹 Cleaning up ${triggerInfos.length} sync triggers...`);

    const client = await this.destPool.connect();

    try {
      for (const triggerInfo of triggerInfos) {
        try {
          // Drop trigger
          await client.query(
            `DROP TRIGGER IF EXISTS ${triggerInfo.triggerName} ON public.${triggerInfo.tableName}`
          );

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
      const sourceCountResult = await client.query(`SELECT COUNT(*) FROM public.${tableName}`);
      const targetCountResult = await client.query(`SELECT COUNT(*) FROM shadow.${tableName}`);

      const sourceRowCount = parseInt(sourceCountResult.rows[0].count);
      const targetRowCount = parseInt(targetCountResult.rows[0].count);

      // Get checksums (using a simple approach with primary key ordering)
      const sourceChecksumResult = await client.query(`
        SELECT md5(string_agg(md5(ROW(*)::text), '' ORDER BY id)) as checksum 
        FROM public.${tableName}
      `);
      const targetChecksumResult = await client.query(`
        SELECT md5(string_agg(md5(ROW(*)::text), '' ORDER BY id)) as checksum 
        FROM shadow.${tableName}
      `);

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

      // Also clean up any backup tables from preserved table operations
      const backupTables = await client.query(`
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name LIKE '%_backup_${timestamp}'
      `);

      for (const row of backupTables.rows) {
        try {
          await client.query(`DROP TABLE "${row.table_name}" CASCADE;`);
          this.log(`🗑️  Cleaned up backup table: ${row.table_name}`);
        } catch (error) {
          const errorMessage = error instanceof Error ? error.message : String(error);
          this.log(
            `⚠️  Warning: Could not clean up backup table ${row.table_name}: ${errorMessage}`
          );
        }
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
          const maxResult = await this.destPool.query(
            `SELECT COALESCE(MAX(${sequence.columnName}), 0) as max_val FROM ${tableName}`
          );
          const maxValue = parseInt(maxResult.rows[0].max_val);
          const nextValue = maxValue + 1;

          await this.destPool.query(`SELECT setval('${sequence.sequenceName}', $1)`, [nextValue]);
          this.log(`✅ Reset sequence ${sequence.sequenceName} to ${nextValue}`);
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
          const indexDef = index.definition.replace(table.tableName, tableName);

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
    console.log(`[${timestamp}] ${message}`);
  }

  /**
   * Log error messages
   */
  private logError(message: string, error: any): void {
    const timestamp = new Date().toISOString();
    const errorMessage = `[${timestamp}] ❌ ${message}: ${error}`;
    console.error(errorMessage);
    this.stats.errors.push(errorMessage);
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
}

/**
 * Parse database URL into config object
 */
export function parseDatabaseUrl(url: string): DatabaseConfig {
  const parsed = new URL(url);
  return {
    host: parsed.hostname,
    port: parseInt(parsed.port) || 5432,
    database: parsed.pathname.substring(1),
    user: parsed.username,
    password: parsed.password,
  };
}
