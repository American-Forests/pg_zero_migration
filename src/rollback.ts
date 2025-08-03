/**
 * Database Rollback Operations
 *
 * Handles rollback operations for database migrations, including backup validation,
 * schema restoration, and rollback execution.
 */

import { Pool, PoolClient } from 'pg';
import { DatabaseConfig } from './migration-core.js';

export interface BackupInfo {
  timestamp: string;
  schemaName: string;
  createdAt: Date;
  tableCount: number;
  totalSize: string;
  tables: BackupTableInfo[];
}

export interface BackupTableInfo {
  tableName: string;
  rowCount: number;
  size: string;
}

export interface BackupValidationResult {
  isValid: boolean;
  errors: string[];
  warnings: string[];
  tableValidations: TableValidationResult[];
}

export interface TableValidationResult {
  tableName: string;
  isValid: boolean;
  hasData: boolean;
  hasStructure: boolean;
  errors: string[];
}

export interface BackupContents {
  timestamp: string;
  tables: BackupTableInfo[];
  totalRows: number;
  totalSize: string;
}

export class DatabaseRollback {
  private pool: Pool;
  private config: DatabaseConfig;

  constructor(config: DatabaseConfig) {
    this.config = config;
    this.pool = new Pool(config);
  }

  /**
   * Get all available backup tables with their metadata
   */
  async getAvailableBackups(): Promise<BackupInfo[]> {
    this.log('🔍 Scanning for available backup tables...');

    const client = await this.pool.connect();
    try {
      // Find all backup tables in public schema
      const tablesResult = await client.query(`
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
        AND table_name LIKE 'backup_%'
        ORDER BY table_name DESC
      `);

      const backups: BackupInfo[] = [];

      if (tablesResult.rows.length === 0) {
        this.log('📊 Found 0 backup tables');
        return backups;
      }

      // Create a single backup entry representing all backup tables
      const backupTables: BackupTableInfo[] = [];

      for (const row of tablesResult.rows) {
        const tableName = row.table_name;

        try {
          // Get row count
          const countResult = await client.query(
            `SELECT COUNT(*) as count FROM public."${tableName}"`
          );
          const rowCount = parseInt(countResult.rows[0].count);

          // Get table size
          const sizeResult = await client.query(`
            SELECT pg_size_pretty(pg_total_relation_size('public."${tableName}"')) as size
          `);
          const size = sizeResult.rows[0].size;

          backupTables.push({
            tableName,
            rowCount,
            size,
          });
        } catch (error) {
          this.log(`⚠️  Could not get info for backup table ${tableName}: ${error}`);
          backupTables.push({
            tableName,
            rowCount: 0,
            size: 'unknown',
          });
        }
      }

      // Calculate total size for all backup tables
      const totalSizeResult = await client.query(`
        SELECT pg_size_pretty(
          SUM(pg_total_relation_size('public."' || table_name || '"'))
        ) as total_size
        FROM information_schema.tables 
        WHERE table_schema = 'public'
        AND table_name LIKE 'backup_%'
      `);

      const totalSize = totalSizeResult.rows[0].total_size || '0 bytes';

      backups.push({
        timestamp: 'latest', // Simplified for table-swap approach
        schemaName: 'public', // backup tables are in public schema
        createdAt: new Date(),
        tableCount: backupTables.length,
        totalSize,
        tables: backupTables,
      });

      this.log(
        `📊 Found ${backups.length} backup sets containing ${backupTables.length} backup tables`
      );
      return backups;
    } finally {
      client.release();
    }
  }

  /**
   * Validate backup integrity before rollback (for table-swap approach)
   */
  async validateBackup(backupTimestamp: string): Promise<BackupValidationResult> {
    this.log(`🔍 Validating backup ${backupTimestamp}...`);

    const client = await this.pool.connect();

    try {
      const result: BackupValidationResult = {
        isValid: true,
        errors: [],
        warnings: [],
        tableValidations: [],
      };

      // Check if backup tables exist in public schema
      const backupTablesResult = await client.query(`
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
        AND table_name LIKE 'backup_%'
        ORDER BY table_name
      `);

      if (backupTablesResult.rows.length === 0) {
        result.isValid = false;
        result.errors.push('No backup tables found. Run migration first to create backups.');
        return result;
      }

      // Validate each backup table
      for (const row of backupTablesResult.rows) {
        const tableName = row.table_name;
        const validation: TableValidationResult = {
          tableName,
          isValid: true,
          hasData: false,
          hasStructure: true,
          errors: [],
        };

        try {
          // Check table structure
          const columnsResult = await client.query(
            `
            SELECT COUNT(*) as column_count 
            FROM information_schema.columns 
            WHERE table_schema = 'public' AND table_name = $1
          `,
            [tableName]
          );

          if (parseInt(columnsResult.rows[0].column_count) === 0) {
            validation.hasStructure = false;
            validation.isValid = false;
            validation.errors.push('Table has no columns');
          }

          // Check if table has data
          const countResult = await client.query(
            `SELECT COUNT(*) as count FROM public."${tableName}"`
          );
          const rowCount = parseInt(countResult.rows[0].count);
          validation.hasData = rowCount > 0;

          if (rowCount === 0) {
            result.warnings.push(`Table ${tableName} is empty`);
          }
        } catch (error) {
          validation.isValid = false;
          validation.errors.push(`Validation error: ${error}`);
          result.isValid = false;
        }

        result.tableValidations.push(validation);
      }

      // Check for critical backup tables (corresponding to important tables)
      const criticalBackupTables = ['backup_User', 'backup_user']; // Add more as needed
      const backupTableNames = result.tableValidations.map(v => v.tableName);

      for (const criticalTable of criticalBackupTables) {
        if (!backupTableNames.includes(criticalTable)) {
          result.warnings.push(`Critical backup table ${criticalTable} not found`);
        }
      }

      if (result.errors.length > 0) {
        result.isValid = false;
      }

      this.log(`✅ Backup validation complete: ${result.isValid ? 'VALID' : 'INVALID'}`);
      return result;
    } finally {
      client.release();
    }
  }

  /**
   * Perform rollback to specified backup (for table-swap approach)
   */
  async rollback(backupTimestamp: string, keepTables: string[] = []): Promise<void> {
    this.log(`🔄 Starting rollback to backup ${backupTimestamp}...`);

    // Validate backup first
    const validation = await this.validateBackup(backupTimestamp);
    if (!validation.isValid) {
      throw new Error(`Backup validation failed: ${validation.errors.join(', ')}`);
    }

    const client = await this.pool.connect();

    try {
      this.log('\n⚠️  DESTRUCTIVE ROLLBACK - NO UNDO AVAILABLE');
      this.log(`• Current active tables will be dropped`);
      this.log(`• Backup tables will be renamed to active tables`);

      if (keepTables.length > 0) {
        this.log(`\nCURRENT DATA TO BE KEPT:`);
        this.log(`• Tables to preserve: ${keepTables.join(', ')}`);
      }

      this.log('\n✅ Proceeding with table-swap rollback...');

      // Disable foreign key constraints for rollback operations
      this.log('🔓 Disabling foreign key constraints for rollback...');
      await client.query('SET session_replication_role = replica;');

      // Get all backup tables
      const backupTablesResult = await client.query(`
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
        AND table_name LIKE 'backup_%'
        ORDER BY table_name
      `);

      for (const row of backupTablesResult.rows) {
        const backupTableName = row.table_name;
        const activeTableName = backupTableName.replace('backup_', '');

        try {
          // Drop the current active table
          await client.query(`DROP TABLE IF EXISTS public."${activeTableName}" CASCADE;`);
          this.log(`• Dropped current table: ${activeTableName}`);

          // Rename backup table to active table
          await client.query(
            `ALTER TABLE public."${backupTableName}" RENAME TO "${activeTableName}";`
          );
          this.log(`• Restored table: ${backupTableName} → ${activeTableName}`);

          // Rename associated sequences
          const sequenceName = `${activeTableName}_id_seq`;
          const backupSequenceName = `backup_${sequenceName}`;
          try {
            await client.query(
              `ALTER SEQUENCE public."${backupSequenceName}" RENAME TO "${sequenceName}";`
            );
            this.log(`• Restored sequence: ${backupSequenceName} → ${sequenceName}`);
          } catch (seqError) {
            this.log(`⚠️  Could not restore sequence ${sequenceName}: ${seqError}`);
          }

          // Rename constraints (primary keys, foreign keys, etc.)
          const constraintsResult = await client.query(
            `
            SELECT constraint_name
            FROM information_schema.table_constraints
            WHERE table_schema = 'public' 
            AND table_name = $1
            AND constraint_name LIKE 'backup_%'
          `,
            [activeTableName]
          );

          for (const constraint of constraintsResult.rows) {
            const backupConstraintName = constraint.constraint_name;
            const activeConstraintName = backupConstraintName.replace('backup_', '');
            try {
              await client.query(
                `ALTER TABLE public."${activeTableName}" RENAME CONSTRAINT "${backupConstraintName}" TO "${activeConstraintName}";`
              );
              this.log(`• Restored constraint: ${backupConstraintName} → ${activeConstraintName}`);
            } catch (constraintError) {
              this.log(
                `⚠️  Could not restore constraint ${activeConstraintName}: ${constraintError}`
              );
            }
          }
        } catch (error) {
          this.log(`❌ Failed to restore table ${activeTableName}: ${error}`);
          throw error;
        }
      }

      // Handle keep-tables if specified (copy from temporary backup)
      if (keepTables.length > 0) {
        this.log('\n📋 Implementing keep-tables functionality...');
        this.log('⚠️  Keep-tables functionality not yet implemented for table-swap rollback');
      }

      // Re-enable foreign key constraints
      this.log('🔒 Re-enabling foreign key constraints...');
      await client.query('SET session_replication_role = origin;');

      this.log('\n✅ Table-swap rollback completed successfully!');
      this.log(`📦 Backup tables have been consumed and restored as active tables`);
    } catch (error) {
      this.log('\n❌ Rollback failed, manual intervention may be required...');

      // Ensure foreign key constraints are re-enabled even on failure
      try {
        this.log('🔒 Re-enabling foreign key constraints after rollback error...');
        await client.query('SET session_replication_role = origin;');
      } catch (fkError) {
        this.log(`⚠️  Warning: Could not re-enable foreign key constraints: ${fkError}`);
      }

      throw error;
    } finally {
      client.release();
    }
  }

  /**
   * Copy specified tables from shadow to public schema (for keep-tables functionality)
   */
  private async copyKeepTables(client: PoolClient, keepTables: string[]): Promise<void> {
    this.log(`📋 Copying ${keepTables.length} tables from current data...`);

    for (const tableName of keepTables) {
      try {
        // Check if table exists in shadow (current) schema
        const shadowTableExists = await client.query(
          `
          SELECT EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_schema = 'shadow' AND table_name = $1
          )
        `,
          [tableName]
        );

        // Check if table exists in public (restored) schema
        const publicTableExists = await client.query(
          `
          SELECT EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_name = $1
          )
        `,
          [tableName]
        );

        if (shadowTableExists.rows[0].exists && publicTableExists.rows[0].exists) {
          // Clear the restored table and copy current data
          await client.query(`DELETE FROM public."${tableName}"`);
          await client.query(
            `INSERT INTO public."${tableName}" SELECT * FROM shadow."${tableName}"`
          );

          const countResult = await client.query(`SELECT COUNT(*) FROM public."${tableName}"`);
          const rowCount = parseInt(countResult.rows[0].count);
          this.log(`  ✅ Copied ${rowCount} rows for table ${tableName}`);
        } else {
          this.log(`  ⚠️  Skipping ${tableName} - table not found in both schemas`);
        }
      } catch (error) {
        this.log(`  ❌ Failed to copy table ${tableName}: ${error}`);
      }
    }
  }

  /**
   * Attempt to recover from a failed rollback
   */
  private async recoverFromFailedRollback(client: PoolClient): Promise<void> {
    try {
      // Check if shadow schema exists
      const shadowCheck = await client.query(`
        SELECT EXISTS (
          SELECT 1 FROM information_schema.schemata 
          WHERE schema_name = 'shadow'
        );
      `);

      if (shadowCheck.rows[0].exists) {
        // Restore shadow to public
        await client.query('DROP SCHEMA IF EXISTS public CASCADE;');
        await client.query('ALTER SCHEMA shadow RENAME TO public;');
        this.log('✅ Original state restored from shadow');
      } else {
        this.log('❌ Cannot recover: shadow schema not found');
      }
    } catch (recoverError) {
      this.log(`❌ Recovery failed: ${recoverError}`);
    }
  }

  /**
   * List contents of a specific backup
   */
  async listBackupContents(backupTimestamp: string): Promise<BackupContents> {
    const backups = await this.getAvailableBackups();
    const backup = backups.find(b => b.timestamp === backupTimestamp);

    if (!backup) {
      throw new Error(`Backup ${backupTimestamp} not found`);
    }

    const totalRows = backup.tables.reduce((sum, table) => sum + table.rowCount, 0);

    return {
      timestamp: backup.timestamp,
      tables: backup.tables,
      totalRows,
      totalSize: backup.totalSize,
    };
  }

  /**
   * Clean up a specific backup schema
   */
  async cleanupBackup(backupTimestamp: string): Promise<void> {
    this.log(`🧹 Cleaning up backup ${backupTimestamp}...`);

    const schemaName = `backup_${backupTimestamp}`;
    const client = await this.pool.connect();

    try {
      // Check if backup exists
      const validation = await this.validateBackup(backupTimestamp);
      if (!validation.isValid) {
        throw new Error(`Cannot cleanup invalid backup: ${validation.errors.join(', ')}`);
      }

      // Drop the backup schema
      await client.query(`DROP SCHEMA IF EXISTS ${schemaName} CASCADE;`);
      this.log(`✅ Backup schema ${schemaName} removed`);
    } finally {
      client.release();
    }
  }

  /**
   * Get backup size estimation for rollback planning
   */
  async estimateRollbackTime(
    backupTimestamp: string
  ): Promise<{ estimatedMinutes: number; dataSize: string; tableCount: number }> {
    const contents = await this.listBackupContents(backupTimestamp);

    // Simple estimation: ~1 minute per 100MB of data + base overhead
    const baseMinutes = 2; // Base rollback overhead
    const sizeEstimate = contents.tables.length * 0.5; // Rough estimate based on table count

    return {
      estimatedMinutes: Math.max(baseMinutes, Math.ceil(sizeEstimate)),
      dataSize: contents.totalSize,
      tableCount: contents.tables.length,
    };
  }

  /**
   * Close database connections
   */
  async close(): Promise<void> {
    await this.pool.end();
  }

  /**
   * Logging helper
   */
  private log(message: string): void {
    console.log(`[${new Date().toISOString()}] ${message}`);
  }
}
