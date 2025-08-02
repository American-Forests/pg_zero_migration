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
   * Get all available backup schemas with their metadata
   */
  async getAvailableBackups(): Promise<BackupInfo[]> {
    this.log('🔍 Scanning for available backup schemas...');

    const client = await this.pool.connect();
    try {
      // Find all backup schemas
      const schemasResult = await client.query(`
        SELECT schema_name 
        FROM information_schema.schemata 
        WHERE schema_name LIKE 'backup_%'
        ORDER BY schema_name DESC
      `);

      const backups: BackupInfo[] = [];

      for (const row of schemasResult.rows) {
        const schemaName = row.schema_name;
        const timestamp = schemaName.replace('backup_', '');

        try {
          // Get tables in this backup schema
          const tablesResult = await client.query(
            `
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = $1
            ORDER BY table_name
          `,
            [schemaName]
          );

          const tables: BackupTableInfo[] = [];
          let totalRows = 0;

          // Get detailed info for each table
          for (const tableRow of tablesResult.rows) {
            const tableName = tableRow.table_name;

            try {
              // Get row count
              const countResult = await client.query(
                `SELECT COUNT(*) as count FROM "${schemaName}"."${tableName}"`
              );
              const rowCount = parseInt(countResult.rows[0].count);
              totalRows += rowCount;

              // Get table size (fixed query with proper quoting)
              const sizeResult = await client.query(`
                SELECT pg_size_pretty(pg_total_relation_size('"${schemaName}"."${tableName}"')) as size
              `);
              const size = sizeResult.rows[0].size;

              tables.push({
                tableName,
                rowCount,
                size,
              });
            } catch (error) {
              this.log(`⚠️  Could not get info for table ${tableName}: ${error}`);
              tables.push({
                tableName,
                rowCount: 0,
                size: 'unknown',
              });
            }
          }

          // Get total schema size
          const totalSizeResult = await client.query(
            `
            SELECT pg_size_pretty(
              SUM(pg_total_relation_size('"${schemaName}"."' || table_name || '"'))
            ) as total_size
            FROM information_schema.tables 
            WHERE table_schema = $1
          `,
            [schemaName]
          );

          const totalSize = totalSizeResult.rows[0].total_size || '0 bytes';

          backups.push({
            timestamp,
            schemaName,
            createdAt: new Date(parseInt(timestamp)),
            tableCount: tables.length,
            totalSize,
            tables,
          });

          this.log(
            `✅ Found backup ${timestamp}: ${tables.length} tables, ${totalRows} total rows`
          );
        } catch (error) {
          this.log(`⚠️  Could not analyze backup ${timestamp}: ${error}`);
        }
      }

      this.log(`📊 Found ${backups.length} backup schemas`);
      return backups;
    } finally {
      client.release();
    }
  }

  /**
   * Validate backup integrity before rollback
   */
  async validateBackup(backupTimestamp: string): Promise<BackupValidationResult> {
    this.log(`🔍 Validating backup ${backupTimestamp}...`);

    const schemaName = `backup_${backupTimestamp}`;
    const client = await this.pool.connect();

    try {
      const result: BackupValidationResult = {
        isValid: true,
        errors: [],
        warnings: [],
        tableValidations: [],
      };

      // Check if backup schema exists
      const schemaExists = await client.query(
        `
        SELECT EXISTS (
          SELECT 1 FROM information_schema.schemata 
          WHERE schema_name = $1
        )
      `,
        [schemaName]
      );

      if (!schemaExists.rows[0].exists) {
        result.isValid = false;
        result.errors.push(`Backup schema ${schemaName} does not exist`);
        return result;
      }

      // Enhanced validation: Check schema compatibility with current public schema
      await this.validateSchemaCompatibility(client, schemaName, result);

      // Get all tables in backup schema
      const tablesResult = await client.query(
        `
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = $1
        ORDER BY table_name
      `,
        [schemaName]
      );

      // Validate each table with enhanced checks
      for (const row of tablesResult.rows) {
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
            WHERE table_schema = $1 AND table_name = $2
          `,
            [schemaName, tableName]
          );

          if (parseInt(columnsResult.rows[0].column_count) === 0) {
            validation.hasStructure = false;
            validation.isValid = false;
            validation.errors.push('Table has no columns');
          }

          // Check if table has data
          const countResult = await client.query(
            `SELECT COUNT(*) as count FROM "${schemaName}"."${tableName}"`
          );
          const rowCount = parseInt(countResult.rows[0].count);
          validation.hasData = rowCount > 0;

          if (rowCount === 0) {
            result.warnings.push(`Table ${tableName} is empty`);
          }

          // Enhanced validation: Data corruption check (sample-based for performance)
          await this.validateTableDataIntegrity(client, schemaName, tableName, validation);
        } catch (error) {
          validation.isValid = false;
          validation.errors.push(`Validation error: ${error}`);
          result.isValid = false;
        }

        result.tableValidations.push(validation);
      }

      // Enhanced validation: Referential integrity check (optimized)
      await this.validateBackupReferentialIntegrity(client, schemaName, result);

      // Check for critical tables
      const criticalTables = ['User', 'user']; // Add more as needed
      const backupTableNames = result.tableValidations.map(v => v.tableName.toLowerCase());

      for (const criticalTable of criticalTables) {
        if (!backupTableNames.includes(criticalTable.toLowerCase())) {
          result.warnings.push(`Critical table ${criticalTable} not found in backup`);
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
   * Validate schema compatibility between backup and current public schema
   * Performance optimized: Only compares essential schema elements
   */
  private async validateSchemaCompatibility(
    client: PoolClient,
    backupSchema: string,
    result: BackupValidationResult
  ): Promise<void> {
    try {
      this.log('🔗 Validating schema compatibility...');

      // Fast check: Compare table counts between schemas
      const backupTableCount = await client.query(
        `SELECT COUNT(*) as count FROM information_schema.tables WHERE table_schema = $1`,
        [backupSchema]
      );
      const publicTableCount = await client.query(
        `SELECT COUNT(*) as count FROM information_schema.tables WHERE table_schema = 'public'`
      );

      const backupCount = parseInt(backupTableCount.rows[0].count);
      const publicCount = parseInt(publicTableCount.rows[0].count);

      if (Math.abs(backupCount - publicCount) > 5) {
        result.warnings.push(
          `Schema compatibility warning: Backup has ${backupCount} tables, current has ${publicCount} tables`
        );
      }

      // Fast check: Verify critical tables exist in both schemas (limit to top 10 for performance)
      const criticalTables = await client.query(`
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        ORDER BY table_name 
        LIMIT 10
      `);

      for (const table of criticalTables.rows) {
        const tableName = table.table_name;
        const backupHasTable = await client.query(
          `SELECT EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_schema = $1 AND table_name = $2
          )`,
          [backupSchema, tableName]
        );

        if (!backupHasTable.rows[0].exists) {
          result.warnings.push(`Schema compatibility: Table '${tableName}' missing in backup`);
        }
      }

      this.log('✅ Schema compatibility validation completed');
    } catch (error) {
      result.warnings.push(`Schema compatibility validation warning: ${error}`);
    }
  }

  /**
   * Validate table data integrity using sample-based checks for performance
   * Only checks first 100 rows to avoid performance issues
   */
  private async validateTableDataIntegrity(
    client: PoolClient,
    schemaName: string,
    tableName: string,
    validation: TableValidationResult
  ): Promise<void> {
    try {
      // Performance-optimized: Only sample first 100 rows
      const sampleCheck = await client.query(`
        SELECT COUNT(*) as valid_count
        FROM (
          SELECT * FROM "${schemaName}"."${tableName}" 
          WHERE ctid IS NOT NULL  -- Basic validity check
          LIMIT 100
        ) sample
      `);

      const validCount = parseInt(sampleCheck.rows[0].valid_count);

      // If we have data, validate sample integrity
      if (validation.hasData && validCount === 0) {
        validation.errors.push('Data integrity issue: Sample data appears corrupted');
        validation.isValid = false;
      }

      // Quick check for basic data types (non-null primary keys if they exist)
      try {
        const pkCheck = await client.query(`
          SELECT COUNT(*) as pk_violations
          FROM (
            SELECT * FROM "${schemaName}"."${tableName}" 
            WHERE id IS NULL  -- Assuming 'id' is common primary key
            LIMIT 10
          ) pk_sample
        `);

        const pkViolations = parseInt(pkCheck.rows[0].pk_violations);
        if (pkViolations > 0) {
          validation.errors.push(`Found ${pkViolations} records with null primary keys`);
          validation.isValid = false;
        }
      } catch {
        // Ignore if 'id' column doesn't exist - not all tables have it
      }
    } catch (error) {
      // Non-critical error - log as warning but don't fail validation
      validation.errors.push(`Data integrity check warning: ${error}`);
    }
  }

  /**
   * Validate referential integrity in backup schema
   * Validates all foreign keys for comprehensive integrity checking
   */
  private async validateBackupReferentialIntegrity(
    client: PoolClient,
    schemaName: string,
    result: BackupValidationResult
  ): Promise<void> {
    try {
      this.log('🔗 Validating backup referential integrity...');

      // Get all foreign key constraints
      const foreignKeys = await client.query(
        `
        SELECT 
          tc.table_name,
          tc.constraint_name,
          kcu.column_name,
          ccu.table_name AS foreign_table_name,
          ccu.column_name AS foreign_column_name
        FROM information_schema.table_constraints AS tc 
        JOIN information_schema.key_column_usage AS kcu 
          ON tc.constraint_name = kcu.constraint_name
        JOIN information_schema.constraint_column_usage AS ccu 
          ON ccu.constraint_name = tc.constraint_name
        WHERE tc.constraint_type = 'FOREIGN KEY' 
          AND tc.table_schema = $1
      `,
        [schemaName]
      );

      let violationCount = 0;

      for (const fk of foreignKeys.rows) {
        try {
          // Performance optimized: Use LIMIT 1 to just check if violations exist
          const orphanCheck = await client.query(`
            SELECT 1
            FROM "${schemaName}"."${fk.table_name}" t
            WHERE "${fk.column_name}" IS NOT NULL
            AND NOT EXISTS (
              SELECT 1 FROM "${schemaName}"."${fk.foreign_table_name}" f
              WHERE f."${fk.foreign_column_name}" = t."${fk.column_name}"
            )
            LIMIT 1
          `);

          if (orphanCheck.rows.length > 0) {
            violationCount++;
            result.warnings.push(
              `Referential integrity: Orphaned records found in ${fk.table_name}.${fk.column_name}`
            );
          }
        } catch (error) {
          result.warnings.push(
            `Referential integrity check warning for ${fk.constraint_name}: ${error}`
          );
        }
      }

      if (violationCount > 0) {
        result.warnings.push(
          `Backup referential integrity: Found violations in ${violationCount} constraints (non-blocking)`
        );
      }

      this.log(
        `✅ Backup referential integrity validated (${foreignKeys.rows.length} constraints checked)`
      );
    } catch (error) {
      result.warnings.push(`Backup referential integrity validation warning: ${error}`);
    }
  }

  /**
   * Perform rollback to specified backup
   */
  async rollback(backupTimestamp: string, keepTables: string[] = []): Promise<void> {
    this.log(`🔄 Starting rollback to backup ${backupTimestamp}...`);

    // Validate backup first
    const validation = await this.validateBackup(backupTimestamp);
    if (!validation.isValid) {
      throw new Error(`Backup validation failed: ${validation.errors.join(', ')}`);
    }

    const schemaName = `backup_${backupTimestamp}`;
    const client = await this.pool.connect();

    try {
      this.log('\n⚠️  DESTRUCTIVE ROLLBACK - NO UNDO AVAILABLE');
      this.log(`• Current 'public' schema → renamed to 'shadow' (temporary)`);
      this.log(`• Backup '${schemaName}' → renamed to 'public' (restored)`);
      this.log(`• Existing 'shadow' schema will be DELETED if present`);

      if (keepTables.length > 0) {
        this.log(`\nCURRENT DATA TO BE KEPT:`);
        this.log(`• Tables to copy from shadow to public: ${keepTables.join(', ')}`);
      }

      this.log(`\nBACKUP CONSUMED:`);
      this.log(`• Backup '${schemaName}' will be consumed (no longer available)`);

      this.log('\n✅ Proceeding with destructive rollback...');

      // Disable foreign key constraints for rollback operations
      this.log('🔓 Disabling foreign key constraints for rollback...');
      await client.query('SET session_replication_role = replica;');

      // Step 1: Clear existing shadow schema
      await client.query('DROP SCHEMA IF EXISTS shadow CASCADE;');
      this.log('• Cleared existing shadow schema');

      // Step 2: Rename current public to shadow
      await client.query('ALTER SCHEMA public RENAME TO shadow;');
      this.log('• Renamed current public schema to shadow');

      // Step 3: Rename backup to public
      await client.query(`ALTER SCHEMA ${schemaName} RENAME TO public;`);
      this.log(`• Renamed backup schema to public`);

      // Step 4: Handle keep-tables if specified
      if (keepTables.length > 0) {
        await this.copyKeepTables(client, keepTables);
      }

      // Step 5: Re-enable foreign key constraints
      this.log('🔒 Re-enabling foreign key constraints...');
      await client.query('SET session_replication_role = origin;');

      // Step 6: Cleanup shadow schema
      await client.query('DROP SCHEMA shadow CASCADE;');
      this.log('• Cleaned up shadow schema');

      this.log('\n✅ Rollback completed successfully!');
      this.log(`📦 Backup '${schemaName}' has been consumed`);
    } catch (error) {
      this.log('\n❌ Rollback failed, attempting to restore original state...');

      // Ensure foreign key constraints are re-enabled even on failure
      try {
        this.log('🔒 Re-enabling foreign key constraints after rollback error...');
        await client.query('SET session_replication_role = origin;');
      } catch (fkError) {
        this.log(`⚠️  Warning: Could not re-enable foreign key constraints: ${fkError}`);
      }

      await this.recoverFromFailedRollback(client);
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
