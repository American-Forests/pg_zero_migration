/**
 * CLI Integration Tests for migration.ts
 *
 * These tests verify the CLI functionality including log file creation
 * by executing the migration CLI commands directly.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import path from 'path';
import fs from 'fs';
import { execa } from 'execa';
import { DbTestLoaderMulti } from './test-loader-multi.js';

describe('Migration CLI Integration Tests', () => {
  // Test database configuration
  const testDbNameSource = `test_cli_migration_source_${Date.now()}_${Math.random()
    .toString(36)
    .substring(2, 8)}`;
  const testDbNameDest = `test_cli_migration_dest_${Date.now()}_${Math.random()
    .toString(36)
    .substring(2, 8)}`;

  const testPgHost = process.env.TEST_PGHOST || 'localhost';
  const testPgPort = process.env.TEST_PGPORT || '5432';
  const testPgUser = process.env.TEST_PGUSER || 'postgres';
  const testPgPassword = process.env.TEST_PGPASSWORD || 'postgres';

  const expectedSourceUrl = `postgresql://${testPgUser}:${testPgPassword}@${testPgHost}:${testPgPort}/${testDbNameSource}`;
  const expectedDestUrl = `postgresql://${testPgUser}:${testPgPassword}@${testPgHost}:${testPgPort}/${testDbNameDest}`;

  let multiLoader: DbTestLoaderMulti;

  beforeEach(async () => {
    console.log(`Setting up CLI test databases: ${testDbNameSource} -> ${testDbNameDest}`);

    // Initialize the multi-loader for TES schema
    const tesSchemaPath = path.join(process.cwd(), 'src', 'test', 'tes_schema.prisma');
    multiLoader = new DbTestLoaderMulti(expectedSourceUrl, expectedDestUrl, tesSchemaPath);

    // Initialize loaders and create databases
    multiLoader.initializeLoaders();
    await multiLoader.createTestDatabases();
    await multiLoader.setupDatabaseSchemas();
  });
  afterEach(async () => {
    console.log('Cleaning up CLI test databases...');
    if (multiLoader) {
      await multiLoader.cleanupTestDatabases();
    }

    // Clean up any log files created during testing
    const cwd = process.cwd();
    const logFiles = fs
      .readdirSync(cwd)
      .filter(file => file.startsWith('migration_') && file.endsWith('.log'));

    for (const logFile of logFiles) {
      try {
        fs.unlinkSync(path.join(cwd, logFile));
        console.log(`🧹 Cleaned up log file: ${logFile}`);
      } catch (error) {
        console.warn(`Warning: Could not clean up log file ${logFile}:`, error);
      }
    }
    console.log('✓ CLI test cleanup completed');
  });

  it('should do multiple migrations then rollback and clear successfully', async () => {
    console.log('🚀 Starting comprehensive CLI multi-migration test...');

    const sourceLoader = multiLoader.getSourceLoader();
    const destLoader = multiLoader.getDestLoader();

    if (!sourceLoader || !destLoader) {
      throw new Error('Test loaders not initialized');
    }

    // Load test data into both databases
    await sourceLoader.loadTestData();
    await destLoader.loadTestData();

    // Get the actual database URLs that were created
    const sourceConnectionInfo = sourceLoader.getConnectionInfo();
    const destConnectionInfo = destLoader.getConnectionInfo();
    const actualSourceUrl = sourceConnectionInfo.url;
    const actualDestUrl = destConnectionInfo.url;

    console.log(`📋 Using source database: ${actualSourceUrl}`);
    console.log(`📋 Using destination database: ${actualDestUrl}`);

    // Get the current working directory for log file detection
    const cwd = process.cwd();
    const migrationScript = path.join(cwd, 'src', 'migration.ts');
    const nodeCommand = 'npx';
    const preservedTables = 'BlockgroupOnScenario,AreaOnScenario,Scenario,User';

    // Track log files throughout the test
    const getLogFiles = () =>
      fs.readdirSync(cwd).filter(file => file.startsWith('migration_') && file.endsWith('.log'));

    const initialLogFiles = getLogFiles();
    console.log(`📋 Found ${initialLogFiles.length} existing log files before migrations`);

    // ========================================
    // PHASE 1: One-Phase Migration (start command)
    // ========================================
    console.log('\n🔄 PHASE 1: Running one-phase migration (start command)...');

    const startArgs = [
      'tsx',
      migrationScript,
      'start',
      '--source',
      actualSourceUrl,
      '--dest',
      actualDestUrl,
      '--preserved-tables',
      preservedTables,
    ];

    console.log(`Command: ${nodeCommand} ${startArgs.join(' ')}`);
    await execa(nodeCommand, startArgs, {
      cwd,
      env: { ...process.env, NODE_ENV: 'test' },
    });

    console.log('✅ One-phase migration completed successfully');

    // Verify first log file created
    const afterStartLogFiles = getLogFiles();
    const startLogFiles = afterStartLogFiles.filter(file => !initialLogFiles.includes(file));
    expect(startLogFiles).toHaveLength(1);

    const startLogPath = path.join(cwd, startLogFiles[0]);
    const startLogContent = fs.readFileSync(startLogPath, 'utf-8');

    // Validate start log content (keeping original validation logic)
    expect(startLogContent).toContain('DATABASE MIGRATION LOG');
    expect(startLogContent).toContain('Migration Outcome: SUCCESS');
    expect(startLogContent).toContain('Start Time:');
    expect(startLogContent).toContain('End Time:');
    expect(startLogContent).toContain('Duration:');
    expect(startLogContent).toContain('Source Database:');
    expect(startLogContent).toContain('Destination Database:');
    expect(startLogContent).toContain('Migration Statistics:');
    expect(startLogContent).toContain('Phase 1: Creating source dump');
    expect(startLogContent).toContain('Phase 4: Performing atomic table swap');

    console.log(`✅ First migration log validated: ${startLogFiles[0]}`);

    // ========================================
    // PHASE 2: Two-Phase Migration (prepare command)
    // ========================================
    console.log('\n🔄 PHASE 2: Running two-phase migration - prepare...');

    const prepareArgs = [
      'tsx',
      migrationScript,
      'prepare',
      '--source',
      actualSourceUrl,
      '--dest',
      actualDestUrl,
      '--preserved-tables',
      preservedTables,
    ];

    console.log(`Command: ${nodeCommand} ${prepareArgs.join(' ')}`);
    await execa(nodeCommand, prepareArgs, {
      cwd,
      env: { ...process.env, NODE_ENV: 'test' },
    });

    console.log('✅ Prepare phase completed successfully');

    // Note: prepare command doesn't create a log file - only start command does
    console.log(`✅ Prepare command completed without log file creation`);

    // ========================================
    // PHASE 3: Two-Phase Migration (swap command)
    // ========================================
    console.log('\n🔄 PHASE 3: Running two-phase migration - swap...');

    const swapArgs = ['tsx', migrationScript, 'swap', '--dest', actualDestUrl];

    console.log(`Command: ${nodeCommand} ${swapArgs.join(' ')}`);
    await execa(nodeCommand, swapArgs, {
      cwd,
      env: { ...process.env, NODE_ENV: 'test' },
    });

    console.log('✅ Swap phase completed successfully');

    // Note: swap command doesn't create a log file - only start command does
    console.log(`✅ Swap command completed without log file creation`);

    // ========================================
    // PHASE 4: List Backups (should show 1)
    // ========================================
    console.log('\n🔄 PHASE 4: Listing backups (expecting 1)...');

    const listArgs = ['tsx', migrationScript, 'list', '--dest', actualDestUrl, '--json'];

    console.log(`Command: ${nodeCommand} ${listArgs.join(' ')}`);
    const listResult = await execa(nodeCommand, listArgs, {
      cwd,
      env: { ...process.env, NODE_ENV: 'test' },
    });

    console.log('✅ List command completed successfully');

    // Parse and verify backup list
    const backupList = JSON.parse(listResult.stdout);
    expect(Array.isArray(backupList)).toBe(true);
    expect(backupList).toHaveLength(1); // Only 1 backup since second migration replaced first

    // Verify backup has expected metadata
    const backup = backupList[0];
    expect(backup).toHaveProperty('timestamp');
    expect(backup).toHaveProperty('tableCount');
    expect(backup.tableCount).toBeGreaterThan(0);
    console.log(`✅ Backup validated: timestamp=${backup.timestamp}, tables=${backup.tableCount}`);

    // ========================================
    // PHASE 5: Rollback (should consume the backup)
    // ========================================
    console.log('\n🔄 PHASE 5: Rolling back to backup...');

    const rollbackArgs = ['tsx', migrationScript, 'rollback', '--latest', '--dest', actualDestUrl];

    console.log(`Command: ${nodeCommand} ${rollbackArgs.join(' ')}`);
    await execa(nodeCommand, rollbackArgs, {
      cwd,
      env: { ...process.env, NODE_ENV: 'test' },
    });

    console.log('✅ Rollback completed successfully');

    // ========================================
    // PHASE 6: List Backups Again (should show 0)
    // ========================================
    console.log('\n🔄 PHASE 6: Listing backups after rollback (expecting 0)...');

    const listAfterRollbackResult = await execa(nodeCommand, listArgs, {
      cwd,
      env: { ...process.env, NODE_ENV: 'test' },
    });

    const backupListAfterRollback = JSON.parse(listAfterRollbackResult.stdout);
    expect(Array.isArray(backupListAfterRollback)).toBe(true);
    expect(backupListAfterRollback).toHaveLength(0);

    console.log('✅ No backups remain after rollback');

    // ========================================
    // PHASE 7: Verify No Cleanup Needed (should show 0 backups)
    // ========================================
    console.log('\n🔄 PHASE 7: Final verification (expecting 0 backups)...');

    const finalListResult = await execa(nodeCommand, listArgs, {
      cwd,
      env: { ...process.env, NODE_ENV: 'test' },
    });

    const finalBackupList = JSON.parse(finalListResult.stdout);
    expect(Array.isArray(finalBackupList)).toBe(true);
    expect(finalBackupList).toHaveLength(0);

    console.log('✅ No backups remain - cleanup not needed');

    // ========================================
    // FINAL VALIDATION
    // ========================================
    const finalLogFiles = getLogFiles();
    const totalNewLogFiles = finalLogFiles.filter(file => !initialLogFiles.includes(file));
    expect(totalNewLogFiles).toHaveLength(1); // Only start command creates log files

    console.log('\n🎉 Comprehensive CLI multi-migration test completed successfully!');
    console.log(`📊 Summary:`);
    console.log(`   - Log files created: ${totalNewLogFiles.length} (only start command)`);
    console.log(`   - Migrations performed: 2 (1 single-phase + 1 two-phase)`);
    console.log(`   - Backup behavior: Second migration replaced first backup`);
    console.log(`   - Commands tested: start, prepare, swap, list, rollback`);
    console.log(`   - Final backup count: 0 (consumed by rollback)`);
  }, 60000); // 60 second timeout for comprehensive CLI execution
});
