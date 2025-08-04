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

  it('should create migration log file when running CLI migration command', async () => {
    console.log('🚀 Starting CLI migration log file test...');

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

    // List existing log files before migration to avoid conflicts
    const existingLogFiles = fs
      .readdirSync(cwd)
      .filter(file => file.startsWith('migration_') && file.endsWith('.log'));

    console.log(`📋 Found ${existingLogFiles.length} existing log files before migration`);

    // Build the CLI command to run migration
    const migrationScript = path.join(cwd, 'src', 'migration.ts');
    const nodeCommand = 'npx';
    const args = [
      'tsx',
      migrationScript,
      'start',
      '--source',
      actualSourceUrl,
      '--dest',
      actualDestUrl,
      '--preserved-tables',
      'BlockgroupOnScenario,AreaOnScenario,Scenario,User',
    ];

    console.log('🔄 Running CLI migration command...');
    console.log(`Command: ${nodeCommand} ${args.join(' ')}`);

    // Execute the CLI command
    const result = await execa(nodeCommand, args, {
      cwd,
      env: {
        ...process.env,
        NODE_ENV: 'test',
      },
    });

    console.log('✅ CLI migration completed successfully');
    console.log('Migration output:', result.stdout);

    // Find the newly created log file
    const allLogFiles = fs
      .readdirSync(cwd)
      .filter(file => file.startsWith('migration_') && file.endsWith('.log'));

    const newLogFiles = allLogFiles.filter(file => !existingLogFiles.includes(file));
    expect(newLogFiles).toHaveLength(1);

    const logFilePath = path.join(cwd, newLogFiles[0]);
    console.log(`📄 Found log file: ${newLogFiles[0]}`);

    // Verify log file exists and is readable
    expect(fs.existsSync(logFilePath)).toBe(true);
    const logContent = fs.readFileSync(logFilePath, 'utf-8');
    expect(logContent.length).toBeGreaterThan(0);

    // Verify log file contains expected header information
    expect(logContent).toContain('DATABASE MIGRATION LOG');
    expect(logContent).toContain('Migration Outcome: SUCCESS');
    expect(logContent).toContain('Start Time:');
    expect(logContent).toContain('End Time:');
    expect(logContent).toContain('Duration:');

    // Verify database connection information is present
    expect(logContent).toContain('Source Database:');
    expect(logContent).toContain('Destination Database:');
    expect(logContent).toContain(`Host: ${testPgHost}:${testPgPort}`);

    // Verify migration statistics are present
    expect(logContent).toContain('Migration Statistics:');
    expect(logContent).toContain('Tables Processed:');
    expect(logContent).toContain('Records Migrated:');
    expect(logContent).toContain('Warnings:');
    expect(logContent).toContain('Errors:');

    // Verify phase timing information is present
    expect(logContent).toContain('Phase 1: Creating source dump');
    expect(logContent).toContain('Phase 2: Creating shadow tables in destination public schema');
    expect(logContent).toContain('Phase 4: Performing atomic table swap');

    // Verify log contains timing information with ISO 8601 timestamps
    const timestampPattern = /\[\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z\]/;
    expect(timestampPattern.test(logContent)).toBe(true);

    // Verify log contains duration information
    expect(logContent).toMatch(/successfully \(\d+(\.\d+)?s\)|successfully \(\d+ms\)/);

    console.log('✅ Log file verification completed successfully');
    console.log(`Log file size: ${logContent.length} bytes`);
  }, 30000); // 30 second timeout for CLI execution
});
