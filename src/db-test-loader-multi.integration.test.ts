/**
 * Integration Tests for DbTestLoaderMulti
 *
 * These tests use real database connections, file system operations, and external commands.
 * They are designed to test the actual multi-database functionality against real PostgreSQL databases.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import path from 'path';
import { DbTestLoaderMulti } from './db-test-loader-multi.js';

describe('DbTestLoaderMulti Integration Tests', () => {
  // Test database configuration
  const testDbNameSource = `test_multi_source_${Date.now()}_${Math.random()
    .toString(36)
    .substr(2, 9)}`;
  const testDbNameDest = `test_multi_dest_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;

  // DbTestLoaderMulti appends suffixes to ensure unique names
  const expectedSourceDb = `${testDbNameSource}_migration_source`;
  const expectedDestDb = `${testDbNameDest}_migration_dest`;

  // Build database URLs from environment variables with defaults
  const testPgUser = process.env.TEST_PGUSER || 'postgres';
  const testPgPassword = process.env.TEST_PGPASSWORD || 'postgres';
  const testPgHost = process.env.TEST_PGHOST || 'localhost';
  const testPgPort = process.env.TEST_PGPORT || '5432';

  const sourceUrl =
    process.env.TEST_SOURCE_DATABASE_URL ||
    `postgresql://${testPgUser}:${testPgPassword}@${testPgHost}:${testPgPort}/${testDbNameSource}`;
  const destUrl =
    process.env.TEST_DEST_DATABASE_URL ||
    `postgresql://${testPgUser}:${testPgPassword}@${testPgHost}:${testPgPort}/${testDbNameDest}`;

  // Expected URLs with the suffix appended by DbTestLoaderMulti
  const expectedSourceUrl = `postgresql://${testPgUser}:${testPgPassword}@${testPgHost}:${testPgPort}/${expectedSourceDb}`;
  const expectedDestUrl = `postgresql://${testPgUser}:${testPgPassword}@${testPgHost}:${testPgPort}/${expectedDestDb}`;

  const schemaPath = path.resolve(__dirname, 'simple_schema.prisma');
  const sourceFixturePath = path.resolve(__dirname, 'simple_fixture.json');
  const destFixturePath = path.resolve(__dirname, 'simple_fixture.json');

  let multiLoader: DbTestLoaderMulti;

  beforeEach(async () => {
    // Create multi-database loader
    multiLoader = new DbTestLoaderMulti(sourceUrl, destUrl, schemaPath);
  });

  afterEach(async () => {
    try {
      // Cleanup: drop test databases
      await multiLoader.cleanupTestDatabases();
    } catch {
      // Silently handle cleanup errors - they may happen if tests fail
      // The cleanup will be attempted again by the next test
    }
  });

  describe('Fast Integration Test', () => {
    it('should successfully manage dual database setup and verify record counts', async () => {
      // Test URL construction and database naming
      const connectionInfo = multiLoader.getConnectionInfo();
      expect(connectionInfo.sourceUrl).toBe(expectedSourceUrl);
      expect(connectionInfo.destUrl).toBe(expectedDestUrl);
      expect(connectionInfo.sourceDb).toBe(expectedSourceDb);
      expect(connectionInfo.destDb).toBe(expectedDestDb);

      // Initialize loaders with fixture data for record verification
      const initialized = multiLoader.initializeLoaders(sourceFixturePath, destFixturePath);
      expect(initialized).toBe(true);

      // Verify loaders are accessible and independent
      const sourceLoader = multiLoader.getSourceLoader()!;
      const destLoader = multiLoader.getDestLoader()!;
      expect(sourceLoader).toBeDefined();
      expect(destLoader).toBeDefined();
      expect(sourceLoader).not.toBe(destLoader);

      // Create test databases
      const created = await multiLoader.createTestDatabases();
      expect(created).toBe(true);

      // Setup database schemas
      const schemaSetup = await multiLoader.setupDatabaseSchemas();
      expect(schemaSetup).toBe(true);

      // Load test data into both databases
      await sourceLoader.loadTestData();
      await destLoader.loadTestData();

      // Verify both databases contain the correct number of records
      const sourceCounts = await sourceLoader.getDataCounts();
      const destCounts = await destLoader.getDataCounts();

      // Both databases should have the same record counts from simple_fixture.json
      expect(sourceCounts.User).toBe(2);
      expect(sourceCounts.Post).toBe(2);
      expect(sourceCounts.Comment).toBe(2);

      expect(destCounts.User).toBe(2);
      expect(destCounts.Post).toBe(2);
      expect(destCounts.Comment).toBe(2);

      // Verify connection info remains consistent
      const finalConnectionInfo = multiLoader.getConnectionInfo();
      expect(finalConnectionInfo.sourceDb).toBe(expectedSourceDb);
      expect(finalConnectionInfo.destDb).toBe(expectedDestDb);
    }, 15000);
  });
});
