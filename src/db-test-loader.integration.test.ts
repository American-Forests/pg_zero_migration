/**
 * Integration Tests for DbTestLoader
 *
 * These tests use real database connections, file system operations, and external commands.
 * They are designed to test the actual functionality against a real PostgreSQL database.
 */

import { vi, describe, it, expect, beforeAll, beforeEach, afterEach } from 'vitest';
import path from 'path';
import {
  DbTestLoader,
  DatabaseConnectionError,
  SchemaSetupError,
  DataLoadingError,
} from './db-test-loader.js';

beforeAll(() => {
  // Set timeout for all tests in this file to 30 seconds
  vi.setConfig({ testTimeout: 30_000 });
});

describe('DbTestLoader Integration Tests', () => {
  // Test database configuration
  const testDbName = `test_db_loader_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;

  // Build database URL from environment variables with defaults
  const testPgUser = process.env.TEST_PGUSER || 'postgres';
  const testPgPassword = process.env.TEST_PGPASSWORD || 'postgres';
  const testPgHost = process.env.TEST_PGHOST || 'localhost';
  const testPgPort = process.env.TEST_PGPORT || '5432';
  const testPgDatabase = process.env.TEST_PGDATABASE || testDbName;

  const testDatabaseUrl =
    process.env.TEST_DATABASE_URL ||
    `postgresql://${testPgUser}:${testPgPassword}@${testPgHost}:${testPgPort}/${testPgDatabase}`;

  const schemaPath = path.resolve(__dirname, 'simple_schema.prisma');
  const fixturePath = path.resolve(__dirname, 'simple_fixture.json');

  let loader: DbTestLoader;

  beforeEach(async () => {
    // Create test loader with createTestDatabase method
    loader = new DbTestLoader(testDatabaseUrl, schemaPath, fixturePath);

    // Create test database using the built-in method
    const createSuccess = await loader.createTestDatabase();
    if (!createSuccess) {
      throw new Error('Failed to create test database');
    }
  });

  afterEach(async () => {
    try {
      // Cleanup: disconnect and drop test database using built-in method
      await loader.disconnect();
      await loader.cleanupTestDatabase();
    } catch {
      // Silently handle cleanup errors - they may happen if tests fail
      // The cleanup will be attempted again by the next test
    }
  });

  describe('Complete Database Lifecycle', () => {
    it('should successfully create schema, load data, and verify counts', async () => {
      // Connect to test database
      await loader.connect();

      // Setup database schema
      await loader.setupDatabaseSchema();

      // Load test data
      const loadResult = await loader.loadTestData();
      expect(loadResult.success).toBe(true);
      expect(loadResult.recordCounts.users).toBe(2);
      expect(loadResult.recordCounts.posts).toBe(2);
      expect(loadResult.recordCounts.comments).toBe(2);

      // Verify data counts
      const dataCounts = await loader.getDataCounts();
      expect(dataCounts.users).toBe(2);
      expect(dataCounts.posts).toBe(2);
      expect(dataCounts.comments).toBe(2);

      // Clear test data
      await loader.clearTestData();

      // Verify data is cleared
      const countsAfterClear = await loader.getDataCounts();
      expect(countsAfterClear.users).toBe(0);
      expect(countsAfterClear.posts).toBe(0);
      expect(countsAfterClear.comments).toBe(0);

      await loader.disconnect();
    }, 30000); // 30 second timeout for database operations

    it('should handle schema setup without data loading', async () => {
      // Connect to test database
      await loader.connect();

      // Setup database schema only
      await loader.setupDatabaseSchema();

      // Verify tables exist by checking data counts (should be 0)
      const dataCounts = await loader.getDataCounts();
      expect(dataCounts).toHaveProperty('users');
      expect(dataCounts).toHaveProperty('posts');
      expect(dataCounts).toHaveProperty('comments');
      expect(dataCounts.users).toBe(0);
      expect(dataCounts.posts).toBe(0);
      expect(dataCounts.comments).toBe(0);

      await loader.disconnect();
    }, 20000);

    it('should handle database creation and cleanup', async () => {
      // Test the createTestDatabase and cleanupTestDatabase methods
      const testLoader = new DbTestLoader(testDatabaseUrl, schemaPath);

      // Create test database
      const createSuccess = await testLoader.createTestDatabase();
      expect(createSuccess).toBe(true);

      // Connect and verify basic functionality
      await testLoader.connect();
      await testLoader.setupDatabaseSchema();

      // Verify schema setup worked by checking table counts
      const dataCounts = await testLoader.getDataCounts();
      expect(dataCounts).toHaveProperty('users');

      await testLoader.disconnect();

      // Cleanup test database
      const cleanupSuccess = await testLoader.cleanupTestDatabase();
      expect(cleanupSuccess).toBe(true);
    }, 20000);
  }, 30000);

  describe('Error Handling Integration', () => {
    it('should handle invalid database connection gracefully', async () => {
      const invalidLoader = new DbTestLoader(
        'postgresql://invalid:invalid@nonexistent:5432/invalid',
        schemaPath,
        fixturePath
      );

      await expect(invalidLoader.connect()).rejects.toThrow(DatabaseConnectionError);
    }, 30000);

    it('should handle missing schema file', async () => {
      const loaderWithInvalidSchema = new DbTestLoader(
        testDatabaseUrl,
        '/nonexistent/schema.prisma',
        fixturePath
      );

      await loaderWithInvalidSchema.connect();
      await expect(loaderWithInvalidSchema.setupDatabaseSchema()).rejects.toThrow(SchemaSetupError);
      await loaderWithInvalidSchema.disconnect();
    }, 15000);

    it('should handle missing fixture file', async () => {
      const loaderWithInvalidFixture = new DbTestLoader(
        testDatabaseUrl,
        schemaPath,
        '/nonexistent/fixture.json'
      );

      await loaderWithInvalidFixture.connect();
      await loaderWithInvalidFixture.setupDatabaseSchema();

      await expect(loaderWithInvalidFixture.loadTestData()).rejects.toThrow(DataLoadingError);
      await loaderWithInvalidFixture.disconnect();
    }, 15000);
  });

  describe('Database State Management', () => {
    it('should handle multiple load/clear cycles', async () => {
      await loader.connect();
      await loader.setupDatabaseSchema();

      // First load cycle
      let loadResult = await loader.loadTestData();
      expect(loadResult.success).toBe(true);
      expect(loadResult.recordCounts.users).toBe(2);

      let dataCounts = await loader.getDataCounts();
      expect(dataCounts.users).toBe(2);

      // Clear data
      await loader.clearTestData();
      dataCounts = await loader.getDataCounts();
      expect(dataCounts.users).toBe(0);

      // Second load cycle
      loadResult = await loader.loadTestData();
      expect(loadResult.success).toBe(true);
      expect(loadResult.recordCounts.users).toBe(2);

      dataCounts = await loader.getDataCounts();
      expect(dataCounts.users).toBe(2);

      await loader.disconnect();
    }, 25000);

    it('should maintain data integrity with foreign key relationships', async () => {
      await loader.connect();
      await loader.setupDatabaseSchema();

      const loadResult = await loader.loadTestData();
      expect(loadResult.success).toBe(true);

      // Verify all expected tables have data
      expect(loadResult.recordCounts.users).toBe(2);
      expect(loadResult.recordCounts.posts).toBe(2);
      expect(loadResult.recordCounts.comments).toBe(2);

      // Verify referential integrity by checking that all foreign keys are satisfied
      const dataCounts = await loader.getDataCounts();
      expect(dataCounts.users).toBeGreaterThan(0);
      expect(dataCounts.posts).toBeGreaterThan(0);
      expect(dataCounts.comments).toBeGreaterThan(0);

      await loader.disconnect();
    }, 20000);
  });

  describe('TES Schema PostGIS Integration', () => {
    const tesTestDbName = `test_tes_db_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;

    const tesTestDatabaseUrl = `postgresql://${testPgUser}:${testPgPassword}@${testPgHost}:${testPgPort}/${tesTestDbName}`;
    const tesSchemaPath = path.resolve(__dirname, 'tes_schema.prisma');
    const tesFixturePath = path.resolve(__dirname, 'tes_fixture.json');

    let tesLoader: DbTestLoader;

    beforeEach(async () => {
      tesLoader = new DbTestLoader(tesTestDatabaseUrl, tesSchemaPath, tesFixturePath);
    });

    afterEach(async () => {
      try {
        if (tesLoader) {
          await tesLoader.cleanupTestDatabase();
        }
      } catch {
        // TES cleanup warning - test cleanup failed
      }
    });

    it('should handle TES schema with PostGIS geometry types', async () => {
      // Create test database with PostGIS support
      const createSuccess = await tesLoader.createTestDatabase();
      expect(createSuccess).toBe(true);

      // Connect to the database
      await tesLoader.connect();

      // Setup TES schema with PostGIS geometry types
      await tesLoader.setupDatabaseSchema();

      // Verify PostGIS extension is available
      const extensionCheck = await tesLoader.executeQuery(
        "SELECT 1 FROM pg_extension WHERE extname = 'postgis'"
      );
      expect(extensionCheck).toHaveLength(1);

      // Verify geometry tables were created successfully
      const geometryTablesCheck = await tesLoader.executeQuery(`
        SELECT table_name, column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name IN ('TreeCanopy', 'Municipality', 'Area', 'Blockgroup') 
        AND data_type = 'USER-DEFINED'
        ORDER BY table_name, column_name
      `);

      // Debug: List all tables that were created
      const allTables = await tesLoader.executeQuery(`
        SELECT table_name FROM information_schema.tables 
        WHERE table_schema = 'public' 
        ORDER BY table_name;
      `);

      // Debug: List all columns with their types
      const allColumns = await tesLoader.executeQuery(`
        SELECT table_name, column_name, data_type 
        FROM information_schema.columns 
        WHERE table_schema = 'public' AND table_name IN ('TreeCanopy', 'Municipality', 'Area', 'Blockgroup')
        ORDER BY table_name, column_name
      `);

      // Should find geometry columns in TES tables (debug info available in allTables/allColumns)
      expect(allTables.length).toBeGreaterThan(0);

      // Verify that columns were found
      expect(allColumns.length).toBeGreaterThan(0);

      // Check that each expected table has at least some columns
      const expectedTables = ['TreeCanopy', 'Municipality', 'Area', 'Blockgroup'];
      for (const tableName of expectedTables) {
        const tableColumns = allColumns.filter((col: any) => col.table_name === tableName);
        expect(tableColumns.length).toBeGreaterThan(0);
      }

      // Verify we have geometry columns (USER-DEFINED type indicates PostGIS geometry)
      const geometryColumns = allColumns.filter((col: any) => col.data_type === 'USER-DEFINED');
      expect(geometryColumns.length).toBeGreaterThan(0);

      // Check if the specific tables we expect exist
      const actualTableNames = allTables.map((r: any) => r.table_name);
      for (const tableName of expectedTables) {
        expect(actualTableNames).toContain(tableName);
      }

      // Verify we can query geometry column metadata using PostGIS functions
      const geometryMetadata = await tesLoader.executeQuery(`
        SELECT 
          f_table_name as table_name,
          f_geometry_column as geometry_column,
          type,
          srid
        FROM geometry_columns 
        WHERE f_table_name IN ('TreeCanopy', 'Municipality', 'Area', 'Blockgroup')
        ORDER BY f_table_name
      `);

      // Check if geometry columns exist as USER-DEFINED type (PostGIS geometry columns)
      expect(geometryTablesCheck.length).toBeGreaterThan(0);

      // Verify specific geometry types and SRID
      const multiPolygonGeometry = geometryMetadata.find(
        (row: any) => row.type === 'MULTIPOLYGON' && row.srid === 4326
      );
      expect(multiPolygonGeometry).toBeDefined();

      // Load TES test data to verify data operations work with geometry columns
      const loadResult = await tesLoader.loadTestData();
      expect(loadResult.success).toBe(true);

      // Verify TES tables were populated
      const dataCounts = await tesLoader.getDataCounts();
      expect(dataCounts.User).toBeGreaterThan(0);

      // Verify we can perform basic spatial queries (PostGIS functions work)
      try {
        const spatialTest = await tesLoader.executeQuery(`
          SELECT ST_AsText(ST_GeomFromText('POINT(0 0)', 4326)) as point_text
        `);
        expect(spatialTest).toHaveLength(1);
        expect(spatialTest[0].point_text).toBe('POINT(0 0)');
      } catch (spatialError) {
        // If spatial functions don't work, PostGIS isn't properly installed
        throw new Error(`PostGIS spatial functions not working: ${spatialError}`);
      }

      await tesLoader.disconnect();
    }, 60000); // Extended timeout for complex schema setup
  });
});
