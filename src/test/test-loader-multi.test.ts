/**
 * Tests for DbTestLoaderMulti class
 *
 * These tests verify the functionality of the multi-database test loader
 * including URL parsing, loader initialization, and database management.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { DbTestLoaderMulti, MultiDatabaseError, type ConnectionInfo } from './test-loader-multi.js';
import { DbTestLoader } from './test-loader.js';

// Mock the dependencies
vi.mock('./test-loader.js', () => ({
  DbTestLoader: vi.fn().mockImplementation(() => ({
    createTestDatabase: vi.fn().mockResolvedValue(true),
    cleanupTestDatabase: vi.fn().mockResolvedValue(true),
    setupDatabaseSchema: vi.fn().mockResolvedValue(undefined),
  })),
}));

vi.mock('pg', () => ({
  Client: vi.fn().mockImplementation(() => ({
    connect: vi.fn().mockResolvedValue(undefined),
    end: vi.fn().mockResolvedValue(undefined),
    query: vi.fn().mockResolvedValue({
      rows: [{ version: 'PostgreSQL 14.0 on x86_64-pc-linux-gnu' }],
    }),
  })),
}));

vi.mock('execa', () => ({
  execa: vi.fn().mockImplementation((command: string, args: string[]) => {
    if (command === 'node' && args[0] === '--version') {
      return Promise.resolve({ stdout: 'v18.17.0' });
    }
    if (command === 'npx' && args[0] === 'prisma' && args[1] === '--version') {
      return Promise.resolve({ stdout: 'prisma                  : 5.0.0' });
    }
    return Promise.resolve({ stdout: '' });
  }),
}));

describe('DbTestLoaderMulti', () => {
  let dbMulti: DbTestLoaderMulti;
  const testSourceUrl = 'postgresql://user:pass@localhost:5432/testdb';
  const testDestUrl = 'postgresql://user:pass@remote:5432/destdb';
  const testSchemaPath = './simple_schema.prisma';

  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe('Constructor - URL Parsing and Database Naming', () => {
    it('should parse URLs correctly for same host scenario', () => {
      const dbMulti = new DbTestLoaderMulti(testSourceUrl, testSourceUrl, testSchemaPath);

      expect(dbMulti.sourceDb).toBe('testdb_migration_source');
      expect(dbMulti.destDb).toBe('testdb_migration_dest');
      expect(dbMulti.sourceUrl).toBe(
        'postgresql://user:pass@localhost:5432/testdb_migration_source'
      );
      expect(dbMulti.destUrl).toBe('postgresql://user:pass@localhost:5432/testdb_migration_dest');
      expect(dbMulti.sourceParsed.hostname).toBe('localhost');
      expect(dbMulti.destParsed.hostname).toBe('localhost');
    });

    it('should parse URLs correctly for different host scenario', () => {
      const dbMulti = new DbTestLoaderMulti(testSourceUrl, testDestUrl, testSchemaPath);

      expect(dbMulti.sourceDb).toBe('testdb_migration_source');
      expect(dbMulti.destDb).toBe('destdb_migration_dest');
      expect(dbMulti.sourceUrl).toBe(
        'postgresql://user:pass@localhost:5432/testdb_migration_source'
      );
      expect(dbMulti.destUrl).toBe('postgresql://user:pass@remote:5432/destdb_migration_dest');
      expect(dbMulti.sourceParsed.hostname).toBe('localhost');
      expect(dbMulti.destParsed.hostname).toBe('remote');
    });

    it('should default to source URL when dest URL not provided', () => {
      const dbMulti = new DbTestLoaderMulti(testSourceUrl, undefined, testSchemaPath);

      expect(dbMulti.destBaseUrl).toBe(testSourceUrl);
      expect(dbMulti.sourceDb).toBe('testdb_migration_source');
      expect(dbMulti.destDb).toBe('testdb_migration_dest');
    });

    it('should handle URL without explicit database name', () => {
      const urlWithoutDb = 'postgresql://user:pass@localhost:5432';
      const dbMulti = new DbTestLoaderMulti(urlWithoutDb, undefined, testSchemaPath);

      expect(dbMulti.sourceDb).toBe('test_migration_source');
      expect(dbMulti.destDb).toBe('test_migration_dest');
    });

    it('should throw MultiDatabaseError for invalid URLs', () => {
      const invalidUrl = 'not-a-url';

      expect(() => new DbTestLoaderMulti(invalidUrl, undefined, testSchemaPath)).toThrow(
        MultiDatabaseError
      );
    });

    it('should build admin URLs correctly', () => {
      const dbMulti = new DbTestLoaderMulti(testSourceUrl, testDestUrl, testSchemaPath);

      expect(dbMulti.sourceAdminUrl).toBe('postgresql://user:pass@localhost:5432/postgres');
      expect(dbMulti.destAdminUrl).toBe('postgresql://user:pass@remote:5432/postgres');
    });

    it('should handle URLs with different ports', () => {
      const sourceUrlWithPort = 'postgresql://user:pass@localhost:5433/testdb';
      const destUrlWithPort = 'postgresql://user:pass@remote:5434/destdb';
      const dbMulti = new DbTestLoaderMulti(sourceUrlWithPort, destUrlWithPort, testSchemaPath);

      expect(dbMulti.sourceUrl).toBe(
        'postgresql://user:pass@localhost:5433/testdb_migration_source'
      );
      expect(dbMulti.destUrl).toBe('postgresql://user:pass@remote:5434/destdb_migration_dest');
    });
  });

  describe('Loader Initialization', () => {
    beforeEach(() => {
      dbMulti = new DbTestLoaderMulti(testSourceUrl, testDestUrl, testSchemaPath);
    });

    it('should initialize loaders successfully with fixture files', () => {
      const result = dbMulti.initializeLoaders('./simple_fixture.json', './simple_fixture.json');

      expect(result).toBe(true);
      expect(DbTestLoader).toHaveBeenCalledTimes(2);
      expect(DbTestLoader).toHaveBeenCalledWith(
        dbMulti.sourceUrl,
        testSchemaPath,
        './simple_fixture.json'
      );
      expect(DbTestLoader).toHaveBeenCalledWith(
        dbMulti.destUrl,
        testSchemaPath,
        './simple_fixture.json'
      );
    });

    it('should initialize loaders successfully without fixture files', () => {
      const result = dbMulti.initializeLoaders();

      expect(result).toBe(true);
      expect(DbTestLoader).toHaveBeenCalledTimes(2);
      expect(DbTestLoader).toHaveBeenCalledWith(dbMulti.sourceUrl, testSchemaPath, undefined);
      expect(DbTestLoader).toHaveBeenCalledWith(dbMulti.destUrl, testSchemaPath, undefined);
    });

    it('should fail to initialize loaders without schema path', () => {
      const dbMultiNoSchema = new DbTestLoaderMulti(testSourceUrl, testDestUrl);
      const result = dbMultiNoSchema.initializeLoaders('./simple_fixture.json');

      expect(result).toBe(false);
      expect(DbTestLoader).not.toHaveBeenCalled();
    });

    it('should handle loader initialization errors', () => {
      vi.mocked(DbTestLoader).mockImplementationOnce(() => {
        throw new Error('Failed to create loader');
      });

      const result = dbMulti.initializeLoaders('./simple_fixture.json');

      expect(result).toBe(false);
    });
  });

  describe('Database Operations', () => {
    beforeEach(() => {
      dbMulti = new DbTestLoaderMulti(testSourceUrl, testDestUrl, testSchemaPath);
      dbMulti.initializeLoaders('./simple_fixture.json');
    });

    it('should fail to create databases when loaders not initialized', async () => {
      const uninitializedDbMulti = new DbTestLoaderMulti(
        testSourceUrl,
        testDestUrl,
        testSchemaPath
      );
      const result = await uninitializedDbMulti.createTestDatabases();

      expect(result).toBe(false);
    });

    it('should handle database creation errors', async () => {
      // Mock the first loader to return false for createTestDatabase
      const mockDbTestLoader = vi.mocked(DbTestLoader);
      mockDbTestLoader.mockImplementationOnce(
        () =>
          ({
            createTestDatabase: vi.fn().mockResolvedValue(false),
            cleanupTestDatabase: vi.fn().mockResolvedValue(true),
            setupDatabaseSchema: vi.fn().mockResolvedValue(undefined),
          }) as any
      );

      // Need to re-initialize loaders with the new mock
      const dbMultiTest = new DbTestLoaderMulti(testSourceUrl, testDestUrl, testSchemaPath);
      dbMultiTest.initializeLoaders('./simple_fixture.json');

      const result = await dbMultiTest.createTestDatabases();

      expect(result).toBe(false);
    });
  });

  describe('Connection Information', () => {
    it('should return correct connection info for same host', () => {
      const dbMulti = new DbTestLoaderMulti(testSourceUrl, testSourceUrl, testSchemaPath);
      const connInfo: ConnectionInfo = dbMulti.getConnectionInfo();

      expect(connInfo.sourceUrl).toBe(
        'postgresql://user:pass@localhost:5432/testdb_migration_source'
      );
      expect(connInfo.destUrl).toBe('postgresql://user:pass@localhost:5432/testdb_migration_dest');
      expect(connInfo.sourceDb).toBe('testdb_migration_source');
      expect(connInfo.destDb).toBe('testdb_migration_dest');
      expect(connInfo.sourceHost).toBe('localhost');
      expect(connInfo.destHost).toBe('localhost');
      expect(connInfo.multiHost).toBe(false);
    });

    it('should return correct connection info for different hosts', () => {
      const dbMulti = new DbTestLoaderMulti(testSourceUrl, testDestUrl, testSchemaPath);
      const connInfo: ConnectionInfo = dbMulti.getConnectionInfo();

      expect(connInfo.sourceUrl).toBe(
        'postgresql://user:pass@localhost:5432/testdb_migration_source'
      );
      expect(connInfo.destUrl).toBe('postgresql://user:pass@remote:5432/destdb_migration_dest');
      expect(connInfo.sourceDb).toBe('testdb_migration_source');
      expect(connInfo.destDb).toBe('destdb_migration_dest');
      expect(connInfo.sourceHost).toBe('localhost');
      expect(connInfo.destHost).toBe('remote');
      expect(connInfo.multiHost).toBe(true);
    });
  });

  describe('Loader Access', () => {
    beforeEach(() => {
      dbMulti = new DbTestLoaderMulti(testSourceUrl, testDestUrl, testSchemaPath);
    });

    it('should return undefined loaders before initialization', () => {
      expect(dbMulti.getSourceLoader()).toBeUndefined();
      expect(dbMulti.getDestLoader()).toBeUndefined();
    });

    it('should return loaders after initialization', () => {
      dbMulti.initializeLoaders('./simple_fixture.json');

      expect(dbMulti.getSourceLoader()).toBeDefined();
      expect(dbMulti.getDestLoader()).toBeDefined();
    });
  });

  describe('Error Classes', () => {
    it('should create MultiDatabaseError with cause', () => {
      const cause = new Error('Original error');
      const error = new MultiDatabaseError('Test error', cause);

      expect(error.name).toBe('MultiDatabaseError');
      expect(error.message).toBe('Test error');
      expect(error.cause).toBe(cause);
    });

    it('should create MultiDatabaseError without cause', () => {
      const error = new MultiDatabaseError('Test error');

      expect(error.name).toBe('MultiDatabaseError');
      expect(error.message).toBe('Test error');
      expect(error.cause).toBeUndefined();
    });
  });
});
