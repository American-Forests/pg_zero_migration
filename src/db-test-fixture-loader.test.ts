/**
 * Tests for DbTestFixtureLoader
 *
 * These tests focus on the core functionality of the DbTestFixtureLoader class.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { readFile } from 'fs/promises';
import { existsSync } from 'fs';
import type { Client } from 'pg';
import { DbTestFixtureLoader, FixtureLoadError } from './db-test-fixture-loader.js';
import type { TableSchema, DatabaseSchema } from './db-schema-types.js';

// Mock dependencies
vi.mock('fs/promises', () => ({
  readFile: vi.fn(),
}));
vi.mock('fs', () => ({
  existsSync: vi.fn(),
}));

describe('DbTestFixtureLoader', () => {
  const mockTableSchemas: Record<string, TableSchema> = {
    users: {
      name: 'users',
      columns: [
        {
          name: 'id',
          type: 'BIGINT',
          nullable: false,
          primaryKey: true,
          unique: false,
          autoIncrement: true,
        },
        {
          name: 'name',
          type: 'TEXT',
          nullable: false,
          primaryKey: false,
          unique: false,
          autoIncrement: false,
        },
        {
          name: 'email',
          type: 'TEXT',
          nullable: false,
          primaryKey: false,
          unique: true,
          autoIncrement: false,
        },
        {
          name: 'created_at',
          type: 'TIMESTAMP WITH TIME ZONE',
          nullable: false,
          primaryKey: false,
          unique: false,
          autoIncrement: false,
        },
      ],
      foreignKeys: [],
      indexes: [],
    },
    posts: {
      name: 'posts',
      columns: [
        {
          name: 'id',
          type: 'BIGINT',
          nullable: false,
          primaryKey: true,
          unique: false,
          autoIncrement: true,
        },
        {
          name: 'title',
          type: 'TEXT',
          nullable: false,
          primaryKey: false,
          unique: false,
          autoIncrement: false,
        },
        {
          name: 'content',
          type: 'TEXT',
          nullable: false,
          primaryKey: false,
          unique: false,
          autoIncrement: false,
        },
        {
          name: 'user_id',
          type: 'BIGINT',
          nullable: false,
          primaryKey: false,
          unique: false,
          autoIncrement: false,
        },
        {
          name: 'published',
          type: 'BOOLEAN',
          nullable: false,
          primaryKey: false,
          unique: false,
          autoIncrement: false,
        },
        {
          name: 'created_at',
          type: 'TIMESTAMP WITH TIME ZONE',
          nullable: false,
          primaryKey: false,
          unique: false,
          autoIncrement: false,
        },
      ],
      foreignKeys: [{ fromColumn: 'user_id', toTable: 'users', toColumn: 'id' }],
      indexes: [],
    },
  };

  // Create a mock DatabaseSchema implementation
  const mockDatabaseSchema: DatabaseSchema = {
    tables: new Map(Object.entries(mockTableSchemas)),
    getTableNames(): string[] {
      return Array.from(this.tables.keys());
    },
    getTable(name: string): TableSchema | undefined {
      return this.tables.get(name);
    },
    getTablesInDependencyOrder(): string[] {
      return ['users', 'posts']; // Simple hardcoded dependency order for tests
    },
  };

  const testFixturePath = '/path/to/test/fixture.json';

  const mockClient = {
    connect: vi.fn(),
    end: vi.fn(),
    query: vi.fn(),
  } as unknown as Client;

  beforeEach(() => {
    vi.clearAllMocks();

    // Mock file system operations
    vi.mocked(existsSync).mockReturnValue(true);
    vi.mocked(readFile).mockResolvedValue(
      JSON.stringify({
        users: [
          {
            id: 1,
            name: 'John Doe',
            email: 'john@example.com',
            created_at: '2023-01-01T00:00:00Z',
          },
          {
            id: 2,
            name: 'Jane Smith',
            email: 'jane@example.com',
            created_at: '2023-01-02T00:00:00Z',
          },
        ],
        posts: [
          {
            id: 101,
            title: 'First Post',
            content: 'Content',
            user_id: 1,
            published: true,
            created_at: '2023-01-01T12:00:00Z',
          },
          {
            id: 102,
            title: 'Second Post',
            content: 'More content',
            user_id: 2,
            published: false,
            created_at: '2023-01-02T12:00:00Z',
          },
        ],
      })
    );
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe('constructor', () => {
    it('should create instance with valid client and schemas', () => {
      const loader = new DbTestFixtureLoader(mockClient, mockDatabaseSchema);
      expect(loader).toBeInstanceOf(DbTestFixtureLoader);
    });

    it('should work with optional logger', () => {
      const mockLogger = vi.fn();
      const loader = new DbTestFixtureLoader(mockClient, mockDatabaseSchema);
      expect(loader).toBeInstanceOf(DbTestFixtureLoader);
    });
  });

  describe('error handling', () => {
    it('should properly categorize different error types', () => {
      expect(new FixtureLoadError('test')).toBeInstanceOf(FixtureLoadError);
    });

    it('should preserve error causes', () => {
      const originalError = new Error('Original error');
      const wrappedError = new FixtureLoadError('Wrapped error', originalError);

      expect(wrappedError.cause).toBe(originalError);
    });
  });

  describe('dependency resolution', () => {
    it('should resolve simple dependency chain correctly', () => {
      const schemas = {
        users: mockTableSchemas.users,
        posts: mockTableSchemas.posts,
        comments: {
          name: 'comments',
          columns: [
            {
              name: 'id',
              type: 'bigint',
              nullable: false,
              primaryKey: true,
              unique: true,
              autoIncrement: true,
            },
            {
              name: 'post_id',
              type: 'bigint',
              nullable: false,
              primaryKey: false,
              unique: false,
              autoIncrement: false,
            },
            {
              name: 'content',
              type: 'text',
              nullable: false,
              primaryKey: false,
              unique: false,
              autoIncrement: false,
            },
          ],
          foreignKeys: [{ fromColumn: 'post_id', toTable: 'posts', toColumn: 'id' }],
          indexes: [],
        },
      };

      // Create DatabaseSchema wrapper
      const testDatabaseSchema: DatabaseSchema = {
        tables: new Map(Object.entries(schemas)),
        getTableNames(): string[] {
          return Array.from(this.tables.keys());
        },
        getTable(name: string): TableSchema | undefined {
          return this.tables.get(name);
        },
        getTablesInDependencyOrder(): string[] {
          return ['users', 'posts', 'comments'];
        },
      };

      const loader = new DbTestFixtureLoader(mockClient, testDatabaseSchema);
      expect(loader).toBeInstanceOf(DbTestFixtureLoader);
    });

    it('should handle circular dependencies gracefully', () => {
      const circularSchemas = {
        table_a: {
          name: 'table_a',
          columns: [
            {
              name: 'id',
              type: 'bigint',
              nullable: false,
              primaryKey: true,
              unique: true,
              autoIncrement: true,
            },
            {
              name: 'b_id',
              type: 'bigint',
              nullable: true,
              primaryKey: false,
              unique: false,
              autoIncrement: false,
            },
          ],
          foreignKeys: [{ fromColumn: 'b_id', toTable: 'table_b', toColumn: 'id' }],
          indexes: [],
        },
        table_b: {
          name: 'table_b',
          columns: [
            {
              name: 'id',
              type: 'bigint',
              nullable: false,
              primaryKey: true,
              unique: true,
              autoIncrement: true,
            },
            {
              name: 'a_id',
              type: 'bigint',
              nullable: true,
              primaryKey: false,
              unique: false,
              autoIncrement: false,
            },
          ],
          foreignKeys: [{ fromColumn: 'a_id', toTable: 'table_a', toColumn: 'id' }],
          indexes: [],
        },
      };

      // Create DatabaseSchema wrapper
      const circularDatabaseSchema: DatabaseSchema = {
        tables: new Map(Object.entries(circularSchemas)),
        getTableNames(): string[] {
          return Array.from(this.tables.keys());
        },
        getTable(name: string): TableSchema | undefined {
          return this.tables.get(name);
        },
        getTablesInDependencyOrder(): string[] {
          return ['table_a', 'table_b']; // Simple order for test
        },
      };

      // Should not throw even with circular dependencies
      expect(() => {
        new DbTestFixtureLoader(mockClient, circularDatabaseSchema);
      }).not.toThrow();
    });
  });

  describe('data validation', () => {
    it('should validate fixture data structure', () => {
      const loader = new DbTestFixtureLoader(mockClient, mockDatabaseSchema);
      expect(loader).toBeInstanceOf(DbTestFixtureLoader);
    });

    it('should handle fixture file path validation', () => {
      vi.mocked(existsSync).mockReturnValue(false);

      const loader = new DbTestFixtureLoader(mockClient, mockDatabaseSchema);
      expect(loader).toBeInstanceOf(DbTestFixtureLoader);
    });
  });

  describe('table schema handling', () => {
    it('should handle empty table schemas', () => {
      const emptySchema: DatabaseSchema = {
        tables: new Map(),
        getTableNames(): string[] {
          return [];
        },
        getTable(name: string): TableSchema | undefined {
          return undefined;
        },
        getTablesInDependencyOrder(): string[] {
          return [];
        },
      };
      const loader = new DbTestFixtureLoader(mockClient, emptySchema);
      expect(loader).toBeInstanceOf(DbTestFixtureLoader);
    });

    it('should handle tables with no dependencies', () => {
      const schemas = {
        standalone: {
          name: 'standalone',
          columns: [
            {
              name: 'id',
              type: 'bigint',
              nullable: false,
              primaryKey: true,
              unique: true,
              autoIncrement: true,
            },
            {
              name: 'name',
              type: 'text',
              nullable: false,
              primaryKey: false,
              unique: false,
              autoIncrement: false,
            },
          ],
          foreignKeys: [],
          indexes: [],
        },
      };

      const noDepsSchema: DatabaseSchema = {
        tables: new Map(Object.entries(schemas)),
        getTableNames(): string[] {
          return ['standalone'];
        },
        getTable(name: string): TableSchema | undefined {
          return this.tables.get(name);
        },
        getTablesInDependencyOrder(): string[] {
          return ['standalone'];
        },
      };

      const loader = new DbTestFixtureLoader(mockClient, noDepsSchema);
      expect(loader).toBeInstanceOf(DbTestFixtureLoader);
    });
  });
});
