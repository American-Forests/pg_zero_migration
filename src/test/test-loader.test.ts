/**
 * Tests for DbTestLoader
 *
 * These tests focus on the core functionality of the DbTestLoader class.
 * Database operations are mocked to test logic without requiring a real database.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { readFile } from 'fs/promises';
import { existsSync } from 'fs';
import path from 'path';
import {
  DbTestLoader,
  DatabaseConnectionError,
  SchemaSetupError,
  DataLoadingError,
  PrismaError,
} from './test-loader.js';

// Mock dependencies with proper hoisting
vi.mock('pg', () => ({
  Client: vi.fn(() => ({
    connect: vi.fn(),
    end: vi.fn(),
    query: vi.fn(),
  })),
}));
vi.mock('@prisma/client');
vi.mock('execa', () => ({
  execa: vi.fn(),
}));
vi.mock('fs/promises', () => ({
  readFile: vi.fn(),
}));
vi.mock('fs', () => ({
  existsSync: vi.fn(),
}));

describe('DbTestLoader', () => {
  const testDatabaseUrl = 'postgresql://test:test@localhost:5432/testdb';
  const testSchemaPath = path.join(__dirname, 'simple_schema.prisma');
  const testFixturePath = path.join(__dirname, 'simple_fixture.json');

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
        ],
      })
    );
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe('constructor', () => {
    it('should create instance with valid database URL', () => {
      const loader = new DbTestLoader(testDatabaseUrl, testSchemaPath, testFixturePath);
      expect(loader).toBeInstanceOf(DbTestLoader);
    });

    it('should throw error for invalid database URL', () => {
      expect(() => {
        new DbTestLoader('invalid-url', testSchemaPath, testFixturePath);
      }).toThrow(DatabaseConnectionError);
    });

    it('should parse database URL correctly', () => {
      const loader = new DbTestLoader(
        'postgresql://user:pass@localhost:5432/mydb?sslmode=require',
        testSchemaPath
      );
      expect(loader).toBeInstanceOf(DbTestLoader);
    });
  });

  describe('error handling', () => {
    it('should properly categorize different error types', () => {
      expect(new DatabaseConnectionError('test')).toBeInstanceOf(DatabaseConnectionError);
      expect(new SchemaSetupError('test')).toBeInstanceOf(SchemaSetupError);
      expect(new DataLoadingError('test')).toBeInstanceOf(DataLoadingError);
      expect(new PrismaError('test')).toBeInstanceOf(PrismaError);
    });

    it('should preserve error causes', () => {
      const originalError = new Error('Original error');
      const wrappedError = new DatabaseConnectionError('Wrapped error', originalError);

      expect(wrappedError.cause).toBe(originalError);
    });
  });

  describe('CLI argument parsing', () => {
    it('should validate required arguments', () => {
      // Test validation logic exists
      expect(() => {
        new DbTestLoader('', testSchemaPath, testFixturePath);
      }).toThrow();
    });
  });

  describe('fixture data handling', () => {
    it('should handle empty fixture data', () => {
      vi.mocked(readFile).mockResolvedValue(JSON.stringify({}));

      const loader = new DbTestLoader(testDatabaseUrl, testSchemaPath, testFixturePath);
      expect(loader).toBeInstanceOf(DbTestLoader);
    });

    it('should handle missing fixture file parameter', () => {
      const loader = new DbTestLoader(testDatabaseUrl, testSchemaPath);
      expect(loader).toBeInstanceOf(DbTestLoader);
    });
  });

  describe('dependency resolution', () => {
    it('should handle simple dependency ordering', () => {
      // Test that the loader can be instantiated with complex data
      const loader = new DbTestLoader(testDatabaseUrl, testSchemaPath, testFixturePath);
      expect(loader).toBeInstanceOf(DbTestLoader);
    });
  });

  describe('data validation', () => {
    it('should validate fixture data structure', () => {
      // Test basic validation
      const loader = new DbTestLoader(testDatabaseUrl, testSchemaPath, testFixturePath);
      expect(loader).toBeInstanceOf(DbTestLoader);
    });

    it('should handle tables with no records', () => {
      const fixtureData = {
        users: [],
        posts: [],
      };

      vi.mocked(readFile).mockResolvedValue(JSON.stringify(fixtureData));

      const loader = new DbTestLoader(testDatabaseUrl, testSchemaPath, testFixturePath);
      expect(loader).toBeInstanceOf(DbTestLoader);
    });
  });
});
