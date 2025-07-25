#!/usr/bin/env node
/**
 * Database Test Loader - Raw SQL Implementation
 *
 * This module provides the DbTestLoader class that handles database schema setup
 * and fixture data loading using raw SQL instead of Prisma Client.
 *
 * The class encapsulates:
 * - Parsing Prisma schemas to intermediate format
 * - Setting up database schema using raw SQL
 * - Loading test data from JSON fixture files with dependency management
 * - Managing test data lifecycle
 *
 * Usage:
 *   import { DbTestLoader } from './db-test-loader.js';
 *
 *   const loader = new DbTestLoader(databaseUrl, schemaPath, fixtureFile);
 *   await loader.setupDatabaseSchema();
 *   await loader.loadTestData();
 *   await loader.clearTestData();
 *
 * Requirements:
 *   - PostgreSQL database
 *   - Node.js
 *   - npm packages: pg
 *   - Valid Prisma schema file
 */

import { Client, ClientConfig } from 'pg';
import { DbSchemaParser } from './db-schema-parser.js';
import { DbSqlGenerator } from './db-sql-generator.js';
import { DbTestFixtureLoader } from './db-test-fixture-loader.js';
import type { DatabaseSchema } from './db-schema-types.js';

/**
 * Custom error classes for better error handling
 */
export class DatabaseConnectionError extends Error {
  constructor(
    message: string,
    public cause?: Error
  ) {
    super(message);
    this.name = 'DatabaseConnectionError';
  }
}

export class SchemaSetupError extends Error {
  constructor(
    message: string,
    public cause?: Error
  ) {
    super(message);
    this.name = 'SchemaSetupError';
  }
}

export class DataLoadingError extends Error {
  constructor(
    message: string,
    public cause?: Error
  ) {
    super(message);
    this.name = 'DataLoadingError';
  }
}

export class PrismaError extends Error {
  constructor(
    message: string,
    public cause?: Error
  ) {
    super(message);
    this.name = 'PrismaError';
  }
}

// Legacy interfaces for compatibility
export interface LoadResult {
  success: boolean;
  tablesLoaded: string[];
  recordsLoaded: number;
  recordCounts: DataCounts; // Add this for test compatibility
  sequencesUpdated: string[];
}

export interface DataCounts {
  [tableName: string]: number;
}

/**
 * Handles database schema setup and fixture data loading using raw SQL.
 */
export class DbTestLoader {
  private databaseUrl: string;
  private schemaPath: string;
  private fixtureFile?: string;
  private client: Client;
  private schema?: DatabaseSchema;
  private fixtureLoader?: DbTestFixtureLoader;
  private isConnected = false;

  constructor(databaseUrl: string, schemaPath: string, fixtureFile?: string) {
    this.databaseUrl = databaseUrl;
    this.schemaPath = schemaPath;
    this.fixtureFile = fixtureFile;

    const config = this.parseDatabaseUrl(databaseUrl);
    this.client = new Client(config);
  }

  /**
   * Parse database URL into connection config
   */
  private parseDatabaseUrl(url: string): ClientConfig {
    try {
      const parsed = new URL(url);
      return {
        host: parsed.hostname,
        port: parseInt(parsed.port) || 5432,
        database: parsed.pathname.slice(1), // Remove leading /
        user: parsed.username,
        password: parsed.password,
      };
    } catch (error) {
      throw new DatabaseConnectionError(`Invalid database URL: ${url}`, error as Error);
    }
  }

  /**
   * Connect to the database
   */
  async connect(): Promise<void> {
    if (this.isConnected) return;

    try {
      await this.client.connect();
      this.isConnected = true;

      // Test connection
      await this.client.query('SELECT 1');
      process.stdout.write('✅ Database connection established\n');
    } catch (error) {
      // If connection fails due to client already being connected/ended, create new client
      if (error instanceof Error && error.message.includes('Client has already')) {
        const config = this.parseDatabaseUrl(this.databaseUrl);
        this.client = new Client(config);
        await this.client.connect();
        this.isConnected = true;
        await this.client.query('SELECT 1');
        process.stdout.write('✅ Database connection established\n');
      } else {
        throw new DatabaseConnectionError('Failed to connect to database', error as Error);
      }
    }
  }

  /**
   * Disconnect from the database
   */
  async disconnect(): Promise<void> {
    try {
      if (this.isConnected) {
        await this.client.end();
        this.isConnected = false;
      }

      process.stdout.write('✅ Database connections closed\n');
    } catch (error) {
      process.stderr.write(`⚠️ Error closing database connections: ${error}\n`);
    }
  }

  /**
   * Create database if it doesn't exist
   */
  async createDatabase(): Promise<void> {
    if (!this.isConnected) {
      await this.connect();
    }

    try {
      // Parse database name from URL
      const config = this.parseDatabaseUrl(this.databaseUrl);
      const databaseName = config.database;

      process.stdout.write(`🔧 Creating database '${databaseName}' if it doesn't exist...\n`);

      // Check if database exists
      const dbCheckQuery = 'SELECT 1 FROM pg_database WHERE datname = $1';
      const result = await this.client.query(dbCheckQuery, [databaseName]);

      if (result.rows.length === 0) {
        // Database doesn't exist, create it
        await this.client.query(`CREATE DATABASE "${databaseName}"`);
        process.stdout.write(`✅ Created database '${databaseName}'\n`);
      } else {
        process.stdout.write(`ℹ️ Database '${databaseName}' already exists\n`);
      }
    } catch (error) {
      throw new DatabaseConnectionError('Failed to create database', error as Error);
    }
  }

  /**
   * Drop database if it exists
   */
  async dropDatabase(): Promise<void> {
    if (!this.isConnected) {
      await this.connect();
    }

    try {
      const config = this.parseDatabaseUrl(this.databaseUrl);
      const databaseName = config.database;

      process.stdout.write(`🗑️ Dropping database '${databaseName}' if it exists...\n`);

      // Terminate any active connections to the database first
      await this.client.query(
        `
        SELECT pg_terminate_backend(pg_stat_activity.pid)
        FROM pg_stat_activity
        WHERE pg_stat_activity.datname = $1
        AND pid <> pg_backend_pid()
      `,
        [databaseName]
      );

      // Drop the database
      await this.client.query(`DROP DATABASE IF EXISTS "${databaseName}"`);
      process.stdout.write(`✅ Dropped database '${databaseName}'\n`);
    } catch (error) {
      throw new DatabaseConnectionError('Failed to drop database', error as Error);
    }
  }

  /**
   * Setup database schema from Prisma schema file
   */
  async setupDatabaseSchema(): Promise<void> {
    if (!this.isConnected) {
      await this.connect();
    }

    try {
      process.stdout.write('🔧 Setting up database schema...\n');

      // Parse the Prisma schema
      this.schema = DbSchemaParser.parse(this.schemaPath);

      // Generate and execute CREATE TABLE statements
      const createStatements = DbSqlGenerator.generateCompleteSchema(this.schema);

      for (const statement of createStatements) {
        await this.client.query(statement);
      }

      // Initialize fixture loader
      this.fixtureLoader = new DbTestFixtureLoader(this.client, this.schema);

      process.stdout.write('✅ Database schema setup completed\n');
    } catch (error) {
      throw new SchemaSetupError('Failed to setup database schema', error as Error);
    }
  }

  /**
   * Load test data from fixture file
   */
  async loadTestData(): Promise<LoadResult> {
    if (!this.fixtureFile) {
      return {
        success: true,
        tablesLoaded: [],
        recordsLoaded: 0,
        recordCounts: {},
        sequencesUpdated: [],
      };
    }

    if (!this.fixtureLoader) {
      throw new DataLoadingError('Schema not setup. Call setupDatabaseSchema() first.');
    }

    try {
      process.stdout.write('📥 Loading test data...\n');

      await this.fixtureLoader.loadFixtureFile(this.fixtureFile);

      const tableCounts = await this.fixtureLoader.getTableCounts();
      const tablesLoaded = Object.keys(tableCounts).filter(table => tableCounts[table] > 0);
      const recordsLoaded = Object.values(tableCounts).reduce((sum, count) => sum + count, 0);

      process.stdout.write('✅ Test data loaded successfully\n');

      return {
        success: true,
        tablesLoaded,
        recordsLoaded,
        recordCounts: tableCounts, // Add this for test compatibility
        sequencesUpdated: [], // Could track this if needed
      };
    } catch (error) {
      throw new DataLoadingError('Failed to load test data', error as Error);
    }
  }

  /**
   * Clear all test data
   */
  async clearTestData(): Promise<void> {
    if (!this.fixtureLoader) {
      throw new DataLoadingError('Schema not setup. Call setupDatabaseSchema() first.');
    }

    try {
      process.stdout.write('🧹 Clearing test data...\n');
      await this.fixtureLoader.clearAllData();
      process.stdout.write('✅ Test data cleared\n');
    } catch (error) {
      throw new DataLoadingError('Failed to clear test data', error as Error);
    }
  }

  /**
   * Get record counts for all tables
   */
  async getDataCounts(): Promise<DataCounts> {
    if (!this.fixtureLoader) {
      throw new DataLoadingError('Schema not setup. Call setupDatabaseSchema() first.');
    }

    try {
      return await this.fixtureLoader.getTableCounts();
    } catch (error) {
      throw new DataLoadingError('Failed to get data counts', error as Error);
    }
  }

  /**
   * Get connection information
   */
  getConnectionInfo(): {
    url: string;
    connected: boolean;
    databaseName?: string;
  } {
    const config = this.parseDatabaseUrl(this.databaseUrl);
    return {
      url: this.databaseUrl,
      connected: this.isConnected,
      databaseName: config.database,
    };
  }

  /**
   * Execute a raw SQL query and return results
   */
  async executeQuery<T = any>(sql: string, params?: any[]): Promise<T[]> {
    if (!this.isConnected) {
      // Create a new client if needed
      const config = this.parseDatabaseUrl(this.databaseUrl);
      this.client = new Client(config);
      await this.connect();
    }

    try {
      const result = await this.client.query(sql, params);
      return result.rows as T[];
    } catch (error) {
      throw new DatabaseConnectionError(`Failed to execute query: ${sql}`, error as Error);
    }
  }

  /**
   * Get the database client for advanced operations
   */
  getClient(): Client {
    return this.client;
  }

  /**
   * Create a test database (for integration testing)
   */
  async createTestDatabase(): Promise<boolean> {
    try {
      // Parse URL to extract database name
      const config = this.parseDatabaseUrl(this.databaseUrl);
      const databaseName = config.database;

      if (!databaseName) {
        throw new DatabaseConnectionError('No database name found in URL');
      }

      // Connect to 'postgres' database to create the test database
      const adminConfig = { ...config, database: 'postgres' };
      const adminClient = new Client(adminConfig);

      await adminClient.connect();

      // Check if database exists first
      const checkResult = await adminClient.query('SELECT 1 FROM pg_database WHERE datname = $1', [
        databaseName,
      ]);

      if (checkResult.rows.length === 0) {
        // Create the database
        await adminClient.query(`CREATE DATABASE "${databaseName}"`);
      }

      await adminClient.end();

      // Connect to the newly created database to enable PostGIS extension
      const newDbClient = new Client(config);
      await newDbClient.connect();

      try {
        // Enable PostGIS extension for TES schema geometry support
        await newDbClient.query('CREATE EXTENSION IF NOT EXISTS "postgis"');
        await newDbClient.query('CREATE EXTENSION IF NOT EXISTS "uuid-ossp"');
      } catch (extensionError) {
        // Non-fatal - log warning but continue
        process.stdout.write(
          `Warning: Could not enable extensions: ${
            extensionError instanceof Error ? extensionError.message : String(extensionError)
          }\n`
        );
      } finally {
        await newDbClient.end();
      }

      return true;
    } catch (error) {
      process.stdout.write(
        `Failed to create test database: ${
          error instanceof Error ? error.message : String(error)
        }\n`
      );
      return false;
    }
  }

  /**
   * Clean up test database (for integration testing)
   */
  async cleanupTestDatabase(): Promise<boolean> {
    try {
      // Disconnect first
      await this.disconnect();

      // Parse URL to extract database name
      const config = this.parseDatabaseUrl(this.databaseUrl);
      const databaseName = config.database;

      if (!databaseName) {
        throw new DatabaseConnectionError('No database name found in URL');
      }

      // Connect to 'postgres' database to drop the test database
      const adminConfig = { ...config, database: 'postgres' };
      const adminClient = new Client(adminConfig);

      await adminClient.connect();

      // Terminate existing connections to the database
      await adminClient.query(
        `
        SELECT pg_terminate_backend(pg_stat_activity.pid)
        FROM pg_stat_activity
        WHERE pg_stat_activity.datname = $1 
        AND pid <> pg_backend_pid()
      `,
        [databaseName]
      );

      // Drop the database
      await adminClient.query(`DROP DATABASE IF EXISTS "${databaseName}"`);

      await adminClient.end();
      return true;
    } catch (error) {
      process.stdout.write(
        `Failed to cleanup test database: ${
          error instanceof Error ? error.message : String(error)
        }\n`
      );
      return false;
    }
  }
}
