#!/usr/bin/env node
/**
 * Multi-Database Test Loader Module
 *
 * This module provides the DbTestLoaderMulti class that handles test database
 * creation, schema setup, and cleanup for integration testing.
 *
 * The class supports:
 * - Creating test databases on potentially different hosts
 * - Managing database schemas using DbTestLoader
 * - Dependency checking for required tools
 * - Cleanup operations
 *
 * Usage:
 *   import { DbTestLoaderMulti } from './db-test-loader-multi.js';
 *
 *   const manager = new DbTestLoaderMulti(sourceUrl, destUrl, schemaPath);
 *   await manager.createTestDatabases();
 *   manager.initializeLoaders(sourceFixture, destFixture);
 *   await manager.setupDatabaseSchemas();
 *
 * Requirements:
 *   - PostgreSQL server(s) running
 *   - Node.js and Prisma CLI available
 *   - npm packages: @prisma/client, pg, vitest
 */

import { URL } from 'url';
import { existsSync } from 'fs';
import { execa } from 'execa';
import type { Client } from 'pg';
import { DbTestLoader } from './db-test-loader.js';

/**
 * Custom error classes for multi-database operations
 */
export class MultiDatabaseError extends Error {
  constructor(
    message: string,
    public cause?: Error
  ) {
    super(message);
    this.name = 'MultiDatabaseError';
  }
}

export class DependencyCheckError extends Error {
  constructor(
    message: string,
    public cause?: Error
  ) {
    super(message);
    this.name = 'DependencyCheckError';
  }
}

/**
 * Connection information interface
 */
export interface ConnectionInfo {
  sourceUrl: string;
  destUrl: string;
  sourceDb: string;
  destDb: string;
  sourceHost: string;
  destHost: string;
  multiHost: boolean;
}

/**
 * Manages test database creation, schema setup, and cleanup using Prisma and pg client.
 * Supports source and destination databases on different hosts.
 *
 * Database Naming:
 * - Source database: {base_db}_migration_source
 * - Destination database: {base_db}_migration_dest
 * - This ensures different database names when using the same host, preventing data loss
 * - The naming pattern works for both same-host and cross-host scenarios
 */
export class DbTestLoaderMulti {
  public readonly sourceBaseUrl: string;
  public readonly destBaseUrl: string;
  public readonly sourceParsed: URL;
  public readonly destParsed: URL;
  public readonly sourceDb: string;
  public readonly destDb: string;
  public readonly sourceUrl: string;
  public readonly destUrl: string;
  public readonly sourceAdminUrl: string;
  public readonly destAdminUrl: string;
  public readonly prismaSchemaPath?: string;

  private sourceLoader?: DbTestLoader;
  private destLoader?: DbTestLoader;

  constructor(sourceBaseUrl: string, destBaseUrl?: string, prismaSchemaPath?: string) {
    this.sourceBaseUrl = sourceBaseUrl;
    this.destBaseUrl = destBaseUrl || sourceBaseUrl; // Default to same host
    this.prismaSchemaPath = prismaSchemaPath;

    // Parse URLs
    try {
      this.sourceParsed = new URL(sourceBaseUrl);
      this.destParsed = new URL(this.destBaseUrl);
    } catch (error) {
      throw new MultiDatabaseError(`Invalid database URL format`, error as Error);
    }

    // Generate test database names
    const sourceBaseDb = this.sourceParsed.pathname.slice(1) || 'test';
    const destBaseDb = this.destParsed.pathname.slice(1) || 'test';

    // Ensure different database names if hosts are the same
    if (this.sourceParsed.hostname === this.destParsed.hostname) {
      // Same host - ensure different database names
      this.sourceDb = `${sourceBaseDb}_migration_source`;
      this.destDb = `${destBaseDb}_migration_dest`;
    } else {
      // Different hosts - can use same base name pattern
      this.sourceDb = `${sourceBaseDb}_migration_source`;
      this.destDb = `${destBaseDb}_migration_dest`;
    }

    // Build connection URLs for source host
    const sourceBaseWithoutDb = this.buildBaseUrl(this.sourceParsed);
    this.sourceUrl = `${sourceBaseWithoutDb}/${this.sourceDb}`;
    this.sourceAdminUrl = `${sourceBaseWithoutDb}/postgres`;

    // Build connection URLs for destination host (potentially different)
    const destBaseWithoutDb = this.buildBaseUrl(this.destParsed);
    this.destUrl = `${destBaseWithoutDb}/${this.destDb}`;
    this.destAdminUrl = `${destBaseWithoutDb}/postgres`;

    // Log configuration
    console.log(`Source database: ${this.sourceDb} on ${this.sourceParsed.hostname}`);
    console.log(`Destination database: ${this.destDb} on ${this.destParsed.hostname}`);
    console.log(`Prisma schema: ${this.prismaSchemaPath}`);
  }

  /**
   * Build base URL from parsed URL components
   */
  private buildBaseUrl(parsedUrl: URL): string {
    let baseWithoutDb = `${parsedUrl.protocol}//`;

    if (parsedUrl.username) {
      baseWithoutDb += parsedUrl.username;
      if (parsedUrl.password) {
        baseWithoutDb += `:${parsedUrl.password}`;
      }
      baseWithoutDb += '@';
    }

    baseWithoutDb += parsedUrl.hostname;

    if (parsedUrl.port) {
      baseWithoutDb += `:${parsedUrl.port}`;
    }

    return baseWithoutDb;
  }

  /**
   * Initialize database loaders with fixture files
   */
  initializeLoaders(sourceFixture?: string, destFixture?: string): boolean {
    if (!this.prismaSchemaPath) {
      console.error('Cannot initialize loaders without Prisma schema');
      return false;
    }

    try {
      this.sourceLoader = new DbTestLoader(this.sourceUrl, this.prismaSchemaPath, sourceFixture);

      this.destLoader = new DbTestLoader(this.destUrl, this.prismaSchemaPath, destFixture);

      console.log('✓ Database loaders initialized');
      return true;
    } catch (error) {
      console.error(`✗ Failed to initialize database loaders: ${error}`);
      return false;
    }
  }

  /**
   * Check if required dependencies are available
   */
  async checkDependencies(): Promise<boolean> {
    console.log('Checking dependencies...');

    // Check pg client (should be available if this code is running)
    try {
      const pgModule = await import('pg');
      console.log('✓ pg client available');
    } catch (error) {
      console.error('✗ pg client not found: Install with: npm install pg');
      return false;
    }

    // Check @prisma/client
    try {
      const prismaModule = await import('@prisma/client');
      console.log('✓ @prisma/client available');
    } catch (error) {
      console.error('✗ @prisma/client not found: Install with: npm install @prisma/client');
      return false;
    }

    // Check vitest
    try {
      const vitestModule = await import('vitest');
      console.log('✓ vitest available');
    } catch (error) {
      console.error('✗ vitest not found: Install with: npm install vitest');
      return false;
    }

    // Check PostgreSQL connections for both hosts
    console.log('Testing database connections...');

    // Test source database connection
    try {
      const { Client } = await import('pg');
      const sourceClient = new Client(this.sourceAdminUrl);
      await sourceClient.connect();
      const result = await sourceClient.query('SELECT version()');
      const version = result.rows[0].version;
      console.log(`✓ Source PostgreSQL connection: ${version.substring(0, 50)}...`);
      await sourceClient.end();
    } catch (error) {
      console.error(`✗ Source PostgreSQL connection failed: ${error}`);
      return false;
    }

    // Test destination database connection (may be different host)
    if (this.destAdminUrl !== this.sourceAdminUrl) {
      try {
        const { Client } = await import('pg');
        const destClient = new Client(this.destAdminUrl);
        await destClient.connect();
        const result = await destClient.query('SELECT version()');
        const version = result.rows[0].version;
        console.log(`✓ Destination PostgreSQL connection: ${version.substring(0, 50)}...`);
        await destClient.end();
      } catch (error) {
        console.error(`✗ Destination PostgreSQL connection failed: ${error}`);
        return false;
      }
    } else {
      console.log('✓ Source and destination on same host');
    }

    // Check Node.js and Prisma CLI
    try {
      const nodeResult = await execa('node', ['--version']);
      console.log(`✓ Node.js ${nodeResult.stdout}`);
    } catch (error) {
      console.error('✗ Node.js not found');
      return false;
    }

    try {
      const prismaResult = await execa('npx', ['prisma', '--version']);
      // Extract version from output
      const versionLines = prismaResult.stdout
        .split('\n')
        .filter(line => line.toLowerCase().includes('prisma'));
      const version = versionLines[0] || 'unknown';
      console.log(`✓ Prisma CLI ${version}`);
    } catch (error) {
      console.error('✗ Prisma CLI not found: Install with: npm install -g prisma');
      return false;
    }

    console.log('✓ All dependencies available');
    return true;
  }

  /**
   * Create test databases using DbTestLoader instances
   */
  async createTestDatabases(): Promise<boolean> {
    console.log('Creating test databases...');

    if (!this.sourceLoader || !this.destLoader) {
      console.error('✗ Database loaders not initialized');
      return false;
    }

    try {
      // Create source database
      console.log('Creating source database...');
      const successSource = await this.sourceLoader.createTestDatabase();

      // Create destination database
      console.log('Creating destination database...');
      const successDest = await this.destLoader.createTestDatabase();

      if (successSource && successDest) {
        console.log('✓ Created test databases successfully');
        return true;
      } else {
        console.error('✗ Failed to create one or more test databases');
        return false;
      }
    } catch (error) {
      console.error(`✗ Failed to create test databases: ${error}`);
      return false;
    }
  }

  /**
   * Setup database schemas using Prisma CLI via database loaders
   */
  async setupDatabaseSchemas(): Promise<boolean> {
    console.log('Setting up database schemas...');

    if (!this.sourceLoader || !this.destLoader) {
      console.error('✗ Database loaders not initialized');
      return false;
    }

    try {
      // Setup source database schema
      console.log('Setting up source database schema...');
      await this.sourceLoader.setupDatabaseSchema();

      // Setup destination database schema
      console.log('Setting up destination database schema...');
      await this.destLoader.setupDatabaseSchema();

      console.log('✓ All database schemas setup complete');
      return true;
    } catch (error) {
      console.error(`✗ Schema setup failed: ${error}`);
      return false;
    }
  }

  /**
   * Clean up test databases using DbTestLoader instances
   */
  async cleanupTestDatabases(): Promise<boolean> {
    console.log('Cleaning up test databases...');

    if (!this.sourceLoader || !this.destLoader) {
      console.warn('⚠ Database loaders not initialized, cannot cleanup');
      return false;
    }

    let successSource = true;
    let successDest = true;

    try {
      // Cleanup source database
      console.log('Cleaning up source database...');
      successSource = await this.sourceLoader.cleanupTestDatabase();

      // Cleanup destination database
      console.log('Cleaning up destination database...');
      successDest = await this.destLoader.cleanupTestDatabase();

      if (successSource && successDest) {
        console.log('✓ Test databases cleaned up successfully');
        return true;
      } else {
        console.warn('⚠ Some test databases may not have been cleaned up properly');
        return false;
      }
    } catch (error) {
      console.error(`✗ Failed to cleanup test databases: ${error}`);
      return false;
    }
  }

  /**
   * Get connection information for tests
   */
  getConnectionInfo(): ConnectionInfo {
    return {
      sourceUrl: this.sourceUrl,
      destUrl: this.destUrl,
      sourceDb: this.sourceDb,
      destDb: this.destDb,
      sourceHost: this.sourceParsed.hostname || 'localhost',
      destHost: this.destParsed.hostname || 'localhost',
      multiHost: this.sourceParsed.hostname !== this.destParsed.hostname,
    };
  }

  /**
   * Get the source database loader
   */
  getSourceLoader(): DbTestLoader | undefined {
    return this.sourceLoader;
  }

  /**
   * Get the destination database loader
   */
  getDestLoader(): DbTestLoader | undefined {
    return this.destLoader;
  }
}
