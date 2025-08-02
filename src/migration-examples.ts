#!/usr/bin/env node

/**
 * Example usage script for the database migration tool
 * This demonstrates common migration scenarios
 */

import { DatabaseMigrator } from './migration-core.js';

// Example 1: Basic migration
async function basicMigration() {
  const sourceConfig = {
    host: 'source-db.example.com',
    port: 5432,
    database: 'tes_app',
    user: 'migration_user',
    password: 'secure_password',
  };

  const destConfig = {
    host: 'dest-db.example.com',
    port: 5432,
    database: 'tes_app_new',
    user: 'migration_user',
    password: 'secure_password',
  };

  const migrator = new DatabaseMigrator(sourceConfig, destConfig);
  await migrator.migrate();
}

// Example 2: Migration with preserved tables
async function migrationWithPreservedTables() {
  const sourceConfig = {
    host: 'old-server.example.com',
    port: 5432,
    database: 'tes_app_old',
    user: 'migration_user',
    password: 'secure_password',
  };

  const destConfig = {
    host: 'dest-db.example.com',
    port: 5432,
    database: 'tes_app_new',
    user: 'migration_user',
    password: 'secure_password',
  }; // Preserve authentication and session data
  const preservedTables = ['users', 'sessions', 'tokens', '_prisma_migrations'];

  const migrator = new DatabaseMigrator(sourceConfig, destConfig, preservedTables);

  await migrator.migrate();
}

// Example 3: Dry run analysis
async function dryRunAnalysis() {
  const sourceConfig = {
    host: process.env.SOURCE_DB_HOST || 'localhost',
    port: parseInt(process.env.SOURCE_DB_PORT || '5432'),
    database: process.env.SOURCE_DB_NAME || 'tes_dev',
    user: process.env.SOURCE_DB_USER || 'postgres',
    password: process.env.SOURCE_DB_PASSWORD || '',
  };

  const destConfig = {
    host: process.env.DEST_DB_HOST || 'localhost',
    port: parseInt(process.env.DEST_DB_PORT || '5432'),
    database: process.env.DEST_DB_NAME || 'tes_staging',
    user: process.env.DEST_DB_USER || 'postgres',
    password: process.env.DEST_DB_PASSWORD || '',
  };

  const preservedTables = (process.env.PRESERVED_TABLES || '').split(',');
  const dryRun = true; // Enable dry run mode

  const migrator = new DatabaseMigrator(sourceConfig, destConfig, preservedTables, dryRun);

  await migrator.migrate();
}

// Export examples for use in other scripts
export { basicMigration, migrationWithPreservedTables, dryRunAnalysis };
