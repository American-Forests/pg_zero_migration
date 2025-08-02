# Database Migration Tool

A zero-downtime PostgreSQL database migration tool with parallel processing capabilities. This tool performs complete schema and data migrations between PostgreSQL databases while maintaining high availability.

## Overview

The Database Migration Tool provides enterprise-grade database migration capabilities with the following key features:

- **Zero-downtime migrations** using atomic schema swapping
- **Parallel processing** with pg_restore for maximum performance  
- **Shadow schema strategy** to minimize impact on production
- **Comprehensive data integrity verification**
- **Rollback capabilities** with automatic backup creation
- **Foreign key constraint management**
- **Sequence synchronization**
- **Index recreation**
- **Preserved table support** for maintaining specific data

## How It Works

The migration process follows a carefully orchestrated 8-phase approach:

### Phase 1: Create Source Dump

1. **Source Preparation**: Source database tables are temporarily made read-only and moved from `public` to `shadow` schema.  This is necessary for dump to be restored to shadow in destination
2. **Binary Dump Creation**: Creates a high-performance binary dump using `pg_dump`
3. **Source Restoration**: Restores source database tables back to public schema

### Phase 2: Restore Source Dump to Destination Shadow Schema

1. **Destination Setup**: Drops existing shadow schema on destination and disables foreign key constraints
2. **Parallel Restoration**: Uses `pg_restore` with multiple parallel jobs to restore data to shadow schema

### Phase 3: Setup Preserved Table Synchronization

1. **Preserved Table Validation**: Validates that preserved tables exist in destination schema
2. **Real-time Sync Setup**: Creates triggers for real-time synchronization of preserved tables to shadow ensuring up to date right until schema swap
3. **Initial Sync**: Copies current preserved table data to shadow schema

### Phase 4: Perform Atomic Schema Swap

1. **Backup Creation**: Moves current public schema to timestamped backup schema
2. **Schema Activation**: Promotes shadow schema to become the new public schema  
3. **New Shadow Creation**: Creates fresh shadow schema for future migrations

### Phase 5: Cleanup Sync Triggers and Validate Consistency

1. **Trigger Cleanup**: Removes real-time sync triggers from preserved tables
2. **Data Validation**: Validates consistency between migrated data

### Phase 6: Reset Sequences

1. **Sequence Synchronization**: Synchronizes all sequence values to match source database
2. **Sequence Validation**: Verifies sequence values are correctly set

### Phase 7: Recreate Indexes

1. **Index Recreation**: Rebuilds indexes for optimal performance
2. **Spatial Index Handling**: Special handling for PostGIS spatial indexes
3. **Constraint Re-enabling**: Restores foreign key constraints

## Destination Database Protection

The migration tool implements multiple layers of protection to prevent overwhelming the destination database during restore operations:

### Shadow Schema Isolation

- **Parallel Operations**: Data restore happens in an isolated `shadow` schema while the destination continues serving traffic from the `public` schema
- **Resource Separation**: Restore operations consume separate database resources, preventing interference with destination queries
- **Atomic Cutover**: The final schema swap is instantaneous using PostgreSQL's atomic `ALTER SCHEMA RENAME` operations

### Connection Management

- **Connection Pooling**: Uses PostgreSQL connection pools with configurable limits (default: 20 max connections)
- **Dedicated Restore Connections**: Restore operations use a separate connection pool to avoid starving application connections
- **Automatic Cleanup**: Connections are properly closed and released after each migration phase

### Performance Controls

- **Parallel Job Limiting**: pg_restore parallel jobs are configurable and capped at CPU count to prevent resource exhaustion
- **Binary Dump Format**: Uses PostgreSQL's binary dump format for maximum transfer efficiency and reduced I/O load
- **Incremental Processing**: Large operations are broken into phases to distribute load over time

### Load Distribution

- **Read-Only Source Protection**: Source database is temporarily made read-only during dump creation to ensure consistency
- **Constraint Management**: Foreign key constraints are temporarily disabled during restore to reduce validation overhead
- **Index Recreation**: Indexes are rebuilt after data loading for optimal performance without blocking restore operations

### Monitoring and Safeguards

- **Progress Tracking**: Real-time monitoring of migration progress with detailed logging
- **Resource Monitoring**: Built-in checks for available disk space and database connectivity
- **Timeout Protection**: Configurable timeouts prevent runaway operations from consuming resources indefinitely
- **Automatic Rollback**: Failed migrations automatically trigger cleanup and rollback procedures

### Destination Continuity

- **Zero-Downtime Design**: Destination applications continue operating normally during the entire migration process
- **Instant Activation**: New schema becomes active immediately via atomic operations (typically <100ms)
- **Preserved Table Sync**: Critical tables can be kept synchronized in real-time during migration
- **Rollback Capability**: Complete rollback to original state available if issues are detected

These protections ensure that even large-scale migrations can be performed safely on destination systems without service disruption or performance degradation.

## Key Features

### Zero-Downtime Operation

- Uses atomic schema swapping to minimize service interruption
- Shadow schema strategy ensures production data remains available during dump/restore
- Rollback capabilities protect against migration failures

### High-Performance Processing

- Leverages `pg_restore` parallel processing (up to 8 parallel jobs)
- Binary dump format for maximum transfer efficiency
- Optimized for large database migrations

### Data Integrity & Safety

- Comprehensive pre-migration validation
- Automatic backup creation before schema changes
- Foreign key constraint handling
- Sequence value preservation
- Data verification at each step

### Enterprise Features

- Preserved table support for maintaining specific data
- Comprehensive logging and progress reporting
- Dry-run mode for testing migrations
- Extensible architecture for custom requirements

## CLI Parameters

The migration tool supports the following command-line parameters:

### Required Parameters

- `--source-host` - Source database hostname
- `--source-port` - Source database port (default: 5432)
- `--source-database` - Source database name
- `--source-user` - Source database username
- `--source-password` - Source database password
- `--dest-host` - Destination database hostname  
- `--dest-port` - Destination database port (default: 5432)
- `--dest-database` - Destination database name
- `--dest-user` - Destination database username
- `--dest-password` - Destination database password

### Optional Parameters

- `--dry-run` - Run migration in dry-run mode (no actual changes)
- `--preserve-tables` - Comma-separated list of tables to preserve in destination
- `--temp-dir` - Directory for temporary files (default: /tmp)
- `--parallel-jobs` - Number of parallel pg_restore jobs (default: 8, max: CPU count)
- `--timeout` - Migration timeout in seconds (default: 3600)
- `--verbose` - Enable verbose logging
- `--help` - Display help information

### Example Usage

```bash
# Basic migration
./db_migration \
  --source-host localhost \
  --source-database source_db \
  --source-user postgres \
  --source-password secret \
  --dest-host production.example.com \
  --dest-database dest_db \
  --dest-user postgres \
  --dest-password secret

# Migration with preserved tables and dry-run
./db_migration \
  --source-host localhost \
  --source-database source_db \
  --source-user postgres \
  --source-password secret \
  --dest-host production.example.com \
  --dest-database dest_db \
  --dest-user postgres \
  --dest-password secret \
  --preserve-tables user_sessions,audit_logs \
  --dry-run \
  --verbose
```

## Testing

The project includes comprehensive integration tests that validate migration functionality with real PostgreSQL databases.

### Running Tests

```bash
# Run all tests
yarn test

# Run only unit tests  
yarn test:unit

# Run only integration tests
yarn test:integration

# Run tests with verbose output
yarn test:integration --reporter=verbose

# Run specific test file
yarn test:integration migration.integration.test.ts

# Run specific test in test file
yarn test:integration --run migration.integration.test.ts -t "should perform complete migration"

# Override a test variable
TEST_PGHOST=192.168.4.24 yarn test:integration
```

### Test Environment Variables

The following environment variables can be used to configure the test environment:

#### Database Configuration

- `TEST_PGHOST` - PostgreSQL host for integration tests (default: localhost)
- `TEST_PGPORT` - PostgreSQL port for integration tests (default: 5432)  
- `TEST_PGUSER` - PostgreSQL username for integration tests (default: postgres)
- `TEST_PGPASSWORD` - PostgreSQL password for integration tests
- `TEST_PGDATABASE` - Base database name for tests (default: postgres)

#### Test Behavior

- `TEST_CLEANUP_ENABLED` - Enable/disable test database cleanup (default: true)
- `TEST_TIMEOUT` - Test timeout in milliseconds (default: 30000)
- `TEST_PARALLEL_JOBS` - Number of parallel jobs for test migrations (default: 4)
- `TEST_PRESERVE_LOGS` - Keep detailed logs for debugging (default: false)

### Example Test Commands

```bash
# Run integration tests against remote PostgreSQL server
TEST_PGHOST=192.168.4.24 yarn test:integration

# Run tests with custom database credentials
TEST_PGHOST=db.example.com \
TEST_PGUSER=test_user \
TEST_PGPASSWORD=test_pass \
yarn test:integration

# Run tests with debugging enabled
TEST_PRESERVE_LOGS=true \
TEST_TIMEOUT=60000 \
yarn test:integration --reporter=verbose

# Run single test with custom environment
TEST_PGHOST=192.168.4.24 \
yarn test:integration --run --reporter=verbose migration.integration.test.ts
```

### Test Database Requirements

Integration tests require:

- PostgreSQL 12+ server with CREATE DATABASE privileges
- Extensions: `postgis`, `uuid-ossp`
- Network connectivity to the test database server
- Sufficient permissions to create/drop test databases

### Test Structure

- **Unit Tests**: Test individual components in isolation
- **Integration Tests**: Test complete migration workflows with real databases
- **Performance Tests**: Validate migration performance with large datasets
- **Error Handling Tests**: Test recovery scenarios and error conditions

The integration test suite creates temporary databases, loads test data, performs migrations, and verifies data integrity before cleaning up all test resources.

## Architecture

### Core Components

- **DatabaseMigrator**: Main migration orchestrator
- **DbSchemaParser**: Prisma schema file parser  
- **DbSqlGenerator**: SQL generation utilities
- **DbTestLoader**: Test database management utilities
- **DbTestLoaderMulti**: Multi-database test coordination

### Dependencies

- **PostgreSQL**: 12+ with postgis and uuid-ossp extensions
- **Node.js**: 18+ with TypeScript support
- **pg**: PostgreSQL client library
- **execa**: Process execution for pg_dump/pg_restore
- **prisma**: Schema parsing and validation

### Configuration

The tool uses a flexible configuration system supporting:

- Command-line arguments
- Environment variables  
- Configuration files
- Runtime parameter validation

## Error Handling & Recovery

### Automatic Rollback

- Failed migrations trigger automatic rollback
- Original schema preserved in backup schema
- Source database restored to original state
- Comprehensive error logging and reporting

### Manual Recovery

- Backup schemas remain available for manual inspection
- `cleanupBackupSchema()` method for cleanup after verification
- Detailed migration logs for troubleshooting
- Connection pool management prevents resource leaks

## Performance Considerations

### Optimization Features

- Parallel processing with configurable job count
- Binary dump format for efficient data transfer
- Shadow schema strategy minimizes production impact
- Optimized sequence and index handling

### Monitoring

- Real-time progress reporting
- Performance metrics collection
- Resource usage tracking
- Migration timing analysis

## Best Practices

### Pre-Migration

1. Test migrations in staging environment
2. Verify database connectivity and permissions
3. Ensure sufficient disk space for temporary files
4. Plan for rollback scenarios

### During Migration

1. Monitor migration progress and logs
2. Avoid schema changes on source database
3. Ensure stable network connectivity
4. Have rollback plan ready

### Post-Migration

1. Verify data integrity and application functionality
2. Monitor application performance
3. Clean up backup schemas after verification
4. Document migration results and any issues

## Troubleshooting

### Common Issues

- **Connection failures**: Verify database credentials and network connectivity
- **Permission errors**: Ensure user has CREATE DATABASE and schema modification privileges
- **Disk space**: Verify sufficient space for dumps and temporary files
- **Version compatibility**: Check PostgreSQL client/server version compatibility

### Debug Mode

Enable verbose logging and preserve logs for detailed troubleshooting:

```bash
TEST_PRESERVE_LOGS=true yarn test:integration --reporter=verbose
```

## Contributing

### Development Setup

1. Install dependencies: `yarn install`
2. Set up test database with required extensions
3. Configure test environment variables
4. Run tests: `yarn test`

### Code Quality

- TypeScript strict mode enabled
- ESLint configuration for code consistency  
- Prettier formatting rules
- Comprehensive test coverage requirements

### Pull Request Process

1. Add tests for new functionality
2. Ensure all tests pass
3. Update documentation as needed
4. Follow conventional commit message format