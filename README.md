# Database Migration Tool

A zero-downtime PostgreSQL database migration tool with parallel processing capabilities. This tool performs complete schema and data migrations between PostgreSQL databases while maintaining high availability.

## Overview

The Database Migration Tool provides enterprise-grade database migration capabilities with the following key features:

- **Zero-downtime migrations** using atomic table swapping
- **Parallel processing** with pg_restore for maximum performance  
- **Shadow table strategy** to minimize impact on production
- **Comprehensive data integrity verification**
- **Rollback capabilities** with automatic backup creation
- **Foreign key constraint management**
- **Sequence synchronization**
- **Index recreation**
- **Preserved table support** for maintaining specific data

## How It Works

The migration process follows a carefully orchestrated 8-phase approach:

### Phase 1: Create Source Dump

1. **Source Preparation**: Source database is temporarily made read-only during dump creation for consistency
2. **Binary Dump Creation**: Creates a high-performance binary dump using `pg_dump`

### Phase 2: Create Shadow Tables in Destination Public Schema

1. **Destination Setup**: Cleans up existing shadow tables and disables foreign key constraints
2. **Parallel Restoration**: Uses `pg_restore` with multiple parallel jobs to restore data as shadow tables with "shadow_" prefix

### Phase 3: Setup Preserved Table Synchronization

1. **Preserved Table Validation**: Validates that preserved tables exist in destination schema
2. **Real-time Sync Setup**: Creates triggers for real-time synchronization of preserved tables to shadow tables ensuring up to date right until table swap
3. **Initial Sync**: Copies current preserved table data to shadow tables

### Phase 4: Perform Atomic Table Swap

1. **Backup Creation**: Renames current tables to "backup_" prefix
2. **Table Activation**: Renames shadow tables to become the new active tables
3. **Atomic Transaction**: All table renames happen in a single transaction with deferred constraints

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

### Shadow Table Isolation

- **Parallel Operations**: Data restore happens in isolated shadow tables while the destination continues serving traffic from the active tables
- **Resource Separation**: Restore operations consume separate database resources, preventing interference with destination queries
- **Atomic Cutover**: The final table swap is instantaneous using PostgreSQL's atomic `ALTER TABLE RENAME` operations

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
- **Instant Activation**: New tables become active immediately via atomic operations (typically 40-80ms)
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

## Two-Phase Migration Workflow

The migration tool supports both single-phase and two-phase execution modes to provide maximum flexibility for different operational requirements.

### Single-Phase Mode (Traditional)

Use the `start` command for immediate end-to-end migration:

```bash
npm run migration -- start \
  --source postgres://user:pass@source-host:port/db \
  --dest postgres://user:pass@dest-host:port/db \
  --preserved-tables users,sessions
```

This executes all phases (1-7) sequentially in a single operation.

### Two-Phase Mode (Recommended for Production)

#### Phase 1: Preparation

Use the `prepare` command to set up the shadow schema and sync triggers:

```bash
npm run migration -- prepare \
  --source postgres://user:pass@source-host:port/db \
  --dest postgres://user:pass@dest-host:port/db \
  --preserved-tables users,sessions
```

**What happens during preparation:**
- Creates source database dump
- Restores dump to destination shadow schema  
- Sets up real-time sync triggers for preserved tables
- Validates schema integrity
- Saves migration state for later completion

**Key benefits:**
- No downtime during preparation phase
- Preserved tables remain synchronized in real-time
- Can validate shadow schema before committing to swap
- Preparation can be done during low-traffic periods

#### Phase 2: Completion

Use the `swap` command to complete the migration when ready:

```bash
npm run migration -- swap \
  --dest postgres://user:pass@dest-host:port/db
```

**What happens during swap:**
- Performs atomic table swap (typically 40-80ms downtime)
- Cleans up sync triggers
- Resets sequences and recreates indexes
- Validates migration completion

#### Monitoring Between Phases

Use the `status` command to monitor migration state:

```bash
# Human-readable status
npm run migration -- status \
  --dest postgres://user:pass@dest-host:port/db

# JSON output for automation
npm run migration -- status \
  --dest postgres://user:pass@dest-host:port/db \
  --json
```

#### Time Between Phases

- **No time limit**: The time between `prepare` and `swap` can be minutes, hours, or days
- **Continuous sync**: Preserved tables remain synchronized automatically
- **State persistence**: Migration state is saved to disk and survives restarts
- **Validation**: Status command helps verify readiness before swap

### Migration State Management

The tool uses file-based state management to track migration progress:

- **State file location**: `.migration-state-{database}.json` in working directory
- **Automatic cleanup**: State is cleared after successful completion
- **Manual inspection**: State file can be examined for troubleshooting
- **Resumable operations**: Failed operations can be investigated and resumed

## CLI Commands

The migration tool provides the following commands:

### Migration Commands

- `start` - Complete database migration (traditional single-phase mode)
- `prepare` - Prepare migration (create dump, setup shadow schema, sync triggers)
- `swap` - Complete migration (atomic schema swap and finalization)
- `status` - Show current migration status

### Management Commands

- `list` - List all available backup schemas
- `rollback` - Rollback to a previous backup
- `cleanup` - Delete old backup schemas
- `verify` - Verify backup integrity

### Command-Specific Parameters

#### start / prepare commands
- `--source <url>` - Source database connection string (required)
- `--dest <url>` - Destination database connection string (required) 
- `--preserved-tables <table1,table2>` - Tables to preserve from destination (optional)
- `--dry-run` - Preview mode without executing changes (optional)

#### swap command
- `--dest <url>` - Destination database connection string (required)
- `--timestamp <ts>` - Specific migration timestamp to complete (optional)

#### status command  
- `--dest <url>` - Destination database connection string (required)
- `--json` - Output status as JSON (optional)

#### rollback command
- `--latest` - Rollback to most recent backup (mutually exclusive with --timestamp)
- `--timestamp <ts>` - Rollback to specific backup timestamp
- `--keep-tables <table1,table2>` - Tables to preserve during rollback (optional)

#### cleanup command
- `--before <date>` - Delete backups before specified date (ISO format or timestamp)

#### verify command
- `--timestamp <ts>` - Backup timestamp to verify

### Global Options

- `--dry-run` - Preview changes without executing (available for most commands)
- `--json` - Output as JSON format (available for list/status commands)
- `--help` - Display help information

### Example Usage

#### Single-Phase Migration (Traditional)

```bash
# Complete migration in one command
npm run migration -- start \
  --source postgres://user:pass@source-host:5432/source_db \
  --dest postgres://user:pass@dest-host:5432/dest_db \
  --preserved-tables users,sessions

# Dry run before actual migration  
npm run migration -- start \
  --source postgres://user:pass@source-host:5432/source_db \
  --dest postgres://user:pass@dest-host:5432/dest_db \
  --preserved-tables users,sessions \
  --dry-run
```

#### Two-Phase Migration (Recommended)

```bash
# Phase 1: Prepare migration (can be done during low-traffic periods)
npm run migration -- prepare \
  --source postgres://user:pass@source-host:5432/source_db \
  --dest postgres://user:pass@dest-host:5432/dest_db \
  --preserved-tables users,sessions

# Check migration status (can be run multiple times)
npm run migration -- status \
  --dest postgres://user:pass@dest-host:5432/dest_db

# Phase 2: Complete migration (minimal downtime - when ready)
npm run migration -- swap \
  --dest postgres://user:pass@dest-host:5432/dest_db
```

#### Management Commands

```bash
# List available backups
npm run migration -- list
npm run migration -- list --json

# Rollback to latest backup
npm run migration -- rollback --latest

# Rollback to specific backup
npm run migration -- rollback --timestamp 1722614400000

# Verify backup integrity
npm run migration -- verify --timestamp 1722614400000

# Cleanup old backups
npm run migration -- cleanup --before "2025-07-15"
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