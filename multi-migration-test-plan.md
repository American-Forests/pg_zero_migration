# Multi-Migration CLI Integration Test Plan

## Overview
Update the existing CLI integration test to run multiple migrations using the same source and destination databases, demonstrating that multiple backups can be created and managed.

## Test Structure

### Phase 1: One-Phase Migration (`start` command)
- **Command**: `start --source <url> --dest <url> --preserved-tables BlockgroupOnScenario,AreaOnScenario,Scenario,User`
- **Purpose**: Complete migration (prepare + swap) in one command
- **Expected Output**: 
  - One log file with migration statistics
  - First backup schema created in destination database
- **Validation**:
  - Log file contains expected headers, timing, and statistics
  - Migration outcome shows SUCCESS
  - Phase timing information present

### Phase 2: Two-Phase Migration (`prepare` then `swap` commands)
- **Command 1**: `prepare --source <url> --dest <url> --preserved-tables BlockgroupOnScenario,AreaOnScenario,Scenario,User`
  - Creates dump and sets up shadow schema
  - Expected: Console output only (no log file)
- **Command 2**: `swap --dest <url>`
  - Completes atomic schema swap
  - Expected: Console output only (no log file)
- **Purpose**: Demonstrate two-phase migration workflow
- **Expected Output**:
  - Two additional log files (prepare + swap)
  - Second backup schema created in destination database

### Phase 3: List Backups (`list` command)
- **Command**: `list --dest <url>`
- **Purpose**: Verify backup is visible and properly cataloged
- **Expected Output**:
  - JSON or formatted output showing 1 backup schema (second migration replaced first)
  - Backup metadata including timestamp, table count
- **Validation**:
  - Exactly 1 backup listed (not 2, since second migration cleaned first)
  - Backup has valid timestamp and metadata

### Phase 4: Rollback Management (`rollback` command)
- **Command**: `rollback --latest --dest <url>`
- **Purpose**: Test rollback functionality using the backup
- **Expected Output**:
  - Rollback operation success message
  - Backup consumed, none remaining
- **Validation**:
  - Rollback completes successfully
  - Database state restored to backup point

### Phase 5: Verify No Backups Remain (`list` command)
- **Command**: `list --dest <url>`
- **Purpose**: Confirm no backups remain after rollback
- **Expected Output**:
  - JSON or formatted output showing 0 backup schemas
- **Validation**:
  - Exactly 0 backups listed (backup was consumed by rollback)

## Implementation Details

### Log File Management
- Track existing log files before each command
- Identify new log files after each command
- Validate content of log files that are created:
  - `start` command: Complete migration log with headers, timing, and statistics
  - `prepare` command: Console output only (no log file created)
  - `swap` command: Console output only (no log file created)

### Database State Validation
- Use same source and destination databases throughout
- Preserve specified tables: `BlockgroupOnScenario,AreaOnScenario,Scenario,User`
- Load TES fixture data into both databases initially
- Verify backup schemas accumulate in destination database

### Test Assertions
1. **Log File Validation**: Each command creates appropriate log file
2. **Content Validation**: Log files contain expected headers, timing, statistics
3. **Backup Count**: List command shows exactly 2 backups after all migrations
4. **Backup Metadata**: Each backup has valid timestamp and table count

### Command Structure
All commands use:
- `npx tsx src/migration.ts <command> [options]`
- Same connection URLs throughout test
- Same preserved tables configuration
- TEST_PGHOST environment variable support

## Expected Results

- **Total Log Files**: 1 (only start command creates log files)
- **Total Backups**: 1 initially, then 0 after rollback
- **Commands Tested**: start, prepare, swap, list, rollback
- **Test Duration**: Extended to 60 seconds to accommodate multiple operations
- **Validation Points**: ~15-20 assertions covering all CLI operations
- **Key Behavior**: Second migration replaces first backup automatically

## Risk Mitigation
- Cleanup all log files in afterEach hook
- Use unique database names with timestamp and random suffix
- Proper error handling for each CLI command execution
- Separate validation for each phase to isolate failures
