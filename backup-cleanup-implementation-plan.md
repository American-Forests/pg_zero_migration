# Backup Cleanup Implementation Plan

## Overview
Implement safe backup cleanup during the prepare phase to reduce swap downtime while maintaining data safety.

## Problem Statement
Currently, multiple migrations on the same database fail because backup tables already exist from previous migrations. The error "relation 'backup_Area' already exists" occurs during the swap phase.

## Solution Strategy
Move backup cleanup to the prepare phase, but only after successful prepare completion to maintain safety.  And remove any use of timestamp in the backup table name since there will only be one backup set at any given time.

## Implementation Plan

### 1. Core Function: `cleanupExistingBackups()`

**Location**: `migration-core.ts`

```typescript
/**
 * Cleanup existing backup tables, sequences, and constraints
 * Only called after successful prepare phase to maintain safety
 */
private async cleanupExistingBackups(): Promise<void> {
  const startTime = Date.now();
  logWithTimestamp('🧹 Cleaning up existing backup tables...');
  
  // Get list of all backup tables
  const backupTables = await this.getBackupTables();
  
  if (backupTables.length === 0) {
    logWithTimestamp('ℹ️ No existing backup tables to clean up');
    return;
  }
  
  logWithTimestamp(`🗑️ Found ${backupTables.length} backup tables to remove`);
  
  // Drop all backup tables with CASCADE to handle constraints
  for (const table of backupTables) {
    await this.destClient.query(`DROP TABLE IF EXISTS ${table} CASCADE`);
    logWithTimestamp(`🗑️ Dropped backup table: ${table}`);
  }
  
  const duration = Date.now() - startTime;
  logWithTimestamp(`✅ Backup cleanup completed (${duration}ms)`);
}

/**
 * Get list of existing backup tables
 */
private async getBackupTables(): Promise<string[]> {
  const result = await this.destClient.query(`
    SELECT tablename 
    FROM pg_tables 
    WHERE schemaname = 'public' 
    AND tablename LIKE 'backup_%'
    ORDER BY tablename
  `);
  
  return result.rows.map(row => row.tablename);
}
```

### 2. Dry-Run Enhancement: `detectExistingBackups()`

**Location**: `migration-core.ts`

```typescript
/**
 * Detect existing backups for dry-run reporting
 */
private async detectExistingBackups(): Promise<BackupInfo | null> {
  const backupTables = await this.getBackupTables();
  
  if (backupTables.length === 0) {
    return null;
  }
  
  return {
    tableCount: backupTables.length,
    tables: backupTables,
    // Try to extract timestamp from first backup table name
    estimatedTimestamp: this.extractTimestampFromBackupName(backupTables[0])
  };
}

interface BackupInfo {
  tableCount: number;
  tables: string[];
  estimatedTimestamp?: number;
}
```

### 3. Update Prepare Migration Flow

**Location**: `migration-core.ts`

```typescript
/**
 * Enhanced prepare migration with backup cleanup
 */
async prepareMigration(): Promise<PreparationResult> {
  try {
    // Existing prepare logic
    const result = await this.performPrepareMigration();
    
    // Only cleanup backups after successful prepare
    if (result.success) {
      await this.cleanupExistingBackups();
    }
    
    return result;
  } catch (error) {
    // If prepare fails, existing backup remains intact
    logWithTimestamp('❌ Prepare failed, existing backup preserved');
    throw error;
  }
}
```

### 4. Update Start Command Flow

**Location**: `migration.ts`

```typescript
async function handleStartCommand(values: ParsedArgs, dryRun: boolean): Promise<void> {
  // ... existing setup code ...
  
  const migrator = new DatabaseMigrator(sourceConfig, destConfig, preservedTables, dryRun);

  try {
    // Enhanced to include backup cleanup after successful prepare
    const result = await migrator.migrate(); // This calls prepareMigration() internally
    
    // ... existing result handling ...
  } catch (error) {
    // ... existing error handling ...
  }
}
```

### 5. Dry-Run Output Enhancement

**Location**: Update dry-run logic to include backup detection

```typescript
// In dry-run validation
const existingBackup = await this.detectExistingBackups();

if (existingBackup) {
  console.log(`⚠️  Existing backup detected: ${existingBackup.tableCount} tables`);
  console.log(`📝 Note: Existing backup will be replaced after successful prepare`);
}
```

### 6. CLI Test Updates

**Location**: `migration.cli.integration.test.ts`

Update test expectations:
- First migration creates backup
- Second migration's prepare cleans + recreates backup
- List command shows 1 backup (not 2, since second replaces first)
- Update test plan documentation

## Implementation Steps

### Phase 1: Core Functions
1. ✅ Add `getBackupTables()` function
2. ✅ Add `cleanupExistingBackups()` function  
3. ✅ Add `detectExistingBackups()` function

### Phase 2: Integration
4. ✅ Update `prepareMigration()` to call cleanup after success
5. ✅ Update dry-run logic to show backup detection
6. ✅ Ensure both `start` and `prepare` commands use enhanced flow

### Phase 3: Testing
7. ✅ Update CLI test expectations
8. ✅ Update test plan documentation
9. ✅ Test both single-phase and two-phase migration flows

## Safety Guarantees

### Backup Preservation
- Existing backup is NEVER deleted unless prepare phase completes successfully
- If prepare fails, users can still rollback to existing backup
- Clear error messages guide users to recovery options

### Error Handling
- If cleanup fails after successful prepare, log warning but continue
- Swap phase can handle leftover backup tables as fallback
- All operations are logged for troubleshooting

### User Experience
- Dry-run shows what will happen with existing backups
- No interactive prompts required
- Clear logging throughout the process

## Expected Results

### Performance Impact
- **Prepare Phase**: +10-15 seconds (cleanup time)
- **Swap Phase**: -10-15 seconds (cleanup already done)
- **Net Effect**: Swap phase is much faster, total time unchanged

### Test Impact
- CLI test expects 1 backup after two migrations (not 2)
- All other test expectations remain the same
- Test demonstrates real-world multiple migration scenario

### User Benefits
- Multiple migrations on same database work seamlessly
- Reduced downtime during swap phase
- Clear visibility into backup management
- No data loss risk during prepare phase
