# Task: Replace Schema-Swap with Table-Swap Migration System

## Executive Summary

Rip and replace the current schema-based migration system with a table-swap approach.  Keep all the same features and tests and just get them to passing.

## Problem Statement

### Current Schema-Swap Issues
- **Complex Architecture**: Schema manipulation requires extensive management overhead
- **Performance Impact**: ~30s downtime due to schema operations and FK recreation
- **Maintenance Burden**: Complex rollback and error handling for schema operations
- **Extension Compatibility**: Schema swapping can cause issues with PostgreSQL extensions

### Root Cause Analysis
```sql
-- Current schema-swap flow:
ALTER SCHEMA public RENAME TO backup_1754197848355;    -- Complex operation
ALTER SCHEMA shadow RENAME TO public;                  -- Extension dependencies
-- Result: Complex rollback and maintenance overhead
```

## Proposed Solution: Table-Swap Migration

### Core Concept
Instead of swapping schemas, swap individual tables within the `public` schema, keeping the schema structure stable.

### Migration Flow
```sql
-- New table-swap flow:
BEGIN;
SET CONSTRAINTS ALL DEFERRED;

-- For each table:
ALTER TABLE "Area" RENAME TO "backup_Area";            -- Preserve original
ALTER TABLE "shadow_Area" RENAME TO "Area";            -- Activate new data

COMMIT; -- FK validation happens atomically
```

### Key Benefits
- ✅ **Schema Stability**: Extensions remain in `public` schema throughout
- ✅ **Atomic Operations**: Single transaction with deferred constraints (~40-80ms downtime)
- ✅ **Simpler Architecture**: No schema manipulation or extension management
- ✅ **Better Performance**: Direct table renames vs complex schema operations
- ✅ **Clear Backup Strategy**: `backup_TableName` convention

## Implementation Plan

### Phase 1: Core Table-Swap Engine

#### 1.1 Replace Migration Core Architecture
**File**: `src/migration-core.ts`

- [ ] Remove all schema-swap related code and methods
- [ ] Replace `DatabaseMigrator` implementation with table-swap logic
- [ ] Remove shadow schema creation and management
- [ ] Remove schema renaming and extension management code

#### 1.2 Replace Core Migration Logic  
**File**: `src/migration-core.ts` (update existing file)

- [ ] **Phase 1**: Shadow Table Creation
  ```typescript
  async createShadowTables(sourceTables: TableInfo[]): Promise<void>
  // - pg_dump source tables directly from public schema
  // - pg_restore to destination public schema
  // - Rename restored tables to shadow_* prefix
  ```

- [ ] **Phase 2**: Preserved Table Sync Setup
  ```typescript
  async setupPreservedTableSync(preservedTables: TableInfo[]): Promise<void>
  // - Create triggers: backup_Table → shadow_Table sync
  // - Handle real-time updates during migration
  ```

- [ ] **Phase 3**: Atomic Table Swap
  ```typescript
  async performAtomicTableSwap(tables: TableInfo[]): Promise<void>
  // - BEGIN; SET CONSTRAINTS ALL DEFERRED;
  // - Table → backup_Table (all tables)
  // - shadow_Table → Table (all tables)  
  // - COMMIT; (atomic FK validation)
  ```

- [ ] **Phase 4**: Cleanup and Validation
  ```typescript
  async finalizeTableSwap(): Promise<void>
  // - Remove sync triggers
  // - Update sequences
  // - Validate data consistency
  // - Clean up backup tables (optional)
  ```

#### 1.3 Rollback Strategy
- [ ] **Emergency Rollback**: `backup_Table → Table` if migration fails
- [ ] **Validation Rollback**: Check data integrity before finalizing
- [ ] **Cleanup Rollback**: Remove partial shadow tables on early failure

### Phase 2: Integration and Testing

#### 2.1 Update CLI Interface
**File**: `src/migration.ts`

- [ ] Update migration commands to use table-swap implementation
- [ ] Maintain existing CLI interface (no breaking changes)
- [ ] Update help documentation to reflect new implementation
- [ ] Remove any schema-swap specific options

#### 2.2 Convert Existing Tests
**Files**: `src/test/migration.*.test.ts`

- [ ] **Update All Tests**: Convert existing schema-swap tests to table-swap
- [ ] **Preserve Test Coverage**: Maintain all existing test scenarios  
- [ ] **Integration Tests**: Ensure full migration workflows still work
- [ ] **Error Recovery Tests**: Update rollback test scenarios
- [ ] **Performance Tests**: Measure table-swap performance improvements

### Phase 3: Complete Legacy Removal

#### 3.1 Remove Schema-Swap Code
- [ ] Delete all schema-swap implementation code
- [ ] Remove shadow schema creation and management
- [ ] Remove schema renaming utilities
- [ ] Remove extension schema management code

#### 3.2 Code Cleanup and Simplification
- [ ] Simplify migration core architecture (single approach)
- [ ] Clean up configuration options
- [ ] Update documentation and comments
- [ ] Remove unused imports and dependencies

## Implementation Details

### New File Structure

```
src/
├── migration-core.ts                 # Table-swap implementation only
├── migration-table-swap.ts           # Move prototype code here
├── types/
│   └── migration-types.ts             # Simplified type definitions
└── test/
    ├── migration.unit.test.ts
    ├── migration.integration.test.ts
    ├── migration.postgis.test.ts
    └── migration.performance.test.ts
```

### Configuration Changes

```typescript
// Simplified configuration - no strategy selection needed
interface MigrationConfig {
  preservedTables?: string[];
  maxParallelism?: number;                   // Default: CPU cores
  backupTableRetention?: boolean;            // Keep backup_* tables after migration
}
```

### CLI Changes

None

## Success Criteria

### Functional Requirements

- [ ] All existing migration functionality works with table-swap approach
- [ ] Data migrates successfully without type resolution errors
- [ ] Sequential migrations work reliably
- [ ] Preserved tables sync correctly during table-swap operations
- [ ] Rollback functionality restores original state successfully

### Performance Requirements

- [ ] Migration downtime reduced to 40-80ms (vs current ~30s)
- [ ] Overall migration time comparable or faster than schema-swap
- [ ] Memory usage remains within acceptable limits for large datasets
- [ ] No performance regression for migrations

### Quality Requirements

- [ ] All existing tests pass with table-swap approach
- [ ] Code coverage maintained at >90%
- [ ] No breaking changes to existing API
- [ ] Comprehensive error handling and logging
- [ ] Production-ready documentation

## Risk Assessment

### High Risk

- **Data Loss**: Ensure atomic operations and comprehensive rollback
- **FK Constraint Violations**: Proper deferred constraint handling
- **Large Dataset Performance**: Test with realistic data volumes

### Medium Risk

- **Memory Usage**: Monitor for large table operations
- **Compatibility**: Ensure works across PostgreSQL versions
- **Migration Timing**: Balance speed vs reliability

### Mitigation Strategies

- Extensive testing with realistic datasets
- Staged rollout with feature flags
- Comprehensive monitoring and alerting
- Detailed rollback procedures and testing

## Timeline Estimate

- **Phase 1**: 2-3 weeks (Core implementation)
- **Phase 2**: 2-3 weeks (Integration and testing)
- **Phase 3**: 1 week (Legacy removal and cleanup)

**Total**: 5-7 weeks

## Acceptance Criteria

### Must Have

- [ ] Table-swap approach successfully migrates schemas
- [ ] Downtime reduced to <100ms for typical migrations
- [ ] All existing functionality preserved
- [ ] Comprehensive test coverage

### Should Have

- [ ] Performance improvements over previous implementation
- [ ] Automatic optimization detection
- [ ] Detailed migration metrics and monitoring

### Could Have

- [ ] Parallel table processing for large schemas
- [ ] Advanced rollback scenarios and recovery
- [ ] Migration performance comparison tooling

## Conclusion

The table-swap approach represents a fundamental improvement in migration architecture that:

1. **Improves Performance**: Reduces downtime from ~30s to ~40-80ms through atomic operations
2. **Simplifies Architecture**: Removes complex schema manipulation and extension management
3. **Enhances Reliability**: Provides clearer rollback strategies and error recovery

**Total**: 5-7 weeks

## Acceptance Criteria

### Must Have

- [ ] Table-swap approach successfully migrates schemas
- [ ] Downtime reduced to <100ms for typical migrations
- [ ] All existing functionality preserved
- [ ] Comprehensive test coverage

### Should Have

- [ ] Performance improvements over previous implementation
- [ ] Automatic optimization detection
- [ ] Detailed migration metrics and monitoring

### Could Have

- [ ] Parallel table processing for large schemas
- [ ] Advanced rollback scenarios and recovery
- [ ] Migration performance comparison tooling

## Conclusion

The table-swap approach represents a fundamental improvement in migration architecture that:

1. **Improves Performance**: Reduces downtime from ~30s to ~40-80ms through atomic operations
2. **Simplifies Architecture**: Removes complex schema manipulation and extension management
3. **Enhances Reliability**: Provides clearer rollback strategies and error recovery

This implementation will establish pg_zero_migration as a more reliable and performant solution for PostgreSQL schema migrations.
