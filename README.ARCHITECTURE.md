# Architecture and Design Decisions

This document details the technical architecture and design decisions behind the zero-downtime PostgreSQL migration system. For usage and setup instructions, see the main [README.md](./README.md).

## Core Architecture

The migration system implements a **table-swap strategy** with atomic operations to achieve true zero-downtime database migrations between PostgreSQL instances.

### Migration Phases

1. **Source Preparation**: Rename source tables/objects to `shadow_` prefix
2. **Shadow Creation**: Restore renamed objects as shadow tables in destination 
3. **Preserved Sync**: Real-time synchronization for preserved tables (optional)
4. **Atomic Swap**: Instantaneous table renaming in destination database
5. **Cleanup**: Remove write protection and temporary objects

## Key Design Decisions

### Table-Swap vs Schema-Swap Strategy

**Decision**: Use table-level swapping instead of schema-level swapping.

**Rationale**:
- **PostGIS Compatibility**: Spatial data types (`geometry`, `geography`) are tightly coupled to the `public` schema where PostGIS functions are loaded
- **Extension Dependencies**: PostgreSQL extensions often create objects in specific schemas, making cross-schema moves problematic
- **Atomic Operations**: Table renaming within a schema is faster and more atomic than moving objects between schemas
- **Constraint Preservation**: Table-level operations better preserve complex constraint relationships

**Trade-offs**:
- More complex object naming during migration
- Requires careful coordination of sequences, constraints, and indexes
- But provides better compatibility and performance

### Source Database Shadow Renaming

**Decision**: Rename all source objects to `shadow_` prefix before dumping, then restore original names.

**Problem Solved**: `pg_restore` cannot handle name collisions when restoring to a database that already contains tables with the same names.

**Implementation**:
```sql
-- Before dump
ALTER TABLE "User" RENAME TO "shadow_User";
ALTER SEQUENCE "User_id_seq" RENAME TO "shadow_User_id_seq"; 
ALTER INDEX "User_pkey" RENAME TO "shadow_User_pkey";
-- ... dump with shadow_ names ...
-- After dump  
ALTER TABLE "shadow_User" RENAME TO "User";
-- ... restore original names
```

**Benefits**:
- Eliminates `pg_restore` naming conflicts
- Preserves source database integrity during migration
- Enables clean restoration if migration fails
- Maintains referential integrity throughout process

### Write Protection System

**Decision**: Implement trigger-based write protection instead of connection blocking.

**Implementation**:
```sql
CREATE OR REPLACE FUNCTION migration_block_writes()
RETURNS TRIGGER AS $$
BEGIN
  RAISE EXCEPTION 'Data modification blocked during migration process'
    USING ERRCODE = 'P0001', HINT = 'Migration in progress - please wait';
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Applied to each table
CREATE TRIGGER migration_write_block_tablename
BEFORE INSERT OR UPDATE OR DELETE ON "tablename"
FOR EACH ROW EXECUTE FUNCTION migration_block_writes();
```

**Benefits**:
- Allows schema operations (ALTER TABLE) during migration
- Blocks only data modifications (INSERT/UPDATE/DELETE)
- Provides clear error messages to applications
- Automatic cleanup via CASCADE operations

**Cleanup Strategy**:
- Query `information_schema.triggers` for migration triggers
- Drop triggers first, then the function
- Use CASCADE to handle dependency cleanup
- Comprehensive error handling for partial failures

### Atomic Table Swap Mechanism

**Decision**: Use PostgreSQL's transactional DDL for atomic table swapping.

**Process**:
```sql
BEGIN;
-- Rename current tables to backup_*
ALTER TABLE "User" RENAME TO "backup_User";
ALTER SEQUENCE "User_id_seq" RENAME TO "backup_User_id_seq";
-- Rename shadow tables to active names  
ALTER TABLE "shadow_User" RENAME TO "User";
COMMIT; -- Atomic activation
```

**Critical Properties**:
- **Atomicity**: All-or-nothing operation within transaction
- **Consistency**: Referential integrity maintained throughout
- **Isolation**: Other transactions see either old or new state, never partial
- **Durability**: Changes are permanent once committed

**Downtime**: ~40-80ms for the swap transaction

### Backup Strategy

**Decision**: Create backup tables (`backup_*`) instead of backup schemas.

**Rationale**:
- Simpler cleanup and management
- Consistent with table-swap architecture  
- Easier rollback operations
- Better integration with existing tooling

**Implementation**:
- Original tables renamed to `backup_tablename`
- Backup tables remain in `public` schema
- Sequences, constraints, and indexes also renamed with `backup_` prefix
- Rollback reverses the renaming process

### Error Handling and Recovery

**Layered Approach**:
1. **Prevention**: Pre-migration validation and checks
2. **Protection**: Write protection during critical phases
3. **Recovery**: Automatic source database restoration on failure
4. **Rollback**: Manual rollback capability via backup tables

**Source Database Protection**:
- Always restore original table names if renaming fails
- Remove write protection even on errors
- Preserve data integrity as top priority

**Destination Database Recovery**:
- Backup tables enable rollback to pre-migration state
- Write protection prevents corruption during swap
- Comprehensive cleanup of temporary objects

### Performance Optimizations

**Parallel Operations**:
- `pg_dump` and `pg_restore` use multiple parallel jobs
- Calculated based on CPU core count: `Math.min(8, cpus().length)`
- Significant speedup for large databases

**Efficient Data Transfer**:
- Binary dump format (`--format=custom`) for speed
- Compressed transfers reduce I/O overhead
- Streaming operations where possible

**Minimal Locking**:
- Write protection only during critical atomic operations
- Schema operations allowed during preparation phases
- Read operations unaffected throughout migration

## Integration Points

### PostGIS Compatibility

**Spatial Data Handling**:
- Geometry/geography types migrate with table structure
- Spatial indexes preserved during table swap
- PostGIS functions remain accessible in `public` schema
- No cross-schema reference issues

### Constraint Management

**Referential Integrity**:
- Foreign keys preserved through table renaming
- Check constraints maintained during swap
- Unique constraints and indexes transferred atomically
- Sequence ownership updated correctly

### Extension Dependencies

**PostgreSQL Extensions**:
- `uuid-ossp` and `postgis` compatibility verified
- Extension objects remain in expected schemas
- Function dependencies handled correctly
- No extension reload required

## Testing Strategy

### Integration Test Coverage

**Write Protection Validation**:
```sql
-- Verify no migration artifacts remain
SELECT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'migration_block_writes');
SELECT COUNT(*) FROM information_schema.triggers 
WHERE trigger_name LIKE 'migration_write_block_%';
```

**Data Integrity Checks**:
- Source data preservation during migration
- Destination data replacement verification  
- Foreign key constraint validation
- Sequence value continuity

**Rollback Testing**:
- Post-migration data modification
- Backup table restoration
- Referential integrity after rollback
- Schema compatibility validation

### Performance Benchmarking

**Migration Speed Factors**:
- Database size vs migration time correlation
- Parallel job count optimization
- Network bandwidth utilization
- Disk I/O characteristics

## Monitoring and Observability

### Migration Metrics

**Performance Tracking**:
- Total migration duration
- Individual phase timings
- Record count verification
- Error and warning counts

**Resource Utilization**:
- CPU usage during parallel operations
- Memory consumption patterns
- Disk space requirements
- Network transfer rates

### Logging Strategy

**Structured Logging**:
- Timestamped phase transitions
- Detailed object rename operations
- Write protection state changes
- Error context and recovery actions

**Debug Information**:
- Table and constraint counts
- Shadow object discovery
- Backup creation verification
- Cleanup operation results

## Security Considerations

### Permission Requirements

**Database Privileges**:
- `CREATE` privilege on schemas
- `ALTER` privilege on tables and sequences
- `TRIGGER` privilege for write protection
- `SELECT`, `INSERT`, `UPDATE`, `DELETE` for data operations

**Connection Security**:
- SSL/TLS encryption support
- Connection pooling compatibility
- Authentication method flexibility
- Network access control integration

### Data Protection

**Sensitive Data Handling**:
- No data inspection or logging of table contents
- Minimal privilege requirement principle
- Temporary file cleanup
- Secure connection parameter handling

This architecture provides a robust, production-ready solution for zero-downtime PostgreSQL migrations while maintaining data integrity and system performance.
