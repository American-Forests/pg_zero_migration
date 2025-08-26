ToDo

- DONE. add preservation to integration test
- DONE. add high priority validation
- DONE. add medium priority validation
- DONE. Backup Schema Completeness Validation - Important for recovery scenarios
    some hints that postgis system tables do not end up in the backup, so verify available on rollback
- DONE. Pre-Migration Preserved Table Validation - Prevents obvious issues early
- DONE. add rollback test
- DONE. split out the dump/restore in migration-core
- DONE. add write of logfile as part of CLI, allow verbose logging as well to console
- DONE. rebuild the readme

- DONE. recommend any real-time checks/validation that should really be moved to tests
- DONE. move high priority real-time validations to tests:
  - Foreign Key Integrity validation moved to 'should perform complete migration and verify data integrity' test
  - Database Consistency validation moved to 'should handle migration with dry run mode' test  
  - Backup Referential Integrity validation moved to 'should perform rollback after migration' test
  - Sync Consistency validation moved to 'should validate sync triggers during migration workflow' test
  - Schema Compatibility validation moved to 'should perform rollback after migration' test
  - Table Data Integrity validation moved to 'should perform rollback after migration' test
- DONE. move medium priority real-time validations to tests:
  - Trigger Health validation moved to 'should validate sync triggers during migration workflow' test
- DONE. add additional validation coverage in tests (keeping runtime validation):
  - Atomic Schema Swap validation added to 'should perform complete migration and verify data integrity' test
  - Trigger Existence validation added to 'should validate sync triggers during migration workflow' test
- DONE. data modification should include all tables including geometry column
- DONE. data modification should include adding and removing rows from at least one table
- DONE. swap the fixture modification to be done on the source database.
- DONE. ensure production will not be overloaded on restore
- DONE. separate dump/restore from rest of migration so that we can keep production up for as long as possible without a maintenance window.

DONE. (We don't need to, include the swap command to run in the output to prepare) Why do we need to track MigrationStatus at all?  Can we trust the user will call both commands with the right parameters?  And add some checks that warn if shadow schema not ready or preserved tables not present so something is probably wrong.

DONE. I want you to update the migration integration test "should perform complete two-phase migration with preserved tables and sync functionality using TES schema".  I want it to verify that every table, sequence, and constraint have been reversed after the dump such that the source database is in its original state.  It should do a before and after comparison to verify.  Tell me your plan before proceeding

DONE. I want you to update the migration integration test "should perform complete two-phase migration with preserved tables and sync functionality using TES schema".  I want it to verify that after swapping every main table, sequence, and constraint has been moved over to backup.  It should do a before and after comparison to verify.  It should do the same check for the tables, sequences, and constraints swapping from shadow to the main tables. Tell me your plan before proceeding

I think it should be one comprehensive test on a single database.  First it should do a one-phase migration using the tes fixture, then using those same source and destination databases already loaded, it should do a second two-phase migration.  This will create two backups on the same database.  Then test the list, verify, rollback, and cleanup.  What do you think? tell me your plan

- implement multi-migration-test-plan.md (STILL FAILING UNTIL SECOND MIGRATE WORKS, BACKUP ALREADY EXISTS!!!)
- ALMOST DONE: implement backup-cleanup-implementation-plan.md
- remove any expectation of timestamp in the backup table names.  No need for extractTimestampFromBackupName()
- remove backup list command (since only one backup)
- simplify clear command to just a simple check for backups and delete
- update readme with backup behavior
- Update prepare and swap commands to both generate log files

Making DB readonly
- if the shadow table swap is all in a single transaction do we even need to prevent queries from being run on the database? Do we need to drop active sessions (other than our own)? I think the answer is no.  If the swap transaction fails it should automatically rollback.

Backups:
- should be deleted at end of prepare and flagged in the dry-run
- If a migration isn't satisfactory user should rollback before next migration to not lose the original.  This avoids having more than 3x the data in DB at once.
- This is at least better than no backup where you'd have to recover the DB from a snapshot.

- DONE. Need a test that modifies preserved table data after prepare (test sync)
- DONE. (move write protection on source until later) when does write protection get enabled and disabled within prepare and swap?  are we removing write protection at the right/earliest times?
- DONE. For the swap, all you need to do is move production resources to backup and then move shadow resources to production.  You don't need to copy any data and you don't need to do anything different depending on whether preserved or not.  We've already done the hard work with the syncing mechanism beforehand.

- DONE. Is validateAtomicTableSwap a replacement for validateAtomicSchemaSwap?  Does it have equivalent logic or was anything lost?  Can validateAtomicSchemaSwap just be removed?

- DONE. make sure that validateSyncConsistency is in place both at runtime and at test time (more comprehensive).  Add comments to the functions indicating how it is different than the other
- DONE. Make sure tests and validation functions are not losing logic/completeness
  - Let me simplify this by removing the backup schema check since table-swap doesn't use backup schemas
- DONE. Fix remaining integation tests
  - As you fix these one by one, tell me your plan and confirm before proceeding. Try not simplify or remove any logic from existing tests, just convert it to the equivalent for a table swap strategy.
  - DONE. why do you have to disable write protection on production tables in migration.integration.test.ts line 2344?  Production writes should be possible right up until the swap happens so make sure that's true, don't work around it in the test.
  - DONE. fixed issue with write protection being dumped from source and enabled in shadow on import

- bring back old CLI integration test
  - create a single test that uses the TES schema and tests all the CLI commands. It first runs single phase migration using XXX preserved tables.  Then immediately runs a second two-phase migration using the same source and destination database.  This will confirm that the source and destination were returned to their proper state.  Then run a list command and confirm the expected backups available, then run a restore, confirm there is a single backup remaining and a shadown (maybe).  Then clear the remaining backup.

- ensure proper way to make DB read only and still allow FK constrain modifications.  Why not use default_transaction_read_only = true at DB level?
- add ascii art diagram or two explaining how it works

- Find any remaining gaps in testing/validation and recommend additions to integrations tests.  These should be added to one of the existing integration tests when possible in migration.integration.test.ts.  Propose which tests to add it to as part of each recommendation.

- Remove any debug code

- see if it can review diff for commits back to XXXXX commit and confirm no validation logic or tests were lost, only an equivalent conversion to table swap strategy

- add additional test that production tables can be used after two migrations.  Insert a new record into each table.  Use FK knowledge to insert in the proper order.
- Make sure timestamp is still incorporated into backup table names for rollback
- Make sure full sync trigger validation integration testing is done.  Confirm can change the data after initial sync and triggers are in place and changes get synced.
- Find all references to schema remaining and make sure migrated

- verify timestamps are included in logging so that we can measure how long things take
- look for any extra debug output that should be removed
- look for anything else in prisma schema that should be used but is not
- drop single phase migration?

Overall playbook:

- snapshot production after maintenance window started
