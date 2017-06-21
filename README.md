DB Sync Tool
============
This tool syncs two databases.

Finds missing tables and straight copies them across.
Finds excess slave tables and deletes them from the slave.
For tables that exist on both, check structure, if different delete slave and copy across.
For shared tables that have the same structure:
* Fetch the table hash
    * if same do nothing (already synced)
    * if different:
        * fetch the row hashes for each table (stored in separate database so *not* memory intensive)
        * delete excess hashes/rows on slave
        * copy across rows that are on master but not on slave.

## Features
* This tool uses multiprocessing with blocking MySQL queries.
* This tool uses startup queries to allow syncing WITHOUT perforing foreign key checks.
* This tool uses a third database to reduce memory requirements without significantly impacting performance.
* This tool uses an array list of regular expressions to ignore certain tables.
* One can specify in the settings.php file whether tables are extremely large and should be considered as paritioned. One specifies the name of the table and the column id that a table should be partitioned by. The partitions will be synced as if they were separate tables (e.g. taking the hash of the table and then taking row hashes if the "table hash" is different between master and slave.) A rule of thumb might be that any table over 1 million rows should be considered partitioned.

## Release Notes

### 1.2.5
* Fixed an issue whereby tables would always be considered having a different structure if constraints were added to the master over time and thus not in alphabetical order. MySQL would always have these in alphabetical order if you were to use that string to create the table.
