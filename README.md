DB Sync Tool
============
This tool syncs two databases with the following high-level logic:

* Copy across tables to the slave that don't exist.
* Delete tables from slave that dont exist on master.
* For tables that exist on both master and slave, check the table structure
    * If the structure is different, delete slave table and copy master across.
    * If the structure is the same:
        * Fetch the hash of the entire slave and master tables.
            * If the table hashes are the same, do nothing as they are already in sync
            * if the hashes are different:
                * fetch the hashes for every row for each table (stored in separate database to save memory)
                * delete hashes/rows on slave that are not on master
                * copy across rows that are on master but not on slave.
                
If you need to ignore certain tables, you can specify this in the settings file so that they aren't touched.

## Benefits
* Just uses application logic
    * Can easily create an external backup of your [RDS](https://aws.amazon.com/rds/) instance.
    * Doesn't require system host access.
* Doesn't require any downtime or lock up your tables.
* Iterative - Only differences require syncing so can be pretty quick if you run more frequently.


## Features
* Uses multiprocessing to make use of all your cores.
* Startup queries allow syncing WITHOUT perforing foreign key checks.
* Uses a database to find differences rather than storing hashes in memory, reducing memory requirements without significantly impacting performance.
* Use an array list of regular expressions to ignore certain tables.
* For extremely large tables, one can specify a column that the table should be considered as paritioned by. 
   * One specifies the name of the table and the column id that a table should be partitioned by. The partitions will be synced as if they were separate tables (e.g. taking the hash of the table and then taking row hashes if the "table hash" is different between master and slave.) A rule of thumb might be that any table over 1 million rows should be considered partitioned.
