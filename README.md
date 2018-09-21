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


## Requirements

* PHP 7.0+ (CLI)
* Java (JRE).


## How To Use

### Configure Settings
There is a template settings file at `src/settings/settings.php.tmpl`. Copy that file and remove the extension to become `src/settings/settings.php`. 

Next, you need to fill in the settings. It is *recommended* that you use a read-only connection for the "current live database" connection details. This is the database that you will want to be syncing **from** rather than to, so there is no need for any permission other than "SELECT". The "local dev database" is the database you will be updating to reflect the "current live database", whilst the "sync database" is a database to be used in order to facilitate the sync. An empty database should be created for this purpose. For speed, you may wish to use a seperate server for this database, but you may find it easier just to add another empty database on the same server as your dev database. 

I would recommend **not** changing the `CHUNK_SIZE` or `USE_MULTI_PROCESSING` settings, but you probably want to increase the [max_allowed_packet](https://dev.mysql.com/doc/refman/8.0/en/packet-too-large.html) size on your databases to be larger than the defaults.

You probably want to have an empty array for the `IGNORE_TABLES` setting. However, if you need the sync tool to not sync any tables in particular, then be sure to put them here. It uses regular expressions in case you need to do something like ignoring all the tables that start with `data-`. However, most of the time you probably want to use an exact match, so you would put in names like this: `^my_full_table_name_here$`.

When tables are a million or more rows large, it can be hard to keep them in sync. Thus the `PARTITIONED_TABLE_DEFINITIONS` was introduced to allow the tool to sync a chunk of the table at a time. This works best when there is an integer column that can represent a chunk of the data within the table, especially if that chunk of data is unlikely to change much. For example if you have a table that is made up of a bunch of "datasets" stuck together, that are each identified by a "dataset_id" integer identifier, then the `dataset_id` would be a perfect column to put here. If such a table was called "dataset_data" then the line to put in the array would be `'dataset_data' => 'dataset_id',`.

### Execution
After you have filled in all of the settings, you can run the tool with 

```
php src/project/main.php
``` 

Eventually the tool will want to run a commands file in parallel to sync the tables in parallel. You will need a JRE installed in order for the tool to automatically run the bundeled `ThreadWrapper.jar` tool to do this. Alternatively, I find it easier to just build a `multiprocess` command within my PATH [using this tutorial](https://blog.programster.org/easily-parallelize-commands-in-linux), and call `multiprocess commands.txt` in order to run this step and be able to see the output.

## Timestamps Issue
MySQL will default to setting up defaults on timestamps. This means that if your master database has a table like so:

```
CREATE TABLE `report_filters` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `date_created` timestamp,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=5001 DEFAULT CHARSET=utf8
```

... and you haven't explicitly set `explicit_defaults_for_timestamp` in your mysql configuration file, 
then whenever you run the sync tool, the table will be dropped and recreated because the structure is different. 
The structure will continue to be different until you set `explicit_defaults_for_timestamp` in your MySQL config file which we recommend you do. 
[More info](https://blog.programster.org/mysql-timestamps-automatically-update).
