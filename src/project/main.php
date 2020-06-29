<?php

require_once(__DIR__ . '/bootstrap.php');

class Main
{
    public function __construct()
    {
    }


    public function run()
    {
        # reset the query log if it exists
        if (file_exists(LOG_QUERY_FILE)) {
            unlink(LOG_QUERY_FILE);
        }

        # Create the hash tables
        $syncDb = SiteSpecific::getSyncDb();
        $deleteSlaveHashesQuery = "DROP TABLE IF EXISTS `slave_hashes`";
        $syncDb->query($deleteSlaveHashesQuery);

        $deletionQuery = "DROP TABLE IF EXISTS `master_hashes`";
        $syncDb->query($deletionQuery);

        $slaveCreationQuery =
            "CREATE TABLE `slave_hashes` (
                `table_name` varchar(255) NOT NULL DEFAULT '',
                `partition_value` char(32) DEFAULT NULL,
                `primary_key_value` varchar(767) NOT NULL DEFAULT '',
                `hash` char(32) DEFAULT NULL,
                PRIMARY KEY (`table_name`,`primary_key_value`),
                KEY `hash` (`hash`),
                KEY `partition_value` (`partition_value`)
            ) ENGINE=InnoDB";

        $createSlaveHashesTableResult = $syncDb->query($slaveCreationQuery);

        if ($createSlaveHashesTableResult === false) {
            throw new Exception("Failed to create the slave hash table" . $db->error);
        }

        $createMasterHashesTableQuery =
            "CREATE TABLE `master_hashes` (
                `table_name` varchar(255) NOT NULL DEFAULT '',
                `partition_value` char(32) DEFAULT NULL,
                `primary_key_value` varchar(767) NOT NULL DEFAULT '',
                `hash` char(32) DEFAULT NULL,
                PRIMARY KEY (`table_name`,`primary_key_value`),
                KEY `hash` (`hash`),
                KEY `partition_value` (`partition_value`)
            ) ENGINE=InnoDB";

        $createMasterHashesTableResult = $syncDb->query($createMasterHashesTableQuery);

        if ($createMasterHashesTableResult === false) {
            throw new Exception("Failed to create the slave hash table" . $db->error);
        }

        $syncer = SynchronizerFactory::getSynchronizer();
        $syncer->syncDatabase();
    }
}


$main = new Main();
$main->run();
