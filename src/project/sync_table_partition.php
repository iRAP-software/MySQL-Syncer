<?php

/* 
 * Sync part of a table, not the entire thing
 */

if (isset($argv[1]))
{
    $table_name = $argv[1];
}
else
{
    die("You need to specify the table name" . PHP_EOL);
}

if (!isset($argv[2]))
{
    die("You need to specify the name of the partition column." . PHP_EOL);
}

if (!isset($argv[3]))
{
    die("You need to specify the partition column expected value." . PHP_EOL);
}

$tableName = $argv[1];
$columnName = $argv[2];
$columnValue = $argv[3];


require_once(__DIR__ . '/bootstrap.php');
$syncer = SynchronizerFactory::getSynchronizer();
$syncer->sync_table_partition($tableName, $columnName, $columnValue);

