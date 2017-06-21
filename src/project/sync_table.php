<?php

/* 
 * Sync an entire table
 */
if (isset($argv[1]))
{
    $table_name = $argv[1];
    require_once(__DIR__ . '/bootstrap.php');
    $syncer = SynchronizerFactory::getSynchronizer();
    $syncer->sync_table($table_name);
}
else
{
    print "you need to specify the table name" . PHP_EOL;
}

