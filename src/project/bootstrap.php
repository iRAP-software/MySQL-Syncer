<?php

/* 
 * Initializations script responsible for setting up autoloaders/settings etc.
 */
require_once(__DIR__ . '/../settings/settings.php');
require_once(__DIR__ . '/vendor/autoload.php');

// Ensure we are running on UTC time to prevent possible issues with syncing timestamp.
// https://dba.stackexchange.com/questions/201767/mysql-5-6-error-1292-22007-incorrect-datetime-value-2015-03-29-010112
date_default_timezone_set('UTC'); 

$classDirs = array(__DIR__);
$autoloader = new iRAP\Autoloader\Autoloader($classDirs);
