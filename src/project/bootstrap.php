<?php

/* 
 * Initializations script responsible for setting up autoloaders/settings etc.
 */
require_once(__DIR__ . '/../settings/settings.php');
require_once(__DIR__ . '/vendor/autoload.php');

$classDirs = array(__DIR__);
$autoloader = new iRAP\Autoloader\Autoloader($classDirs);
