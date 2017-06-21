<?php

/* 
 * This tiny class is just to allow the main.php and sync_table entrypoint to easily share the
 * same bit of code gracefully.
 */

class SynchronizerFactory
{
    private static $s_synchronizer = null;
    
    
    public static function getSynchronizer()
    {
        if (self::$s_synchronizer == null)
        {
            $startupQueries = array(
                "SET group_concat_max_len = 18446744073709547520",
                "SET FOREIGN_KEY_CHECKS=0"
            );
            
            $masterConnection = new DatabaseConnection(
                MASTER_DB_HOST,
                MASTER_DB_USER,
                MASTER_DB_PASSWORD,
                MASTER_DB_NAME,
                MASTER_DB_PORT,
                $startupQueries
            );
            
            $slaveConnection  = new DatabaseConnection(
                SLAVE_DB_HOST,
                SLAVE_DB_USER,
                SLAVE_DB_PASSWORD,
                SLAVE_DB_NAME,
                SLAVE_DB_PORT,
                $startupQueries
            );
            
            self::$s_synchronizer = new Synchronizer($masterConnection, $slaveConnection, IGNORE_TABLES);
        }
        
        return self::$s_synchronizer;
    }
}

