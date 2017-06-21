<?php

class Synchronizer
{
    private static $s_logs_dir;
    
    private $m_master;
    private $m_slave;
    private $m_tables = array();
    private $m_ignoreTablesRegexps;
    
    
    /**
     * 
     * @param Connection $master - connection object for the database we wish to use as a source.
     * @param Connection $slave - connection object for the database we wish to update to match the master/source.
     * @param Array $ignoreTableRegexps - list of table names that we will not synchronize.
     */
    public function __construct(DatabaseConnection $master, DatabaseConnection $slave, $ignoreTableRegexps=array())
    {
        $this->m_master = $master;
        $this->m_slave = $slave;
        $this->m_ignoreTablesRegexps = $ignoreTableRegexps;
     
        self::$s_logs_dir = __DIR__ . "/logs";
    }
    
    
    /**
     * Sync the master database to the slave database.
     * @param void
     * @return void.
     */
    public function syncDatabase()
    {
        iRAP\CoreLibs\Filesystem::deleteDir(self::$s_logs_dir);
        
        $num_tables = count($this->m_tables);
        
        $master_tables = $this->m_master->fetch_table_names();
        $slave_tables  = $this->m_slave->fetch_table_names();
        
        // Remove tables that match any of the regexps in IGNORE_TABLES
        $master_tables = $this->removeIgnoredTables($master_tables);
        $slave_tables = $this->removeIgnoredTables($slave_tables);
        
        $excess_slave_tables = array_diff($slave_tables, $master_tables);
        $missing_tables      = array_diff($master_tables, $slave_tables);
        $shared_tables       = array_diff($master_tables, $missing_tables);
        
        $nonPartitionedTables = array_diff($shared_tables, array_keys(PARTITIONED_TABLE_DEFINITIONS));
        $partitionedTables = array_diff($shared_tables, $nonPartitionedTables);
        
        $sharedNonPartitionedTables = array();
        $sharedPartitionedTables = array();
        
        # remove all tables in slave that are not in master.
        print "removing tables that are on the slave which aren't on the master...." . PHP_EOL;
        foreach ($excess_slave_tables as $excess_table)
        {
            $this->m_slave->drop_table($excess_table);
        }
        
        # Create the missing tables and directly copy the data from master into slave
        print "Copying missing tables...." . PHP_EOL;
        foreach ($missing_tables as $missing_table)
        {
            $this->copy_table($missing_table);
        }
        
        # Check existing tables to see where they are already the same and only sync the differences.
        # If the structure has changed, then we need to recreate and copy the data.
        print "Syncing shared tables...." . PHP_EOL;
        
        if (USE_MULTI_PROCESSING)
        {
            $commands = array();
            
            foreach ($nonPartitionedTables as $table_name)
            {
                $commands[] = '/usr/bin/php ' . __DIR__ . '/sync_table.php ' . $table_name;
            }
            
            $commands = array_merge($commands, $this->getCommandsForPartitionedTables($partitionedTables));
            
            $content = implode(PHP_EOL, $commands);
            $filepath = __DIR__ . '/commands.txt';
            file_put_contents($filepath, $content);
            
            $cores = \iRAP\CoreLibs\Core::getNumProcessors();
            
            $command = 'java -jar ' . __DIR__ . '/ThreadWrapper.jar "' . $cores . '" "' . $filepath . '"';
            print "running command: " . PHP_EOL . $command . PHP_EOL;
            print "This may take a very long time, but you can watch progress in the logs directory." . 
                    PHP_EOL;
            shell_exec($command);
        }
        else
        {
            $num_tables = count($shared_tables);
            
            foreach ($shared_tables as $index => $table_name)
            {
                print "syncing $table_name (" . ($index+1) . "/" . $num_tables . ")" . PHP_EOL;
                $this->sync_table($table_name);
                print PHP_EOL;
            }
        }
    }
    
    
    /**
     * Helper function that will generate the commands for syncing partitioned tables.
     * @param array $partitionedTables
     * @return string
     * @throws Exception
     */
    private function getCommandsForPartitionedTables(array $partitionedTables)
    {
        $commands = array();
        
        foreach ($partitionedTables as $partitionedTableName)
        {
            if (!isset(PARTITIONED_TABLE_DEFINITIONS[$partitionedTableName]))
            {
                print print_r(PARTITIONED_TABLE_DEFINITIONS, true);
                throw new Exception("Failed to fetch partition column name for $partitionedTableName");
            }
            
            $partitionColumnName = PARTITIONED_TABLE_DEFINITIONS[$partitionedTableName];
            $selectPartitionsQuery = "SELECT DISTINCT(`$partitionColumnName`) FROM `$partitionedTableName`";
            $masterResult = $this->m_master->run_query($selectPartitionsQuery);
            $slaveResult = $this->m_slave->run_query($selectPartitionsQuery);
            
            if ($masterResult === false || $slaveResult === false)
            {
                throw new Exception("Failed to select partition values for $partitionedTableName: $selectPartitionsQuery");
            }
            
            $masterPartitions = array();
            $slavePartitions = array();
            while(($row = $masterResult->fetch_assoc()) !== null)
            {
                $masterPartitions[] = $row[$partitionColumnName];
            }
            
            while(($row = $slaveResult->fetch_assoc()) !== null)
            {
                $slavePartitions[] = $row[$partitionColumnName];
            }
            
            $excessPartitions = iRAP\CoreLibs\ArrayLib::fastDiff($slavePartitions, $masterPartitions);
            $missingPartitions = iRAP\CoreLibs\ArrayLib::fastDiff($masterPartitions, $slavePartitions);
            
            foreach ($masterPartitions as $partitionValue)
            {
                $commands[] = "/usr/bin/php " . __DIR__ . "/sync_table_partition.php $partitionedTableName $partitionColumnName '$partitionValue'";
            }
            
            foreach ($excessPartitions as $excessPartitionValue)
            {
                print "$partitionedTableName: deleting excess partition {$excessPartitionValue}" . PHP_EOL;
                
                $query = "DELETE FROM `$partitionedTableName` WHERE `$partitionColumnName`='$excessPartitionValue'";
                print "query: $query \n";
                $result = $this->m_slave->run_query($query);
                
                if ($result === FALSE)
                {
                    throw new Exception("Failed to delete excess partition {$excessPartitionValue} on slave");
                }
            }
        }
        
        return $commands;
    }
    
    
    /**
     * Called from the java threadwrapper, this function is responsible for syncing a single
     * table that already exists on both the master and slave and logging all output to a file
     * where it can be seen.
     * @param type $table_name
     */
    public function sync_table($table_name)
    {
        print "$table_name: Syncing" . PHP_EOL;
        
        $master_table = new TableConnection($this->m_master, $table_name);
        $slave_table  = new TableConnection($this->m_slave, $table_name);
        
        $master_creation_string = $master_table->fetch_create_table_string();
        $slave_creation_string  = $slave_table->fetch_create_table_string();
        
        # Remove the auto_increment bit which does not affect table structure.
        $pattern = "%(AUTO_INCREMENT=[0-9]+ )%";
        $master_creation_string_filtered = preg_replace($pattern, "", $master_creation_string);
        $slave_creation_string_filtered  = preg_replace($pattern, "", $slave_creation_string);
        
        if ($master_creation_string_filtered === $slave_creation_string_filtered)
        {
            # Perform a quick hashsum to see if tables data are already in sync
            $master_table_hash = $master_table->fetch_table_hash();
            $slave_table_hash  = $slave_table->fetch_table_hash();
            
            if ($master_table_hash !== $slave_table_hash)
            {
                print "$table_name: master table hash !== slave table hash" . PHP_EOL;
                print "$table_name: [" . $master_table_hash . "] !== [" . $slave_table_hash . "]" . PHP_EOL;

                if ($master_table->has_primary_key())
                {
                    $this->sync_table_data($master_table, $slave_table);
                }
                else
                {
                    if (COPY_TABLES_WITH_NO_PRIMARY)
                    {
                        print "$table_name: WARNING - Cannot sync table as it has no primary key. Fully copying." . PHP_EOL;
                        $this->copy_table($table_name);
                    }
                    else
                    {
                        print "$table_name: WARNING - Cannot sync table as it has no primary key. Skipping." . PHP_EOL;
                    }
                }
            }
            else
            {
                print "$table_name: Tables were already in sync." . PHP_EOL;
            }
        }
        else
        {
            # Table structures are not the same so perform a direct full copy.
            print "$table_name: Table structure changed so copying table." . PHP_EOL;
            $this->copy_table($table_name);
        }
        
        print "$table_name: Synced." . PHP_EOL;        
    }
    
    
    /**
     * Called from the java threadwrapper, this function is responsible for syncing a single
     * table that already exists on both the master and slave and logging all output to a file
     * where it can be seen.
     * @param type $table_name
     */
    public function sync_table_partition($table_name, $columnName, $expectedColumnValue)
    {
        print "$table_name: Syncing partition with value: $expectedColumnValue" . PHP_EOL;
        
        $master_table = new TableConnection($this->m_master, $table_name);
        $slave_table  = new TableConnection($this->m_slave, $table_name);
        
        $master_creation_string = $master_table->fetch_create_table_string();
        $slave_creation_string  = $slave_table->fetch_create_table_string();
        
        # Remove the auto_increment bit which does not affect table structure.
        $pattern = "%(AUTO_INCREMENT=[0-9]+ )%";
        $master_creation_string_filtered = preg_replace($pattern, "", $master_creation_string);
        $slave_creation_string_filtered  = preg_replace($pattern, "", $slave_creation_string);
        
        if ($master_creation_string_filtered === $slave_creation_string_filtered)
        {
            # Perform a quick hashsum to see if tables data are already in sync
            $master_table_hash = $master_table->fetch_table_partition_hash($columnName, $expectedColumnValue);
            $slave_table_hash  = $slave_table->fetch_table_partition_hash($columnName, $expectedColumnValue);
            
            if ($master_table_hash !== $slave_table_hash)
            {
                print "$table_name ($expectedColumnValue): master table partition hash !== slave table partition hash" . PHP_EOL;
                print "$table_name ($expectedColumnValue): [" . $master_table_hash . "] !== [" . $slave_table_hash . "]" . PHP_EOL;
                
                if ($master_table->has_primary_key())
                {
                    $this->sync_table_partition_data(
                        $master_table, 
                        $slave_table, 
                        $columnName, 
                        $expectedColumnValue
                    );
                }
                else
                {
                    if (COPY_TABLES_WITH_NO_PRIMARY)
                    {
                        print "$table_name: WARNING - Cannot sync table as it has no primary key. Fully copying." . PHP_EOL;
                        $this->copy_table($table_name);
                    }
                    else
                    {
                        print "$table_name: WARNING - Cannot sync table as it has no primary key. Skipping." . PHP_EOL;
                    }
                }
            }
            else
            {
                print "$table_name $expectedColumnValue: Table partitions were already in sync." . PHP_EOL;
            }
        }
        else
        {
            # Table structures are not the same so perform a direct full copy.
            print "$table_name: Table structure changed so copying table." . PHP_EOL;
            $this->copy_table($table_name);
        }
        
        print "$table_name: Synced." . PHP_EOL;        
    }
    
    
    /**
     * Helper function that strips the provided array of values that match any of the 
     * regular expressions in the IGNORE_TABLES list.
     * @param array $input
     * @return array
     */
    private function removeIgnoredTables(array $input)
    {
        return $this->removeRegExpsFromArray($input, $this->m_ignoreTablesRegexps);
    }
    
    
    /**
     * Helper function that strips out elements from the provided array that match the regular
     * expressions passed in.
     * @param array $input
     * @return array
     */
    private function removeRegExpsFromArray(array $input, $regExps)
    {
        foreach ($regExps as $regexp)
        {
            $pattern = '|' . $regexp . '|';
            $input = preg_replace($pattern, '', $input);
        }
        
        return array_filter($input);
    }
    
    
    /**
     * Performs a direct copy for a table from the master to the slave.
     * This will drop the table if it already exists.
     * This will ensure that the table structures etc are exactly the same but is extremely slow!
     * @param String $table_name - the name of the table.
     * @return void.
     */
    private function copy_table($table_name)
    {
        print "$table_name: Fully copying" . PHP_EOL;
        $master_table = new TableConnection($this->m_master, $table_name);
        $create_table_string = $master_table->fetch_create_table_string();
        $this->m_slave->drop_table($table_name); # drops it if exists, fine if it doesnt already.
        $createTableResult = $this->m_slave->run_query($create_table_string);
        
        if ($createTableResult === FALSE)
        {
            print PHP_EOL . $create_table_string . PHP_EOL;
            throw new Exception("There as an error creating your table with the query above.");
        }
        
        $slave_table  = new TableConnection($this->m_slave, $table_name);
        
        $num_rows = CHUNK_SIZE;
        $offset = 0;
        
        while (count(($rows = $master_table->fetch_range($offset, $num_rows))) > 0)
        {
            $slave_table->insert_rows($rows);
            $offset += CHUNK_SIZE;
        }
        
        print "$table_name: Copy complete" . PHP_EOL;
    }
    
    
    /**
     * Helper function to sync().
     * This method is only responsible for syncing the data.
     * Table structure should already match before getting here!
     * @param TableConnection $master_table - the table we are syncing from (source)
     * @param TableConnection $slave_table - the table we are syncing to (destination)
     * @return void.
     */
    private function sync_table_data(TableConnection $master_table, TableConnection $slave_table)
    {
        print "fetching master table hash map..." . PHP_EOL;
        $master_table->fetch_hash_map($master_table->get_table_name(), TRUE);
        print "fetching slave table hash map..." . PHP_EOL;
        $slave_table->fetch_hash_map($master_table->get_table_name(), FALSE);
                
        $this->delete_excess_rows($slave_table);
        $this->sync_missing_rows($master_table, $slave_table);
        
        # clean up
        $syncDb = SiteSpecific::getSyncDb();
        $deletion_query = "DELETE FROM `master_hashes` WHERE `table_name`='" . $master_table->get_table_name() . "'";
        $syncDb->query($deletion_query);
        
        $deletion_query = "DELETE FROM `slave_hashes` WHERE `table_name`='" . $master_table->get_table_name() . "'";
        $syncDb->query($deletion_query);
    }
    
    
    /**
     * Helper function to sync().
     * This method is only responsible for syncing the data.
     * Table structure should already match before getting here!
     * @param TableConnection $master_table - the table we are syncing from (source)
     * @param TableConnection $slave_table - the table we are syncing to (destination)
     * @return void.
     */
    private function sync_table_partition_data(TableConnection $master_table, TableConnection $slave_table, $partitionColumn, $partitionColumnValue)
    {
        print "fetching master table hash map..." . PHP_EOL;
        $master_table->fetch_partition_hash_map($master_table->get_table_name(), TRUE, $partitionColumn, $partitionColumnValue);
        print "fetching slave table hash map..." . PHP_EOL;
        $slave_table->fetch_partition_hash_map($master_table->get_table_name(), FALSE, $partitionColumn, $partitionColumnValue);
        
    
        
        $this->delete_excess_rows($slave_table, $partitionColumnValue);
        $this->sync_missing_rows($master_table, $slave_table, $partitionColumnValue);
        
        # clean up
        $syncDb = SiteSpecific::getSyncDb();
        
        $masterHashesDeleteQuery = 
            "DELETE FROM `master_hashes`" . 
            " WHERE `table_name`='" . $master_table->get_table_name() . "' " . 
            " AND `partition_value`='" . md5($partitionColumnValue) . "'";
        
        $syncDb->query($masterHashesDeleteQuery);
        
        $slaveHashesDeleteQuery = 
            "DELETE FROM `slave_hashes`" . 
            " WHERE `table_name`='" . $master_table->get_table_name() . "'" . 
            " AND `partition_value`='" . md5($partitionColumnValue) . "'";
        
        $syncDb->query($slaveHashesDeleteQuery);
    }
    
    
    /**
     * Helper function to sync_table_data. This will copy rows that are in the master but not
     * in the slave.
     * @param TableConnection $masterTable
     * @param TableConnection $slaveTable
     * @param mixed $partitionValue - optional parameter that if specified will result in us only 
     *                                syncing missing rows for the specified partition value.
     * @throws Exception
     */
    private function sync_missing_rows(TableConnection $masterTable, TableConnection $slaveTable, $partitionValue = null)
    {
        print "Finding rows missng from slave.";
        
        $syncDb = SiteSpecific::getSyncDb();
        
        if ($partitionValue !== null)
        {
            $md5PartitionValue = md5($partitionValue);
            
            $query = 
                "SELECT `primary_key_value` FROM `master_hashes` " . 
                "WHERE " . 
                    " `table_name`='" . $slaveTable->get_table_name() . "' " . 
                    " AND `partition_value`='" . $md5PartitionValue . "'" .
                    " AND `hash` NOT IN (" .
                        " SELECT `hash` FROM `slave_hashes`" . 
                        " WHERE `table_name`='" . $slaveTable->get_table_name() . "'" .
                        " AND `partition_value`= '" . $md5PartitionValue . "'" .
                    ")";
        }
        else
        {
            $query = 
                "SELECT `primary_key_value` FROM `master_hashes`" . 
                " WHERE " . 
                    " `table_name`='" . $slaveTable->get_table_name() . "'" . 
                    " AND `hash` NOT IN (" .
                        " SELECT `hash` FROM `slave_hashes`" . 
                        " WHERE `table_name`='" . $slaveTable->get_table_name() . "'" .
                    ")";
        }
        
        $result = $syncDb->query($query);
        
        if ($result === FALSE)
        {
            throw new Exception("Failed to select primary keys in master that arent in slave. " . $syncDb->error);
        }
        
        # Add missing rows
        if ($result->num_rows > 0)
        {
            $keysMissingFromSlave = array();
            $counter = 0;
            
            while (($row = $result->fetch_assoc()) != null)
            {
                $counter++;
                $keysMissingFromSlave[] = explode(",", $row['primary_key_value']);
                
                if ($counter % CHUNK_SIZE == 0)
                {
                    if ($partitionValue !== null)
                    {
                        // when performing by partition, we need to run an extra delete based on
                        // primarry keys before insert in case the primary key we are going to
                        // insert is already taken in another partition id.
                        $slaveTable->delete_rows($keysMissingFromSlave);
                    }
                    
                    # Add missing rows
                    print "inserting " . count($keysMissingFromSlave) . " missing rows" . PHP_EOL;
                    $missing_rows = $masterTable->fetch_rows($keysMissingFromSlave);
                    $slaveTable->insert_rows($missing_rows);
                    $keysMissingFromSlave = array();
                }
            }
            
            if (count($keysMissingFromSlave) > 0)
            {
                if ($partitionValue !== null)
                {
                    // when performing by partition, we need to run an extra delete based on
                    // primarry keys before insert in case the primary key we are going to
                    // insert is already taken in another partition id.
                    $slaveTable->delete_rows($keysMissingFromSlave);
                }
                
                print "inserting " . count($keysMissingFromSlave) . " missing rows" . PHP_EOL;
                $missing_rows = $masterTable->fetch_rows($keysMissingFromSlave);
                $slaveTable->insert_rows($missing_rows);
            }
        }
    }
    
    
    /**
     * Helper function to sync_table_data. This will delete rows in the slave that are not
     * in the master.
     */
    private function delete_excess_rows(TableConnection $slaveTable, $partitionValue=null)
    {
        print "Finding excess rows on slave..." . PHP_EOL;
        
        $syncDb = SiteSpecific::getSyncDb();
        
        if ($partitionValue !== null)
        {
            $query = 
                "SELECT `primary_key_value` FROM `slave_hashes` " . 
                "WHERE" .
                    " `table_name`='" . $slaveTable->get_table_name() . "'" . 
                    " AND `partition_value`='" . md5($partitionValue) . "'" .
                    " AND `hash` NOT IN (" .
                        " SELECT `hash` FROM `master_hashes`" . 
                        " WHERE `table_name`='" . $slaveTable->get_table_name() . "'" . 
                        " AND `partition_value`='" . md5($partitionValue) . "'" .
                    ")";
        }
        else
        {
            $query = 
                "SELECT `primary_key_value` FROM `slave_hashes` " . 
                "WHERE" .
                    " `table_name`='" . $slaveTable->get_table_name() . "'" . 
                    " AND `hash` NOT IN (" .
                    " SELECT `hash` FROM `master_hashes` " . 
                    " WHERE `table_name`='" .  $slaveTable->get_table_name() . "' " .
                ")";
        }
        
        
        $result = $syncDb->query($query);
        
        if ($result == false)
        {
            throw new Exception("Failed to fetch hashes excess row hashes. " . $syncDb->error);
        }
        
        $counter = 0;
        $deletion_keys = array();
        
        while (($row = $result->fetch_assoc()) != null)
        {
            $counter++;
            $deletion_keys[] = explode(",", $row['primary_key_value']);
            
            # Perform a sync of this chunk
            if ($counter % CHUNK_SIZE == 0)
            {
                $slaveTable->delete_rows($deletion_keys);
                $deletion_keys = array();
            }
        }
        
        if (count($deletion_keys) > 0)
        {
            $slaveTable->delete_rows($deletion_keys);
        }
    }
    
    
    /**
     * Returns an array of "sets" that are in arr1 but not arr2
     * @param type $arr1
     * @param type $arr2
     */
    private static function array_set_diff($arr1, $arr2)
    {
        $missing_sets = array();
        
        foreach ($arr1 as $search_set)
        {
            $found = false;
            
            foreach ($arr2 as $subArray2)
            {
                if ($subArray2 === $search_set)
                {
                    $found = true;
                    break;
                }
            }
            
            if (!$found)
            {
                $missing_sets[] = $search_set;
            }
        }
        
        return $missing_sets;
    }
    
    
    /**
     * Same as array_diff except this returns the indexes of the values that are in array1 that are not in array2
     * (compares values, but returns the indexes)
     * @param type $array1
     * @param type $array2
     */
    private static function array_index_diff($array1, $array2)
    {
        $indexes = array();
        $flipped_array2 = array_flip($array2); # swaps indexes and values
       
        foreach ($array1 as $index => $value)
        {
            if (!isset($flipped_array2[$value])) 
            {
                $indexes[] = $index;
            }
        }
        
        return $indexes;
    }
    
    
    /**
     * Faster version of array_diff that relies on not needing to keep indexes of the missing 
     * values
     * (compares values, but returns the indexes)
     * @param type $array1
     * @param type $array2
     */
    private static function fast_array_diff($array1, $array2)
    {
        $missing_values = array();
        $flipped_array2 = array_flip($array2); # swaps indexes and values
       
        foreach ($array1 as $value)
        {
            if (!isset($flipped_array2[$value])) 
            {
                $missing_values[] = $value;
            }
        }
        
        return $missing_values;
    }
    
    
    /**
     * Returns all the values that are in array1 and array2.
     * Relies on the values being integers or strings
     * Will only return a value once, even if it appears multiple times in the array.
     * Does not maintain indexes.
     * @param type $array1 - array of integers or strings to compare
     * @param type $array2 - array of integers or strings to compare
     * @return type
     */
    private static function fast_array_intersect($array1, $array2)
    {
        $shared_values = array();
        $flipped_array2 = array_flip($array2); # swaps indexes and values
       
        foreach ($array1 as $value)
        {
            if (isset($flipped_array2[$value])) 
            {
                $shared_values[] = $value;
            }
        }
        
        return $shared_values;
    }
    
    
    /**
     * Converts a multi-dimensional array into a single dimensional array of hashes.
     * Usefor for comparison of sets.
     * http://stackoverflow.com/questions/2254220/php-best-way-to-md5-multi-dimensional-array
     * @param array $input_array
     * @return type
     */
    private static function get_hashes(Array $input_array)
    {
        $results = array();
        
        foreach ($input_array as $index => $value)
        {
            $results[$index] = md5(json_encode($value));
        }
        
        return $results;
    }
    
    
    /**
     * Fetches an array of values for the specified indexes from the provided array.
     * All the specified indexes must be within the haystack.
     * This does NOT keep index association (e.g. returns a list of values)
     * @param type $haystack - the array we are pulling values from
     * @param type $indexes - array list of keys we want the values of.
     */
    private static function array_get_values($haystack, Array $indexes)
    {
        $values = array();
        
        foreach ($indexes as $index)
        {
            if (!isset($haystack[$index]))
            {
                throw new Exception($err_msg);
            }
            
            $values[] = $haystack[$index];
        }
        
        return $values;
    }
}