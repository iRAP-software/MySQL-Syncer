<?php

class TableConnection
{
    private $m_table;
    private $m_columns;
    private $m_mysqliConn;
    private $m_primary_key;
    
    
    /**
     * Factory method to create a TableConnection more simply through the use of an already existing
     * DatabaseConnection object.
     * @param DatabaseConnection $conn
     * @param type $table
     */
    public function __construct(DatabaseConnection $conn, $table)
    {
        $this->m_table = $table;
        $this->m_mysqliConn = $conn->getMysqli();
        
        $this->fetchPrimaryKey();
        $this->fetchColumns();
    }
    
    
    /**
     * Fetches the rows from the table with the index being the hash and the value being an array
     * that forms the primary key value.
     */
    public function fetchHashMap($table_name, $is_master)
    {
        $syncDb = SiteSpecific::getSyncDb();
        
        $hashTableName = ($is_master) ? "master_hashes" : "slave_hashes";
        $deletionQuery = "DELETE FROM `" . $hashTableName . "` WHERE `table_name`='" . $table_name . "'";
        $syncDb->query($deletionQuery);
        
        $wrappedColumnList = \iRAP\CoreLibs\ArrayLib::wrapElements($this->m_columns, "`");
        
        $offset = 0;
        
        do {
            $rows = array();
            
            $sql =
                "SELECT " . $this->getPrimaryKeyString() . ", " .
                "MD5( CONCAT_WS('#'," . implode(',', $wrappedColumnList) . ")) as hash " .
                "FROM `" . $this->m_table . "`" .
                " LIMIT " . CHUNK_SIZE .
                " OFFSET " . $offset;
            
            $result = $this->m_mysqliConn->query($sql);
            
            if ($result === false) {
                throw new Exception("Failed to select row hashes. " . $sql . PHP_EOL . $this->m_mysqliConn->error);
            }
            
            while (($row = $result->fetch_assoc()) != null) {
                $primaryKeyValue = array();
                
                foreach ($this->m_primary_key as $column_name) {
                    $primaryKeyValue[] = $row[$column_name];
                }
                
                $primaryKeyValueString = implode(",", $primaryKeyValue);
                
                $insertion_row = array(
                    'table_name'        => $table_name,
                    'hash'              => $row['hash'],
                    'primary_key_value' => $primaryKeyValueString
                );
                
                $rows[] = $insertion_row;
            }
            
            if (count($rows) > 0) {
                $insertionQuery = \iRAP\CoreLibs\MysqliLib::generateBatchInsertQuery(
                    $rows,
                    ($is_master === true) ? "master_hashes" : "slave_hashes",
                    $syncDb
                );
                
                $insertion_result = $syncDb->query($insertionQuery);
                
                if ($insertion_result === false) {
                    throw new Exception("Failed to insert hash data. " . $syncDb->error . PHP_EOL . $insertionQuery);
                }
                
                $rows = array();
            }
            
            $offset += CHUNK_SIZE;
        } while ($result->num_rows > 0);
    }
    
    
    /**
     * Fetches the rows from the table with the index being the hash and the value being an array
     * that forms the primary key value.
     */
    public function fetchPartitionHashMap($tableName, $isMaster, $columnName, $columnValue)
    {
        $syncDb = SiteSpecific::getSyncDb();
        
        $hashTableName = ($isMaster) ? "master_hashes" : "slave_hashes";
        $deletion_query = "DELETE FROM `" . $hashTableName . "` WHERE `table_name`='" . $tableName . "' AND `$columnName`='$columnValue'";
        $syncDb->query($deletion_query);
        
        $wrappedColumnList = \iRAP\CoreLibs\ArrayLib::wrapElements($this->m_columns, "`");
        
        $offset = 0;
        
        do {
            $rows = array();
            
            $sql =
                "SELECT " . $this->getPrimaryKeyString() . ", " .
                " MD5( CONCAT_WS('#'," . implode(',', $wrappedColumnList) . ")) as hash " .
                " FROM `" . $this->m_table . "`" .
                " WHERE `" . $columnName . "`='" . $columnValue . "'" .
                " LIMIT " . CHUNK_SIZE .
                " OFFSET " . $offset;
            
            $result = $this->m_mysqliConn->query($sql);
            
            if ($result === false) {
                throw new Exception("Failed to select row hashes. " . $sql . PHP_EOL . $this->m_mysqliConn->error);
            }
            
            while (($row = $result->fetch_assoc()) != null) {
                $primaryKeyValue = array();
                
                foreach ($this->m_primary_key as $column_name) {
                    $primaryKeyValue[] = $row[$column_name];
                }
                
                $primaryKeyValueString = implode(",", $primaryKeyValue);
                
                $insertion_row = array(
                    'table_name'        => $tableName,
                    'partition_value'   => md5($columnValue),
                    'hash'              => $row['hash'],
                    'primary_key_value' => $primaryKeyValueString
                );
                
                $rows[] = $insertion_row;
            }
            
            if (count($rows) > 0) {
                $insertionQuery = \iRAP\CoreLibs\MysqliLib::generateBatchInsertQuery(
                    $rows,
                    ($isMaster === true) ? "master_hashes" : "slave_hashes",
                    $syncDb
                );
                
                $insertion_result = $syncDb->query($insertionQuery);
                
                if ($insertion_result === false) {
                    throw new Exception("Failed to insert hash data. " . $syncDb->error . PHP_EOL . $insertionQuery);
                }
                
                $rows = array();
            }
            
            $offset += CHUNK_SIZE;
        } while ($result->num_rows > 0);
    }
    
    
    /**
     * Fetch all the rows that have the specified primary key values
     * @param Array $primaryKeyValues - array of keys where each key is an array because keys may
     * be formed of multiple columns
     * @return type
     */
    public function fetchRows($primaryKeyValues)
    {
        $rows = array();
        
        if (count($primaryKeyValues) > 0) {
            $primary_key_chunks = array_chunk($primaryKeyValues, CHUNK_SIZE);
            
            foreach ($primary_key_chunks as $primary_key_value_set) {
                $keyValueSets = array();
                
                foreach ($primary_key_value_set as $index => $set) {
                    $quoted_set = \iRAP\CoreLibs\ArrayLib::wrapElements($set, "'");
                    $keyValueSets[] = "(" . implode(',', $quoted_set) . ")";
                }
                
                
                $sql = "SELECT * FROM `" . $this->m_table . "` " .
                       "WHERE (" . $this->getPrimaryKeyString() . ") IN (" . implode(',', $keyValueSets) . ")";
                
                $result = $this->m_mysqliConn->query($sql);
                
                if ($result === false) {
                    throw new Exception("problem with query: " . $sql);
                }
                
                while (($row = $result->fetch_assoc()) != null) {
                    $rows[] = $row;
                }
            }
        }
        
        return $rows;
    }
    
    
    /**
     * Fetches all of the data from the database.
     * WARNING - This could potentially be a huge memory hog!
     * @param void
     * @return type
     */
    public function fetchAllRows()
    {
        $rows = array();
         
        $sql = "SELECT * FROM `" . $this->m_table . "`";
        $result = $this->m_mysqliConn->query($sql);
        
        /* @var $result \mysqli_result */
        while (($row = $result->fetch_assoc()) != null) {
            $rows[] = $row;
        }
        
        return $rows;
    }
    
    
    /**
     * Fetches a range of data from the table.
     * @param int $start - the starting position (this does not need there to be an id integer column)
     * @param int $num_rows - the number of rows you would like to fetch. This method will return less than this
     *                        if there are not that many rows left in the table.
     * @return Array - associative array of the results.
     */
    public function fetchRange($start, $num_rows)
    {
        $rows = array();
        
        $sql = "SELECT * FROM `" . $this->m_table . "` " .
               "LIMIT " . $num_rows . ' OFFSET ' . $start;
        
        $result = $this->m_mysqliConn->query($sql);
        
        /* @var $result \mysqli_result */
        while (($row = $result->fetch_assoc()) != null) {
            $rows[] = $row;
        }
        
        return $rows;
    }
    
    
    /**
     * Fetches all the primary key values in the table. (not the name of the primary key)
     * The result will be left in array form because more than one column may represent the key!
     * @return Array - list of all the primary key values (usually this is just an array of one, but since multiple
     *                 values can form the primary key, this could be an array of multiple values)
     */
    public function fetchPrimaryKeyValues()
    {
        $values = array();
        $sql = "SELECT " . $this->getPrimaryKeyString() . " FROM `" . $this->m_table . "`";
        $resultSet = $this->m_mysqliConn->query($sql);
        
        if ($resultSet === false) {
            throw new Exception("query failed: [" . $sql . ']');
        }
        
        /* @var $resultSet mysqli_result */
        while (($row = $resultSet->fetch_array(MYSQLI_NUM)) != null) { # fetches only numerical rather than assoc as well
            $values[] = $row;
        }
        
        return $values;
    }
    
    
    /**
     * Trys to insert the row (column-name/valu pairs) into this table.
     * @param Array $rows - assoc array of column names to values to insert.
     * @return void
     */
    public function insertRows($rows)
    {
        print "inserting " . count($rows) . " rows." . PHP_EOL;
        
        $chunks = array_chunk($rows, CHUNK_SIZE, $preserve_keys = true);
        
        # Use multi query if the rows are going to vary in structure, but they really shouldnt.
        $USE_MULTI_QUERY = false;
        
        if ($USE_MULTI_QUERY) {
            foreach ($chunks as $row_set) {
                $multi_query = new iRAP\MultiQuery\MultiQuery($this->m_mysqliConn);
                
                foreach ($row_set as $row) {
                    $query =
                        "INSERT INTO `" . $this->m_table . "` " .
                        "SET " . iRAP\CoreLibs\Core::generateMysqliEscapedPairs($row, $this->m_mysqliConn);
                    
                    $multi_query->addQuery($query);
                    
                    if (LOG_QUERIES) {
                        $line = $query . PHP_EOL;
                        file_put_contents(LOG_QUERY_FILE, $line, FILE_APPEND);
                    }
                }
                
                $multi_query->run();
            }
        } else {
            if (count($rows) > 0) {
                $keys = array_keys($rows[0]);
                $escaped_keys = array();
                
                foreach ($keys as $key) {
                    $escaped_keys[] = mysqli_escape_string($this->m_mysqliConn, $key);
                }
                
                $quoted_keys = iRAP\CoreLibs\ArrayLib::wrapElements($escaped_keys, '`');
                
                foreach ($chunks as $row_set) {
                    $value_strings = array();
                    
                    foreach ($row_set as $row) {
                        $values = array_values($row);
                        
                        $escaped_values = array();
                        foreach ($values as $value) {
                            if ($value !== null) {
                                $escaped_values[] = mysqli_escape_string($this->m_mysqliConn, $value);
                            } else {
                                $escaped_values[] = null;
                            }
                        }
                        
                        $quoted_escaped_values = iRAP\CoreLibs\ArrayLib::mysqliWrapValues($escaped_values);
                        $value_strings[] = " (" . implode(',', $quoted_escaped_values) . ")";
                    }
                    
                    $query =
                        "INSERT INTO `" . $this->m_table . "` " .
                        "(" . implode(',', $quoted_keys) . ") " .
                        "VALUES " . implode(',', $value_strings);
                    
                    $result = $this->runQuery($query);
                    
                    if ($result === false) {
                        die("query failed:" . $query . PHP_EOL . $this->m_mysqliConn->error . PHP_EOL);
                    }
                }
            }
        }
    }
    
    
    /**
     * Deletes all rows that have the specified primary key values.
     * @param Array $keys - array list of values matching the primary keys of the rows we wish to remove.
     */
    public function deleteRows($keys)
    {
        print "Deleting " . count($keys) . " rows" . PHP_EOL;
        $key_value_sets = array();
            
        foreach ($keys as $index => $set) {
            $quoted_set = \iRAP\CoreLibs\ArrayLib::wrapElements($set, "'");
            $key_value_sets[] = "(" . implode(',', $quoted_set) . ")";
        }
        
        $sql =
            "DELETE FROM `" . $this->m_table . "` " .
            "WHERE (" . $this->getPrimaryKeyString() . ") " .
            "IN (" . implode(',', $key_value_sets) . ")";
        
        $result = $this->runQuery($sql);
        
        if ($result === false) {
            throw new Exception("Failed to delete rows. " . $sql);
        }
    }
    
    
    /**
     * Fetches the sql statement that would be required to create this table from scratch.
     * e.g.
     * CREATE TABLE `table_name` (
     *  `column1` varchar(255) NOT NULL,
     *  `column2` decimal(6,4) NOT NULL,
     * PRIMARY KEY (`column1`)
     * ) ENGINE=InnoDB DEFAULT CHARSET=utf8 |
     *
     * @param void
     * @return type
     */
    public function fetchCreateTableString()
    {
        $query = "SHOW CREATE TABLE `" . $this->m_table . "`";
        
        $result = $this->m_mysqliConn->query($query);
        $first_row = $result->fetch_array();
        $creation_string = $first_row[1]; # the first column is the table name.
        return $this->alphabetizeConstraints($creation_string);
    }
    
    
    /**
     * This method takes a MySQL table defintion and alphabetizes the CONSTRAINTS because if you
     * were to create a table from this string directly, then mysql will do this anyway, so if you
     * were to compare tables without doing this, then they will always appear to have a different
     * definition, when really they are the same.
     */
    private function alphabetizeConstraints($tableDefinition)
    {
        $lines = explode("\n", $tableDefinition);
        $constraintLines = array(); // CONSTRAINTS lines (to be sorted)
        $prefixLines = array(); // lines before the CONSTRAINTS section
        $suffixLines = array(); // lines after the CONSTRAINTS section
        $constraintsStarted = false;
        
        foreach ($lines as $line) {
            $trimmedLine = trim($line);
            
            if (\iRAP\CoreLibs\StringLib::startsWith($trimmedLine, "CONSTRAINT")) {
                // Strip off commas and re-add them after we have sorted.
                if (\iRAP\CoreLibs\StringLib::endsWith($line, ",")) {
                    $line = substr($line, 0, -1);
                }
                
                $constraintsStarted = true;
                $constraintLines[] = $line;
            } else {
                if (!$constraintsStarted) {
                    $prefixLines[] = $line;
                } else {
                    $suffixLines[] = $line;
                }
            }
        }
        
        sort($constraintLines);
        
        $glue = "," . PHP_EOL;
        $constraintString = implode($glue, $constraintLines);
        
        $definitionLines = array_merge($prefixLines, array($constraintString), $suffixLines);
        $output = implode(PHP_EOL, $definitionLines);
        return $output;
    }
    
    
    /**
     * Generates a hash for the entire table so we can quickly compare tables to see if they are
     * already in syc.
     * Reference: http://stackoverflow.com/questions/3102972/mysql-detecting-changes-in-data-with-a-hash-function-over-a-part-of-table
     * @return String $hashValue - the md5 of the entire table of data.
     */
    public function fetchTableHash()
    {
        $tableHash = "";
        
        $columns = $this->m_columns;
        
        $wrapped_column_list = array();
            
        # Using coalesce to prevent null values causing sync issues as raised in the
        # NullColumnTest test. E.g. [2, null, null] and [null, 2, null] would be considered equal
        # otherwise.
        foreach ($columns as $index => $column) {
            $wrapped_column_list[$index] = "COALESCE(`" . $column . "`, 'NULL')";
        }
        
        $this->m_mysqliConn->query("SET group_concat_max_len = 18446744073709547520");
            
        # do NOT use GROUP_CONCAT here since that has a very small default limit which results in not noticing
        # differences on large tables
        $query =
            "SELECT MD5( GROUP_CONCAT(MD5( CONCAT_WS('#'," . implode(',', $wrapped_column_list) . ")))) " .
            "AS `hash` " .
            "FROM `" . $this->m_table . "`";
        
        /* @var $result mysqli_result */
        $result = $this->m_mysqliConn->query($query);
        
        if ($result !== false) {
            $row = $result->fetch_assoc();
            $tableHash = $row['hash'];
        } else {
            die("Failed to fetch table hash" . PHP_EOL . $this->m_mysqliConn->error);
            throw new Exception("Failed to fetch table hash");
        }
        
        $result->free();
        return $tableHash;
    }
    
    
    /**
     * Generates a hash for the entire table so we can quickly compare tables to see if they are
     * already in syc.
     * Reference: http://stackoverflow.com/questions/3102972/mysql-detecting-changes-in-data-with-a-hash-function-over-a-part-of-table
     * @return String $hashValue - the md5 of the entire table of data.
     */
    public function fetchTablePartitionHash($columnName, $columnValue)
    {
        $tableHash = "";
        
        $columns = $this->m_columns;
        
        $wrapped_column_list = array();
            
        # Using coalesce to prevent null values causing sync issues as raised in the
        # NullColumnTest test. E.g. [2, null, null] and [null, 2, null] would be considered equal
        # otherwise.
        foreach ($columns as $index => $column) {
            $wrapped_column_list[$index] = "COALESCE(`" . $column . "`, 'NULL')";
        }
        
        $this->m_mysqliConn->query("SET group_concat_max_len = 18446744073709547520");
            
        # do NOT use GROUP_CONCAT here since that has a very small default limit which results in not noticing
        # differences on large tables
        $query =
            "SELECT MD5( GROUP_CONCAT(MD5( CONCAT_WS('#'," . implode(',', $wrapped_column_list) . ")))) " .
            "AS `hash` " .
            "FROM `" . $this->m_table . "` WHERE `$columnName`='$columnValue' ORDER BY " . $this->getPrimaryKeyString();
        
        /* @var $result mysqli_result */
        $result = $this->m_mysqliConn->query($query);
        
        if ($result !== false) {
            $row = $result->fetch_assoc();
            $tableHash = $row['hash'];
        } else {
            die("Failed to fetch table hash" . PHP_EOL . $this->m_mysqliConn->error);
            throw new Exception("Failed to fetch table hash");
        }
        
        $result->free();
        return $tableHash;
    }
    
    
    /**
     * Fetches the hashes for each row from the database.
     * This will utilize quite a bit of the mysql hosts CPU.
     * @param type $keys
     * @return type
     */
    public function fetchRowHashes($keys)
    {
        $hashes = array();
        
        $key_sets = array_chunk($keys, 10000);
        
        foreach ($key_sets as $key_set) {
            $multi_query = new iRAP\MultiQuery\MultiQuery($this->m_mysqliConn);
            
            foreach ($key_set as $primaryKeyValue) {
                $primaryKeyValue     = \iRAP\CoreLibs\ArrayLib::wrapElements($primaryKeyValue, "'");
                $wrapped_column_list = \iRAP\CoreLibs\ArrayLib::wrapElements($this->m_columns, "`");
                
                $query =
                    "SELECT MD5( CONCAT_WS('#'," . implode(',', $wrapped_column_list) . " ) ) " .
                    "AS `hash` " .
                    "FROM `" . $this->m_table . "` " .
                    "WHERE (" . $this->getPrimaryKeyString() . ") = (" . implode(",", $primaryKeyValue) . ")";
                
                $multi_query->addQuery($query);
            }
            
            $multi_query->run();
            
            foreach ($key_set as $index => $redundant) {
                $result_set = $multi_query->get_result($index);
                $row = $result_set[0]; #  there should only be one row
                $hashes[] = $row['hash'];
            }
        }
        
        return $hashes;
    }
    
    
    /**
     * Replace all the data_rows in the database by the primary keys specified in the index_values. The order of the
     * index_values
     * We delibereately delete all the rows before inserting the updates because we do not want to run into issues
     * with other unique keys etc.
     * @param type $index_values
     * @param type $data_rows
     */
    public function replaceRows($index_values, $data_rows)
    {
        $multi_query = new iRAP\MultiQuery\MultiQuery($this->m_mysqliConn);
                
        $key_value_sets = array();
        
        # The primary key could itself be
        foreach ($index_values as $index => $key_set) {
            $escaped_key_set = array();
            foreach ($key_set as $key) {
                $escaped_key_set[] = mysqli_escape_string($this->m_mysqliConn, $key);
            }
            
            $quoted_set = \iRAP\CoreLibs\ArrayLib::wrapElements($escaped_key_set, "'");
            $key_value_sets[] = "(" . implode(',', $quoted_set) . ")";
        }
        
        print "deleting rows that need replacing" . PHP_EOL;
        
        $delete_query =
            "DELETE FROM `" . $this->m_table . "` " .
            "WHERE (" . $this->getPrimaryKeyString() . ") " .
            "IN (" . implode(",", $key_value_sets) . ")";
        
        $deletion_result = $this->runQuery($delete_query);
        
        print "inserting " . count($data_rows) . " replacement rows." . PHP_EOL;
        $this->insertRows($data_rows);
    }
    
    
    /**
     * Dynamically discovers the primary key for this table and sets this objects member variable accordingly.
     * @param void
     * @return void
     */
    private function fetchPrimaryKey()
    {
        $this->m_primary_key = array();
        
        $query = "show index FROM `" . $this->m_table . "`";
        /*@var $result mysqli_result */
        $result = $this->m_mysqliConn->query($query);
        $this->m_primary_key = null;
        
        while (($row = $result->fetch_assoc()) != null) {
            if ($row["Key_name"] === "PRIMARY") {
                $this->m_primary_key[] = $row["Column_name"];
            }
        }
        
        if (count($this->m_primary_key) == 0) {
            $this->m_primary_key = null;
            print "WARNING: " . $this->m_table . " does not have a primary key!" . PHP_EOL;
        }
        
        $result->free();
    }
    
    
    /**
     * Fetches the names of the columns for this particular table.
     * @return type
     */
    private function fetchColumns()
    {
        $sql = "SHOW COLUMNS FROM `" . $this->m_table . "`";
        $result = $this->m_mysqliConn->query($sql);
        
        $this->m_columns = array();
        
        while (($row = $result->fetch_array()) != null) {
            $this->m_columns[] = $row[0];
        }
        
        $result->free();
    }
    
    
    /**
     * Convert the primary key array into a string that can be used in queries.
     * e.g. array('id') would become: "(`id`)"
     * array(group, filter) would become "(`group`, `filter`)"
     * @return type
     */
    private function getPrimaryKeyString()
    {
        $wrapped_elements = \iRAP\CoreLibs\ArrayLib::wrapElements($this->m_primary_key, '`');
        $csv = implode(',', $wrapped_elements);
        return $csv;
    }
    
    
    /**
     * Returns whether this table has a primary key or not.
     * @return boolean
     */
    public function hasPrimaryKey() : bool
    {
        $result = true;
        
        if ($this->m_primary_key === null) {
            $result = false;
        }
                
        return $result;
    }
    
    
    /**
     * Fetch the number of rows in the table
     * @return int
     */
    public function getNumRows()
    {
        $query = "SELECT COUNT(*) FROM `" . $this->m_table . "`";
        $result = $this->m_mysqliConn->query($query);
        /* @var $result \mysqli_result */
        $row = $result->fetch_array(MYSQLI_NUM);
        $result->free();
        return $row[0];
    }
    
    
    /**
     * Helper function that will execute queries on the database if we are not running a dry run
     * Hence all "write" queries should utilize this method, but "read" queries shouldnt if they still need to run
     * on a dry run
     * @param String $query - the query we want to send.
     * @return - the result from the query (or true if we are executing a "dry run")
     */
    private function runQuery($query)
    {
        $result = true;
        
        if (LOG_QUERIES) {
            $line = $query . PHP_EOL;
            file_put_contents(LOG_QUERY_FILE, $line, FILE_APPEND);
        }
        
        $result = $this->m_mysqliConn->query($query);
        
        return $result;
    }
    
    # accessors
    public function getTableName()
    {
        return $this->m_table;
    }
}

