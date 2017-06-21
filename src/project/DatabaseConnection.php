<?php

class DatabaseConnection
{
    private $m_mysqli_conn;
    
    private $m_table_list = null;
    
    
    /**
     * Construct a connection object. 
     * @param type $host - the host that the database is on, such as localhost
     * @param type $user - the user to connect with
     * @param type $password - the password that corresponds to the user
     * @param type $database - the name of the database on the host
     * @param type $table - the table we wish to synchronize
     * @param type $primary_key - the primary key of the table. May also just be a unique index
     * @param type $port - the port if it is not the default 3306
     * @param array $startupQueries - the queries that should be executed on startup for configuration.
     */
    public function __construct($host, $user, $password, $database, $port, $startupQueries)
    {   
        $this->m_mysqli_conn = new mysqli($host, $user, $password, $database, $port) 
            or die("Failed to connect to $host database");
        
        if (!$this->m_mysqli_conn->set_charset("utf8")) 
        {
            printf("Error loading character set utf8: %s\n", $mysqli->error);
            die();
        }
        
        if (count($startupQueries) > 0)
        {
            foreach ($startupQueries as $startupQuery)
            {
                $result = $this->m_mysqli_conn->query($startupQuery);
                
                if ($result === false)
                {
                   throw new \Exception("Startup query failed: " . $this->m_mysqli_conn->error); 
                }
            }
        }
    }
    
    
    /**
     * Fetch the list of tables that exist in the database. This will cache the result so that subsequent calls will
     * return almost immediately.
     * @return Array<String> - list of table names
     */
    public function fetch_table_names()
    {
        if ($this->m_table_list === null)
        {
            $tables = array();
            
            $query = "SHOW TABLES";
            $result = $this->m_mysqli_conn->query($query);
            
            while (($row = $result->fetch_array()) != null)
            {
                $tables[] = $row[0];
            }
            
            $this->m_table_list = $tables;
        }
        
        return $this->m_table_list;
    }
    
    
    /**
     * Executes a passed in query if we are not using a DRY_RUN.
     * This should be used for all "write" queries but not any "read" queries that need to actually execute on a dry 
     * run
     * It is better to use this object's other methods wherever possible.
     * @param string $query - the query to execute.
     * @return \mysqli_result
     */
    public function run_query($query)
    {
        $result = true;
        
        if (LOG_QUERIES)
        {
            $line = $query . PHP_EOL;
            file_put_contents(LOG_QUERY_FILE, $line, FILE_APPEND);
        }
        
        $result = $this->m_mysqli_conn->query($query);
        
        return $result;
    }
    
    
    /**
     * Drop a table in the database by name.
     * @param String $table_name - the name of the table to drop.
     * @return void - throws exception if failed.
     */
    public function drop_table($table_name)
    {
        print "Dropping table " . $table_name . PHP_EOL;
        $query = "DROP TABLE IF EXISTS `" . $table_name . "`";
        $this->run_query($query);
    }
    
    
    # Accessors
    public function get_mysqli() { return $this->m_mysqli_conn; }
}

