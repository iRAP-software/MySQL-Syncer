<?php

/* 
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

class SiteSpecific
{
    public static function getSyncDb()
    {
        static $db = null;
        
        if ($db == null)
        {
            $db = new mysqli(SYNC_DB_HOST, SYNC_DB_USER, SYNC_DB_PASSWORD, SYNC_DB_NAME, SYNC_DB_PORT);
        }
        
        return $db;
    }
}