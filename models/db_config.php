<?php

namespace modules\db\models;

use m\model;

class db_config extends model
{
    public $__id = 'type';
    protected $table_name = 'db_config';
    protected $fields = [
        'type' => 'varchar',
        'host' => 'varchar',
        'port' => 'int',
        'db_name' => 'varchar',
        'user' => 'varchar',
        'password' => 'varchar',
        'encoding' => 'varchar',
    ];

    public static $types = [
        'mysqli' => 'MySQLi',
//        'pgsql' => 'PostgreSQL',
//        'sqlite' => 'SQLite',
//        'sqlite3' => 'SQLite3',
        'mongodb' => 'MongoDB',
//        'oci8' => 'Oracle OCI8',
//        'fbird' => 'Firebird'
    ];
}