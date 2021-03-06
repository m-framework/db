<?php

namespace modules\db\libraries;

use m\core;
use m\custom_exception;
use m\registry;
use m\config;
use m\i18n;

class pgsql extends sql
{
    private
        $db_name,
        $db_character,
        $result,
        $settings = [];

    public
        $error,
        $logs = [],
        $_table,
        $_join,
        $_count,
        $__id = 'id',
        $fields,
        $join_type = 'INNER';

    private static $field_types = [
        'bigint' => 'int',
        'integer' => 'int',
        'character varying' => 'varchar',
        'timestamp with time zone' => 'timestamp',
        'timestamp' => 'timestamp',
        'text' => 'text',
        'date' => 'date',
        'float' => 'float',
        'bytea' => 'varbinary',
        'smallint' => 'tinyint',
    ];

    function __construct(array $db_init)
    {
        $this->db_name = $db_init['db_name'];
        $this->db_character = $db_init['db_encoding'];

        $this->settings = $db_init;

        return $this;
    }

    public function __get($name)
    {
        switch ($name) {
            case 'db':

                if (!($this->db = pg_connect(
                    "host=" . $this->settings['db_host'] .
                    " dbname=" . $this->settings['db_name'] .
                    " user=" . $this->settings['db_user'] .
                    " password=" . $this->settings['db_password'])))
                {
                   throw new custom_exception(i18n::get('Can\'t connect to DB') . ' ' . $this->settings['db_name'] .
                        ' ' . i18n::get('with specified username and password'), 404);
                }

                pg_query($this->db, "SET NAMES '" . $this->settings['db_encoding'] . "';");
                pg_query($this->db, "SET CLIENT_ENCODING TO '" . $this->settings['db_encoding'] . "';");

                return $this->db;
            default:
                return false;
        }
    }

    public function has_result()
    {
        return !empty($this->result);
    }

    public function query($sql)
    {

        $sql = str_replace('`', '', $sql);
        $sql_log = htmlspecialchars($sql);

        if (config::get('db_logs') && php_sapi_name() !== 'cli') {
            $query_time = microtime(true);
            $debug_backtrace = $this->prepare_backtrace(debug_backtrace(false));
        }

        $this->result = pg_query($this->db, $sql);

        if (isset($query_time) && isset($sql_log) && isset($debug_backtrace)) {
            $this->set_db_logs($query_time, $sql_log, $debug_backtrace);
        }

        return empty($this->result) ? false : $this;
    }

    public function fetch_assoc($sql)
    {
        if ($pg_query = pg_query($this->db, $sql)) {
            return pg_fetch_array($pg_query, null, PGSQL_ASSOC);
        }
        return false;
    }

    public function fetch_array($sql)
    {
        if ($pg_query = pg_query($this->db, $sql)) {
            return pg_fetch_array($pg_query, null, PGSQL_NUM);
        }
        return false;
    }

    public function fetch_row($sql)
    {
        if ($pg_query = pg_query($this->db, $sql)) {
            $row = pg_fetch_array($pg_query, null, PGSQL_NUM);
            if (!empty($row['0'])) {
                return $row['0'];
            }
        }
        return false;
    }

    public function error()
    {
        return $this->error = pg_last_error($this->db);
    }

    public function last_id()
    {
        return $this->fetch_row('SELECT lastval();');
    }

    public function found_rows()
    {
        return empty($this->result) ? [] : pg_affected_rows($this->result);
    }

    public function num_rows()
    {
        return empty($this->result) ? 0 : pg_num_rows($this->result);
    }

    public function all_tables()
    {
        $this->query("select table_name from information_schema.tables where table_schema = 'public';");

        $tables = [];

        while($row = pg_fetch_assoc($this->result, NULL)) {
            if (!empty($row['table_name'])) {
                $tables[] = $row['table_name'];
            }
        }

        return $tables;
    }

    public function fields($table)
    {
        $this->query("SELECT column_name,data_type FROM information_schema.COLUMNS WHERE TABLE_NAME = '" . $table . "';");

        $fields = [];

        while($row = pg_fetch_assoc($this->result, NULL)) {
            if (!empty($row['column_name'])) {
                $fields[] = [
                    'Type' => empty(static::$field_types[$row['data_type']]) ? $row['data_type'] : static::$field_types[$row['data_type']],
                    'Field' => $row['column_name'],
                ];
            }

        }

        return $fields;
    }

    public function one()
    {
        if (!$this->has_result())
            return false;

        $row = pg_fetch_row($this->result);
        $this->result = null;
        return (isset($row['0'])) ? $row['0'] : false;
    }

    public function all($t = 'assoc', $class_name = null)
    {
        if (!$this->has_result() || $this->result == null) {
            return false;
        }

        $arr = [];

        switch($t) {
            case 'assoc':
                while($row = pg_fetch_assoc($this->result, NULL)) {
                    $arr[] = $row;
                    unset($row);
                }
                break;
            case 'array':
                while($row = pg_fetch_array($this->result, NULL)) {
                    $arr[] = $row;
                    unset($row);
                }
                break;
            case 'object':

                while($row = pg_fetch_object($this->result, NULL, $class_name)) {
                    $row->_count = 1;

                    $row_vars = get_object_vars($row);
                    if (!empty($row_vars) && is_array($row_vars)) {
                        foreach ($row_vars as $row_var => $row_val) {
                            if (method_exists($row, '_override_' . $row_var)) {
                                $row->{'_override_' . $row_var}();
                            }
                        }
                    }

                    if (isset($row->{$row->__id}) && !empty($row->{$row->__id})) {
                        $arr[$row->{$row->__id}] = $row;
                    }
                    else {
                        $arr[] = $row;
                    }

                    unset($row);
                }
                break;
        }


        $this->_count = $this->found_rows();
        pg_free_result($this->result);
        $this->result = null;

        return !empty($arr) ? $arr : false;
    }

    public function modify_table($fields)
    {
        if (empty($this->fields) || empty($fields) || !is_array($this->fields) || empty($this->_table))
            return false;

        $_fields = $fields;

        $need_alter = array_diff_assoc($this->fields, $fields);

//        if (!empty($need_alter)) {
//            core::out($need_alter);
//        }

        /**
         * In version 1.0 we add new fields in table.
         * No deletion of data from DB by changing a table structure !
         */
        $add_columns = array_diff(array_keys($this->fields), array_keys($fields));

        if (empty($need_alter) && empty($add_columns)) {
            return false;
        }

        /**
         * Build MODIFY multiple query
         */
        $q = 'ALTER TABLE ' . $this->_table . ' ' . "\n";
        $fields = [];
        foreach ($need_alter as $column => $type) {

            if (in_array($column, $add_columns)) {
                continue;
            }

            if (in_array($column, ['group'])) {
                $column = '"' . $column . '"';
            }

            if (!empty($this->fields[$column]) && $this->fields[$column] == 'longtext' && $_fields[$column] == 'text') {
                continue;
            }

            switch ($type) {
                case 'int':
                    $fields[] = '  ALTER COLUMN ' . $column . ' TYPE integer';
                    $fields[] = '  ALTER COLUMN ' . $column . ' DROP DEFAULT';
                    $fields[] = '  ALTER COLUMN ' . $column . ' SET DEFAULT NULL';
                    break;
                case 'varchar':
                    $fields[] = '  ALTER COLUMN ' . $column . ' TYPE varchar(255)';
                    $fields[] = '  ALTER COLUMN ' . $column . ' DROP DEFAULT';
                    $fields[] = '  ALTER COLUMN ' . $column . ' SET DEFAULT NULL';
                    break;
                case 'tinyint':
                    $fields[] = '  ALTER COLUMN ' . $column . ' TYPE smallint';
                    $fields[] = '  ALTER COLUMN ' . $column . ' DROP DEFAULT';
                    $fields[] = '  ALTER COLUMN ' . $column . ' SET DEFAULT NULL';
                    break;
                case 'timestamp':
                    $fields[] = '  ALTER COLUMN ' . $column . ' TYPE timestamptz(6)';
                    $fields[] = '  ALTER COLUMN ' . $column . ' DROP DEFAULT';
                    if ($column == 'date') {
                        $fields[] = '  ALTER COLUMN ' . $column . ' SET DEFAULT now() NOT NULL';
                    }
                    else {
                        $fields[] = '  ALTER COLUMN ' . $column . ' SET DEFAULT NULL';
                    }
                    break;
                case 'date':
                    $fields[] = '  ALTER COLUMN ' . $column . ' TYPE date';
                    $fields[] = '  ALTER COLUMN ' . $column . ' DROP DEFAULT';
                    $fields[] = '  ALTER COLUMN ' . $column . ' SET DEFAULT NULL';
                    break;
                case 'time':
                    $fields[] = '  ALTER COLUMN ' . $column . ' TYPE time';
                    $fields[] = '  ALTER COLUMN ' . $column . ' DROP DEFAULT';
                    $fields[] = '  ALTER COLUMN ' . $column . ' SET DEFAULT NULL';
                    break;
                case 'text':
                case 'longtext':
                    $fields[] = '  ALTER COLUMN ' . $column . ' TYPE text';
                    $fields[] = '  ALTER COLUMN ' . $column . ' DROP DEFAULT';
                    $fields[] = '  ALTER COLUMN ' . $column . ' SET DEFAULT NULL';
                    break;
                case 'float':
                    $fields[] = '  ALTER COLUMN ' . $column . ' TYPE float(25)';
                    $fields[] = '  ALTER COLUMN ' . $column . ' DROP DEFAULT';
                    $fields[] = '  ALTER COLUMN ' . $column . ' SET DEFAULT NULL';
                    break;
                case 'varbinary':
                    $fields[] = '  ALTER COLUMN ' . $column . ' TYPE bytea';
                    $fields[] = '  ALTER COLUMN ' . $column . ' DROP DEFAULT';
                    $fields[] = '  ALTER COLUMN ' . $column . ' SET DEFAULT NULL';
                    break;
                default:
                    $fields[] = '  ALTER COLUMN ' . $column . ' TYPE varchar(255)';
                    $fields[] = '  ALTER COLUMN ' . $column . ' DROP DEFAULT';
                    $fields[] = '  ALTER COLUMN ' . $column . ' SET DEFAULT NULL';
            }
        }

        /**
         * For prevent no-sense queries like "ALTER TABLE articles;"
         */
        if (!empty($fields)) {
            $q .= implode(",\n", $fields) . ';';

            $this->query($q);
        }

        /**
         * Build ADD multiple query
         */

        if (empty($add_columns)) {
            return $this->error() ? $this->error . "<br>\n\n" . $q : true;
        }

        foreach ($add_columns as $add_id => $add_column) {
            $add_columns[$add_column] = $this->fields[$add_column];
            unset($add_columns[$add_id]);
        }

        $q = 'ALTER TABLE ' . $this->_table . ' ' . "\n";
        $fields = [];
        foreach ($add_columns as $column => $type) {

            if (in_array($column, ['group'])) {
                $column = '"' . $column . '"';
            }

            switch ($type) {
                case 'int':
                    $fields[] = 'ADD COLUMN ' . $column . ' integer DEFAULT NULL';
                    break;
                case 'varchar':
                    $fields[] = 'ADD COLUMN ' . $column . ' varchar(255) DEFAULT NULL';
                    break;
                case 'tinyint':
                    $fields[] = 'ADD COLUMN ' . $column . ' smallint DEFAULT NULL';
                    break;
                case 'timestamp':
                    $fields[] = 'ADD COLUMN ' . $column . ' timestamptz(6)' . ($column == 'date' ? " DEFAULT now() NOT NULL" : ' DEFAULT NULL');
                    break;
                case 'date':
                    $fields[] = 'ADD COLUMN ' . $column . ' date DEFAULT NULL';
                    break;
                case 'time':
                    $fields[] = 'ADD COLUMN ' . $column . ' time DEFAULT NULL';
                    break;
                case 'text':
                case 'longtext':
                    $fields[] = 'ADD COLUMN ' . $column . ' text DEFAULT NULL';
                    break;
                case 'float':
                    $fields[] = 'ADD COLUMN ' . $column . ' float(25) DEFAULT NULL';
                    break;
                case 'varbinary':
                    $fields[] = 'ADD COLUMN ' . $column . ' bytea DEFAULT NULL';
                    break;
                default:
                    $fields[] = 'ADD COLUMN ' . $column . ' varchar(255) DEFAULT NULL';
            }
        }

        if (!empty($fields)) {
            $q .= implode(",\n", $fields) . ';';

            $this->query($q);
        }

        return $this->error() ? $this->error . "<br>\n\n" . $q : true;
    }

    public function build_table()
    {
        if (empty($this->fields) || !is_array($this->fields) || empty($this->_table))
            return false;

        $primary = $auto_increment = '';
        $keys = [];
        $fields = [];

        // "public".
        $q = "CREATE TABLE IF NOT EXISTS " . $this->_table . " (\n";

        $n = 1;
        foreach ($this->fields as $field => $type) {

            if (in_array($field, ['group'])) {
                $field = '"' . $field . '"';
            }

            $fields[] = $field;

            if (gettype($type) == 'string') {
                switch ($type) {
                    case 'int':
                        $q .= $field;

                        if ($n == 1) {
                            $q .= " bigserial";
                            if ($field == 'id' || in_array($this->_table, ['users'])) {
                                //$q .= " PRIMARY KEY";
                            }
                            $keys[] = $field;
                        } else {
                            $q .= " integer DEFAULT NULL";
                        }
                        $q .= ",\n";
                        break;
                    case 'varchar':
                        $q .= "  " . $field . " varchar(255) ";
                        if ($n == 1) {
                            $q .= "NOT NULL";
                            $keys[] = $field;
                        } else {
                            $q .= "DEFAULT NULL";
                        }
                        $q .= ",\n";
                        break;
                    case 'tinyint':
                        $q .= "  " . $field . " smallint DEFAULT NULL,\n";
                        break;
                    case 'timestamp':
                        $q .= "  " . $field . " timestamptz(6) " .
                            ($field == 'date' ? " DEFAULT now() NOT NULL" : ' DEFAULT NULL') . ",\n";
                        break;
                    case 'date':
                        $q .= "  " . $field . " date DEFAULT NULL,\n";
                        break;
                    case 'time':
                        $q .= "  " . $field . " time DEFAULT NULL,\n";
                        break;
                    case 'text':
                    case 'longtext':
                        $q .= "  " . $field . " text DEFAULT NULL,\n";
                        break;
                    case 'float':
                        $q .= "  " . $field . " float(25) DEFAULT NULL,\n";
                        break;
                    case 'varbinary':
                        $q .= "  " . $field . " bytea DEFAULT NULL,\n";
                        break;
                    default:
                        $q .= "  " . $field . " varchar(255) DEFAULT NULL,\n";
                }
                $n++;
            }
            else if (gettype($type) == 'array' && !empty($type['type']) && gettype($type['type']) == 'string') {
                switch ($type['type']) {
                    case 'enum':
                        if (!empty($type['values'])) {
                            $enum_q = "CREATE TYPE  " . $field . " enum('" . implode("','", $type['values']) . "');\n";

                            $q .= "  " . $field . " $field,\n";
                        }
                        break;
                }
            }
        }

        $q .= "  PRIMARY KEY (" . implode(', ', $keys) . ")\n";
        $q .= ")\n";
        $q .= "WITH (OIDS=FALSE)\n\n;\n";

        if (!empty($enum_q)) {
            $this->query($enum_q);
        }

        $this->query($q);

        return $this->error() ? $this->error . "<br>\n\n" . $q : true;
    }

    public function disconnect()
    {
        if (empty($this->db))
            return false;

        return pg_close($this->db);
    }
}
