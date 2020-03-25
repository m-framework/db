<?php

namespace modules\db\libraries;

use m\custom_exception;
use m\registry;
use m\config;
use m\i18n;

class mysqli extends sql
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

                if (!($this->db = mysqli_connect(
                    $this->settings['db_host'],
                    $this->settings['db_user'],
                    $this->settings['db_password'],
                    $this->settings['db_name']
                ))) {
                   throw new custom_exception(i18n::get('Can\'t connect to DB') . ' `' . $this->settings['db_name'] .
                        '` ' . i18n::get('with specified username and password'), 404);
                }

                mysqli_query($this->db, 'SET NAMES ' . $this->settings['db_encoding']);
                mysqli_query($this->db, 'SET storage_engine=InnoDB;');
                mysqli_query($this->db, 'SET character_set_client ' . $this->settings['db_encoding']);
                mysqli_query($this->db, 'SET character_set_connection ' . $this->settings['db_encoding']);
                mysqli_query($this->db, 'SET character_set_database ' . $this->settings['db_encoding']);
                mysqli_query($this->db, 'SET character_set_results ' . $this->settings['db_encoding']);
                mysqli_query($this->db, 'SET character_set_server ' . $this->settings['db_encoding']);
                mysqli_query($this->db, 'SET NAMES ' . $this->settings['db_encoding']);

                return $this->db;
            default:
                return false;
        }
    }

    public function has_result()
    {
        return !empty($this->result) && $this->result instanceof \mysqli_result;
    }

    public function query($sql)
    {
        $sql_log = htmlspecialchars($sql);

        if (config::get('db_logs') && php_sapi_name() !== 'cli') {
            $query_time = microtime(true);
            ob_start();
            debug_print_backtrace(DEBUG_BACKTRACE_IGNORE_ARGS);
            $debug_backtrace = ob_get_contents();
            ob_clean();
        }

        if ($this->result = mysqli_query($this->db, $sql)) {
            if (config::get('db_logs')) {
                $query_time = empty($query_time) ? '' : ' (' . round((microtime(true) - $query_time) * 1000, 3) . 's)';

                if ((registry::get('is_ajax') || php_sapi_name() == 'cli') && !empty($sql_log)) {
                    registry::append('db_logs', $sql_log);
                }
                else if (!empty($debug_backtrace) && !empty($query_time) && !empty($sql_log)) {
                    registry::append('db_logs', '<b>' . $sql_log . '</b> ' . $query_time . "\n" . $debug_backtrace);
                }
            }

            return $this;
        }
        else if (config::get('db_logs')&& !empty($sql_log)) {
            if ((registry::get('is_ajax') || php_sapi_name() == 'cli') && !empty($sql_log)) {
                registry::append('db_logs', $sql_log);
            }
            else if (!empty($debug_backtrace) && !empty($query_time) && !empty($sql_log)) {
                $query_time = empty($query_time) ? '' : ' (' . round((microtime(true) - $query_time) * 1000, 3) . 's)';
                registry::append('db_logs', '<b>' . $sql_log . '</b> ' . $query_time . "\n" . $debug_backtrace);
            }
        }

        return false;
    }

    public function fetch_assoc($sql)
    {
        if ($mysqli_query = mysqli_query($this->db, $sql)) {
            return mysqli_fetch_assoc($mysqli_query);
        }
        return false;
    }

    public function fetch_array($sql)
    {
        if ($mysqli_query = mysqli_query($this->db, $sql)) {
            return mysqli_fetch_array($mysqli_query);
        }
        return false;
    }

    public function fetch_row($sql)
    {
        if ($mysqli_query = mysqli_query($this->db, $sql)) {
            $row = mysqli_fetch_row($mysqli_query);
            if (!empty($row['0'])) {
                return $row['0'];
            }
        }
        return false;
    }

    public function error()
    {
        return $this->error = mysqli_error($this->db);
    }

    public function last_id()
    {
        return mysqli_insert_id($this->db);
    }

    public function found_rows()
    {
        return $this->fetch_row('SELECT FOUND_ROWS()');
    }

    public function all_tables()
    {
        $this->query('SHOW TABLES FROM `' . $this->db_name . '`');
        return $this->all();
    }

    public function fields($table)
    {
        $this->query('SHOW COLUMNS FROM `' . $table . '`');
        return $this->all();
    }

    public function one()
    {
        if (!$this->has_result())
            return false;

        $row = mysqli_fetch_row($this->result);
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
                while($row = mysqli_fetch_assoc($this->result)) {
                    $arr[] = $row;
                    unset($row);
                }
                break;
            case 'array':
                while($row = mysqli_fetch_array($this->result)) {
                    $arr[] = $row;
                    unset($row);
                }
                break;
            case 'object':

                while ($this->result !== null && $row = mysqli_fetch_object($this->result, $class_name)) {

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
                mysqli_free_result($this->result);
                break;
        }

        $this->_count = $this->found_rows();
        $this->result = null;
        return !empty($arr) ? $arr : false;
    }

    public function modify_table($fields)
    {
        if (empty($this->fields) || empty($fields) || !is_array($this->fields) || empty($this->_table))
            return false;

        $need_alter = array_diff_assoc($this->fields, $fields);
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
        $q = 'ALTER TABLE `' . $this->_table . '` ' . "\n";
        $fields = [];
        foreach ($need_alter as $column => $type) {

            if (in_array($column, $add_columns)) {
                continue;
            }

            switch ($type) {
                case 'int':
                    $fields[] = 'MODIFY COLUMN `' . $column . '` int(11)';
                    break;
                case 'varchar':
                    $fields[] = 'MODIFY COLUMN `' . $column . '` varchar(255) DEFAULT NULL';
                    break;
                case 'tinyint':
                    $fields[] = 'MODIFY COLUMN `' . $column . '` tinyint(1) unsigned DEFAULT NULL';
                    break;
                case 'timestamp':
                    $fields[] = 'MODIFY COLUMN `' . $column . '` timestamp NULL DEFAULT ' .
                        ($column == 'date' ? 'CURRENT_TIMESTAMP' : 'NULL');
                    break;
                case 'date':
                    $fields[] = 'MODIFY COLUMN `' . $column . '` date DEFAULT NULL';
                    break;
                case 'time':
                    $fields[] = 'MODIFY COLUMN `' . $column . '` time DEFAULT NULL';
                    break;
                case 'text':
                    $fields[] = 'MODIFY COLUMN `' . $column . '` text DEFAULT NULL';
                    break;
                case 'longtext':
                    $fields[] = 'MODIFY COLUMN `' . $column . '` longtext DEFAULT NULL';
                    break;
                case 'float':
                    $fields[] = 'MODIFY COLUMN `' . $column . '` float(11,2) DEFAULT NULL';
                    break;
                case 'varbinary':
                    $fields[] = 'MODIFY COLUMN `' . $column . '` varbinary(16) DEFAULT NULL';
                    break;
                default:
                    $fields[] = 'MODIFY COLUMN `' . $column . '` varchar(255) DEFAULT NULL';
            }
        }

        /**
         * For prevent no-sense queries like "ALTER TABLE `articles`;"
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

        $q = 'ALTER TABLE `' . $this->_table . '` ' . "\n";
        $fields = [];
        foreach ($add_columns as $column => $type) {
            switch ($type) {
                case 'int':
                    $fields[] = 'ADD COLUMN `' . $column . '` int(11)';
                    break;
                case 'varchar':
                    $fields[] = 'ADD COLUMN `' . $column . '` varchar(255) DEFAULT NULL';
                    break;
                case 'tinyint':
                    $fields[] = 'ADD COLUMN `' . $column . '` tinyint(1) unsigned DEFAULT NULL';
                    break;
                case 'timestamp':
                    $fields[] = 'ADD COLUMN `' . $column . '` timestamp NULL DEFAULT ' .
                        ($column == 'date' ? 'CURRENT_TIMESTAMP' : 'NULL');
                    break;
                case 'date':
                    $fields[] = 'ADD COLUMN `' . $column . '` date DEFAULT NULL';
                    break;
                case 'time':
                    $fields[] = 'ADD COLUMN `' . $column . '` time DEFAULT NULL';
                    break;
                case 'text':
                    $fields[] = 'ADD COLUMN `' . $column . '` text DEFAULT NULL';
                    break;
                case 'float':
                    $fields[] = 'ADD COLUMN `' . $column . '` float(11,2) DEFAULT NULL';
                    break;
                case 'varbinary':
                    $fields[] = 'ADD COLUMN `' . $column . '` varbinary(16) DEFAULT NULL';
                    break;
                default:
                    $fields[] = 'ADD COLUMN `' . $column . '` varchar(255) DEFAULT NULL';
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

        $q = "CREATE TABLE IF NOT EXISTS `" . $this->_table . "` (\n";

        $n = 1;
        foreach ($this->fields as $field => $type) {

            $fields[] = $field;

            if (gettype($type) == 'string') {
                switch ($type) {
                    case 'int':
                        $q .= "  `" . $field . "` int(11) unsigned ";
                        if ($n == 1) {
                            $primary = $field;
                            $q .= "NOT NULL";
                            if ($field == 'id' || in_array($this->_table, ['users'])) {
                                $q .= " AUTO_INCREMENT";
                            }
                        } else {
                            $q .= "DEFAULT NULL";
                        }
                        $q .= ",\n";
                        $keys[] = $field;
                        break;
                    case 'varchar':
                        $q .= "  `" . $field . "` varchar(255) ";
                        if ($n == 1) {
                            $q .= "NOT NULL";
                        } else {
                            $q .= "DEFAULT NULL";
                        }
                        $q .= ",\n";
                        if ($field == 'alias') {
                            $keys[] = $field;
                        }
                        break;
                    case 'tinyint':
                        $q .= "  `" . $field . "` tinyint(1) unsigned DEFAULT NULL,\n";
                        break;
                    case 'timestamp':
                        $q .= "  `" . $field . "` timestamp NULL DEFAULT " .
                            ($field == 'date' ? "CURRENT_TIMESTAMP" : "NULL") . ",\n";
                        break;
                    case 'date':
                        $q .= "  `" . $field . "` date DEFAULT NULL,\n";
                        break;
                    case 'time':
                        $q .= "  `" . $field . "` time DEFAULT NULL,\n";
                        break;
                    case 'text':
                        $q .= "  `" . $field . "` text DEFAULT NULL,\n";
                        break;
                    case 'longtext':
                        $q .= "  `" . $field . "` longtext DEFAULT NULL,\n";
                        break;
                    case 'float':
                        $q .= "  `" . $field . "` float(11,2) DEFAULT NULL,\n";
                        break;
                    case 'varbinary':
                        $q .= "  `" . $field . "` varbinary(16) DEFAULT NULL,\n";
                        break;
                    default:
                        $q .= "  `" . $field . "` varchar(255) DEFAULT NULL,\n";
                }
                $n++;
            }
            else if (gettype($type) == 'array' && !empty($type['type']) && gettype($type['type']) == 'string') {
                switch ($type['type']) {
                    case 'enum':
                        if (!empty($type['values'])) {
                            $q .= "  `" . $field . "` enum('" . implode("','", $type['values']) . "') DEFAULT '" . reset($type['values']) . "',\n";
                        }
                        break;
                }
            }
        }

        if (empty($primary) && !empty($fields['0'])) {
            $primary = $fields['0'];
            $q = str_replace("`" . $field . "` varchar(255) DEFAULT NULL", "`" . $field . "` varchar(255) NOT NULL", $q);
        }

        $q .= "  PRIMARY KEY (`" . $primary . "`)";

        if (!empty($keys)) {

            $q .= ",\n";

            if (count($keys) > 16) {
                $keys = array_slice($keys, 0, 16);
            }

            $q .= "  KEY `" . $this->_table . "` (`" . implode('`,`', $keys) . "`)\n";
        }

        $q .= ") ENGINE=InnoDB DEFAULT CHARSET=" . (!empty($this->db_character) ? $this->db_character : 'utf8') . ";";

        $this->query($q);

        return $this->error() ? $this->error . "<br>\n\n" . $q : true;
    }

    public function disconnect()
    {
        if (empty($this->db))
            return false;

        return mysqli_close($this->db);
    }
}
