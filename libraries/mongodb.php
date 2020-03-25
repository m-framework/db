<?php

namespace modules\db\libraries;

use m\core;
use m\custom_exception;
use m\registry;
use m\db;
use m\config;
use m\i18n;

class mongodb
{
    private
        $db_name,
        $db_character,
        $result,
        $settings = [],
        $last_id;

    public
        $error,
        $logs = [],
        $_table,
        $_join,
        $_count,
        $__id = 'id',
        $fields;

    function __construct(array $db_init)
    {
        $this->db_name = $db_init['db_name'];
        $this->db_character = $db_init['db_encoding'];

        $this->settings = array_merge([
            'db_host' => '127.0.0.1',
            'db_port' => '27017',
            'db_name' => 'test',
            'db_user' => '',
            'db_password' => '',
        ], $db_init);

        return $this;
    }

    private function __clone() {}

    public function __get($name)
    {
        switch ($name) {
            case 'db':

                if (!($this->db = new \MongoDB\Driver\Manager('mongodb://' .
                    $this->settings['db_user'] . ':' .
                    $this->settings['db_password'] . '@' .
                    $this->settings['db_host'] . ':' .
                    $this->settings['db_port'] . '/' .
                    $this->settings['db_name']))) {

                   throw new custom_exception(i18n::get('Can\'t connect to DB') . ' `' . $this->settings['db_name'] .
                        '` ' . i18n::get('with specified username and password'), 404);
                }

                return $this->db;
            default:
                return false;
        }
    }

    public function has_result()
    {
        return !empty($this->result) && $this->result instanceof \MongoDB\Driver\Cursor;
    }

    public function query(\MongoDB\Driver\Query $query)
    {
        if ($this->result = $this->db->executeQuery($this->db_name . '.' . $this->_table, $query)) {
            return $this;
        }
        return false;
    }

    public function command(\MongoDB\Driver\Command $command)
    {
        if (config::get('db_logs') && php_sapi_name() !== 'cli') {

            $query_time = microtime(true);
            ob_start();
            debug_print_backtrace(DEBUG_BACKTRACE_IGNORE_ARGS);
            $debug_backtrace = ob_get_contents();
            ob_clean();

            $command_log = registry::get('mongo_query');
        }

        if ($this->result = $this->db->executeCommand($this->db_name, $command)) {

            if (config::get('db_logs')) {

                $query_time = empty($query_time) ? '' : ' (' . round((microtime(true) - $query_time), 5) . 's)';

                if ((registry::get('is_ajax') || php_sapi_name() == 'cli') && !empty($command_log)) {
                    registry::append('db_logs', $command_log);
                }
                else if (!empty($debug_backtrace) && !empty($query_time) && !empty($command_log)) {
                    registry::append('db_logs', '<b>' . $command_log . '</b> ' . $query_time . "\n" . $debug_backtrace);
                }
            }

            return $this;
        }
        else if (config::get('db_logs')&& !empty($command_log)) {
            if ((registry::get('is_ajax') || php_sapi_name() == 'cli') && !empty($command_log)) {
                registry::append('db_logs', $command_log);
            }
            else if (!empty($debug_backtrace) && !empty($query_time) && !empty($command_log)) {
                $query_time = empty($query_time) ? '' : ' (' . round((microtime(true) - $query_time) * 1000, 3) . 'ms)';
                registry::append('db_logs', '<b>' . $command_log . '</b> ' . $query_time . "\n" . $debug_backtrace);
            }
        }

        return false;
    }

    public function error()
    {
//        if (empty($this->result)) {
//            return null;
//        }

        $cursor = $this->db->executeCommand($this->db_name, new \MongoDB\Driver\Command(['listCollections' => 1]));

        if (!empty($cursor)) {
            foreach ($cursor->toArray() as $collection) {
                $collections[] = ['Tables_in_' . $this->db_name => $collection->name];
            }
        }

        return $this->error = empty($response) || empty($response->err) ? null : $response->err;
    }

    public function last_id()
    {
        if (empty($this->last_id)) {
            return null;
        }

        $last_id = $this->last_id;
        $this->last_id = null;

//        $query = new \MongoDB\Driver\Query([], ['batchSize' => 1]);
//        $cursor = $this->db->executeQuery($this->db_name . '.' . $this->_table, $query);
//        $last_id = $cursor->getId();

        return $last_id;
    }

    public function found_rows()
    {
        return empty($this->_count) ? 0 : (int)$this->_count;
    }

    public function all_tables()
    {
        $collections = [];

        $cursor = $this->db->executeCommand($this->db_name, new \MongoDB\Driver\Command(['listCollections' => 1]));

        if (!empty($cursor)) {
            foreach ($cursor->toArray() as $collection) {
                $collections[] = ['Tables_in_' . $this->db_name => $collection->name];
            }
        }

        return $collections;
    }

    public function fields($table)
    {
        return [];
        $record = (array)$this->db->$table->findOne([]);
        return empty($record) ? [] : array_keys($record);
    }

    public function prepare_conditions($cond = null, $merge_and = null)
    {
        if (empty($cond)) {
            return [];
        }

        foreach ($cond as $k => $cond_val) {

            if (is_array($cond_val) && count($cond_val) > 0) {

                $cond_val_keys = array_keys($cond_val);
                $first_key = current($cond_val_keys);

                // single value in array wrap,
                // e.g. ['page' => [123], 'site' => ['345']]
                if ($k === 'and' && count($cond_val) >= 1 && $first_key === 0) {
                    $and_cond = [];

                    foreach ($cond_val as $and_cond_val) {
                        $prepared_and_cond_val = $this->prepare_conditions($and_cond_val);
                        if (!empty($prepared_and_cond_val)) {
                            $and_cond[] = $prepared_and_cond_val;
                        }
                    }

                    $cond['$and'] = $and_cond;

                    unset($cond['and']);
                }

                // single value in array wrap,
                // e.g. ['page' => [123], 'site' => ['345']]
                else if (!is_numeric($k) && is_string($k) && count($cond_val) == 1 && $first_key === 0
                    && (is_string(current($cond_val)) || is_numeric(current($cond_val)) || current($cond_val) === null)
                ) {
                    $cond[$k] = is_string(current($cond_val)) ? strval(current($cond_val)) : current($cond_val);
                }

                // Values are arrays with text keys
                // Possible keys: '>', '<', '>=', '<=', '==', '!=', 'not'
                // e.g. ['id' => ['!=' => 121], 'page' => ['>' => 500]]
                else if (!is_numeric($k) && is_string($k) && count($cond_val) == 1 && is_string($first_key)
                    && (is_string(current($cond_val)) || is_numeric(current($cond_val)) || current($cond_val) === null
                        || $this->is_bson(current($cond_val))

                        // Take care: allows first elem of $cond_val as Array
                        || (is_array(current($cond_val)) && array_values(current($cond_val)) == current($cond_val))

                        || (mb_strtolower($first_key) == 'between' && is_array(current($cond_val)) && count(current($cond_val)) == 2)
                    )) {

                    $val = is_string(current($cond_val)) ? strval(current($cond_val)) : current($cond_val);

                    switch (mb_strtolower($first_key)) {
                        case '<':
                            $cond[$k] = ['$lt' => $val];
                            break;
                        case '>':
                            $cond[$k] = ['$gt' => $val];
                            break;
                        case '==':
                            $cond[$k] = ['$eq' => $val];
                            break;
                        case '>=':
                            $cond[$k] = ['$gte' => $val];
                            break;
                        case '<=':
                            $cond[$k] = ['$lte' => $val];
                            break;
                        case '!=':
                        case 'not':
                            if (is_array($val) && array_values($val) === $val) {
                                $cond[$k] = ['$nin' => $val];
                            }
                            else {
                                $cond[$k] = ['$ne' => $val];
                            }
                            break;
                        case 'between':
                            if (!empty($val['0']) && !empty($val['1'])) {
                                $cond[$k] = ['$gte' => $val['0'], '$lte' => $val['1']];
                            }
                            break;
//                        case 'and':
//                            if (!empty($val)) {
//                                $cond[$k] = ['$and' => $this->prepare_conditions($val)];
//                            }
//                            break;
                    }
                }

                // Integer keys of multiple values, so values are in simple array without keys
                // e.g. ['page' => [1234,1235,'1236']]
                // e.g. [... , [['page' => 12345], ['site' => [123,124,125]], ['active' => ['not' => null]]], ... ]
                else if ($cond_val === array_values($cond_val)) {

                    // `IN` conditions
                    if (is_string(current($cond_val)) || is_numeric(current($cond_val)) || current($cond_val) === null
                        || $this->is_bson(current($cond_val))) {

                        foreach ($cond_val as $sub_k => $cv) {
                            if (is_numeric($cv)) {
                                $cond_val[$sub_k] = is_numeric($cv) ? $cv : strval($cv);
                            }
                        }

                        $cond[$k] = ['$in' => $cond_val];
                    }

                    // `OR` conditions
                    // e.g. [... , [['page' => 123], ['page' => 258]], ... ]
                    else if (is_array(current($cond_val))) {

                        $or = [];

                        foreach ($cond_val as $cond_val_arr) {

                            $or_val = $this->prepare_conditions($cond_val_arr, false);

                            if (!empty($or_val)) {
                                $or[] = $or_val;
                            }
                        }

                        if (!empty($or)) {
                            $cond['$or'] = $or;
                        }

                        unset($cond[$k]);
                    }

                    else {
                        unset($cond[$k]);
                    }
                }
                else {
                    unset($cond[$k]);
                }
            }

            // Number in value
            else if (!is_numeric($k) && is_string($k) && is_numeric($cond_val)) {
                $cond[$k] = $cond_val;
            }

            // ObjectId in `_id` parameter
            else if (is_string($k) && $k == '_id' && is_string($cond_val) && mb_strlen($cond_val) == 24) {
                $cond[$k] = new \MongoDB\BSON\ObjectId($cond_val);
            }

            // BSON object in value or NULL
            else if (!is_numeric($k) && is_string($k) &&
                (is_string($cond_val) || $cond_val === null || $this->is_bson($cond_val))) {
                $cond[$k] = $cond_val;
            }

            // Exclude from conditions wrong data
            else {
                unset($cond[$k]);
            }
        }

        return empty($cond) ? [] : empty($merge_and) ? $cond : ['$and' => $cond];
    }

    private function is_bson($obj)
    {
        return $obj instanceof \MongoDB\BSON\Regex
            || $obj instanceof \MongoDB\BSON\ObjectId
            || $obj instanceof \MongoDB\BSON\Timestamp
            || $obj instanceof \MongoDB\BSON\Javascript
            || $obj instanceof \MongoDB\BSON\Binary
            || $obj instanceof \MongoDB\BSON\UTCDateTime;
    }

    /*
     * SELECT method
     *
     * @param array $w `What`
     * @param array $j `Joined tables`
     * @param array $c `Condition` e.g. ["id"=>"12345"] or ["`name` LIKE '%Vasia Pupkin%'", 'city' => ['00017', '152']
     * @param array $g `Group by`
     * @param array $o `Order by`
     * @param array $l `Limit`
     */
    public function select(
        array $w = ['*'],
        array $j = [],
        array $c = [],
        array $g = [],
        array $o = [],
        array $l = [],
        $options = null
    ) {
//        $query = new \MongoDB\Driver\Query([]);
//        $this->query($query);

        if (config::get('db_logs') && php_sapi_name() !== 'cli') { //
            $tmp_debug_arr = ['w' => $w, 'c' => $c, 'o' => $o, 'l' => $l];
        }

        $pipeline = [];

        // Specific fields
        if (!empty($w) && $w !== ['*'] && count($w) > 0) {
            if (array_values($w) === $w) {
                foreach ($w as $k => $_w) {
                    $w[$_w] = 1;
                    unset($w[$k]);
                }
            }
            $pipeline[] = ['$project' => $w];
        }

        // Sorting
        /**
         *   TODO:  In pipeline can be several {$sort: ... }  objects
         *
         */
        if (!empty($o) && $o !== ['id' => 'ASC'] && $o !== [$this->__id => 'ASC']) {

            foreach ($o as $field => $sort) {
                // Can be several sorting fields
                if (is_int($field) && is_array($sort)) {
                    foreach ($sort as $inner_field => $inner_sort) {
                        if (!empty($inner_field) && mb_strtolower($inner_sort, 'UTF-8') === 'desc') {
//                            $sort[$inner_field] = -1;
                            $pipeline[] = ['$sort' => [$inner_field => -1]];
                        }
                        else if (!empty($inner_field) && mb_strtolower($inner_sort, 'UTF-8') === 'asc') {
//                            $sort[$inner_field] = 1;
                            $pipeline[] = ['$sort' => [$inner_field => 1]];
                        }
                    }
                }
                else if (!empty($field) && is_string($sort) && mb_strtolower($sort, 'UTF-8') === 'desc') {
//                    $o[$field] = -1;
                    $pipeline[] = ['$sort' => [$field => -1]];
                }
                else if (!empty($field) && is_string($sort) &&  mb_strtolower($sort, 'UTF-8') === 'asc') {
//                    $o[$field] = 1;
                    $pipeline[] = ['$sort' => [$field => 1]];
                }
//                else {
//                    unset($o[$field]);
//                }
            }

//            if (!empty($o)) {
//                $pipeline[] = ['$sort' => $o];
//            }
        }

        // Conditions
        if (!empty($c)) {
            $c = $this->prepare_conditions($c);

            if (!empty($c)) {
                $pipeline[] = ['$match' => $c];
            }
        }

        // Limit and Skip (pagination)
        if (!empty($l)) {
            if (count($l) == 1 && !empty($l['0'])) {
                $pipeline[] = ['$limit' => (int)$l['0']];
            }
            else if (count($l) == 2 && isset($l['0']) && !empty($l['1'])) {
                $pipeline[] = ['$limit' => (int)$l['1'] + (int)$l['0']];
                $pipeline[] = ['$skip' => (int)$l['0']];
            }
        }



        // TODO: grouping, like on MySQL with multiple results   ($group)

        // TODO: join other collection   ($unwind)





        if (config::get('db_logs') && php_sapi_name() !== 'cli') { //

            $json_pipeline = json_encode(empty($pipeline) ? [] : $pipeline, JSON_UNESCAPED_UNICODE|JSON_NUMERIC_CHECK);

            $json_pipeline = preg_replace('!{\"\$oid\":\"([a-z0-9]{24})\"}!si', 'ObjectId("$1")', $json_pipeline);

            $mongo_query = 'db.' . $this->_table . '.aggregate(';
            $mongo_query .= $json_pipeline;
            $mongo_query .= ').pretty();';

//            registry::set('mongo_query', 'Mongo console: '. $mongo_query . "<br><br>Select query:" .
//                json_encode($tmp_debug_arr, JSON_UNESCAPED_UNICODE|JSON_NUMERIC_CHECK));
            registry::set('mongo_query', $mongo_query);

            unset($tmp_debug_arr);
            unset($mongo_query);
        }

        if (empty($this->result)) {
            $this->result = new \stdClass;
        }

        $aggregate = new \MongoDB\Driver\Command([
            'aggregate' => $this->_table, // $this->db_name . '.' .
            'pipeline' => $pipeline,
            'allowDiskUse' => false,
            'cursor' => $this->result,
        ]);

        $this->command($aggregate);

        unset($aggregate);
        unset($pipeline);
        unset($w);
        unset($c);
        unset($o);
        unset($l);

        return $this;
    }


    public function count($c = [], $j = null, $g = null)
    {
        $condition = $this->prepare_conditions($c);

        if (config::get('db_logs')) {

            $command_log = 'db.' . $this->_table . '.count(';
            $command_log .= json_encode(empty($condition) ? [] : $condition, JSON_UNESCAPED_UNICODE|JSON_NUMERIC_CHECK);
            $command_log .= ');';

            $query_time = microtime(true);
            ob_start();
            debug_print_backtrace(DEBUG_BACKTRACE_IGNORE_ARGS);
            $debug_backtrace = ob_get_contents();
            ob_clean();
        }

        $Result = $this->db->executeCommand(
            $this->db_name,
            new \MongoDB\Driver\Command([
                'count' => $this->_table,
                'query' => $condition
            ])
        );

        if (config::get('db_logs')) {

            $query_time = empty($query_time) ? '' : ' (' . round((microtime(true) - $query_time), 5) . 's)';

            if ((registry::get('is_ajax') || php_sapi_name() == 'cli') && !empty($command_log)) {
                registry::append('db_logs', $command_log);
            }
            else if (!empty($debug_backtrace) && !empty($query_time) && !empty($command_log)) {
                registry::append('db_logs', '<b>' . $command_log . '</b> ' . $query_time . "\n" . $debug_backtrace);
            }
        }

        return (int)current($Result->toArray())->n;
    }

    public function count_distinct($distinct_field, $c = [], $j = null, $g = null)
    {
        $c = $this->prepare_conditions($c);

        $Command = new \MongoDB\Driver\Command(['distinct' => $this->_table, 'key' => $distinct_field, 'query' => $c]);

        $results = $this->db->executeCommand($this->db_name, $Command);

        return empty($results) || !($results instanceof \MongoDB\Driver\Cursor) ? 0 :
            count(current($results->toArray())->values);
    }

    /*
     * a short SELECT method
     *
     * @param string $w `What`
     * @param string $c `Condition`
     * @param string $l `Limit`
     * @param string $o `Order by`
     */
    public function s(array $w, array $c, $l = null, $o = null)
    {
        if (empty($w)) {
            $w = ['*'];
        }

        if (empty($l)) {
            $l = ['1'];
        }
        else if (!is_array($l)) {
            $l = [$l];
        }

//        if (empty($o) && !empty($this->__id)) {
//            $o = [$this->__id => 'ASC'];
//        }
//        else if (!is_array($o)) {
//            $o = [$o];
//        }
        if ($o === null) {
            $o = [];
        }

        return $this->select($w, [], $c, [], $o, $l);
    }

    public function one()
    {
        if (!$this->has_result()) {
            return false;
        }

        $record = (array)current($this->result->toArray());

        if (!empty($record['_id'])) {
            unset($record['_id']);
        }

        $record = current($record);

        return empty($record) ? false : $record;
    }

    public function all($t = 'assoc', $class_name = null)
    {
        if (!$this->has_result() || $this->result == null) {
            return false;
        }

        $result_arr = $this->result->toArray();

        $this->_count = count($result_arr);

        $arr = [];

        switch($t) {
            case 'assoc':
                foreach ($result_arr as $record) {
                    $arr[] = (array)$record;
                    unset($record);
                }
                break;
            case 'object':
                foreach ($result_arr as $record) {

                    $record_vars = get_object_vars($record);

                    $object = new $class_name(null, true);

                    $object->import($record_vars);

                    $object->_count = 1;

                    if (!empty($record_vars) && is_array($record_vars)) {
                        foreach ($record_vars as $record_var => $record_val) {
                            if (method_exists($object, '_override_' . $record_var)) {
                                $object->{'_override_' . $record_var}();
                            }
                        }
                    }

                    if (isset($object->{$object->__id}) && !empty($object->{$object->__id})) {

                        if (is_string($object->{$object->__id})) {
                            $str_id = $object->{$object->__id};
                        }
                        else if (is_object($object->{$object->__id}) && method_exists($object->{$object->__id}, '__toString')) {
                            $str_id = $object->{$object->__id}->__toString();
                        }
                        else {
                            $arr[] = $object;
                            continue;
                        }

                        $arr[$str_id] = $object;
                    }
                    else {
                        $arr[] = $object;
                    }

                    unset($record);
                    unset($object);
                }

                break;
        }

        $this->result = null;
        unset($result_arr);

        return !empty($arr) ? $arr : [];
    }

    public function insert($in = [])
    {
        if (is_object($in)) {
            $in = (array)$in;
        }

        if (empty($in) || !is_array($in)) {
            return false;
        }

        $bulk = new \MongoDB\Driver\BulkWrite;

        if (empty($in['_id'])) {
            $in['_id'] = new \MongoDB\BSON\ObjectId();
        }
        else if (is_string($in['_id']) && preg_match('/^[a-z0-9]{24}$/', $in['_id'])) {
            $in['_id'] = new \MongoDB\BSON\ObjectId($in['_id']);
        }

        if (config::get('db_logs')) {

            $command_log = 'db.' . $this->_table . '.insert(';
            $command_log .= json_encode($in, JSON_UNESCAPED_UNICODE|JSON_NUMERIC_CHECK);
            $command_log .= ');';

            $query_time = microtime(true);
            ob_start();
            debug_print_backtrace(DEBUG_BACKTRACE_IGNORE_ARGS);
            $debug_backtrace = ob_get_contents();
            ob_clean();
        }

        $this->last_id = $in['_id'];

        $bulk->insert($in);

        try {
            $this->db->executeBulkWrite($this->db_name . '.' . $this->_table, $bulk);
            $this->_count = $bulk->count();


            if (config::get('db_logs')) {

                $query_time = empty($query_time) ? '' : ' (' . round((microtime(true) - $query_time), 5) . 's)';

                if ((registry::get('is_ajax') || php_sapi_name() == 'cli') && !empty($command_log)) {
                    registry::append('db_logs', $command_log);
                }
                else if (!empty($debug_backtrace) && !empty($query_time) && !empty($command_log)) {
                    registry::append('db_logs', '<b>' . $command_log . '</b> ' . $query_time . "\n" . $debug_backtrace);
                }
            }

        } catch (\MongoDB\Driver\Exception\BulkWriteException $e) {
            //core::out([$e->getMessage()]);
        }

        return true;
    }

    public function truncate()
    {
        //return $this->command('TRUNCATE TABLE `' . $this->_table . '`');
        return true;
    }

    public function update(array $u = [], $c = [], $o = null, $l = null)
    {
        if (empty($o))
            $o = [];

        if (empty($l))
            $l = [];

        $bulk = new \MongoDB\Driver\BulkWrite;

        $c = $this->prepare_conditions($c);

        if (empty($c)) {
            return false;
        }

        if (!empty($u['_id']) && !($u['_id'] instanceof \MongoDB\BSON\ObjectId)) {
            unset($u['_id']);
        }



        if (config::get('db_logs')) {


            $json_c = json_encode($c, JSON_UNESCAPED_UNICODE|JSON_NUMERIC_CHECK);
            $json_c = preg_replace('!{\"\$oid\":\"([a-z0-9]{24})\"}!si', 'ObjectId("$1")', $json_c);

            $json_u = json_encode($u, JSON_UNESCAPED_UNICODE|JSON_NUMERIC_CHECK);
            $json_u = preg_replace('!{\"\$oid\":\"([a-z0-9]{24})\"}!si', 'ObjectId("$1")', $json_u);

            $command_log = 'db.' . $this->_table . '.update(';
            $command_log .= $json_c;
            $command_log .= ',{"$set":';
            $command_log .= $json_u;
            $command_log .= '});';

            $query_time = microtime(true);
            ob_start();
            debug_print_backtrace(DEBUG_BACKTRACE_IGNORE_ARGS);
            $debug_backtrace = ob_get_contents();
            ob_clean();
        }



        $bulk->update($c, ['$set' => $u]);

        try {
            $this->db->executeBulkWrite($this->db_name . '.' . $this->_table, $bulk);

            $this->_count = $bulk->count();

            if (config::get('db_logs')) {

                $query_time = empty($query_time) ? '' : ' (' . round((microtime(true) - $query_time), 5) . 's)';

                if ((registry::get('is_ajax') || php_sapi_name() == 'cli') && !empty($command_log)) {
                    registry::append('db_logs', $command_log);
                }
                else if (!empty($debug_backtrace) && !empty($query_time) && !empty($command_log)) {
                    registry::append('db_logs', '<b>' . $command_log . '</b> ' . $query_time . "\n" . $debug_backtrace);
                }
            }

        } catch (\MongoDB\Driver\Exception\BulkWriteException $e) {
            //core::out([$e->getMessage()]);
        }

        return true;
    }

    public function delete($c = [], $l = ["1"])
    {
        if (empty($c)) {
            return false;
        }

        $bulk = new \MongoDB\Driver\BulkWrite;

        $c = $this->prepare_conditions($c);

        if (config::get('db_logs')) {

            $json_c = json_encode($c, JSON_UNESCAPED_UNICODE|JSON_NUMERIC_CHECK);
            $json_c = preg_replace('!{\"\$oid\":\"([a-z0-9]{24})\"}!si', 'ObjectId("$1")', $json_c);

            $command_log = 'db.' . $this->_table . '.remove(';
            $command_log .= $json_c;
            $command_log .= ');';

            $query_time = microtime(true);
            ob_start();
            debug_print_backtrace(DEBUG_BACKTRACE_IGNORE_ARGS);
            $debug_backtrace = ob_get_contents();
            ob_clean();
        }

        if (empty($c)) {
            return false;
        }

        $bulk->delete($c);

        try {
            $this->db->executeBulkWrite($this->db_name . '.' . $this->_table, $bulk);
            $this->_count = $bulk->count();

            if (config::get('db_logs')) {

                $query_time = empty($query_time) ? '' : ' (' . round((microtime(true) - $query_time), 5) . 's)';

                if ((registry::get('is_ajax') || php_sapi_name() == 'cli') && !empty($command_log)) {
                    registry::append('db_logs', $command_log);
                }
                else if (!empty($debug_backtrace) && !empty($query_time) && !empty($command_log)) {
                    registry::append('db_logs', '<b>' . $command_log . '</b> ' . $query_time . "\n" . $debug_backtrace);
                }
            }

        } catch (\MongoDB\Driver\Exception\BulkWriteException $e) {

        }

        return true;
    }

    public function delete_by_ids(array $ids)
    {
        $bulk = new \MongoDB\Driver\BulkWrite;

        $in['_id'] = new \MongoDB\BSON\ObjectId();
        $bulk->delete([$this->__id => $ids]);

        try {
            $this->db->executeBulkWrite($this->db_name . '.' . $this->_table, $bulk);
            $this->_count = $bulk->count();
        } catch (\MongoDB\Driver\Exception\BulkWriteException $e) {

        }

        return true;
    }

    public function modify_table($fields)
    {
        return true;

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

            $this->command($q);
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

            $this->command($q);
        }

        return $this->error() ? $this->error . "<br>\n\n" . $q : true;
    }

    public function build_table()
    {
        return true;

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

        $this->command($q);

        return $this->error() ? $this->error . "<br>\n\n" . $q : true;
    }

    public function disconnect()
    {
        if (empty($this->db))
            return false;

//        return mysqli_close($this->db);
    }

    public function get_result()
    {
        return $this->result;
    }
}
