<?php

namespace modules\db\libraries;

use m\core;
use m\custom_exception;
use m\registry;
use m\db;
use m\config;
use m\i18n;

abstract class sql
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

    private function __clone() {}

    /**
     * Will be rewritten in child classes
     *
     * @param $sql - string
     * @return bool
     */
    public function query($sql)
    {
        return false;
    }

    private function sql_between($k, array $arr)
    {
        $arr_val = array_slice($arr['between'], 0, 2);

        if (!is_numeric($arr_val['0']) && !is_numeric($arr_val['1'])) {
            $arr_val['0'] = "'" . $arr_val['0'] . "'";
            $arr_val['1'] = "'" . $arr_val['1'] . "'";
        }

        return $k . ' BETWEEN ' . $arr_val['0'] . ' AND ' . $arr_val['1'];
    }

    private function sql_not($k, $arr)
    {
        if (strrpos($k, '.') === false) {
            $k = '`' . $k . '`';
        }

        $not_array = [];

        if (!(current($arr) === null) && !(current($arr) === 'null') && !(current($arr) === 'NULL')
            && is_array($arr) && count($arr) == count($arr, COUNT_RECURSIVE)) {

            $not_array[] = $this->sql_not_in($k, $arr);
        }
        else {
            $arr = current($arr) === null ? [null] : (array)current($arr);

            foreach ($arr as $not_value) {
                $not_array[] = $k . ($not_value === null ? ' IS NOT NULL' : "<>'" . $not_value . "'");
            }
        }

        return implode(' AND ', $not_array);
    }

    private function sql_equal($k, array $arr)
    {
        $equal_values = (array)current($arr);
        reset($arr);
        $equal_sign = key($arr);

        $equal_array = [];

        if (strrpos($k, '.') === false) {
            $k = '`' . $k . '`';
        }

        foreach ($equal_values as $equal_value) {
            $equal_array[] = $k . $equal_sign . "'" . $equal_value . "'";
        }

        return implode(' AND ', $equal_array);
    }

    private function sql_in($field, array $arr)
    {
        foreach ($arr as &$val) {
            if (is_string($val) && !is_numeric($val)) {
                $val = "'" . addslashes($val) . "'";
            }
        }

        return $field . ' IN (' . implode(',', $arr) . ')';
    }

    private function sql_not_in($field, array $arr)
    {
        foreach ($arr as &$val) {
            if (is_string($val) && !is_numeric($val)) {
                $val = "'" . addslashes($val) . "'";
            }
        }

        return $field . ' NOT IN (' . implode(',', $arr) . ')';
    }

    private function sql_conditions(array $c)
    {
        $conditions = [];

        foreach ($c as $k => $condition) {

            if (is_integer($k) && is_string($condition)) {
                $conditions[] = '(' . $condition . ')';
            }
            /**
             * >, >=, <, <=
             */
            else if (is_string($k) && is_array($condition) && count($condition) == 1
                && (isset($condition['>']) || isset($condition['>=']) || isset($condition['<']) || isset($condition['<=']))) {

                $conditions[] = $this->sql_equal($k, $condition);
            }
            /**
             * Not equal
             */
            else if (is_string($k) && is_array($condition) && count($condition) == 1
                && (array_key_exists('not', $condition) || array_key_exists('!=', $condition))) {

                $conditions[] = $this->sql_not($k, $condition);
            }
            /**
             * Between
             */
            else if (is_string($k) && is_array($condition) && count($condition) == 1 && !empty($condition['between'])
                && is_array($condition['between'])) {

                $conditions[] = $this->sql_between($k, $condition);
            }

            /**
             * structure OR in conditions if an array of conditions set
             */
            else if (is_integer($k) && is_array($condition)) {

                $or_conditions = [];

                foreach ($condition as $condition_key => $or_values) {

                    /**
                     * >, >=, <, <=
                     *
                     * 'field' => ['>' => 2]
                     * 'field' => ['<=' => ['value1', 'value2', 'value3']]
                     */
                    if (is_string($condition_key) && is_array($or_values) && count($or_values) == 1
                        && (isset($or_values['>']) || isset($or_values['>=']) || isset($or_values['<']) || isset($or_values['<=']))) {

                        $or_conditions[] = $this->sql_equal($condition_key, $or_values);
                    }
                    /**
                     * Not equal
                     *
                     * 'field' => ['not' => 2]
                     * 'field' => ['not' => null]
                     * 'field' => ['!=' => ['value1', 'value2', 'value3']]
                     */
                    else if (is_string($condition_key) && is_array($or_values) && count($or_values) == 1
                        && (isset($or_values['not']) || isset($or_values['!=']))) {

                        $or_conditions[] = $this->sql_not($condition_key, $or_values);
                    }
                    /**
                     * Between
                     */
                    else if (is_string($condition_key) && is_array($or_values) && count($or_values) == 1
                        && !empty($or_values['between']) && is_array($or_values['between'])) {

                        $or_conditions[] = $this->sql_between($condition_key, $or_values);
                    }
                    /**
                     * Several arrays in place where must be associative array - OR conditions
                     */
                    else if (is_integer($condition_key) && is_array($or_values)) {

                        $or_values_sql = $this->sql_conditions($or_values);

                        if (!empty($or_values_sql)) {

                            if (is_array($or_values_sql)) {
                                $or_values_sql = implode(' AND ', $or_values_sql);
                            }

                            $or_conditions[] = '(' . $or_values_sql . ')';
                        }
                    }
                    /**
                     *
                     */
                    else if (is_string($condition_key) && !is_array($or_values)) {

                        if (is_string($condition_key) && strrpos($condition_key, '.') === false) {
                            $condition_key = '`' . $condition_key . '`';
                        }

                        if (is_integer($or_values) || is_float($or_values)) {
                            $or_conditions[] = $condition_key . '=' . $or_values;
                        } else if (is_string($or_values)) {
                            $or_conditions[] = $condition_key . "='" . $or_values . "'";
                        }
                    }
                }

                if (!empty($or_conditions)) {
                    $conditions[] = "(" . implode(" OR ", $or_conditions) . ")";
                }
            }
            /**
             * structure IN() in conditions
             * An array should be scalar w/o included arrays
             */
            else if (is_string($k) && is_array($condition) && count($condition) == count($condition, COUNT_RECURSIVE)) {

                $k = is_string($k) && strrpos($k, '.') === false ? '`' . $k . '`' : $k;
                $conditions[] = $this->sql_in($k, $condition);
            }
            else if ($condition === null) {

                $k = is_string($k) && strrpos($k, '.') === false ? '`' . $k . '`' : $k;
                $conditions[] = $k . " IS NULL";
            }
            else {

                $k = is_string($k) && strrpos($k, '.') === false ? '`' . $k . '`' : $k;

                if (is_string($condition) && strpos($condition, '`') === false && strpos($condition, $this->_table . '.') === false && strpos($condition, '`' . $this->_table . '`.') === false) {
                    $condition = "'" . $condition. "'";
                }

                $conditions[] = $k . "=" . (string)$condition . "";
            }
        }

        return implode(' AND ', $conditions);
    }

    public function join_type($new_type) {
        if (!empty($new_type) && in_array(mb_strtoupper($new_type, 'UTF-8'), ['INNER', 'LEFT', 'RIGHT', 'FULL OUTER'])) {
            $this->join_type = mb_strtoupper($new_type, 'UTF-8');
        }
        return $this;
    }

    /**
     * Most important method that builds an SQL query string from given conditions arrays
     *
     * @param array $w
     * @param array $j
     * @param array $c
     * @param array $g
     * @param array $o
     * @param array $l
     * @param null $options
     * @return string
     */
    private function build_select_query(
        $w = ['*'],
        $j = [],
        $c = [],
        $g = [],
        $o = [],
        $l = ['0', '1000'],
        $options = null
    ) {

        $having_keys = [];

        if (!empty($g)) {
            foreach ($g as &$group_by) {
                if (strpos($group_by, '.') === false && strpos($group_by, '`') === false)
                    $group_by = '`' . $this->_table . '`.' . $group_by;
            }
        }

        /**
         * Several years ago tried to make a join query via separated method (like in Yii2) but it's not used
         */
        if (!empty($this->_join) && (!empty($options) && empty($options['no_join']))) {
            foreach ($this->_join as $join_arr) {

                if (!empty($join_arr['select']) && !empty($join_arr['table']))
                    foreach ($join_arr['select'] as $k => $v) {
                        if (strpos($v, '`') === false && is_int($k)) {
                            $w[] = '`' . $join_arr['table'] . '`.`' . $v . '`';
                        }
                        else if (strpos($v, '`') === false && !is_int($k)) {
                            $w[] = '`' . $join_arr['table'] . '`.`' . $k . '` AS `' . $v . '`';
                            $having_keys[] = $v;
                        }
                        else {
                            $w[] = $v;
                        }
                    }

                if (!empty($join_arr['side']) && !empty($join_arr['on'])) {
                    $join_str = mb_strtoupper($join_arr['side']) . ' JOIN `' . $join_arr['table'] . '` ON ';

                    $_join_arr = [];
                    foreach ($join_arr['on'] as $k => $v) {

                        if (is_int($k)) {
                            $_join_arr[] = $v;
                        }
                        else if (is_integer($v)) {
                            $_join_arr[] = '`'. $join_arr['table'] . '`.`' . $k . '`' . '=' . $v;
                        }
                        else if (is_string($v)) {
                            // TODO: check is present $v in joined table fields
                            $_join_arr[] = '`'. $join_arr['table'] . '`.`' . $k . '`' . '=`' . $this->_table . '`.`' . $v . '`';
                        }
                    }
                    $j[] = $join_str . implode(" AND ", $_join_arr);
                }

                if (!empty($join_arr['condition']))
                    foreach ($join_arr['condition'] as $k => $v) {

                        if (is_string($v) || is_numeric($v)) {
                            $c[] = '`' . $join_arr['table'] . '`.`' . $k . "`='" . $v . "'";
                        }
                        else if (is_array($v)) {
                            $c[] = $this->sql_in('`' . $join_arr['table'] . '`.`' . $k . '`', $v);
                        }
                    }

                if (!empty($join_arr['group']))
                    foreach ($join_arr['group'] as $v) {

                        if (strpos($v, '`') === false)
                            $g[] = '`' . $join_arr['table'] . '`.`' . $v . '`';
                        else
                            $g[] = $v;
                    }

                if (!empty($join_arr['order']))
                    foreach ($join_arr['order'] as $v) {
                        $o[] = '`' . $join_arr['table'] . '`.`' . $v . '`';
                    }

                if (!empty($join_arr['limit']))
                    $l = $join_arr['limit'];
            }
        }
        else {
            $tmp_w = [];
            if (count(array_filter(array_keys($w), 'is_string')) > 0) {
                foreach ($w as $w_k => $w_v) {
                    if (is_integer($w_k) && is_string($w_v) && mb_strpos($w_v, '*') !== false) {
                        $tmp_w[] = $w_v;
                    }
                    else if (is_integer($w_k) && is_string($w_v) && !in_array($w_v, ['*'])) {
                        $tmp_w[] = '`' . $w_v . '`';
                    }
                    else if ($w_v == '*') {
                        $tmp_w[] = '' . $w_k . '.*';
                    }
                    else {
                        $tmp_w[] = '' . $w_k . ' AS `' . $w_v . '`';
                        $having_keys[] = $w_v;
                    }
                }
                $w = $tmp_w;
            }
        }

        $query = "SELECT " . implode(",", $w) . " FROM `" . $this->_table . "`";

        /**
         * Allows to use rich syntax for JOIN queries:
         * array('joined_table' => array('joined_field' => 'original_table_field', ...), 'type' => 'left|RiGht|INNER'
         */
        if (!empty($j) && is_array($j)) {

            /**
             * If there are no dots in joined fields, add tables names
             */
            foreach ($j as $joined_table => $join_rules) {

                if (array_keys($join_rules) !== range(0, count($join_rules) - 1)) {
                    foreach ($join_rules as $joined_table_k => $dst_table_k) {
                        if (strpos($joined_table_k, '.') === false && strpos($dst_table_k, '.') === false) {
                            unset($join_rules[$joined_table_k]);
                            $join_rules[$joined_table . '.' . $joined_table_k] = '`' . $this->_table . '`.' . $dst_table_k;
                        }
                    }
                }

                $query .= ' ' . $this->join_type . ' JOIN `' . $joined_table . '` ON ' . $this->sql_conditions($join_rules);
            }
        }


        if (!empty($c) && is_array($c)) {

            $having_conditions = [];

            if (!empty($having_keys)) {

                foreach ($having_keys as $having_key) {
                    if (!empty($c[$having_key])) {
                        $having_conditions[$having_key] = $c[$having_key];
                        unset($c[$having_key]);
                    }
                }
            }

            $conditions = $this->sql_conditions($c);

            if (!empty($conditions)) {
                $query .= " WHERE " . $conditions;
            }

            if (!empty($having_conditions)) {
                $having_conditions = $this->sql_conditions($having_conditions);
            }

            if (!empty($having_conditions)) {
                $query .= " HAVING " . $having_conditions;
            }
        }

        if (!empty($g)) {
            $query .= " GROUP BY " . implode(",", $g);
        }

        if (!empty($o)) {

            $order = [];

            if (is_array($o)) {
                foreach ($o as $k => $v) {
                    if (!is_numeric($k) && is_string($k) && is_string($v) && in_array(mb_strtolower($v, 'UTF-8'), ['desc', 'asc'])) {
                        $order[] = $k . ' ' . $v;
                    }

                    if (!is_numeric($k) && is_string($k) && is_string($v) && substr_count($v, ',') >= 1) {
                        $order[] = "FIND_IN_SET(" . $k . ", '" . addslashes($v) . "')";
                    }
                    else if (!is_numeric($k) && is_string($k) && is_array($v)) {
                        $order[] = "FIND_IN_SET(" . $k . ", '" . implode(',', $v) . "')";
                    }

                    if (empty($order) && is_numeric($k) && !empty($v) && is_string($v) && mb_strtolower($v, 'UTF-8') == 'rand()') {
                        $order[] = $v;
                    }
                }
            }

            if (!empty($order)) {
                $query .= " ORDER BY " . implode(",", $order);
            }
        }

        if (!empty($l) && is_array($l) && count($l) <= 2) {
            $query .= " LIMIT " . implode(",", $l);
        }

        return $query;
    }

    /**
     * Count records by given conditions (with join of tables)
     *
     * @param array $c - conditions
     * @param array $j - join tables and conditions (not required)
     * @param array $g - group by (not required)
     * @return int
     */
    public function count(array $c = [], array $j = null, array $g = null)
    {
        $query = $this->query($this->build_select_query(['COUNT(*) AS count'], $j, $c, $g, [], []));
        return empty($query) ? 0 : (int)$query->one();
    }

    /**
     * Sum of records by field and given conditions (with join of tables)
     *
     * @param string $field
     * @param array $c - conditions
     * @param array $j - join tables and conditions (not required)
     * @param array $g - group by (not required)
     * @return float
     */
    public function sum($field, array $c = [], array $j = null, array $g = null)
    {
        $query = $this->query($this->build_select_query(['SUM(`'.$field.'`) AS sum'], $j, $c, $g, [], []));
        return empty($query) ? 0 : (float)$query->one();
    }

    /**
     * Count distinct records by field and given conditions (with join of tables)
     *
     * @param string $field
     * @param array $c - conditions
     * @param array $j - join tables and conditions (not required)
     * @param array $g - group by (not required)
     * @return int
     */
    public function count_distinct($field, array $c = [], array $j = null, array $g = null)
    {
        $query =
            $this->query($this->build_select_query(['COUNT(DISTINCT ' . $field . ') AS count'], $j, $c, $g, [], []));
        return empty($query) ? 0 : (int)$query->one();
    }

    /**
     * Selects records from current model table
     *
     * @param array $w `What`
     * @param array $j `Joined tables`
     * @param array $c `Condition` e.g. ["id"=>"12345"] or ["`name` LIKE '%Ivan Mykolaichuk%'", 'city' => ['00017', '152']
     * @param array $g `Group by`
     * @param array $o `Order by`
     * @param array $l `Limit`
     * @param null $options (not required)
     * @return $this
     */
    public function select(
        array $w = ['*'],
        array $j = [],
        array $c = [],
        array $g = [],
        array $o = [],
        array $l = ['0', '1000'],
        $options = null
    ) {
        if (empty($o) && $o === [] && !empty($this->__id)) {
            $o = [$this->__id => 'ASC'];
        }

        $this->query($this->build_select_query($w, $j, $c, $g, $o, $l, $options));
        return $this;
    }

    /**
     * A short SELECT method
     *
     * @param array $w `What`
     * @param array $c `Condition`
     * @param array $l `Limit` (not required)
     * @param array $o `Order by` (not required)
     * @return $this
     */
    public function s(array $w, array $c, array $l = null, array $o = null)
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

        if (empty($o) && !empty($this->__id)) {
            $o = [$this->__id => 'ASC'];
        }
        else if (!is_array($o) && is_string($o)) {
            $o = [$o];
        }

        return $this->select($w, [], $c, [], $o, $l);
    }


    /**
     * Insert method
     *
     * @param array $in
     * @return mixed (true, false, error text)
     */
    public function insert(array $in = [])
    {
        if (is_object($in)) {
            $in = (array)$in;
        }

        if (empty($in) || !is_array($in)) {
            return false;
        }

        foreach ($in as $k => &$val) {

            if (!is_string($val) && !is_numeric($val)) {
                unset($in[$k]);
                continue;
            }

            $val = (string)$val;

            /**
             * An exception when in data there is a backlashes, e.g. classes paths in table `pages_types_modules`
             */
            if (strpos($val, '\\') !== false && strpos($val, '\\\\') === false) {
                $val = addslashes($val);
            }

            $val = (!empty($val) && $val !== 'null') || $val == '0' ? "'" . addslashes(stripslashes($val)) . "'" : 'NULL';
        }

        $keys = array_keys($in);

        $q = 'INSERT INTO `' . $this->_table . '` ';

        foreach ($keys as $i => $key) {
            if (is_integer($key)) {
                unset($keys[$i]);
            }
        }

        if (!empty($keys) && (count($keys) > 1 || !is_integer($keys['0']))) {

            $q .= '(`' . implode('`,`', array_keys($in)) . '`)';
        }

        $q .= ' VALUES (' . implode(',', $in) . ')';

        $this->query($q);

        return $this->error() ? $this->error : true;
    }

    /**
     * Truncate method for current model table
     *
     * @return bool
     */
    public function truncate()
    {
        return $this->query('TRUNCATE TABLE `' . $this->_table . '`');
    }

    /**
     * Update method for current model table
     *
     * @param array $u - new data that will be set in found record
     * @param array $c - conditions
     * @param array $o - order (not required)
     * @param array $l - limit (not required)
     * @return bool
     */
    public function update(array $u = [], array $c = [], array $o = null, array $l = null)
    {
        if (empty($o))
            $o = [];

        if (empty($l))
            $l = [];

        $q = "UPDATE `".$this->_table."`";

        if (empty($u) || empty($c)) {
            return false;
        }
        $keys = array_keys($u);
        $values = array_values($u);

        $set = [];
        foreach ($keys as $i => $k) {
            $set[] = "`".$k."`=".($values[$i] === 'null' || $values[$i] === null ? 'NULL' : "'".$values[$i]."'") ."";
        }

        $q .= " SET ".implode(", ", $set);

        if (!empty($c) && is_array($c)) {

            $conditions = $this->sql_conditions($c);

            if (!empty($conditions)) {
                $q .= " WHERE " . $conditions;
            }
        }

        if (!empty($o))
            $q .= " ORDER BY ".implode(",", $o);

        if (!empty($l) && is_array($l) && count($l)<=2)
            $q .= " LIMIT ".implode(",", $l);

        return $this->query($q);
    }

    /**
     * Delete method for current model table
     *
     * @param array $c - conditions
     * @param array $l - limit, default: 1
     * @return bool
     */
    public function delete(array $c = [], array $l = [1])
    {
        $q = "DELETE FROM `".$this->_table."`";

        if (empty($c)) {
            return false;
        }

        if (!empty($c) && is_array($c)) {

            $conditions = $this->sql_conditions($c);

            if (!empty($conditions)) {
                $q .= " WHERE " . $conditions;
            }
        }

        if (!empty($l) && is_array($l) && count($l)<=2)
            $q .= " LIMIT ".implode(",", $l);

        $this->query($q);

        return $this->error() ? $this->error : true;
    }

    /**
     * Compact method for remove records by scalar array of given ids
     *
     * @param array $ids
     * @return bool
     */
    public function delete_by_ids(array $ids = [])
    {
        return empty($ids) ? false :
            $this->delete(["`".$this->__id."` IN (".implode(',', (array)$ids).")"], [count($ids)]);
    }
}
