<?php

namespace modules\db\admin;

use libraries\helper\html;
use m\core;
use m\functions;
use m\module;
use m\registry;
use m\view;
use m\i18n;
use m\config;
use modules\admin\admin\overview_data;
use modules\articles\models\articles;
use modules\pages\models\pages;

class export extends module {

    public function _init()
    {
//        error_reporting(E_ALL);
//        ini_set('display_errors', 1);
        ini_set('memory_limit', '4G');

        if (empty($this->get->export)) {
            $this->redirect($this->previous);
        }

        $modules_path = config::get('root_path') . '/m-framework/modules';
        $application_modules_path = config::get('root_path') . config::get('application_path') . 'modules';

        $modules_models = $this->process_path($modules_path);

        $json = json_encode($this->utf8ize($modules_models), JSON_NUMERIC_CHECK | JSON_UNESCAPED_UNICODE);

        header("Content-type: application/json; charset=utf-8");

        header('Pragma: no-cache');
        header('Content-Description: File Download');
        header('Content-disposition: attachment; filename="db.' . $this->site->host . '.' . $this->get->export . '.' . date('Y-m-d') . '.json"');
        header('Content-Transfer-Encoding: binary');
//        header('Content-Length: ' . mb_strlen($json, 'UTF-8'));

        if ($json)
            echo $json;
        else
            echo json_last_error_msg();

        die;
    }

    private function utf8ize( $mixed ) {
        if (is_array($mixed)) {
            foreach ($mixed as $key => $value) {
                $mixed[$key] = $this->utf8ize($value);
            }
        } elseif (is_string($mixed)) {
            return mb_convert_encoding($mixed, "UTF-8", "UTF-8");
        }
        return $mixed;
    }

    private function process_path($path)
    {
        if (!is_dir($path)) {
            return [];
        }

        $models = [];

        $modules = array_diff(scandir($path), ['.', '..']);

        if (empty($modules)) {
            return [];
        }

        foreach ($modules as $module) {

            if (!is_dir($path . '/' . $module . '/models')) {
                continue;
            }

            $module_models = array_diff(scandir($path . '/' . $module . '/models'), ['.', '..']);

            if (empty($module_models)) {
                continue;
            }

            foreach ($module_models as $model) {

                if (substr($model, -4) !== '.php' || !is_file($path . '/' . $module . '/models/' . $model)) {
                    continue;
                }

                $model = 'modules\\' . $module . '\\models\\' . substr($model, 0, -4);

                //echo $model . '::$fields' . "<br>\n";

                if (class_exists($model)) {

                    $model_extension = new $model;

                    $data = [];

                    $records_count = $model_extension->count();

                    if ($records_count > 0) {
                        $chunks = ceil($records_count / 1000);

                        for ($n = 0; $n < $chunks; $n++) {
                            $records = $model_extension->s([], [], 1000)->all();

                            if (!empty($records)) {
                                $data = array_merge($data, $records);
                            }
                        }

                        $models[$model] = $data;

//                        $models[$model] = [
////                        'table_name' => $model_extension->_table,
////                        'fields' => $model_extension->fields,
////                        'count' => $records_count,
//                            'data' => $data,
//                        ];
                    }

                }
            }
        }

        return $models;
    }
}
