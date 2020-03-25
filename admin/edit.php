<?php

namespace modules\db\admin;

use libraries\helper\url;
use m\core;
use m\module;
use m\i18n;
use m\registry;
use m\view;
use m\config;
use m\model;
use m\form;
use modules\articles\models\articles;
use modules\db\models\db_config;
use modules\pages\models\pages;
use modules\users\models\users;
use modules\users\models\users_info;

class edit extends module {

    public function _init()
    {
        if (!isset($this->view->{'db_' . $this->name . '_form'})) {
            return false;
        }

        $model = new db_config();

        if (!empty($this->get->edit) && config::has('db') && !empty(config::get('db')[$this->get->edit])) {
            $model->import(config::get('db')[$this->get->edit]);
            $model->type = $this->get->edit;
        }

        if (!empty($this->post) && count((array)$this->post) > 0) {

            $config_db = config::get_with_default('db', []);

            $model->import($this->post);

            foreach ($model->get_fields() as $field => $type) {
                if (isset($this->post->$field)) {
                    echo $this->post->$field . "<br>";
                    $config_db[$this->post->type][$field] = $this->post->$field;
                }
            }

//            core::out($config_db);

            $conf_path = config::get('root_path') . '/' . $this->site->host . '.php';

            if (is_file($conf_path)) {
                $conf_arr = require($conf_path);

                if (!empty($conf_arr)) {
                    $conf_db = empty($conf_arr['db']) ? [] : $conf_arr['db'];
                    $conf_arr['db'] = array_merge($conf_db, $config_db);
                }

//                core::out([var_export($conf_arr, true)]);

                file_put_contents($conf_path, '<?php' . "\n" . 'return ' . var_export($conf_arr, true) . ";\n");

                $this->redirect(url::to('/admin/db'));
            }
        }

        $types = db_config::$types;

        $types_arr = [];

        foreach ($types as $type => $type_name) {
            $types_arr[] = [
                'value' => $type,
                'name' => $type_name,
            ];
        }

        new form(
            $model,
            [
                'type' => [
                    'field_name' => i18n::get('Type'),
                    'related' => $types_arr,
                    'required' => true,
                ],
                'host' => [
                    'type' => 'varchar',
                    'field_name' => i18n::get('Host'),
                    'required' => true,
                ],
                'port' => [
                    'type' => 'int',
                    'field_name' => i18n::get('Port'),
                ],
                'db_name' => [
                    'type' => 'varchar',
                    'field_name' => i18n::get('DB name'),
                    'required' => true,
                ],
                'user' => [
                    'type' => 'varchar',
                    'field_name' => i18n::get('DB user'),
                    'required' => true,
                ],
                'password' => [
                    'type' => 'varchar',
                    'field_name' => i18n::get('DB password'),
                    'required' => true,
                ],
                'encoding' => [
                    'type' => 'varchar',
                    'field_name' => i18n::get('DB encoding/charset/collation'),
                ],
            ],
            [
                'form' => $this->view->{'db_' . $this->name . '_form'},
                'varchar' => $this->view->edit_row_varchar,
                'int' => $this->view->edit_row_int,
                'password' => $this->view->edit_row_password,
                'related' => $this->view->edit_row_related,
                'saved' => $this->view->edit_row_saved,
                'error' => $this->view->edit_row_error,
            ]
        );
    }
}