<?php

namespace modules\db\admin;

use m\config;
use m\core;
use m\module;

class delete extends module {

    public function _init()
    {
        if (empty($this->get->delete)) {
            $this->redirect($this->previous);
        }

        $conf_path = config::get('root_path') . '/' . $this->site->host . '.php';

        if (is_file($conf_path)) {

            $conf_arr = require($conf_path);

            if (!empty($conf_arr) && isset($conf_arr['db'][$this->get->delete])) {

                unset($conf_arr['db'][$this->get->delete]);

                file_put_contents($conf_path, '<?php' . "\n" . 'return ' . var_export($conf_arr, true) . ";\n");
            }

            $this->redirect($this->previous);
        }
    }
}