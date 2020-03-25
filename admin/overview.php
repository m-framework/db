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

class overview extends module {

    public function _init()
    {
        $items = [];

        if (config::has('db')) {
            $db = config::get('db');

            if (!empty($db) && is_array($db)) {
                foreach ($db as $type => $db_row) {
                    $items[] = $this->view->overview_item->prepare([
                        'type' => $type,
                        'host' => empty($db_row['host']) ? null : $db_row['host'],
                        'db_name' => empty($db_row['db_name']) ? null : $db_row['db_name'],
                        'user' => empty($db_row['user']) ? null : $db_row['user'],
                    ]);
                }
            }
        }

        view::set('content', $this->view->overview->prepare([
            'items' => implode('', $items),
        ]));
    }
}
