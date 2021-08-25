<?php

error_reporting(E_ALL);
ini_set('display_errors', 1);

require_once realpath(__DIR__ . '/..') . '/vendor/autoload.php';

$globalMySqlCredentials = [
    'host'            => getenv('TEST_MYSQL_HOST'),
    'username'        => getenv('TEST_MYSQL_USERNAME'),
    'password'        => getenv('TEST_MYSQL_PASSWORD'),
    'port'            => (int)getenv('TEST_MYSQL_PORT'),
    'tempTableSchema' => getenv('TEST_MYSQL_TEMP_TABLE_SCHEMA'),
];

$globalClickhouseCredentials = [
    'host'     => getenv('TEST_CLICKHOUSE_HOST'),
    'username' => getenv('TEST_CLICKHOUSE_USERNAME'),
    'password' => getenv('TEST_CLICKHOUSE_PASSWORD'),
    'port'     => (int)getenv('TEST_CLICKHOUSE_PORT'),
    'protocol' => getenv('TEST_CLICKHOUSE_PROTOCOL'),
];
