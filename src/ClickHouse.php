<?php
declare(strict_types=1);

namespace Divae\DbConnectors;

use Divae\DbConnectors\Schema\TableCollection;
use Divae\DbConnectors\Testing\TableHandler;
use RuntimeException;
use stdClass;

/**
 * Class for accessing Clickhouse Databases
 */
class ClickHouse extends AbstractConnector implements DatabaseSchemaAware
{

    private const DB_DEFAULT_PORT_HTTP = 8443;
    private const DB_DEFAULT_PORT_CMD = 9000;
    private const DB_DEFAULT_PROTOCOL_HTTP = 'http';

    private const DB_RETURN_FORMAT_JSON_COMPACT = 'JSONCompact';

    public const CREDENTIALS_PASSWORD = 'password';

    private const SESSION_ID_PREFIX = 'chs';

    /**
     * The maximum number of threads to execute the request. This value is passed to the clickhouse-client application
     *
     * @var int
     */
    protected int $maxThreadsClient = 8;

    /**
     * ClickHouse Session ID
     *
     * "You can use any string as the session ID. By default, the session is terminated after 60 seconds of inactivity.
     * [...] Only one query at a time can be executed within a single session."
     *
     * @var null|string
     */
    protected ?string $sessionId = null;

    /**
     * @inheritdoc
     */
    public function __debugInfo(): array
    {
        $debugInfo = parent::__debugInfo();
        $debugInfo['properties']['sessionId'] = $this->sessionId;

        return $debugInfo;
    }

    /**
     * Sets the MaxThreadsClient Value
     *
     * @param int $maxThreadsClient
     */
    public function setMaxThreadsClient(int $maxThreadsClient): void
    {
        if ($maxThreadsClient > 0 and $maxThreadsClient <= 64) {
            $this->maxThreadsClient = $maxThreadsClient;
        }
    }

    /**
     * Connects to ClickHouse server (no implementation needed, as no permanent connection)
     */
    public function connect(): void
    {
        if ($this->counters[self::COUNTER_CREATION_TIME] == 0) {
            $this->counters[self::COUNTER_CREATION_TIME] = microtime(true);
        }
    }

    /**
     * @inheritdoc
     */
    public function close(): void
    {
    }

    /**
     * READ function: Executes given statement using specified options
     *
     * @param       $statement
     * @param array $options
     *
     * @return stdClass
     * @throws DatabaseException
     */
    public function query(
        $statement,
        array $options = [
            self::OPTION_ASYNC => false,
        ]
    ): stdClass {
        $this->counters['total_queries']++;
        $this->counters['read']++;

        $startTime = microtime(true);
        $raw_result = $this->sendPlainQueryToServer($statement, self::DB_RETURN_FORMAT_JSON_COMPACT);

        $result_arr = json_decode($raw_result, true);
        if ($result_arr === null) {
            throw new DatabaseException('Could not decode result: ' . json_last_error_msg());
        }

        $this->counters['rows_read'] += $result_arr['rows'];
        $this->counters['total_execution_time'] += (microtime(true) - $startTime);

        $result = new stdClass();
        $result->pointer = 0;
        $result->content = $result_arr;


        return $result;
    }

    /**
     * Gets status of the last result
     *
     * @return string pending|ready|error|unknown
     * @throws DatabaseException
     */
    public function checkResultReady(): string
    {
        throw new DatabaseException('not implemented');
    }

    /**
     * Fetches result from the last asynchronous query
     *
     * @return void
     * @throws DatabaseException
     */
    public function getAsyncResult()
    {
        throw new DatabaseException('not implemented');
    }

    /**
     * WRITE function: Executes given statement using specified options
     *
     * @param       $statement
     * @param array $options
     *
     * @return int
     * @throws DatabaseException
     */
    public function exec(
        $statement,
        array $options = [
            self::OPTION_ASYNC => false,
        ]
    ): int {
        $this->counters['total_queries']++;
        $this->counters['write']++;

        $startTime = microtime(true);
        $this->sendPlainQueryToServer($statement);
        $this->counters['total_execution_time'] += (microtime(true) - $startTime);

        // right now there is no counter for written lines
        return 0;
    }

    /**
     * Executes a statement via clickhouse-client, used e.g. for SELECT into outfile
     *
     * @param string $statement
     * @param null|string $inputFile
     * @param string|null $cmdPrefix
     *
     * @return int
     */
    public function exec_client(string $statement, ?string $inputFile = null, string $cmdPrefix = null): int
    {
        $exitCode = 0;
        if ($statement) {
            $statement = TableHandler::parseStatement(static::class, $statement);

            $hostname = $this->credentials[self::CREDENTIALS_HOSTNAME] ?? null;
            $port = $this->settings['cmd_port'] ?? self::DB_DEFAULT_PORT_CMD;

            $this->logger->debug('Executing with clickhouse-client: ' . $statement);
            $_exec = 'clickhouse-client'
                . " --host={$hostname} --port {$port}"
                . ($this->credentials[self::CREDENTIALS_USERNAME] ? ' --user ' . escapeshellcmd($this->credentials[self::CREDENTIALS_USERNAME]) : '')
                . ($this->credentials[self::CREDENTIALS_PASSWORD] ? ' --password ' . escapeshellcmd($this->credentials[self::CREDENTIALS_PASSWORD]) : '')
                . ' --max_threads=' . $this->maxThreadsClient . ' --receive_timeout 3600 --send_timeout 3600'
                . ' --query=' . escapeshellarg($statement) . '';

            if ($inputFile) {
                $_exec .= " < {$inputFile}";
            }

            if ($cmdPrefix) {
                $_exec = $cmdPrefix . " " . $_exec;
            }

            $output = [];

            exec($_exec . ' 2>&1 ', $output, $exitCode);

            if ($exitCode != 0) {
                $message = $output[0];
                // No data is no real error in our case!
                if (strpos($message, 'No data to insert')) {
                    $this->logger->debug("No Data to insert");
                    $exitCode = 0;
                } else {
                    $this->logger->critical("Error from clickhouse-client: " . implode("\n", $output));
                    echo $_exec . PHP_EOL;
                }
            }
        }

        return $exitCode;
    }

    /**
     * @inheritdoc
     */
    public function escape($value): string
    {
        return addslashes((string)$value);
    }

    /**
     * Fetches next row from result set
     *
     * @param $result
     *
     * @return array|null
     */
    public function fetchRow($result): ?array
    {
        /* ClickHouse result set looks like this:
        $result = {
        content:
        [
            'meta'       => [
                0 => [
                    'name' => "count()",
                    'type' => "UInt64",
                ],
                1 => [
                    'name' => "uniq(adgroupId)",
                    'type' => "UInt64",
                ],
            ],
            "data"       => [
                0 => [0 => "13592369", 1 => "7091929"],
            ],
            "rows"       => 1,
            "statistics" => [
                "elapsed"    => 0.168291414,
                "rows_read"  => 13592369,
                "bytes_read" => 108738952,
            ],
        ],
        pointer: 0
        }
        */

        if ($result->pointer >= $result->content['rows']) {
            return null;
        }

        $row = array_combine(
            array_column($result->content['meta'], 'name'),
            $result->content['data'][$result->pointer]
        );
        $result->pointer++;

        return $row;
    }

    /**
     * NOT IMPLEMENTED FOR CLICKHOUSE
     *
     * @throws RuntimeException
     */
    public function beginTransaction(): bool
    {
        throw new RuntimeException('not implemented');
    }


    /**
     * NOT IMPLEMENTED FOR CLICKHOUSE
     *
     * @param string|null $context
     * @param string|null $additionalInfo
     *
     * @return bool
     * @throws RuntimeException
     */
    public function commit(string $context = null, string $additionalInfo = null): bool
    {
        throw new RuntimeException('not implemented');
    }

    /**
     * NOT IMPLEMENTED FOR CLICKHOUSE
     *
     * @param string|null $context
     * @param string|null $additionalInfo
     *
     * @return mixed|void
     * @throws RuntimeException
     */
    public function rollback(string $context = null, string $additionalInfo = null): bool
    {
        throw new RuntimeException('not implemented');
    }

    /**
     * NOT IMPLEMENTED FOR CLICKHOUSE
     *
     * @throws RuntimeException
     */
    public function inTransaction(): bool
    {
        throw new RuntimeException('not implemented');
    }

    /**
     * NOT IMPLEMENTED FOR CLICKHOUSE
     *
     * @throws RuntimeException
     */
    public function getInsertId()
    {
        throw new RuntimeException('not implemented');
    }

    /**
     * Sends a raw query to Clickhouse server via http
     *
     * @param string $sql
     * @param string|null $returnFormat
     *
     * @return string
     * @throws DatabaseException
     */
    protected function sendPlainQueryToServer(string $sql, string $returnFormat = null): string
    {
        $sql = TableHandler::parseStatement(static::class, $sql);

        $httpQueryValues = $this->settings;

        // Add SessionID if it exists
        if (!is_null($this->sessionId)) {
            $httpQueryValues['session_id'] = $this->sessionId;
        }

        $protocol = $this->credentials['protocol'] ?? self::DB_DEFAULT_PROTOCOL_HTTP;
        $hostname = $this->credentials[self::CREDENTIALS_HOSTNAME] ?? null;
        $port = $this->credentials['port'] ?? self::DB_DEFAULT_PORT_HTTP;
        $username = $this->credentials[self::CREDENTIALS_USERNAME] ?? null;
        $password = $this->credentials[self::CREDENTIALS_PASSWORD] ?? null;

        $streamOpts = [
            'http' => [
                'method'        => 'POST',
                'ignore_errors' => true,
                'header'        => [
                    'Content-type: application/x-www-form-urlencoded',
                    'Authorization: Basic ' . base64_encode("$username:$password")
                ],
                'content'       => $sql . PHP_EOL . ($returnFormat ? 'FORMAT ' . $returnFormat : ''),
                'timeout'       => 900,  // 15 min for bigger queries should definitely be enough
            ],
        ];


        $context = stream_context_create($streamOpts);
        $url = "{$protocol}://{$hostname}:{$port}/?" . http_build_query($httpQueryValues);
        $raw_result = file_get_contents($url, false, $context);
        if (
            !isset($http_response_header) ||
            !preg_match('~^HTTP/\d\.\d 200 ~i', $http_response_header[0])
        ) {
            $errorMsg = $raw_result !== false
                ? $raw_result
                : 'Request to ClickHouse server failed (file_get_contents returned false) for following query:';

            throw new DatabaseException($errorMsg . "\n" . $sql, 500);
        }

        return $raw_result;
    }

    /**
     * start using session in HTTP requests (e.g. to support temporary tables)
     */
    public function startSession(): void
    {
        $this->sessionId = uniqid(self::SESSION_ID_PREFIX, true);
    }

    /**
     * stop using session in HTTP requests
     *
     * @throws DatabaseException
     */
    public function stopSession(): void
    {
        if (!is_null($this->sessionId)) {
            // set timeout to one second to invalidate session
            $settings = $this->settings;
            $this->settings['session_timeout'] = 1;
            $this->sendPlainQueryToServer('SELECT 1;');
            $this->settings = $settings;
        }
        $this->sessionId = null;
    }

    /**
     * create an empty table based on the definition of another table
     *
     * @param string $sourceTable
     * @param string $destinationTable
     * @param bool $temporary
     *
     * @throws DatabaseException
     */
    public function cloneTableStructure(string $sourceTable, string $destinationTable, bool $temporary = false)
    {
        $sourceTable = str_replace('`', '', $sourceTable);

        // get create statement from source table
        $createStatementResult = $this->query("SHOW CREATE TABLE {$sourceTable}");
        $createStatement = $this->fetchRow($createStatementResult)['statement'];

        // replace table name and do not use replication engine
        $newCreateStatement = preg_replace([
            '/' . preg_quote($sourceTable, '/') . '/',
            '/Replicated([A-Za-z]*)MergeTree\([^)]*\)/',
        ], [
            $destinationTable,
            '\\1MergeTree()'
        ], $createStatement);

        // if replace process was successful, execute create statement
        if ($newCreateStatement != $createStatement) {
            $this->exec($newCreateStatement);
        }
    }

    /**
     * @inheritDoc
     */
    public function queryFirst(string $statement): ?array
    {
        $data = $this->query($statement)->content;

        if ($data['rows'] <= 0) {
            return null;
        }

        $columns = array_column($data['meta'], 'name');
        return array_combine($columns, $data['data'][0]);
    }

    /**
     * @inheritDoc
     */
    public function queryAll(string $statement, $keyName = null): array
    {
        $data = $this->query($statement)->content;

        $all = [];
        $columns = array_column($data['meta'], 'name');
        foreach ($data['data'] as $row_index) {
            // Create an assoc array for the current row (0-index)
            $row = array_combine($columns, $row_index);

            if (!is_null($keyName)) {
                $all[$row[$keyName]] = $row;
            } else {
                $all[] = $row;
            }
        }

        return $all;
    }

    /**
     * @inheritDoc
     */
    public function queryAllFlat($statement, $columnKey = 0, $keyName = null): array
    {
        $all = [];
        $data = $this->query($statement)->content;

        $columns = array_column($data['meta'], 'name');
        foreach ($data['data'] as $row_index) {
            // Create an assoc array for the current row (0-index)
            $row = array_combine($columns, $row_index);
            $this->addColumnWithKey($columnKey, $keyName, $row, $all);
        }

        return $all;
    }

    /**
     * Lists all tables in a given mysql-schema or database
     *
     * @param string $database
     * @return TableCollection
     * @throws DatabaseException
     */
    public function listTablesInDatabase(string $database): TableCollection
    {
        $sql = "
            SELECT `database`, `name`, `engine`
            FROM `system`.`tables`
            WHERE `database` = '" . $this->escape($database) . "'
        ";

        $tables = $this->queryAll($sql);

        return TableCollection::fromClickHouseTableData($this, $tables);
    }

    /**
     * Does the specified table exist?
     *
     * @param string $schema
     * @param string|null $tableName if $tableName is null, we expect the qualified name to be in $schema
     * @param bool $isTemporaryTable check for temporary tables
     * @return bool
     * @throws DatabaseException
     */
    public function hasTable(string $schema, string $tableName = null, bool $isTemporaryTable = false): bool
    {
        if ($tableName !== null) {
            $filter = '`database` = \'' . $this->escape($schema) . '\' AND `name` = \'' . $this->escape($tableName) . '\'';
        } else {
            $filter = 'CONCAT(database, \'.\', name) = \'' . $this->escape($schema) . '\'';
        }

        $filter .= ' AND is_temporary = ' . ($isTemporaryTable ? 1 : 0);
        $sql = "SELECT COUNT(*) count FROM `system`.`tables` WHERE {$filter}";

        return ((int)$this->queryFirst($sql)['count'] > 0);
    }

    /**
     * Returns a list of column names for the specified table. If both parameters are passed, they are treated as
     * SchemaName and TableName. If only one parameter is passed, it is treated as a WHERE condition
     *
     * @param string $schemaName Schema or WHERE Condition
     * @param string|null $tableName Table (if not set, Schema is interpreted as WHERE Condition)
     * @return string[]
     * @throws DatabaseException
     */
    public function getColumnList(string $schemaName, string $tableName = null): array
    {
        if ($tableName === null) {
            $whereCondition = $schemaName;
        } else {
            $whereCondition = "
                `database` = '{$schemaName}'
                AND `table` = '{$tableName}'";
        }

        $sql = "
            SELECT name
            FROM system.columns
            WHERE
                {$whereCondition}
        ";

        return $this->queryAllFlat($sql);
    }

    /**
     * Fetches the number of columns in the specified table
     *
     * @param string $schemaName
     * @param string $tableName
     * @return int
     * @throws DatabaseException
     */
    public function getColumnCount(string $schemaName, string $tableName): int
    {
        $sql = "
            SELECT COUNT(*) as count
            FROM system.columns
            WHERE
                `database` = '{$schemaName}'
                and `table` = '{$tableName}'
        ";

        return (int)$this->queryFirst($sql)['count'];
    }

    /**
     * Gets the schema where temp-tables should be stored
     *
     * @param bool $temporaryTable True if a real temporary table should be created; false if the table should live on after the database connection is closed
     * @return string
     */
    public function getTemporarySchema(bool $temporaryTable): string
    {
        if ($temporaryTable) {
            return "";
        }
        return "default";
    }

    /**
     * Returns the default storage engine for tables (used e.g. in the TempTableHandler)
     *
     * @return string
     */
    public function getDefaultStorageEngine(): string
    {
        return 'LOG';
    }
}
