<?php
declare(strict_types=1);

namespace Divae\DbConnectors;

use Divae\DbConnectors\Schema\TableCollection;
use Divae\DbConnectors\Testing\TableHandler;
use mysqli;
use mysqli_result;
use RuntimeException;

/**
 * Class for accessing MySQL Databases
 *
 */
class MySQL extends AbstractConnector implements SchemaAware, DatabaseSchemaAware
{

    public const CREDENTIALS_PASSWORD = 'password';
    public const CREDENTIALS_PORT = 'port';
    public const CREDENTIALS_DATABASE = 'database';

    public const OPTION_IGNORE_WRITE_QUERIES = 'ignoreWriteQueries';

    private const DB_CONNECTION_TIMEOUT = 20;
    private const DB_DEFAULT_PORT = 3306;

    private const ASYNC_MODE_EXEC = 'exec';
    private const ASYNC_MODE_QUERY = 'query';

    /**
     * @var mysqli|null mysqli instance
     */
    private ?mysqli $connection = null;
    /**
     * @var bool whether $this->beginTransaction() was called or not
     */
    private bool $inTransaction = false;

    /**
     * @inheritdoc
     */
    public function __debugInfo(): array
    {
        $debugInfo = parent::__debugInfo();
        $debugInfo['credentials'][self::CREDENTIALS_PORT] = $this->credentials[self::CREDENTIALS_PORT] ?? 'DEFAULT';
        $debugInfo['properties']['MySQLi_Main'] = $this->connection;

        return $debugInfo;
    }

    /**
     * Attempts to connect to the DB using preconfigured credentials
     *
     * @throws DatabaseException
     */
    public function connect(): void
    {
        if ($this->counters[self::COUNTER_CREATION_TIME] == 0) {
            $this->counters[self::COUNTER_CREATION_TIME] = microtime(true);
        }

        // connection credentials
        $hostname = $this->credentials[self::CREDENTIALS_HOSTNAME];
        $username = $this->credentials[self::CREDENTIALS_USERNAME];
        $password = $this->credentials[self::CREDENTIALS_PASSWORD];
        $database = $this->credentials[self::CREDENTIALS_DATABASE] ?? '';
        $port = $this->credentials[self::CREDENTIALS_PORT] ?? self::DB_DEFAULT_PORT;

        $this->connection = mysqli_init();
        $this->connection->options(MYSQLI_OPT_CONNECT_TIMEOUT, self::DB_CONNECTION_TIMEOUT);
        $this->connection->real_connect($hostname, $username, $password, $database, $port);

        // MySQLi::connect_error is static, so we have to check for a threadId to know we are connected!
        if ($this->connection->thread_id <= 0) {
            $this->logger->error('Cannot connect to the database', [
                'username' => $username,
                'hostname' => $hostname,
                'database' => $database,
                'port'     => $port,
            ]);

            throw new DatabaseException(
                "Cannot connect to the database {$hostname}:{$port}:"
                . "\n ({$this->connection->connect_errno}) {$this->connection->connect_error}");
        }

        // configure connection
        $this->connection->query("SET NAMES 'utf8mb4'");
    }

    /**
     * Closes existing DB connection if it was opened
     */
    public function close(): void
    {
        if ($this->connection) {
            $this->connection->close();
        }

        $this->connection = null;
        $this->inTransaction = false;
    }


    /**
     * READ function: Executes given statement using specified options
     *
     * @param string $statement
     * @param array $options
     *
     * @return bool|mysqli_result mysqli_result or TRUE on asynchronous query
     * @throws DatabaseException
     * @throws RuntimeException
     */
    public function query(
        $statement,
        array $options = [
            self::OPTION_ASYNC                => false,
            self::OPTION_IGNORE_WRITE_QUERIES => false
        ]
    ) {
        $statement = TableHandler::parseStatement(static::class, $statement);

        if (!$this->connection instanceof mysqli) {
            throw new RuntimeException('Not connected to the database');
        }

        $startTime = microtime(true);
        $this->counters[self::COUNTER_TOTAL_QUERIES]++;

        $isAsync = $options[self::OPTION_ASYNC] ?? false;
        // Start asynchronous query
        if ($isAsync) {
            $this->asyncInfo['start_time'] = $startTime;
            $this->asyncInfo['mode'] = self::ASYNC_MODE_QUERY;
            $this->connection->query($statement, MYSQLI_ASYNC);
            $this->counters[self::COUNTER_READ]++;

            return true;
        }

        // Start synchronous query
        $queryResult = $this->connection->query($statement);

        // check for write operations done by read function
        if ($queryResult === true) {
            $this->counters[self::COUNTER_ROWS_WRITTEN] += $this->connection->affected_rows;
            $this->counters[self::COUNTER_WRITE]++;

            if (!$options[self::OPTION_IGNORE_WRITE_QUERIES]) {
                throw new RuntimeException('Write operation in read function:  ' . $statement);
            }
        } // check for failure
        elseif ($queryResult === false) {
            throw new DatabaseException('Error on execution ((' . $this->connection->errno . ')' . $this->connection->error . ') on query: ' . $statement,
                $this->connection->errno);
        } else {
            // get data from MySQL into php cache
            $this->connection->store_result();
            // Store logging information
            $this->counters[self::COUNTER_ROWS_READ] += $queryResult->num_rows;
            $this->counters[self::COUNTER_READ]++;
            $this->counters[self::COUNTER_TOTAL_EXECUTION_TIME] += (microtime(true) - $startTime);
        }
        return $queryResult;
    }

    /**
     * Gets status of the last result
     *
     * @return string pending|ready|error|unknown
     */
    public function checkResultReady(): string
    {
        $success = $error = $reject = [$this->connection];

        $result = mysqli_poll($success, $error, $reject, 0, 200);

        // if connection problem etc
        if ($result === false) {
            unset($this->asyncInfo['mode']);

            return self::RESULT_STATE_ERROR;
        }

        // if no async query results available (yet)
        if (empty($reject) && $result === 0) {
            return self::RESULT_STATE_PENDING;
        }

        // adjust counter if query finished
        if (empty($reject) && array_key_exists('start_time', $this->asyncInfo)) {
            $this->counters[self::COUNTER_TOTAL_EXECUTION_TIME] += (microtime(true) - $this->asyncInfo['start_time']);
            unset($this->asyncInfo['start_time']);
        }

        // if async result is ready
        if (!empty($success)) {
            return self::RESULT_STATE_READY;
        }
        // if some error occurred
        if (!empty($error)) {
            return self::RESULT_STATE_ERROR;
        }

        // no async query executed
        return self::RESULT_STATE_UNKNOWN;
    }

    /**
     * Fetches result from the last asynchronous query
     *
     * @return bool|int|mysqli_result mysqli_result on read queries, int on write or FALSE on failed queries
     */
    public function getAsyncResult()
    {
        $syncMode = $this->asyncInfo['mode'] ?? null;
        unset($this->asyncInfo['mode']);

        if (is_null($syncMode)) {
            return false;
        }

        $queryResult = $this->connection->reap_async_query();
        if ($queryResult === false) {
            return false;
        }

        // READ
        if ($syncMode === self::ASYNC_MODE_QUERY) {
            $this->counters[self::COUNTER_ROWS_READ] += $queryResult->num_rows;
            $this->connection->store_result();

            return $queryResult;
        }

        // WRITE
        $this->counters[self::COUNTER_ROWS_WRITTEN] += $this->connection->affected_rows;

        return $this->connection->affected_rows;

    }

    /**
     * WRITE function: Executes given statement using specified options
     *
     * @param string $statement
     * @param array $options
     *
     * @return int number of rows affected
     * @throws RuntimeException
     * @throws DatabaseException
     */
    public function exec(
        $statement,
        array $options = [
            self::OPTION_ASYNC => false,
        ]
    ): int {
        $statement = TableHandler::parseStatement(static::class, $statement);

        if (!$this->connection instanceof mysqli) {
            throw new RuntimeException('Not connected to the database');
        }

        $startTime = microtime(true);
        $this->counters[self::COUNTER_TOTAL_QUERIES]++;

        $isAsync = $options[self::OPTION_ASYNC] ?? false;
        // Start asynchronous query
        if ($isAsync) {
            $this->asyncInfo['start_time'] = $startTime;
            $this->asyncInfo['mode'] = self::ASYNC_MODE_EXEC;
            $this->connection->query($statement, MYSQLI_ASYNC);
            $this->counters[self::COUNTER_WRITE]++;

            return 0;
        }

        // Start synchronous query
        $queryResult = $this->connection->query($statement);

        // check for read operation made by write function
        if ($queryResult instanceof mysqli_result) {
            throw new RuntimeException('Read operation in write function:  ' . $statement);
        }
        // check for failure
        if ($queryResult === false) {
            throw new DatabaseException('Error on execution ((' . $this->connection->errno . ')' . $this->connection->error . ') on query: ' . $statement);
        }

        // Store logging information
        $this->counters[self::COUNTER_ROWS_WRITTEN] += $this->connection->affected_rows;
        $this->counters[self::COUNTER_WRITE]++;
        $this->counters[self::COUNTER_TOTAL_EXECUTION_TIME] += microtime(true) - $startTime;

        return $this->connection->affected_rows;
    }

    /**
     * @inheritdoc
     */
    public function escape($value): string
    {
        if (is_null($value)) {
            return 'NULL';
        } elseif (is_bool($value)) {
            return (string)(int)$value;
        } else {
            return $this->connection->real_escape_string((string)$value);
        }
    }

    /**
     * Fetches next row from result set
     *
     * @param mysqli_result $result
     *
     * @return array|null
     */
    public function fetchRow($result): ?array
    {
        return $result->fetch_assoc();
    }

    /**
     * Fetches all rows from result set
     *
     * @param mysqli_result $result
     * @param string|null $keyName column to use as row index
     *
     * @return array
     */
    public function fetchAll($result, string $keyName = null): array
    {
        if (!is_null($keyName)) {
            return parent::fetchAll($result, $keyName);
        }

        return $result->fetch_all(MYSQLI_ASSOC);
    }

    /**
     * Initiates transaction
     *
     * @return bool
     * @throws DatabaseException
     */
    public function beginTransaction(): bool
    {
        if ($this->inTransaction) {
            return false;
        } else {
            $this->exec('START TRANSACTION;');
            $this->inTransaction = true;

            return true;
        }
    }

    /**
     * Commits transaction
     *
     * @param string|null $context
     * @param string|null $additionalInfo
     *
     * @return bool
     * @throws DatabaseException
     */
    public function commit(string $context = null, ?string $additionalInfo = null): bool
    {
        $callStack = $this->getCallStackContext();
        $this->inTransaction = false;

        $this->exec("COMMIT /*{$context} {$additionalInfo} CallStack: {$callStack}*/;");

        return true;
    }

    /**
     * Rolls back transaction
     *
     * @param string|null $context
     * @param string|null $additionalInfo
     *
     * @return bool
     * @throws DatabaseException
     */
    public function rollback(string $context = null, ?string $additionalInfo = null): bool
    {
        $callStack = $this->getCallStackContext();
        $this->inTransaction = false;

        $this->exec("ROLLBACK /*{$context} {$additionalInfo} CallStack: {$callStack}*/;");

        return true;
    }

    /**
     * @inheritdoc
     */
    public function inTransaction(): bool
    {
        return $this->inTransaction;
    }

    /**
     * @inheritdoc
     */
    public function getInsertId()
    {
        return $this->connection->insert_id;
    }

    /**
     * Overwrite the internal mysqli object
     *
     * @param mysqli $mysqli
     */
    public function injectConnection(mysqli $mysqli): void
    {
        $this->connection = $mysqli;
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
        $this->exec("CREATE " . ($temporary ? " TEMPORARY" : '') . " TABLE IF NOT EXISTS {$destinationTable} LIKE {$sourceTable}");
    }

    /**
     * Fetch the number of rows affected by the last query
     *
     * @return int
     */
    public function getAffectedRows(): int
    {
        return $this->connection->affected_rows;
    }

    /**
     * Fetch the number of rows affected by the last query
     *
     * @return int
     */
    public function get_affected_rows(): int
    {
        return $this->getAffectedRows();
    }

    /**
     * Get some basic infos about the last error
     *
     * @return array
     */
    public function getErrorInfos(): array
    {
        return [
            'error'     => $this->connection->error,
            'error_no'  => $this->connection->errno,
            'host_info' => $this->connection->host_info,
        ];
    }

    /**
     * Checks the database connection status
     *
     * @return int 0 on success; -1 if no connection has been established at all (or has been closed); returns the mysql error code in every other case
     */
    public function getConnectionErrorCode(): int
    {
        if (!$this->connection) {
            return -1;
        }

        // connection should be established, so ping the connection, which does automatically reconnect if needed
        // https://bugs.php.net/bug.php?id=52561 -> reconnect does not work, so implement it manually
        if ($this->connection->ping()) {

            // Connection exists and is live
            return 0;
        }

        // Connection exists, but is somehow broken
        $errorNumber = $this->connection->errno;

        // a lost connection can lead into 2006 (gone away) and since PHP 7.1 into 2027 (malformed packet)
        if (in_array($errorNumber, [2006, 2027])) {
            $this->logger->notice(
                'MySQL Error 2006 (MySQL server has gone away) occurred. Trying to reconnect to database.',
                [$this->connection->errno, $this->connection->error]
            );
        }

        return $errorNumber;
    }

    /**
     * @inheritDoc
     */
    public function queryFirst($statement): ?array
    {
        $qid = $this->query($statement);

        if ($qid->num_rows <= 0) {
            return null;
        }

        $row = $qid->fetch_assoc();
        $qid->free_result();

        return $row;
    }

    /**
     * @inheritDoc
     */
    public function queryAll($statement, $keyName = null): array
    {
        $all = [];

        $qid = $this->query($statement);

        while (($row = $qid->fetch_assoc()) != false) {
            if (!is_null($keyName)) {
                $all[$row[$keyName]] = $row;
            } else {
                $all[] = $row;
            }
        }
        $qid->free_result();

        return $all;
    }

    /**
     * @inheritDoc
     */
    public function queryAllFlat($statement, $columnKey = 0, $keyName = null): array
    {
        $all = [];
        $qid = $this->query($statement);

        while (($row = $qid->fetch_assoc()) != false) {
            $this->addColumnWithKey($columnKey, $keyName, $row, $all);
        }
        $qid->free_result();

        return $all;
    }

    /**
     * Lists all tables matching to the $whereCondition
     *
     * @param string $whereCondition
     * @return TableCollection
     * @throws DatabaseException
     */
    public function listTables(string $whereCondition = "true"): TableCollection
    {
        $sql = "
            SELECT `TABLE_SCHEMA`, `TABLE_NAME`, `ENGINE`, `TABLE_ROWS`
            FROM `information_schema`.`TABLES`
            WHERE {$whereCondition}
        ";

        $tables = $this->queryAll($sql);

        return TableCollection::fromMySQLTableData($this, $tables);
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
            $filter = '`TABLE_SCHEMA` = "' . $this->escape($schema) . '" AND `TABLE_NAME` = "' . $this->escape($tableName) . '"';
        } else {
            $filter = 'CONCAT(TABLE_SCHEMA, \'.\', TABLE_NAME) = "' . $this->escape($schema) . '"';
        }

        $source = ($isTemporaryTable ? 'TEMPORARY_TABLES' : 'TABLES');
        $sql = "SELECT COUNT(*) count FROM `information_schema`.`{$source}` WHERE {$filter}";

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
                TABLE_SCHEMA = '{$schemaName}'
                AND TABLE_NAME = '{$tableName}'";
        }

        $sql = "
            SELECT COLUMN_NAME
            FROM information_schema.COLUMNS
            WHERE {$whereCondition}
            ORDER BY ORDINAL_POSITION ASC
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
            FROM information_schema.COLUMNS
            WHERE
                TABLE_SCHEMA = '{$schemaName}'
                AND TABLE_NAME = '{$tableName}'
        ";

        return (int)$this->queryFirst($sql)['count'];
    }

    /**
     * Returns the keys of the provided table in the right order. Default to Primary Key
     *
     * @param string $table the table name including the DB-name separated by a '.', i.e. db-name.table-name
     * @param string $keyName
     * @param bool $isTemporaryTable
     *
     * @return array containing the keys of the table
     * @throws DatabaseException
     */
    public function getKeyColumnNames(string $table, string $keyName = 'PRIMARY', bool $isTemporaryTable = false): array
    {
        // Parse full table name to schema.table
        [$schemaName, $tableName] = explode('.', $table, 2);

        if (!$this->hasTable($schemaName, $tableName, $isTemporaryTable)) {
            throw new DatabaseException("Table '" . $table . "' does not exist in DB!");
        }

        //get indices from target table (don't use 'Key' column from DESCRIBE as it mixes PRI and MUL keys)
        $indices = $this->queryAll("
            SHOW INDEXES FROM {$table}
            WHERE Key_name = '{$keyName}'
        ");

        // don't rely on database to provide correct primary key order (required for implode later on), let's sort just in case
        uasort($indices, function ($index1, $index2) {
            return $index1['Seq_in_index'] - $index2['Seq_in_index'];
        });

        // get actual target PKs from 'Column_name' column
        return array_column($indices, 'Column_name');
    }

    /**
     * Returns an array containing the mutual columns of the two provided tables, i.e. the columns that exist in both tables.
     * If one or both of the tables don't exist an exception is thrown.
     *
     * @param string $table1 first table
     * @param string $table2 second table
     * @return array List of mutual column names
     * @throws DatabaseException
     */
    public function getMutualColumnsArray(string $table1, string $table2): array
    {
        $columns1 = $this->getColumnList("CONCAT(`TABLE_SCHEMA`, '.', `TABLE_NAME`) = '" . $this->escape($table1) . "'");
        $columns2 = $this->getColumnList("CONCAT(`TABLE_SCHEMA`, '.', `TABLE_NAME`) = '" . $this->escape($table2) . "'");

        return array_intersect($columns1, $columns2);
    }

    /**
     * Returns a comma-separated list of the mutual columns of the two provided tables,
     * i.e. the columns that exist in both tables.
     *
     * @param string $table1
     * @param string $table2
     *
     * @return string comma-separated list of the mutual columns of the provided tables
     * @throws DatabaseException
     * @see getMutualColumnsArray()
     *
     */
    public function getMutualColumnsList(string $table1, string $table2): string
    {
        $mutualColumns = $this->getMutualColumnsArray($table1, $table2);
        return implode(',', $mutualColumns);
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
        return $this->listTables('`TABLE_SCHEMA` = "' . $this->escape($database) . '"');
    }

    /**
     * Gets the schema where temp-tables should be stored
     *
     * @param bool $temporaryTable True if a real temporary table should be created; false if the table should live on after the database connection is closed
     * @return string
     */
    public function getTemporarySchema(bool $temporaryTable): string
    {
        return $this->credentials['tempTableSchema'] ?? "test";
    }

    /**
     * Returns the default storage engine for tables (used e.g. in the TempTableHandler)
     *
     * @return string
     */
    public function getDefaultStorageEngine(): string
    {
        return 'InnoDB';
    }
}
