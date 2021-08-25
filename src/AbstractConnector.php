<?php
declare(strict_types=1);

namespace Divae\DbConnectors;

use Psr\Log\LoggerAwareInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

/**
 * Parent class for diva-e DB connectors
 *
 * Function names are loosely based on PDO
 */
abstract class AbstractConnector implements LoggerAwareInterface, DatabaseConnector
{

    public const RESULT_STATE_PENDING = 'pending';
    public const RESULT_STATE_READY = 'ready';
    public const RESULT_STATE_ERROR = 'error';
    public const RESULT_STATE_UNKNOWN = 'unknown';

    public const COUNTER_READ = 'read';
    public const COUNTER_WRITE = 'write';
    public const COUNTER_TOTAL_QUERIES = 'total_queries';
    public const COUNTER_TOTAL_EXECUTION_TIME = 'total_execution_time';
    public const COUNTER_ROWS_READ = 'rows_read';
    public const COUNTER_ROWS_WRITTEN = 'rows_written';
    public const COUNTER_CREATION_TIME = 'creation_time';
    public const COUNTER_TOTAL_ELAPSED_TIME = 'total_elapsed_time';
    public const COUNTER_TOTAL_OUT_OF_DATABASE_TIME = 'total_outofdatabase_time';

    public const CREDENTIALS_HOSTNAME = 'host';
    public const CREDENTIALS_USERNAME = 'username';

    public const OPTION_ASYNC = 'async';

    /**
     * @var LoggerInterface logger instance
     */
    protected LoggerInterface $logger;

    /**
     * @var array statistics
     */
    protected array $counters = [
        self::COUNTER_READ                 => 0,
        self::COUNTER_WRITE                => 0,
        self::COUNTER_TOTAL_QUERIES        => 0,
        self::COUNTER_ROWS_WRITTEN         => 0,
        self::COUNTER_ROWS_READ            => 0,
        self::COUNTER_TOTAL_EXECUTION_TIME => 0,
        self::COUNTER_CREATION_TIME        => 0,
    ];

    /**
     * @var array credentials used when connecting to the DB
     */
    protected array $credentials = [];

    /**
     * @var array settings used when connecting to the DB
     */
    protected array $settings = [];

    /**
     * @var bool if connection is read-only
     */
    public bool $readOnly = false;

    /**
     * @var array asynchronous query runtime information
     */
    protected array $asyncInfo = [];

    /**
     * AbstractConnector constructor
     * sets up NullLogger by default
     */
    public function __construct()
    {
        $this->logger = new NullLogger();
    }

    /**
     * Gives human-readable debug output for var_dump()
     *
     * @return array
     */
    public function __debugInfo(): array
    {
        return [
            'credentials' => [
                self::CREDENTIALS_USERNAME => $this->credentials[self::CREDENTIALS_USERNAME] ?? 'NOT_SET',
                self::CREDENTIALS_HOSTNAME => $this->credentials[self::CREDENTIALS_HOSTNAME] ?? 'NOT_SET',
            ],
            'settings'    => array_merge($this->settings, ['read_only' => $this->readOnly]),
            'counters'    => $this->counters,
            'properties'  => ['asyncInfo' => $this->asyncInfo],
        ];
    }

    /**
     * Gets actual credentials
     *
     * @return array
     */
    public function getCredentials(): array
    {
        return $this->credentials;
    }

    /**
     * Sets credentials to use in DB connection
     *
     * @param array $credentials
     */
    public function setCredentials(array $credentials): void
    {
        $this->credentials = $credentials;
    }

    /**
     * Gets actual connection settings
     *
     * @return array
     */
    public function getSettings(): array
    {
        return $this->settings;
    }

    /**
     * Sets settings to use in DB connection
     *
     * @param array $settings
     */
    public function setSettings(array $settings): void
    {
        $this->settings = $settings;
    }

    /**
     * Gets actual "read only" setting value
     *
     * @return bool
     */
    public function getReadOnly(): bool
    {
        return $this->readOnly;
    }

    /**
     * Sets "read only" setting
     *
     * @param bool $readOnly
     */
    public function setReadOnly(bool $readOnly): void
    {
        $this->readOnly = $readOnly;
    }

    /**
     * Sets a logger instance on the object.
     *
     * @param LoggerInterface $logger
     */
    public function setLogger(LoggerInterface $logger): void
    {
        $this->logger = $logger;
    }

    /**
     * Returns statistic information
     * @return array
     */
    public function getCounters(): array
    {
        $counters = $this->counters;
        unset($counters[self::COUNTER_CREATION_TIME]);
        $counters[self::COUNTER_TOTAL_ELAPSED_TIME] = microtime(true)
            - $this->counters[self::COUNTER_CREATION_TIME];
        $counters[self::COUNTER_TOTAL_OUT_OF_DATABASE_TIME] = $counters[self::COUNTER_TOTAL_ELAPSED_TIME]
            - $counters[self::COUNTER_TOTAL_EXECUTION_TIME];

        return $counters;
    }

    /**
     * Attempts to connect to the DB using preconfigured credentials
     */
    abstract public function connect(): void;

    /**
     * Closes existing DB connection if it was opened
     */
    abstract public function close(): void;

    /**
     * READ function: Executes given statement using specified options
     *
     * @param mixed $statement
     * @param array $options
     *
     * @return mixed
     */
    abstract public function query($statement, array $options = ['async' => false]);

    /**
     * Gets status of the last result
     *
     * @return string pending|ready|error|unknown
     */
    abstract public function checkResultReady(): string;

    /**
     * Fetches result from the last asynchronous query
     *
     * @return mixed
     */
    abstract public function getAsyncResult();

    /**
     * WRITE function: Executes given statement using specified options
     *
     * @param mixed $statement
     * @param array $options
     *
     * @return mixed
     */
    abstract public function exec($statement, array $options = []);

    /**
     * Escapes special characters in a string value for use in a statement
     *
     * @param string|int|bool|null $value
     *
     * @return string
     */
    abstract public function escape($value): string;

    /**
     * Fetches next row from result set
     *
     * @param mixed $result
     *
     * @return array|null
     */
    abstract public function fetchRow($result): ?array;

    /**
     * Fetches all rows from result set
     *
     * @param mixed $result
     * @param string|null $keyName column to use as row index
     *
     * @return array
     */
    public function fetchAll($result, string $keyName = null): array
    {
        $rows = [];

        while ($row = $this->fetchRow($result)) {
            if (!is_null($keyName)) {
                $rows[$row[$keyName]] = $row;
            } else {
                $rows[] = $row;
            }
        }

        return $rows;
    }

    /**
     * Initiates transaction
     *
     * @return bool
     */
    abstract public function beginTransaction(): bool;

    /**
     * Commits transaction
     *
     * @param string|null $context
     * @param string|null $additionalInfo
     *
     * @return bool
     */
    abstract public function commit(string $context = null, string $additionalInfo = null): bool;

    /**
     * Rolls back transaction
     *
     * @param string|null $context
     * @param string|null $additionalInfo
     *
     * @return bool
     */
    abstract public function rollback(string $context = null, string $additionalInfo = null): bool;

    /**
     * Checks if inside a transaction
     *
     * @return bool
     */
    abstract public function inTransaction(): bool;

    /**
     * Gets unique ID of last inserted row
     *
     * @return mixed
     */
    abstract public function getInsertId();

    /**
     * Gets call stack context
     *
     * @return string
     */
    protected function getCallStackContext(): string
    {
        $callStack = debug_backtrace(DEBUG_BACKTRACE_IGNORE_ARGS, 10);
        if (count($callStack) == 2) {
            // function called on cli script
            return $callStack[1]['file'] . ": (l {$callStack[1]['line']})";
        }

        unset($callStack[0], $callStack[1]);
        $callStack = array_reverse($callStack);

        $retStrings = [];
        $script = '';
        foreach ($callStack as $entry) {
            $retStrings[] = ($entry['class'] ?? '')
                . ($entry['type'] ?? '')
                . ($entry['function'] ?? '')
                . (isset($entry['line']) ? "(L {$entry['line']})" : '');
            $script = $entry['file'] ?? $script;
        }

        return $script . ': ' . implode(',', $retStrings);
    }

    /**
     * @param mixed $columnKey
     * @param string|null $keyName
     * @param array $row
     * @param array $all
     *
     * @throws DatabaseException
     */
    protected function addColumnWithKey($columnKey, ?string $keyName, array $row, array &$all)
    {
        if (is_int($columnKey)) {
            // If $column_key is int, then drop the column names and reference via index
            $row_for_column_lookup = array_values($row);
        } else {
            $row_for_column_lookup = $row;
        }

        if (!array_key_exists($columnKey, $row_for_column_lookup)) {
            throw new DatabaseException("column key ({$columnKey}) does not exist in result row!");
        }

        if (!is_null($keyName)) {
            $all[$row[$keyName]] = $row_for_column_lookup[$columnKey];
        } else {
            $all[] = $row_for_column_lookup[$columnKey];
        }
    }

    /**
     * create an empty table based on the definition of another table
     *
     * @param string $sourceTable
     * @param string $destinationTable
     * @param bool $temporary
     */
    abstract public function cloneTableStructure(string $sourceTable, string $destinationTable, bool $temporary = false);

    /**
     * Queries the database and returns the first row
     * @param string $statement the query to execute
     *
     * @return null|array first row of result set, null if there were no rows returned
     * @throws DatabaseException
     */
    abstract public function queryFirst(string $statement): ?array;

    /**
     * Runs the query in $statement and returns all result rows
     *
     * @param string $statement the query to execute
     * @param string|null $keyName string if given, resulting array has the keys belonging to this column of the result
     * @return array|null an array containing all rows, null if there were no rows returned
     * @throws DatabaseException
     */
    abstract public function queryAll(string $statement, string $keyName = null): array;

    /**
     * Returns @param string $statement the query to execute
     * @param int|string $columnKey the column key/index to reference; uses the first column (index 0) by default
     * @param null|string $keyName the key name to restrict the query result to (see query_all())
     *
     * @return array|null a flat array containing all values from the specified column index, null if there were no rows returned
     * @throws DatabaseException if the column index does not exist on a non-empty query result
     * @see DatabaseConnector::queryAll() results with the specified column from all result rows collapsed into a single array.
     *
     */
    abstract public function queryAllFlat(string $statement, $columnKey = 0, $keyName = null): array;
}
