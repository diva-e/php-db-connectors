<?php
declare(strict_types=1);

namespace Divae\DbConnectors;

/**
 * Interface for diva-e DB connectors
 *
 * Function names are loosely based on PDO
 */
interface DatabaseConnector
{
    /**
     * Gets actual credentials
     *
     * @return array
     */
    public function getCredentials(): array;

    /**
     * Sets credentials to use in DB connection
     *
     * @param array $credentials
     */
    public function setCredentials(array $credentials): void;

    /**
     * Gets actual "read only" setting value
     *
     * @return bool
     */
    public function getReadOnly(): bool;

    /**
     * Sets "read only" setting
     *
     * @param bool $readOnly
     */
    public function setReadOnly(bool $readOnly): void;

    /**
     * Returns statistic information
     *
     * @return array
     */
    public function getCounters(): array;

    /**
     * Attempts to connect to the DB using preconfigured credentials
     */
    public function connect(): void;

    /**
     * Closes existing DB connection if it was opened
     */
    public function close(): void;

    /**
     * READ function: Executes given statement using specified options
     *
     * @param mixed $statement
     * @param array $options
     *
     * @return mixed
     */
    public function query($statement, array $options = ['async' => false]);

    /**
     * Gets status of the last result
     *
     * @return string pending|ready|error|unknown
     */
    public function checkResultReady(): string;

    /**
     * Fetches result from the last asynchronous query
     *
     * @return mixed
     */
    public function getAsyncResult();

    /**
     * WRITE function: Executes given statement using specified options
     *
     * @param mixed $statement
     * @param array $options
     *
     * @return mixed
     * @throws DatabaseException
     */
    public function exec($statement, array $options = []);

    /**
     * Escapes special characters in a string value for use in a statement
     *
     * @param string|int|bool|null $value
     *
     * @return string
     */
    public function escape($value): string;

    /**
     * Fetches next row from result set
     *
     * @param mixed $result
     *
     * @return array|null
     */
    public function fetchRow($result): ?array;

    /**
     * Fetches all rows from result set
     *
     * @param mixed $result
     * @param string|null $keyName column to use as row index
     *
     * @return array
     */
    public function fetchAll($result, string $keyName = null): array;

    /**
     * Initiates transaction
     *
     * @return bool
     */
    public function beginTransaction(): bool;

    /**
     * Commits transaction
     *
     * @param string|null $context
     * @param string|null $additionalInfo
     *
     * @return bool
     */
    public function commit(string $context = null, string $additionalInfo = null): bool;

    /**
     * Rolls back transaction
     *
     * @param string|null $context
     * @param string|null $additionalInfo
     *
     * @return bool
     */
    public function rollback(string $context = null, string $additionalInfo = null): bool;

    /**
     * Checks if inside a transaction
     *
     * @return bool
     */
    public function inTransaction(): bool;

    /**
     * Gets unique ID of last inserted row
     *
     * @return mixed
     */
    public function getInsertId();

    /**
     * create an empty table based on the definition of another table
     *
     * @param string $sourceTable
     * @param string $destinationTable
     * @param bool $temporary
     *
     * @return mixed
     */
    public function cloneTableStructure(string $sourceTable, string $destinationTable, bool $temporary = false);

    /**
     * Queries the database and returns the first row
     *
     * @param string $statement the query to execute
     *
     * @return null|array first row of result set, null if there were no rows returned
     * @throws DatabaseException
     */
    public function queryFirst(string $statement): ?array;

    /**
     * Runs the query in $statement and returns all result rows
     *
     * @param string $statement the query to execute
     * @param string|null $keyName string if given, resulting array has the keys belonging to this column of the result
     * @return array|null an array containing all rows, null if there were no rows returned
     * @throws DatabaseException
     */
    public function queryAll(string $statement, string $keyName = null): array;

    /**
     * Returns @param string $statement the query to execute
     *
     * @param int|string $columnKey the column key/index to reference; uses the first column (index 0) by default
     * @param null|string $keyName the key name to restrict the query result to (see query_all())
     *
     * @return array|null a flat array containing all values from the specified column index, null if there were no rows returned
     * @throws DatabaseException if the column index does not exist on a non-empty query result
     * @see DatabaseConnector::queryAll() results with the specified column from all result rows collapsed into a single array.
     *
     */
    public function queryAllFlat(string $statement, $columnKey = 0, string $keyName = null): array;
}
