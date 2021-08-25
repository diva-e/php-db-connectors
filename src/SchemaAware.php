<?php
declare(strict_types=1);

namespace Divae\DbConnectors;

use Divae\DbConnectors\Schema\TableCollection;

/**
 * Class for accessing MySQL Databases
 *
 */
interface SchemaAware
{
    /**
     * Lists all tables in a given mysql-schema or database
     *
     * @param string $database
     * @return TableCollection
     * @throws DatabaseException
     */
    public function listTablesInDatabase(string $database): TableCollection;

    /**
     * Does the specified table exist?
     *
     * @param string $schema
     * @param string|null $tableName if $tableName is null, we expect the qualified name to be in $schema
     * @param bool $isTemporaryTable check for temporary tables
     * @return bool
     * @throws DatabaseException
     */
    public function hasTable(string $schema, string $tableName = null, bool $isTemporaryTable = false): bool;

    /**
     * Returns a list of column names for the specified table. If both parameters are passed, they are treated as
     * SchemaName and TableName. If only one parameter is passed, it is treated as a WHERE condition
     *
     * @param string $schemaName Schema or WHERE Condition
     * @param string|null $tableName Table (if not set, Schema is interpreted as WHERE Condition)
     * @return string[]
     * @throws DatabaseException
     */
    public function getColumnList(string $schemaName, string $tableName = null): array;

    /**
     * Fetches the number of columns in the specified table
     *
     * @param string $schemaName
     * @param string $tableName
     * @return int
     * @throws DatabaseException
     */
    public function getColumnCount(string $schemaName, string $tableName): int;

    /**
     * Gets the schema where temp-tables should be stored
     *
     * @param bool $temporaryTable True if a real temporary table should be created; false if the table should live on after the database connection is closed
     * @return string
     */
    public function getTemporarySchema(bool $temporaryTable): string;

    /**
     * Returns the default storage engine for tables (used e.g. in the TempTableHandler)
     *
     * @return string
     */
    public function getDefaultStorageEngine(): string;
}
