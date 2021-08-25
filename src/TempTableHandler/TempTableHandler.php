<?php
declare(strict_types=1);

namespace Divae\DbConnectors\TempTableHandler;

use DateTime;
use DateTimeZone;
use Divae\DbConnectors\DatabaseException;
use Divae\DbConnectors\DatabaseSchemaAware;
use Divae\DbConnectors\Schema\DatabaseTable;
use Exception;
use InvalidArgumentException;

class TempTableHandler
{
    /**
     * Allowed duration for temp tables
     * A valid date/time string.
     * @var string
     */
    protected string $ttlTempTable;

    /**
     * @var DatabaseSchemaAware
     */
    protected DatabaseSchemaAware $database;

    /**
     * Should we use temporary or real tables
     * @var bool
     */
    protected bool $useTemporaryTables;

    /**
     * @var string
     */
    protected string $classUniqueKey;

    /**
     * Map<string,string> of all tables managed with this instance of TempTableHandler (slug -> table name)
     * @var string[]
     */
    protected array $managedTables = [];

    public function __construct(DatabaseSchemaAware $database, string $ttlTempTable, bool $useTemporaryTables = false)
    {
        $this->database = $database;
        $this->classUniqueKey = 'TTH';
        $this->ttlTempTable = $ttlTempTable;
        $this->useTemporaryTables = $useTemporaryTables;
    }

    /**
     * Generates a new temp table name
     * @param string $prefix
     * @return string
     */
    protected function getTempTableName(string $prefix): string
    {
        $tempTableName = '';

        // MySQL needs a schema/database for temporary table. This is to ensure we always have one.
        $schema = $this->database->getTemporarySchema($this->useTemporaryTables);
        if (!empty($schema)) {
            $tempTableName .= $schema . '.';
        }

        // common prefix
        $tempTableName .= "tmp";

        // unique key for this class
        $tempTableName .= '_' . $this->classUniqueKey . '_' . $prefix;

        // time and random number should make sure to be unique
        $tempTableName .= "_" . gmdate('YmdHis') . '_' . rand(1000000,
                9999999);

        return $tempTableName;
    }

    /**
     * Creates a temporary table and returns its name
     * @param string $tableSlug
     * @param array $columns
     * @param string|null $engine If not supplied, a default will be used
     * @return string The new table's name
     * @throws DatabaseException
     */
    protected function createTempTable(string $tableSlug, array $columns, string $engine = null): string
    {
        if (array_key_exists($tableSlug, $this->managedTables)) {
            throw new InvalidArgumentException('The table ' . $tableSlug . ' is already created');
        }

        $temporaryStatement = '';
        $engineStatement = 'ENGINE = ' . ($engine ?? $this->database->getDefaultStorageEngine());
        if ($this->useTemporaryTables) {
            $temporaryStatement = 'TEMPORARY';
            $engineStatement = '';
        }

        // Get a name
        $tableName = $this->getTempTableName($tableSlug);

        $query = "CREATE {$temporaryStatement} TABLE {$tableName} (
            " . implode(",\n            ", $columns) . "
        ) {$engineStatement}
        ";

        $this->database->exec($query);

        $this->managedTables[$tableSlug] = $tableName;

        return $tableName;
    }

    /**
     * Drops a managed table in the database
     * @param string $nameOrSlug
     * @throws DatabaseException
     */
    public function dropTable(string $nameOrSlug): void
    {
        if (array_key_exists($nameOrSlug, $this->managedTables)) {
            // We have a slug
            $slug = $nameOrSlug;
            $tableName = $this->managedTables[$nameOrSlug];
        } else {
            // We have a table name
            $tableName = $nameOrSlug;
            $slug = array_flip($this->managedTables)[$tableName];
        }

        // Drop TableName, delete slug from managed tables
        $sql = "DROP TABLE {$tableName}";
        $this->database->exec($sql);

        unset($this->managedTables[$slug]);
    }

    /**
     * Drops all managed tables in the database
     * @throws DatabaseException
     */
    public function dropAllTables(): void
    {
        foreach ($this->managedTables as $tableName) {
            $sql = "DROP TABLE {$tableName}";
            $this->database->exec($sql);
        }

        // Empty the array, since all tables have been dropped
        $this->managedTables = [];
    }

    /**
     * Cleans the selected table using TRUNCATE
     *
     * @param $nameOrSlug string The table to clean
     * @throws DatabaseException
     */
    public function cleanTable(string $nameOrSlug): void
    {
        if (array_key_exists($nameOrSlug, $this->managedTables)) {
            $tableName = $this->managedTables[$nameOrSlug];
        } else {
            $tableName = $nameOrSlug;
        }

        $sql = "TRUNCATE TABLE {$tableName}";
        $this->database->exec($sql);
    }

    /**
     * Get all columns of specified temp table.
     *
     * @param string $nameOrSlug
     * @param array $excluded_columns
     *
     * @return array list of column names (optional: minus all excluded columns)
     * @throws DatabaseException
     */
    public function getColumnNames(string $nameOrSlug, array $excluded_columns = []): array
    {
        if (array_key_exists($nameOrSlug, $this->managedTables)) {
            $fullTableName = $this->managedTables[$nameOrSlug];
        } else {
            $fullTableName = $nameOrSlug;
        }

        if (strpos($fullTableName, '.') !== false) {
            [$schema, $table] = explode('.', $fullTableName);
        } else {
            $schema = '';
            $table = $fullTableName;
        }

        $columns = $this->database->getColumnList($schema, $table);
        return array_diff($columns, $excluded_columns);
    }


    /**
     * Drop zombie tables from the database
     * Alias for runGC()
     *
     * @throws DatabaseException|Exception
     * @see runGC
     */
    public function dropZombieTables()
    {
        $this->runGC();
    }

    /**
     * Removes zombie tables - these are identified by their creationTime being created longer ago than ttlTempTable
     * GC = Garbage Collection
     *
     * @throws DatabaseException|Exception
     */
    public function runGC()
    {
        // Some initial bootstrapping
        $referenceDate = new DateTime('-' . $this->ttlTempTable);
        // Clear Time component, but leave the date part alone
        $referenceDate->setTime(0, 0);
        // Needed to correctly parse the Timestamp from gmdate()
        $timezone = new DateTimeZone("UTC");

        /**
         * @var DatabaseTable[] $tables
         */
        $tables = $this->database->listTablesInDatabase($this->database->getTemporarySchema($this->useTemporaryTables));

        // Walk through all tables and look for ones that seem applicable to be garbage collected
        foreach ($tables as $table) {
            $name = $table->getName();
            if (substr($name, 0, 4) === 'tmp_') {
                $components = explode('_', $name);

                if (is_numeric($components[sizeof($components) - 1])) {
                    // If there is no text-suffix on the generated table name, the second to last entry is the creation time (in "YmdHis")
                    $creationDate = $components[sizeof($components) - 2];
                } else {
                    // If there is a text-suffix on the generated table name, we need to ignore that
                    $creationDate = $components[sizeof($components) - 3];
                }

                // Now examine if the table is too old: If the creationTime is older than our reference then delete, else keep
                $datetime = DateTime::createFromFormat('YmdHis', $creationDate, $timezone);
                if ($datetime < $referenceDate) {
                    // Result is in the past -> delete with DROP TABLE
                    $this->database->exec("DROP TABLE {$table->getFullName(true)}");
                }
            }
        }
    }
}
