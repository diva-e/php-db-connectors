<?php
declare(strict_types=1);

namespace Divae\DbConnectors\Testing;

use Divae\DbConnectors\DatabaseConnector;
use Divae\DbConnectors\DatabaseException;
use Exception;
use Psr\Log\LoggerInterface;
use Divae\DbConnectors\ClickHouse;

/**
 * tables used for testing are created within a test schema. This class is responsible to create the schema and the test
 * tables and replaces the table references in the queries.
 */
class TableHandler
{
    /**
     * @var bool
     */
    protected static bool $testMode = true;

    /**
     * List of all replaced table names in the form schema -> table names
     * @var string[][] (Map<string, Map<string, string>>)
     */
    protected static array $tablesBySchema = [];

    /**
     * List of all replaced tables in the form as values (schema.table)
     * @var string[]
     */
    protected static array $allTables = [];

    /**
     * @var array
     */
    protected static array $customReplacements = [];

    /**
     * Maps database connector class and the created test schema on that connection
     * @var string[] (Map<string, string))
     */
    protected static array $testSchemas = [];

    /**
     * List of database connectors to clean up afterwards
     * @var DatabaseConnector[]
     */
    protected static array $connectors = [];

    /**
     * Determines if all created test schemas should be dropped on shutdown
     * @var bool
     */
    protected static bool $autoCleanup = false;

    /**
     * @var LoggerInterface|null
     */
    private static ?LoggerInterface $logger = null;

    public static function setLogger(LoggerInterface $logger): void
    {
        self::$logger = $logger;
    }

    /**
     * Stores connector and related test schema name.
     * If no test schema was provided, a random name is generated and the schema is created and dropped at the end.
     *
     * @param DatabaseConnector $connector
     * @param null|string $testSchema
     *
     * @throws DatabaseException
     */
    public static function initTestSchema(DatabaseConnector $connector, ?string $testSchema = null): void
    {
        $connectorClassName = get_class($connector);
        static::$connectors[$connectorClassName] = $connector;

        if (!array_key_exists($connectorClassName, static::$testSchemas)) {
            if (!$testSchema) {
                $testSchema = uniqid('test_' . date('ymdHis_'));
                static::$autoCleanup = true;
                static::createSchema($connectorClassName, $testSchema);
                register_shutdown_function(function ($connectorClassName, $testSchema) {
                    if (static::$autoCleanup) {
                        static::dropSchema($connectorClassName, $testSchema);
                    }
                    static::$autoCleanup = false;
                }, $connectorClassName, $testSchema);
            }
            static::$testSchemas[$connectorClassName] = $testSchema;
        }
    }

    /**
     * Main function to edit the SQL to query test databases
     *
     * @param string $connectorClassName
     * @param string $statement
     *
     * @return string
     */
    public static function parseStatement(string $connectorClassName, string $statement): string
    {
        // replace original table references with the references to the test tables
        if (static::$testMode && array_key_exists($connectorClassName, static::$testSchemas)) {
            $replacements = [];
            if (static::$tablesBySchema) {
                foreach (static::$tablesBySchema as $schema => $tables) {
                    foreach ($tables as $table) {
                        $pattern = '/[`]?' . preg_quote($schema, '/') . '[`]?\\.[`]?' . preg_quote($table,
                                '/') . '[`]?([^a-zA-Z0-9_])/ms';
                        $replacements[$pattern] = '`' . static::$testSchemas[$connectorClassName] . '`.`' . self::getTestTableName($schema,
                                $table) . '`\1';
                    }
                }

                // add replacements for ClickHouse cross server usage
                if ($connectorClassName == ClickHouse::class) {
                    static::addClickhouseRemoteReplacements($replacements);
                }
            }
            $replacements = array_merge($replacements, static::$customReplacements);

            $statement = preg_replace(array_keys($replacements), $replacements, $statement . ' ');
        }

        return $statement;
    }

    /**
     * Generates a name for the table in the test schema.
     *
     * Format is <Table Name>_<Index of this table in $allTables>
     *
     * @param string $schema
     * @param string $tableName
     *
     * @return string
     */
    public static function getTestTableName(string $schema, string $tableName): string
    {
        $tableIndex = array_search($schema . '.' . $tableName, static::$allTables);

        return substr($tableName, 0, 55) . '_' . $tableIndex;
    }

    /**
     * Adds a table to the replacement list and clones the structure to the test schema
     *
     * @param string $connectorClassName
     * @param string $schema
     * @param string $table
     */
    public static function addTable(string $connectorClassName, string $schema, string $table): void
    {
        $schema = trim($schema, ' `');
        $table = trim($table, ' `');


        if (!array_key_exists($schema, static::$tablesBySchema)) {
            static::$tablesBySchema[$schema] = [];
        }

        if (!in_array($table, static::$tablesBySchema[$schema])) {
            static::$allTables[] = $schema . '.' . $table;
            static::$tablesBySchema[$schema][] = $table;
            if (array_key_exists($connectorClassName, static::$connectors) && array_key_exists($connectorClassName,
                    static::$testSchemas)) {
                $oldTestMode = static::$testMode;
                static::disable();
                static::$connectors[$connectorClassName]->cloneTableStructure('`' . $schema . '`.`' . $table . '`',
                    '`' . static::$testSchemas[$connectorClassName] . '`.`' . self::getTestTableName($schema,
                        $table) . '`');
                static::$testMode = $oldTestMode;
            }
        }
    }

    /**
     * Adds a view to the replacement list and recreates it in the test schema
     *
     * @param string $connectorClassName
     * @param string $schema
     * @param string $table
     *
     * @throws DatabaseException
     */
    public static function addView(string $connectorClassName, string $schema, string $table): void
    {
        $schema = trim($schema, ' `');
        $table = trim($table, ' `');


        if (!array_key_exists($schema, static::$tablesBySchema)) {
            static::$tablesBySchema[$schema] = [];
        }

        if (!in_array($table, static::$tablesBySchema[$schema])) {
            static::$allTables[] = $schema . '.' . $table;
            static::$tablesBySchema[$schema][] = $table;
            if (array_key_exists($connectorClassName, static::$connectors) &&
                array_key_exists($connectorClassName, static::$testSchemas)) {

                $oldTestMode = static::$testMode;
                static::disable();

                $showCreateResult = static::$connectors[$connectorClassName]->query("SHOW CREATE VIEW `{$schema}`.`{$table}`");
                $showCreate = $showCreateResult->fetch_assoc();

                static::enable();

                if (array_key_exists('Create View', $showCreate)) {
                    static::$connectors[$connectorClassName]->exec($showCreate['Create View']);
                }

                static::$testMode = $oldTestMode;
            }
        }
    }

    /**
     * Adds a custom replacement to the list
     * @param string $pattern
     * @param string $replace
     */
    public static function addCustomReplacement(string $pattern, string $replace): void
    {
        static::$customReplacements[$pattern] = $replace;
    }

    /**
     * Creates a test schema
     * @param string $connectorClassName
     * @param string $schema
     *
     * @throws DatabaseException
     */
    public static function createSchema(string $connectorClassName, string $schema): void
    {
        if (array_key_exists($connectorClassName, static::$connectors)) {
            static::$connectors[$connectorClassName]->exec("CREATE DATABASE IF NOT EXISTS {$schema}");
        }
    }

    /**
     * Deletes a schema
     * @param string $connectorClassName
     * @param string $schema
     *
     * @throws DatabaseException
     */
    public static function dropSchema(string $connectorClassName, string $schema): void
    {
        if (array_key_exists($connectorClassName, static::$connectors)) {
            static::$connectors[$connectorClassName]->exec("DROP DATABASE IF EXISTS {$schema}");
        }
    }

    /**
     * drop all test tables
     *
     * @param string $connectorClassName
     *
     * @throws DatabaseException
     */
    public static function cleanUp(string $connectorClassName): void
    {
        if (static::$tablesBySchema
            && array_key_exists($connectorClassName, static::$testSchemas)
            && array_key_exists($connectorClassName, static::$connectors)) {
            foreach (static::$tablesBySchema as $tables) {
                foreach ($tables as $table) {
                    $oldTestMode = static::$testMode;
                    static::disable();
                    $query = 'DROP TABLE IF EXISTS `' . static::$testSchemas[$connectorClassName] . '`.`' . $table . '`';
                    static::$connectors[$connectorClassName]->exec($query);
                    static::$testMode = $oldTestMode;
                }
            }
            static::$tablesBySchema[static::$testSchemas[$connectorClassName]] = [];
        }
    }

    /**
     * drop all test schemas and close connections
     */
    public static function shutDownTestSchemas(): void
    {
        // remove schemas and close connections
        foreach (static::$testSchemas as $connectorClassName => $testSchema) {
            if (array_key_exists($connectorClassName, static::$connectors)) {
                // if removal of schema failed for some reason, we catch the exception and move on
                // usual it is removed anyways and the error is a false alarm
                try {
                    static::dropSchema($connectorClassName, static::$testSchemas[$connectorClassName]);
                } catch (Exception $e) {
                    if (self::$logger !== null) {
                        self::$logger->warning($e->getMessage());
                    }
                }
                unset(static::$testSchemas[$connectorClassName]);

                static::$connectors[$connectorClassName]->close();
                unset(static::$connectors[$connectorClassName]);
            }
        }

        // reset replacements
        static::$tablesBySchema = [];
        static::$allTables = [];
        static::$customReplacements = [];
    }

    /**
     * enable table replacement
     */
    public static function enable(): void
    {
        static::$testMode = true;
    }

    /**
     * disable table replacement
     */
    public static function disable(): void
    {
        static::$testMode = false;
    }

    /**
     * @param string $connectorClassName
     *
     * @return null|string
     */
    public static function getTestSchema(string $connectorClassName): ?string
    {
        if (array_key_exists($connectorClassName, static::$testSchemas)) {
            return static::$testSchemas[$connectorClassName];
        }

        return null;
    }

    /**
     * add replacements for Clickhouse cross server usage
     * @url https://clickhouse.yandex/docs/en/query_language/table_functions/remote/
     *
     * @param array $replacements
     */
    protected static function addClickhouseRemoteReplacements(array &$replacements): void
    {
        // get Clickhouse test server settings
        $credentials = static::$connectors[ClickHouse::class]->getCredentials();

        foreach (static::$tablesBySchema as $schema => $tables) {
            foreach ($tables as $table) {
                $tableIndex = array_search($schema . '.' . $table, static::$allTables);

                // replace host, schema and table names with the test names
                $pattern = "/remote\\(\\s*'[^']*'\\s*,\\s*'" . preg_quote($schema,
                        '/') . "'\\s*,\\s*'" . preg_quote($table, '/') . "'\\s*/ims";
                $replacements[$pattern] = "remote('" . $credentials['host'] . "','" . static::$testSchemas[Clickhouse::class] . "','" . substr($table,
                        0, 55) . '_' . $tableIndex . "'";
            }
        }
    }
}