<?php
declare(strict_types=1);

namespace Divae\DbConnectors\Testing;

use Divae\DbConnectors\DatabaseConnector;
use Divae\DbConnectors\DatabaseException;
use Exception;
use InvalidArgumentException;
use Psr\Log\LoggerInterface;
use Divae\DbConnectors\ClickHouse;
use Divae\DbConnectors\MySQL;
use Throwable;

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
     * Map of connector class name to connector/ruleset.
     * @see getConnector
     * @var array
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
     * register connectors for the tests
     * temp schemas are created as soon as a test table is added
     *
     * @param string $className the DBAccess class, which is supposed to be used
     * @param DatabaseConnector $connector
     * @param array|null $schemata
     */
    protected static function addConnector(string $className, DatabaseConnector $connector, ?array $schemata): void
    {

        // Process rules
        if ($schemata !== null) {
            // Lowercase all the schemas for easier comparison
            $schemata = array_map('strtolower', $schemata);
        }
        $entry = [
            'connector'         => $connector,
            'rules'             => $schemata,
            'testschemaCreated' => false,
        ];

        // Then add everything to the $connectors Array
        static::$connectors[$className][] = $entry;
    }

    /**
     * Returns a connector of the given class and that serves the selected schema
     * @param string $className
     * @param string $schema
     * @return DatabaseConnector
     */
    protected static function getConnector(string $className, string $schema): DatabaseConnector
    {
        if (array_key_exists($className, static::$connectors)) {
            $lcSchema = strtolower($schema);
            $defaultConnector = null;

            foreach (static::$connectors[$className] as $connectorRules) {
                if ($connectorRules['rules'] === null) {
                    $defaultConnector = $connectorRules['connector'];
                } else {
                    // Rules is an array
                    if (in_array($lcSchema, $connectorRules['rules'])) {
                        // We found the specific connector for this schema -> return it.
                        return $connectorRules['connector'];
                    }
                }
            }

            // Since we are here, there is no specific connector.
            if ($defaultConnector !== null) {
                return $defaultConnector;
            }

            throw new InvalidArgumentException('No default connector for this class found');
        } else {
            throw new InvalidArgumentException('No connector for this class found');
        }
    }

    /**
     * checks, if the test schema already exists and creates it, if necessary
     *
     * @param string $className
     * @param string $schema
     *
     * @throws InvalidArgumentException|DatabaseException
     */
    public static function ensureTestSchemaExists(string $className, string $schema): void
    {
        if (array_key_exists($className, static::$connectors)) {
            $lcSchema = strtolower($schema);
            $selectedConnectorRules = null;

            foreach (static::$connectors[$className] as &$connectorRules) {
                if ($connectorRules['rules'] === null) {
                    $selectedConnectorRules = &$connectorRules;
                } else {
                    // Rules is an array
                    if (in_array($lcSchema, $connectorRules['rules'])) {
                        // We found the specific connector for this schema -> use it.
                        $selectedConnectorRules = &$connectorRules;
                        break;
                    }
                }
            }

            if ($selectedConnectorRules === null) {
                // There was neither a specific connector nor a default one
                throw new InvalidArgumentException('Neither default nor specific connector for this class found');
            }

            if (!$selectedConnectorRules['testschemaCreated']) {
                /** @var DatabaseConnector $connector */
                $connector = $selectedConnectorRules['connector'];
                $connector->exec('CREATE DATABASE IF NOT EXISTS ' . static::getTestSchema($className));

                $selectedConnectorRules['testschemaCreated'] = true;
            }
        } else {
            throw new InvalidArgumentException('No connector for this class found');
        }
    }

    protected static function removeConnectors(string $connectorClassName): void
    {
        foreach (static::$connectors[$connectorClassName] as $connector) {
            try {
                $connector['connector']->close();
            } catch (Throwable $exception) {
                if (self::$logger !== null) {
                    self::$logger->warning("Cannot close a connector. Error:" . $exception->getMessage(),
                        [$exception]);
                }
            }

        }

        unset(static::$connectors[$connectorClassName]);
    }

    protected static function execOnAllConnectors(string $className, string $query): void
    {
        foreach (static::$connectors[$className] as $connector) {
            try {
                $connector['connector']->exec($query);
            } catch (Throwable $exception) {
                if (self::$logger !== null) {
                    self::$logger->warning('Cannot execute on a connector:\n' . $query . '\nError:\n' . $exception->getMessage(),
                        [$exception]);
                }
            }
        }
    }

    /**
     * Stores connector and related test schema name.
     * If no test schema was provided, a random name is generated and the schema is registered for lazy creation. After
     * the test has concluded it will be dropped.
     *
     * @param DatabaseConnector $connector
     * @param null|string $connectorClassName
     * @param null|array $schemata
     *
     * @throws InvalidArgumentException
     */
    public static function initTestSchema(
        DatabaseConnector $connector,
        ?string $connectorClassName = null,
        ?array $schemata = null
    ) {
        if (is_null($connectorClassName)) {
            $connectorClassName = get_class($connector);
        }

        static::addConnector($connectorClassName, $connector, $schemata);

        if (!array_key_exists($connectorClassName, static::$testSchemas)) {
            $testSchema = uniqid('test_' . date('ymdHis_'));
            static::$autoCleanup = true;
            register_shutdown_function(function ($connectorClassName, $testSchema) {
                if (static::$autoCleanup) {
                    static::dropSchema($connectorClassName, $testSchema);
                }
                static::$autoCleanup = false;
            }, $connectorClassName, $testSchema);
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
     *
     * @throws InvalidArgumentException|DatabaseException
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
            if (
                array_key_exists($connectorClassName, static::$connectors) &&
                array_key_exists($connectorClassName, static::$testSchemas)) {
                static::ensureTestSchemaExists($connectorClassName, $schema);

                $oldTestMode = static::$testMode;
                static::disable();
                $connector = static::getConnector($connectorClassName, $schema);
                $connector->cloneTableStructure('`' . $schema . '`.`' . $table . '`',
                    '`' . static::$testSchemas[$connectorClassName] . '`.`' . self::getTestTableName($schema,
                        $table) . '`');
                static::$testMode = $oldTestMode;
            } else {
                throw new InvalidArgumentException('Connector Class unknown or not initialized');
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
                static::ensureTestSchemaExists($connectorClassName, $schema);

                $oldTestMode = static::$testMode;
                static::disable();

                $connector = static::getConnector($connectorClassName, $schema);
                $showCreateResult = $connector->query("SHOW CREATE VIEW `{$schema}`.`{$table}`");
                $showCreate = $showCreateResult->fetch_assoc();

                static::enable();

                if (array_key_exists('Create View', $showCreate)) {
                    $connector->exec($showCreate['Create View']);
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
     * Deletes a schema
     * @param string $connectorClassName
     * @param string $schema
     */
    public static function dropSchema(string $connectorClassName, string $schema): void
    {
        if (array_key_exists($connectorClassName, static::$connectors)) {
            $connectorList = static::$connectors[$connectorClassName];
            foreach ($connectorList as &$connector) {
                if ($connector['testschemaCreated']) {
                    $connector['connector']->exec("DROP DATABASE IF EXISTS {$schema}");
                    $connector['testschemaCreated'] = false;
                }
            }
        }
    }

    /**
     * drop all test tables
     *
     * @param string $connectorClassName
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
                    self::execOnAllConnectors($connectorClassName, $query);
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

                static::removeConnectors($connectorClassName);
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
        if (!array_key_exists(MySQL::class,
                static::$connectors) or !array_key_exists(Clickhouse::class, static::$connectors)) {
            // Either MySQL or Clickhouse is not enabled -> no replacements
            return;
        }

        foreach (static::$tablesBySchema as $schema => $tables) {
            $mysqlCredentials = static::getConnector(MySQL::class, $schema)->getCredentials();
            $credentials = static::getConnector(Clickhouse::class, $schema)->getCredentials();

            foreach ($tables as $table) {
                $tableIndex = array_search($schema . '.' . $table, static::$allTables);

                // Clickhouse -> Clickhouse connection
                // replace host, schema and table names with the test names
                $pattern = "/remote\\(\\s*'[^']*'\\s*,\\s*'" . preg_quote($schema,
                        '/') . "'\\s*,\\s*'" . preg_quote($table, '/') . "'\\s*/ims";
                $replacements[$pattern] = "remote('" . $credentials['host'] . "','" . static::$testSchemas[Clickhouse::class] . "','" . substr($table,
                        0, 55) . '_' . $tableIndex . "'";

                // Clickhouse -> MySQL connection
                $pattern = '/mysql\\(\\s*\'[^\']+\'\\s*,\\s*\'' . preg_quote($schema,
                        '/') . '\'\\s*,\\s*\'' . preg_quote($table, '/') . '\'\\s*/ims';
                $replacements[$pattern] = 'mysql(\'' . $mysqlCredentials['host'] . '\', \'' . (static::$testSchemas[MySQL::class] ?? '') . '\', \'' . substr($table,
                        0, 55) . '_' . $tableIndex . "'";
            }
        }
    }
}