<?php

namespace Divae\DbConnectors\Test;


use Divae\DbConnectors\AbstractConnector;
use Divae\DbConnectors\ClickHouse;
use Divae\DbConnectors\DatabaseException;
use Divae\DbConnectors\Schema\DatabaseTable;
use PHPUnit\Framework\TestCase;

class ClickHouseTest extends TestCase
{
    // Column-Layout: https://clickhouse-docs.readthedocs.io/en/latest/system_tables/system.one.html

    public function testNothing()
    {
        $clickHouse = new ClickHouse();

        $this->assertInstanceOf(AbstractConnector::class, $clickHouse);
    }

    /**
     * @return ClickHouse
     */
    public function getClickHouseConnection(): ClickHouse
    {
        global $globalClickhouseCredentials;

        $CLH = new ClickHouse();
        $CLH->setCredentials($globalClickhouseCredentials);

        $CLH->connect();
        return $CLH;
    }

    /**
     * @throws DatabaseException
     */
    public function testCanQuery()
    {
        $CLH = $this->getClickHouseConnection();
        $result = $CLH->query("SELECT 1");

        $rowNum = $result->content['rows'];
        $this->assertEquals(1, $rowNum, 'The query did not return 1 row');
    }

    /**
     * @throws DatabaseException
     */
    public function testHasTable()
    {
        $CLH = $this->getClickHouseConnection();

        // Call with 2 parameters
        $this->assertTrue($CLH->hasTable('system', 'one'));
        $this->assertFalse($CLH->hasTable('not_existing', 'does_not_exist'));

        // Same but call with only 1 parameter
        $this->assertTrue($CLH->hasTable('system.one'));
        $this->assertFalse($CLH->hasTable('not_existing.does_not_exist'));

        $CLH->close();
    }

    /**
     * @throws DatabaseException
     */
    public function testListTables()
    {
        $CLH = $this->getClickHouseConnection();

        // Tests if there are some tables from the documentation in the system database
        $tables = $CLH->listTablesInDatabase('system');

        // Dumb down the object list to an array, so we can assertContains()
        // Does not work with array_map, because it wants an array, not an iterable :(
        $tables_raw = [];
        /**
         * @var DatabaseTable $item
         */
        foreach ($tables as $item) {
            $tables_raw[] = $item->getName();
        }

        // List of system tables. Those should be available
        // https://clickhouse-docs.readthedocs.io/en/latest/system_tables/index.html
        $expectedTables = [
            'clusters',
            'columns',
            'databases',
            'dictionaries',
            'events',
            'functions',
            'merges',
            'metrics',
            'numbers',
            'numbers_mt',
            'one',
            'parts',
            'processes',
            'replicas',
            'settings',
            'tables',
            'zookeeper'
        ];

        // Since assertArraySubset is deprecated, check if the tables array contains all the entries we are looking for
        foreach ($expectedTables as $item) {
            $this->assertContains($item, $tables_raw);
        }

    }


    /**
     * @test
     * @throws DatabaseException
     */
    public function testGetColumnListWithTable()
    {
        $expectedColumns = [
            'database',
            'name',
            'uuid',
            'engine',
            'is_temporary',
            'data_paths',
            'metadata_path',
            'metadata_modification_time',
            'dependencies_database',
            'dependencies_table',
            'create_table_query',
            'engine_full',
            'partition_key',
            'sorting_key',
            'primary_key',
            'sampling_key',
            'storage_policy',
            'total_rows',
            'total_bytes',
            'lifetime_rows',
            'lifetime_bytes'
        ];
        $CLH = $this->getClickHouseConnection();

        // Table 1: With data
        $table1 = $CLH->getColumnList('system', 'tables');
        $this->assertIsArray($table1);
        $this->assertEquals($expectedColumns, $table1, 'The Column List for system.tables does not match');

        // Table 2: Not existing table -> no data
        $table2 = $CLH->getColumnList('not_existing', 'does_not_exist');
        $this->assertIsArray($table2);
        $this->assertEmpty($table2);
    }

    /**
     * @test
     * @throws DatabaseException
     */
    public function testGetColumnListWithWHERECondition()
    {
        $expectedColumns = ['position', 'data_compressed_bytes', 'data_uncompressed_bytes', 'marks_bytes'];


        $CLH = $this->getClickHouseConnection();
        $CLH->connect();

        // Table 1: With data
        $table1 = $CLH->getColumnList("`database` = 'system' and `table` = 'columns' and `type` = 'UInt64'");
        $this->assertIsArray($table1);
        $this->assertEquals($expectedColumns, $table1, 'The Column List for system.columns does not match');

        // Table 2: No matching condition -> no data
        $table2 = $CLH->getColumnList('0');
        $this->assertIsArray($table2);
        $this->assertEmpty($table2);
    }

    /**
     * @test
     * @throws DatabaseException
     */
    public function testGetColumnCount()
    {
        $CLH = $this->getClickHouseConnection();
        // Table 1: system.one
        $this->assertEquals(1, $CLH->getColumnCount('system', 'one'), 'Column count for system.one is off');

        // Table 2: Not existing table -> no data
        $table2 = $CLH->getColumnCount('not_existing', 'does_not_exist');
        $this->assertIsInt($table2);
        $this->assertEquals(0, $table2);
    }

    /**
     * @test
     * @throws DatabaseException
     */
    public function testTemporaryTables()
    {
        $CLH = $this->getClickHouseConnection();
        $CLH->startSession();

        $CLH->exec('CREATE TEMPORARY TABLE `mytemptable` (col String)');
        $CLH->exec('INSERT INTO `mytemptable` (col) VALUES (\'abcdef\')');

        $this->assertTrue($CLH->hasTable('', 'mytemptable', true), 'The Temp Table does somehow not exist');
        $this->assertEquals([['col' => "abcdef"]], $CLH->queryAll('SELECT * FROM `mytemptable`'));
    }
}