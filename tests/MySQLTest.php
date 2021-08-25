<?php

namespace Divae\DbConnectors\Test;

use Divae\DbConnectors\AbstractConnector;
use Divae\DbConnectors\DatabaseException;
use Divae\DbConnectors\MySQL;
use Monolog\Handler\StreamHandler;
use Monolog\Logger;
use Monolog\Processor\MemoryPeakUsageProcessor;
use Monolog\Processor\MemoryUsageProcessor;
use mysqli_result;
use PHPUnit\Framework\Error\Warning;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use RuntimeException;

class MySQLTest extends TestCase
{

    /**
     * @var LoggerInterface
     */
    private LoggerInterface $logger;

    private string $tmpDbName = 'test';

    public const PERFORMANCE_SCHEMA_ACCOUNTS_EXPECTED_COLUMNS = [
        'USER',
        'HOST',
        'CURRENT_CONNECTIONS',
        'TOTAL_CONNECTIONS'
    ];

    public function setUp(): void
    {
        parent::setUp();

        $globalLogger = new Logger('divae-db-component-test');
        $globalHandler = new StreamHandler('php://stdout');
        $globalHandler->pushProcessor(new MemoryUsageProcessor(true, false));
        $globalHandler->pushProcessor(new MemoryPeakUsageProcessor(true, false));
        $globalLogger->setHandlers([$globalHandler]);

        $this->logger = $globalLogger;
    }

    private function createMySqlConnector(): MySQL
    {
        $mySql = new MySQL();
        $mySql->setLogger($this->logger);

        global $globalMySqlCredentials;
        $mySql->setCredentials($globalMySqlCredentials);

        return $mySql;
    }

    private function createTempTableName(): string
    {
        return uniqid('test_ia_db_component_');
    }

    /**
     * @throws DatabaseException
     */
    public function testExceptionConnect()
    {
        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Cannot connect to the database');

        $mySql = $this->createMySqlConnector();
        $credentials = $mySql->getCredentials();
        $credentials[MySQL::CREDENTIALS_PASSWORD] = 'wrong';
        $mySql->setCredentials($credentials);

        // Suppress warnings. MySQLi::real_connect will always in this test issue a warning about wrong credentials
        // (its kinda expected).
        @$mySql->connect();
    }

    /**
     * @throws DatabaseException
     */
    public function testExceptionQueryNoConnection()
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Not connected to the database');

        $mySql = $this->createMySqlConnector();
        $mySql->query('SELECT 1;');
    }

    /**
     * @throws DatabaseException
     */
    public function testExceptionExecNoConnection()
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Not connected to the database');

        $mySql = $this->createMySqlConnector();
        $mySql->exec('SELECT 1;');
    }

    /**
     * @throws DatabaseException
     */
    public function testExceptionExecRead()
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Read operation in write function');

        $mySql = $this->createMySqlConnector();
        $mySql->connect();
        $mySql->exec('SELECT 1;');
    }

    /**
     * @throws DatabaseException
     */
    public function testExceptionQueryWrite()
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Write operation in read function');

        $mySql = $this->createMySqlConnector();
        $mySql->connect();
        $mySql->query('SET NAMES \'utf8mb4\';');
    }

    /**
     * @throws RuntimeException
     */
    public function testExceptionQueryError()
    {
        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Error on execution');

        $mysql = $this->createMySqlConnector();
        $mysql->connect();
        $sql = "
            SELECT * FROM not_existing.does_not_exist WHERE 1;
        ";
        $mysql->query($sql);
    }

    /**
     * @throws RuntimeException
     */
    public function testExceptionExecError()
    {
        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Error on execution');

        $mysql = $this->createMySqlConnector();
        $mysql->connect();
        $sql = "
            DROP TABLE not_existing.does_not_exist;
        ";
        $mysql->exec($sql);
    }

    /**
     * @throws DatabaseException
     */
    public function testExec()
    {
        $mysql = $this->createMySqlConnector();
        $mysql->connect();

        $tempTableName = $this->createTempTableName();

        $sql = "
            CREATE TEMPORARY TABLE {$this->tmpDbName}.{$tempTableName}
            (
                column_1 INT PRIMARY KEY AUTO_INCREMENT,
                column_2 VARCHAR(255)
            );
        ";
        $result = $mysql->exec($sql);
        $this->assertSame(0, $result);

        $sql = "
            INSERT INTO {$this->tmpDbName}.{$tempTableName}
            (column_2)
            VALUES
            ('a'),('b'),('c')
        ";
        $result = $mysql->exec($sql);
        $this->assertSame(3, $result);

        $sql = "
            INSERT INTO {$this->tmpDbName}.{$tempTableName}
            (column_2)
            VALUES
            ('d')
        ";
        $mysql->exec($sql);
        $this->assertSame(4, $mysql->getInsertId());

        $sql = "
            DELETE FROM {$this->tmpDbName}.{$tempTableName} WHERE 1
        ";
        $result = $mysql->exec($sql);
        $this->assertSame(4, $result);
    }

    /**
     * @throws DatabaseException
     */
    public function testQuery()
    {
        $mysql = $this->createMySqlConnector();
        $mysql->connect();

        $sql = "
            SELECT 1 AS test
        ";
        $result = $mysql->query($sql);
        $this->assertInstanceOf(mysqli_result::class, $result);
        $row = $mysql->fetchRow($result);
        $this->assertIsArray($row);
        $this->assertSame(['test' => '1'], $row);
        $row = $mysql->fetchRow($result);
        $this->assertNull($row);

        $sql = "
            SELECT 1 AS test, 2 AS rest
        ";
        $result = $mysql->query($sql);
        $this->assertInstanceOf(mysqli_result::class, $result);
        $rows = $mysql->fetchAll($result);
        $this->assertIsArray($rows);
        $this->assertCount(1, $rows);
        $this->assertSame([0 => ['test' => '1', 'rest' => '2']], $rows);
        $rows = $mysql->fetchAll($result);
        $this->assertIsArray($rows);
        $this->assertEmpty($rows);

        $sql = "
            SELECT 1 AS test, 2 AS rest
        ";
        $result = $mysql->query($sql);
        $this->assertInstanceOf(mysqli_result::class, $result);
        $rows = $mysql->fetchAll($result, 'test');
        $this->assertIsArray($rows);
        $this->assertCount(1, $rows);
        $this->assertSame(['1' => ['test' => '1', 'rest' => '2']], $rows);
    }

    /**
     * @throws DatabaseException
     */
    public function testTransaction()
    {
        $mysql = $this->createMySqlConnector();
        $mysql->connect();

        $tempTableName = $this->createTempTableName();

        $sql = "
            CREATE TEMPORARY TABLE {$this->tmpDbName}.{$tempTableName}
            (
                column_1 INT PRIMARY KEY AUTO_INCREMENT,
                column_2 VARCHAR(255)
            );
        ";
        $mysql->exec($sql);

        $this->assertTrue($mysql->beginTransaction());
        $this->assertTrue($mysql->inTransaction());
        $this->assertFalse($mysql->beginTransaction());

        $sql = "
            INSERT INTO {$this->tmpDbName}.{$tempTableName}
            (column_2)
            VALUES
            ('a')
        ";
        $result = $mysql->exec($sql);
        $this->assertSame(1, $result);
        $this->assertSame(1, $mysql->getInsertId());

        $sql = "
            INSERT INTO {$this->tmpDbName}.{$tempTableName}
            (column_2)
            VALUES
            ('b')
        ";
        $result = $mysql->exec($sql);
        $this->assertSame(1, $result);
        $this->assertSame(2, $mysql->getInsertId());

        $result = $mysql->commit('test commit');
        $this->assertTrue($result);

        $this->assertFalse($mysql->inTransaction());

        $sql = "
            SELECT * FROM {$this->tmpDbName}.{$tempTableName} WHERE 1
        ";
        $result = $mysql->query($sql);
        $rows = $mysql->fetchAll($result);
        $this->assertSame([
            ['column_1' => '1', 'column_2' => 'a'],
            ['column_1' => '2', 'column_2' => 'b'],
        ], $rows);

        $this->assertTrue($mysql->beginTransaction());
        $this->assertTrue($mysql->inTransaction());
        $this->assertFalse($mysql->beginTransaction());

        $sql = "
            INSERT INTO {$this->tmpDbName}.{$tempTableName}
            (column_2)
            VALUES
            ('should be reverted')
        ";
        $result = $mysql->exec($sql);
        $this->assertSame(1, $result);
        $this->assertSame(3, $mysql->getInsertId());

        $result = $mysql->rollback('test rollback');
        $this->assertTrue($result);
        $this->assertFalse($mysql->inTransaction());

        $sql = "
            SELECT * FROM {$this->tmpDbName}.{$tempTableName} WHERE 1
        ";
        $result = $mysql->query($sql);
        $rows = $mysql->fetchAll($result);
        $this->assertSame([
            ['column_1' => '1', 'column_2' => 'a'],
            ['column_1' => '2', 'column_2' => 'b'],
        ], $rows);
    }

    /**
     * @throws DatabaseException
     */
    public function testClose()
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Not connected to the database');

        $mysql = $this->createMySqlConnector();
        $mysql->connect();
        $mysql->close();

        $mysql->query('SELECT 1;');
    }

    public function testDebugInfo()
    {
        $mysql = $this->createMySqlConnector();
        $info = $mysql->__debugInfo();
        $this->assertIsArray($info);
        $this->assertArrayHasKey('credentials', $info);
        $this->assertArrayHasKey('settings', $info);
        $this->assertArrayHasKey('counters', $info);
        $this->assertArrayHasKey('properties', $info);
    }

    /**
     * @throws DatabaseException
     */
    public function testEscape()
    {
        $mysql = $this->createMySqlConnector();
        $mysql->connect();
        $this->assertSame('NULL', $mysql->escape(null));
        $this->assertSame('1', $mysql->escape(true));
        $this->assertSame('0', $mysql->escape(false));
        $this->assertSame('123', $mysql->escape(123));
        $this->assertSame('test', $mysql->escape('test'));
        $this->assertSame('', $mysql->escape(''));
        $this->assertSame("a\';DROP TABLE test; SELECT * FROM rest WHERE \'x\' = \'",
            $mysql->escape("a';DROP TABLE test; SELECT * FROM rest WHERE 'x' = '"));
    }

    /**
     * @throws DatabaseException
     */
    public function testQueryAsync()
    {
        $mysql = $this->createMySqlConnector();
        $mysql->connect();
        $this->assertFalse($mysql->getAsyncResult());
        $this->assertSame(AbstractConnector::RESULT_STATE_UNKNOWN, $mysql->checkResultReady());
        $sql = "
            SELECT 1 AS test;
        ";
        $result = $mysql->query($sql, [AbstractConnector::OPTION_ASYNC => true]);
        $this->assertTrue($result);
        $this->assertContains($mysql->checkResultReady(), [
            AbstractConnector::RESULT_STATE_READY,
            AbstractConnector::RESULT_STATE_PENDING,
        ]);
        sleep(1);
        $this->assertSame(AbstractConnector::RESULT_STATE_READY, $mysql->checkResultReady());
        $result = $mysql->getAsyncResult();
        $this->assertInstanceOf(mysqli_result::class, $result);
        $rows = $mysql->fetchAll($result);
        $this->assertSame([['test' => '1']], $rows);

        $sql = "
            SELECT * FROM not_existing.does_not_exist;
        ";
        $result = $mysql->query($sql, [AbstractConnector::OPTION_ASYNC => true]);
        $this->assertTrue($result);
        sleep(1);
        $this->assertSame(AbstractConnector::RESULT_STATE_READY, $mysql->checkResultReady());
        $result = $mysql->getAsyncResult();
        $this->assertFalse($result);
    }

    /**
     * @throws DatabaseException
     */
    public function testExecAsync()
    {
        $mysql = $this->createMySqlConnector();
        $mysql->connect();
        $this->assertFalse($mysql->getAsyncResult());
        $this->assertSame(AbstractConnector::RESULT_STATE_UNKNOWN, $mysql->checkResultReady());

        $tempTableName = $this->createTempTableName();

        $sql = "
            CREATE TEMPORARY TABLE {$this->tmpDbName}.{$tempTableName}
            (
                column_1 INT PRIMARY KEY AUTO_INCREMENT,
                column_2 VARCHAR(255)
            );
        ";
        $result = $mysql->exec($sql);
        $this->assertSame(0, $result);

        $sql = "
            INSERT INTO {$this->tmpDbName}.{$tempTableName}
            (column_2)
            VALUES
            ('a'),('b'),('c')
        ";
        $result = $mysql->exec($sql, [AbstractConnector::OPTION_ASYNC => true]);
        $this->assertSame(0, $result);
        sleep(1);
        $this->assertSame(AbstractConnector::RESULT_STATE_READY, $mysql->checkResultReady());
        $result = $mysql->getAsyncResult();
        $this->assertSame(3, $result);

        $sql = "
            DROP TEMPORARY TABLE {$this->tmpDbName}.{$tempTableName};
        ";
        $result = $mysql->exec($sql);
        $this->assertSame(0, $result);

        $result = $mysql->exec($sql, [AbstractConnector::OPTION_ASYNC => true]);
        $this->assertSame(0, $result);
        sleep(1);
        $this->assertSame(AbstractConnector::RESULT_STATE_READY, $mysql->checkResultReady());
        $result = $mysql->getAsyncResult();
        $this->assertFalse($result);
    }

    public function testCounters()
    {
        $mysql = $this->createMySqlConnector();

        $counters = $mysql->getCounters();
        $this->assertArrayNotHasKey(AbstractConnector::COUNTER_CREATION_TIME, $counters);
        $this->assertArrayHasKey(AbstractConnector::COUNTER_ROWS_READ, $counters);
        $this->assertArrayHasKey(AbstractConnector::COUNTER_WRITE, $counters);
        $this->assertArrayHasKey(AbstractConnector::COUNTER_TOTAL_QUERIES, $counters);
        $this->assertArrayHasKey(AbstractConnector::COUNTER_ROWS_WRITTEN, $counters);
        $this->assertArrayHasKey(AbstractConnector::COUNTER_ROWS_READ, $counters);
        $this->assertArrayHasKey(AbstractConnector::COUNTER_TOTAL_EXECUTION_TIME, $counters);
        $this->assertArrayHasKey(AbstractConnector::COUNTER_TOTAL_ELAPSED_TIME, $counters);
        $this->assertArrayHasKey(AbstractConnector::COUNTER_TOTAL_OUT_OF_DATABASE_TIME, $counters);
    }

    /**
     * @throws DatabaseException
     */
    public function testConnectionErrorCode()
    {
        $mysql = $this->createMySqlConnector();

        // The initial value should be not connected
        $this->assertEquals(-1, $mysql->getConnectionErrorCode(), "Wrong error code");

        // After a successful connect, there should be no error
        $mysql->connect();
        $this->assertEquals(0, $mysql->getConnectionErrorCode(), "Error on connection");

        // After a disconnect we should be back to the original state
        $mysql->close();
        $this->assertEquals(-1, $mysql->getConnectionErrorCode(), "Wrong error code");

    }

    /**
     * @throws DatabaseException
     */
    public function testHasTable()
    {
        $mysql = $this->createMySqlConnector();
        $mysql->connect();

        // Call with 2 parameters
        $this->assertTrue($mysql->hasTable('information_schema', 'TABLES'));
        $this->assertFalse($mysql->hasTable('not_existing', 'does_not_exist'));

        // Same but call with only 1 parameter
        $this->assertTrue($mysql->hasTable('information_schema.TABLES'));
        $this->assertFalse($mysql->hasTable('not_existing.does_not_exist'));

        $mysql->close();
    }

    /**
     * @throws DatabaseException
     */
    public function testListTables()
    {
        $mysql = $this->createMySqlConnector();
        $mysql->connect();

        // Batch 1: information_schema.PLUGINS
        $tables = $mysql->listTables("`TABLE_SCHEMA` = 'information_schema' and `TABLE_NAME` = 'PLUGINS'");

        $this->assertTrue($tables->hasAtLeastOneTable());
        $this->assertTrue($tables->hasExactlyOneTable());

        $table = $tables->get(0);
        $this->assertEquals('information_schema.PLUGINS', $table->getFullName());
        $this->assertEquals('`information_schema`.`PLUGINS`', $table->getFullName(true));

        // Batch 2: information_schema.TABLE*
        $tables = $mysql->listTables("`TABLE_SCHEMA` = 'information_schema' and `TABLE_NAME` like 'TABLE%'");

        $this->assertTrue($tables->hasAtLeastOneTable());
        $this->assertFalse($tables->hasExactlyOneTable());
    }


    /**
     * @throws DatabaseException
     */
    public function testListColumnsFromTables()
    {
        $mysql = $this->createMySqlConnector();
        $mysql->connect();

        // Table: performance_schema.accounts
        $tables = $mysql->listTables("`TABLE_SCHEMA` = 'performance_schema' and `TABLE_NAME` = 'accounts'");

        $this->assertTrue($tables->hasAtLeastOneTable());
        $this->assertTrue($tables->hasExactlyOneTable());

        // Now get the table
        $table = $tables->get(0);
        $this->assertEquals(sizeof(self::PERFORMANCE_SCHEMA_ACCOUNTS_EXPECTED_COLUMNS), $table->countColumns());

        // Get the columns
        $list = $table->listColumns();
        $this->assertIsArray($list);
        $this->assertEquals(self::PERFORMANCE_SCHEMA_ACCOUNTS_EXPECTED_COLUMNS, $list);
    }

    /**
     * @test
     * @throws DatabaseException
     */
    public function testGetColumnListWithTable()
    {
        $mysql = $this->createMySqlConnector();
        $mysql->connect();

        // Table 1: With data
        $table1 = $mysql->getColumnList('performance_schema', 'accounts');
        $this->assertIsArray($table1);
        $this->assertEquals(self::PERFORMANCE_SCHEMA_ACCOUNTS_EXPECTED_COLUMNS, $table1,
            'The Column List for performance_schema.accounts does not match');

        // Table 2: Not existing table -> no data
        $table2 = $mysql->getColumnList('not_existing', 'does_not_exist');
        $this->assertIsArray($table2);
        $this->assertEmpty($table2);
    }

    /**
     * @test
     * @throws DatabaseException
     */
    public function testGetColumnListWithWHERECondition()
    {
        $expectedColumns = ['USER', 'HOST'];

        $mysql = $this->createMySqlConnector();
        $mysql->connect();

        // Table 1: With data (The accounts table is checked in full in the testGetColumnListWithTable() test)
        $table1 = $mysql->getColumnList("
            TABLE_SCHEMA = 'performance_schema' AND
            TABLE_NAME = 'accounts' AND
            IS_NULLABLE = 'YES'"
        );

        $this->assertIsArray($table1);
        $this->assertEquals($expectedColumns, $table1,
            'The Column List for performance_schema.accounts does not match, did testGetColumnListWithTable() succeed?');

        // Table 2: No matching condition -> no data
        $table2 = $mysql->getColumnList('false');
        $this->assertIsArray($table2);
        $this->assertEmpty($table2);
    }

    /**
     * @test
     * @throws DatabaseException
     */
    public function testGetColumnCount()
    {
        $mysql = $this->createMySqlConnector();
        $mysql->connect();

        // Table 1: With data
        $table1 = $mysql->getColumnCount('performance_schema', 'accounts');
        $this->assertIsInt($table1);
        $this->assertEquals(sizeof(self::PERFORMANCE_SCHEMA_ACCOUNTS_EXPECTED_COLUMNS), $table1,
            'The Column List for performance_schema.accounts does not match');

        // Table 2: Not existing table -> no data
        $table2 = $mysql->getColumnCount('not_existing', 'does_not_exist');
        $this->assertIsInt($table2);
        $this->assertEquals(0, $table2);
    }

    /**
     * @throws DatabaseException
     */
    public function testGetKeysOnExistingTable()
    {
        $mysql = $this->createMySqlConnector();
        $mysql->connect();

        // Call with default parameters
        $keys1 = $mysql->getKeyColumnNames('mysql.help_topic');
        $this->assertIsArray($keys1);
        $this->assertEquals(['help_topic_id'], $keys1);

        // Call with another key
        $keys2 = $mysql->getKeyColumnNames('mysql.help_topic', 'name');
        $this->assertIsArray($keys2);
        $this->assertEquals(['name'], $keys2);
    }

    /**
     * @throws DatabaseException
     */
    public function testGetKeysOnNonExistingTable()
    {
        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Table \'mysql.help_topic\' does not exist in DB!');

        $mysql = $this->createMySqlConnector();
        $mysql->connect();

        // Call with non-existing table (mysql.help_topic is a non-temporary table)
        $mysql->getKeyColumnNames('mysql.help_topic', 'PRIMARY', true);
    }

    /**
     * @throws DatabaseException
     */
    public function testMutualColumns()
    {
        $mysql = $this->createMySqlConnector();
        $mysql->connect();
        $array1 = $mysql->getMutualColumnsArray('information_schema.TABLES', 'information_schema.COLUMNS');
        $array2 = $mysql->getMutualColumnsArray('information_schema.COLUMNS', 'information_schema.TABLES');
        $list = $mysql->getMutualColumnsList('information_schema.TABLES', 'information_schema.COLUMNS');

        // Test the array
        $this->assertIsArray($array1);
        $this->assertEquals(['TABLE_CATALOG', 'TABLE_SCHEMA', 'TABLE_NAME'], $array1);

        // Test if you can swap the tables and still get the same result
        $this->assertEquals($array1, $array2);

        // Test the list
        $this->assertIsString($list);
        $this->assertEquals("TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME", $list);
    }
}
