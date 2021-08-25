<?php

namespace Divae\DbConnectors\Test;

include __DIR__ . '/mocks/TimeMocks.php';

use Divae\DbConnectors\DatabaseException;
use Divae\DbConnectors\MySQL;
use Divae\DbConnectors\TempTableHandler\TempTableHandler;
use Divae\DbConnectors\TempTableHandler\TimeMocks;
use Monolog\Handler\StreamHandler;
use Monolog\Logger;
use Monolog\Processor\MemoryPeakUsageProcessor;
use Monolog\Processor\MemoryUsageProcessor;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use ReflectionException;
use ReflectionMethod;
use function gmdate;

class TempTableHandlerTest extends TestCase
{
    /**
     * @var LoggerInterface
     */
    private $logger;

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

    public function tearDown(): void
    {
        parent::tearDown();

        // Disable time machine after each test
        TimeMocks::$enabled = false;
    }

    /**
     * @return MySQL
     * @throws DatabaseException
     */
    private function getMySqlConnection(): MySQL
    {
        $mySql = new MySQL();
        $mySql->setLogger($this->logger);

        global $globalMySqlCredentials;
        $mySql->setCredentials($globalMySqlCredentials);

        $mySql->connect();
        return $mySql;
    }

    /**
     * @throws DatabaseException
     * @throws ReflectionException
     */
    public function testDeleteZombieTable()
    {
        $mysql = $this->getMySqlConnection();

        $tth = new TempTableHandler($mysql, '7 days');

        // Enable time machine (the date/time is a lot in the past)
        TimeMocks::$gmdate = gmdate('YmdHis', mktime(0, 0, 0, 1, 1, 2005));
        TimeMocks::$enabled = true;

        // Create a table that should be garbage collected
        $createTableMethod = new ReflectionMethod(TempTableHandler::class, 'createTempTable');
        $createTableMethod->setAccessible(true);
        $tableName = $createTableMethod->invoke($tth, 'zombie', ['`value` INTEGER NOT NULL']);

        $this->assertTrue($mysql->hasTable($tableName), 'Zombie was not created');
        $tth->runGC();
        $this->assertFalse($mysql->hasTable($tableName), 'Zombie was not deleted');
    }


    /**
     * @throws DatabaseException
     * @throws ReflectionException
     */
    public function testCreateAndDropTable()
    {
        $mysql = $this->getMySqlConnection();

        $tth = new TempTableHandler($mysql, '7 days');

        // Create a table
        $createTableMethod = new ReflectionMethod(TempTableHandler::class, 'createTempTable');
        $createTableMethod->setAccessible(true);

        // Run 1: Drop with slug
        $tableName = $createTableMethod->invoke($tth, /*slug*/ 'table1', /*columns*/ ['`value` INTEGER NOT NULL']);
        $this->assertTrue($mysql->hasTable($tableName), 'Temptable was not created');
        $tth->dropTable("table1");
        $this->assertFalse($mysql->hasTable($tableName), 'Temptable was not deleted');

        // Run 2: Drop with table name
        $tableName = $createTableMethod->invoke($tth, 'table1', ['`value` INTEGER NOT NULL']);
        $this->assertTrue($mysql->hasTable($tableName), 'Temptable was not created');
        $tth->dropTable($tableName);
        $this->assertFalse($mysql->hasTable($tableName), 'Temptable was not deleted');
    }

    /**
     * @throws DatabaseException
     * @throws ReflectionException
     */
    public function testCreateAndDropTemporaryTable()
    {
        $mysql = $this->getMySqlConnection();

        $tth = new TempTableHandler($mysql, '7 days', true);

        // Create a table
        $createTableMethod = new ReflectionMethod(TempTableHandler::class, 'createTempTable');
        $createTableMethod->setAccessible(true);

        // Run 1: Drop with slug
        $tableName = $createTableMethod->invoke($tth, /*slug*/ 'table1', /*columns*/ ['`value` INTEGER NOT NULL']);
        $this->assertTrue($mysql->hasTable($tableName, null, true), 'Temptable was not created - 1');
        $tth->dropTable("table1");
        $this->assertFalse($mysql->hasTable($tableName, null, true), 'Temptable was not deleted - 1');

        // Run 2: Drop with table name
        $tableName = $createTableMethod->invoke($tth, 'table1', ['`value` INTEGER NOT NULL']);
        $this->assertTrue($mysql->hasTable($tableName, null, true), 'Temptable was not created - 2');
        $tth->dropTable($tableName);
        $this->assertFalse($mysql->hasTable($tableName, null, true), 'Temptable was not deleted - 2');
    }

    /**
     * @throws DatabaseException
     * @throws ReflectionException
     */
    public function testCreateAndDropAllTables()
    {
        $mysql = $this->getMySqlConnection();

        $tth = new TempTableHandler($mysql, '7 days');

        // Create a table
        $createTableMethod = new ReflectionMethod(TempTableHandler::class, 'createTempTable');
        $createTableMethod->setAccessible(true);

        $tables = [];
        // 1. Create some tables
        for ($i = 0; $i < 10; $i++) {
            $tableName = $createTableMethod->invoke($tth, /*slug*/ 'table' . $i,
                /*columns*/ ['`value` INTEGER NOT NULL']);
            // 2. Check if all tables exist
            $this->assertTrue($mysql->hasTable($tableName), "Temptable ({$i} -> {$tableName}) was not created");

            $tables[] = $tableName;
        }


        $tth->dropAllTables();

        foreach ($tables as $table) {
            $this->assertFalse($mysql->hasTable($table), "Temptable {$table} was not deleted");
        }
    }

    /**
     * @throws DatabaseException
     * @throws ReflectionException
     */
    public function testTruncateTable()
    {
        $mysql = $this->getMySqlConnection();

        $tth = new TempTableHandler($mysql, '7 days');

        // Create a table
        $createTableMethod = new ReflectionMethod(TempTableHandler::class, 'createTempTable');
        $createTableMethod->setAccessible(true);

        // Create a table
        $tableName = $createTableMethod->invoke($tth, /*slug*/ 'table1', /*columns*/ ['`value` INTEGER NOT NULL']);
        $this->assertTrue($mysql->hasTable($tableName), 'Temptable was not created');

        // Fill the table (10 records)
        for ($i = 1; $i <= 10; $i++) {
            $sql = "INSERT INTO {$tableName} (`value`) VALUES ({$i})";
            $mysql->exec($sql);
        }

        // Check the inserts, clean and check the row count again
        $this->assertEquals(10, $mysql->queryFirst("SELECT COUNT(*) count FROM {$tableName}")['count']);
        $tth->cleanTable('table1');
        $this->assertEquals(0, $mysql->queryFirst("SELECT COUNT(*) count FROM {$tableName}")['count']);

        $tth->dropAllTables();
    }

    /**
     * @throws DatabaseException
     * @throws ReflectionException
     */
    public function testGetColumns()
    {
        $columns = ['col1', 'col2', 'col3'];
        $columnDefinition = [
            '`col1` INTEGER',
            '`col2` VARCHAR(15)',
            '`col3` DATETIME'
        ];

        $mysql = $this->getMySqlConnection();

        $tth = new TempTableHandler($mysql, '7 days');

        // Create a table
        $createTableMethod = new ReflectionMethod(TempTableHandler::class, 'createTempTable');
        $createTableMethod->setAccessible(true);

        // Create a table
        $tableName = $createTableMethod->invoke($tth, /*slug*/ 'table1', /*columns*/ $columnDefinition);
        $this->assertTrue($mysql->hasTable($tableName), 'Temptable was not created');

        $this->assertEquals($columns, $tth->getColumnNames('table1'));

        $tth->dropAllTables();
    }
}
