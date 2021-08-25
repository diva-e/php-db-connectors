<?php

namespace Divae\DbConnectors\Test;

use Divae\DbConnectors\ConnFactory;
use Divae\DbConnectors\DatabaseException;
use Divae\DbConnectors\MySQL;
use Monolog\Handler\StreamHandler;
use Monolog\Logger;
use Monolog\Processor\MemoryPeakUsageProcessor;
use Monolog\Processor\MemoryUsageProcessor;
use PHPUnit\Framework\TestCase;

class ConnFactoryTest extends TestCase
{
    public function setUp(): void
    {
        parent::setUp();

        $globalLogger = new Logger('divae-db-component-test');
        $globalHandler = new StreamHandler('php://stdout');
        $globalHandler->pushProcessor(new MemoryUsageProcessor(true, false));
        $globalHandler->pushProcessor(new MemoryPeakUsageProcessor(true, false));
        $globalLogger->setHandlers([$globalHandler]);

        ConnFactory::setLogger($globalLogger);
    }

    public function tearDown(): void
    {
        parent::tearDown();

        ConnFactory::reset();
    }

    /**
     * @throws DatabaseException
     */
    public function testInitMySQLConnection()
    {
        global $globalMySqlCredentials;

        ConnFactory::setMySQLCredentials(
            'test',
            $globalMySqlCredentials['host'],
            $globalMySqlCredentials['username'],
            $globalMySqlCredentials['password']
        );

        $conn = ConnFactory::getMySQLConnection('test');
        $this->assertInstanceOf(MySQL::class, $conn);

        $this->assertNotEquals(0, $conn->getConnectionErrorCode(), 'Database already connected');

        $conn->connect();
        $this->assertEquals(0, $conn->getConnectionErrorCode(), 'Database not connected after calling `connect()`');
        $conn->close();
        $this->assertNotEquals(0, $conn->getConnectionErrorCode(), 'Disconnect not working properly');
    }
}
