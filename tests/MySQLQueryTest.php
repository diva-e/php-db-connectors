<?php

namespace Divae\DbConnectors\Test;

use Divae\DbConnectors\DatabaseException;
use Divae\DbConnectors\MySQL;
use Divae\DbConnectors\Test\Mocks\DummyResult;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

class MySQLQueryTest extends TestCase
{
    /**
     * @param $return_value
     * @return MySQL|MockObject
     */
    protected function buildDriverWithMockedQueryAll($return_value)
    {
        $_Mock = $this->getMockBuilder(MySQL::class)
            ->setMethods(['query'])
            ->getMock();

        $_Mock->expects($this->once())
            ->method('query')
            ->will($this->returnValue(new DummyResult($return_value)));

        return $_Mock;
    }

    /**
     * Flattening results from queryAll() with single result column must work properly.
     *
     * @throws DatabaseException
     */
    public function testQueryAllFlatSingleColumn()
    {
        $_return_value = [
            [
                'column' => 1,
            ],
            [
                'column' => 2,
            ],
            [
                'column' => 3,
            ]
        ];

        $_expected = [1, 2, 3];

        $theQuery = 'bogus';

        $db_driver = $this->buildDriverWithMockedQueryAll($_return_value);

        /**
         * @var MySQL $db_driver
         */
        $_actual = $db_driver->queryAllFlat($theQuery);

        $this->assertEquals($_expected, $_actual);
    }

    /**
     * Flattening results from queryAll() with multiple result columns must work properly.
     *
     * @throws DatabaseException
     */
    public function testQueryAllFlatValid()
    {
        $_return_value = [
            [
                'column1' => 1,
                'column2' => 7,
            ],
            [
                'column1' => 2,
                'column2' => 8,
            ],
            [
                'column1' => 3,
                'column2' => 9,
            ]
        ];

        $_expected = [7, 8, 9];

        $db_driver = $this->buildDriverWithMockedQueryAll($_return_value);
        $_actual = $db_driver->queryAllFlat('bogus', 1);

        $this->assertEquals($_expected, $_actual);
    }

    /**
     * Empty result from queryAll() must yield empty result when flattening too.
     *
     * @throws DatabaseException
     */
    public function testQueryAllFlatEmptyResult()
    {
        $_expected = [];

        $db_driver = $this->buildDriverWithMockedQueryAll([]);
        $_actual = $db_driver->queryAllFlat('bogus');

        $this->assertEquals($_expected, $_actual);
    }

    /**
     * Trying to flatten result from queryAll() with invalid column must trigger an error.
     */
    public function testQueryAllFlatInvalidColumn()
    {
        $this->expectException(DatabaseException::class);

        $_return_value = [
            [
                'column' => 1,
            ],
            [
                'column' => 2,
            ],
            [
                'column' => 3,
            ]
        ];

        $db_driver = $this->buildDriverWithMockedQueryAll($_return_value);
        $db_driver->queryAllFlat('bogus', 1);
    }
}
