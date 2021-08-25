<?php
declare(strict_types=1);

namespace Divae\DbConnectors\Schema;

use ArrayIterator;
use Divae\DbConnectors\ClickHouse;
use Divae\DbConnectors\MySQL;
use InvalidArgumentException;
use IteratorAggregate;
use Traversable;

class TableCollection implements IteratorAggregate
{
    /**
     * @var DatabaseTable[]
     */
    private array $tables = [];

    /**
     * MySQLTableCollection constructor.
     * This class should only be instantiated via static methods
     */
    private function __construct()
    {
    }

    private function addTable(DatabaseTable $table)
    {
        $this->tables[] = $table;
    }

    public static function fromMySQLTableData(MySQL $database, array $data): self
    {
        $result = new self();
        foreach ($data as $tableData) {
            $result->addTable(new DatabaseTable($database, $tableData['TABLE_SCHEMA'], $tableData['TABLE_NAME'],
                $tableData['ENGINE'], $tableData['TABLE_ROWS']));
        }

        return $result;
    }

    public static function fromClickHouseTableData(ClickHouse $database, array $data): self
    {
        $result = new self();
        foreach ($data as $tableData) {
            $result->addTable(new DatabaseTable($database, $tableData['database'], $tableData['name'],
                $tableData['engine'], null));
        }

        return $result;
    }

    /**
     * Retrieve an external iterator
     * @link https://php.net/manual/en/iteratoraggregate.getiterator.php
     * @return Traversable An instance of an object implementing <b>Iterator</b> or
     * <b>Traversable</b>
     * @since 5.0.0
     */
    public function getIterator()
    {
        return new ArrayIterator($this->tables);
    }

    /**
     * On creating this collection has there been only one table that matched the conditions
     * @return bool
     */
    public function hasExactlyOneTable(): bool
    {
        return (sizeof($this->tables) === 1);
    }

    /**
     * On creating this collection has there been at least one table that matched the conditions
     * @return bool
     */
    public function hasAtLeastOneTable(): bool
    {
        return (sizeof($this->tables) >= 1);
    }

    /**
     * Gets a reference to the specified table
     * @param int $i
     * @return DatabaseTable
     */
    public function get(int $i): DatabaseTable
    {
        if (array_key_exists($i, $this->tables)) {
            return $this->tables[$i];
        }

        throw new InvalidArgumentException("Invalid Table requested");
    }

    /**
     * Retrieves the amount of tables in the collection
     * @return int
     */
    public function length(): int
    {
        return sizeof($this->tables);
    }
}
