<?php
declare(strict_types=1);

namespace Divae\DbConnectors\Schema;

use Divae\DbConnectors\DatabaseException;
use Divae\DbConnectors\SchemaAware;

class DatabaseTable
{
    /**
     * @var string
     */
    private string $schema;

    /**
     * @var string
     */
    private string $name;

    /**
     * @var string|null
     */
    private ?string $engine;

    /**
     * @var string|null (String, because int cannot hold large enough values)
     */
    private ?string $rows;

    /**
     * @var SchemaAware
     */
    private SchemaAware $database;

    /**
     * MySQLTable constructor.
     * @param SchemaAware $database
     * @param string $schema
     * @param string $name
     * @param string|null $engine
     * @param string|null $rows
     */
    public function __construct(SchemaAware $database, string $schema, string $name, ?string $engine, ?string $rows)
    {
        $this->database = $database;
        $this->schema = $schema;
        $this->name = $name;
        $this->engine = $engine;
        $this->rows = $rows;
    }

    /**
     * @return mixed
     */
    public function getSchema(): string
    {
        return $this->schema;
    }

    /**
     * @return mixed
     */
    public function getName(): string
    {
        return $this->name;
    }

    /**
     * @return mixed
     */
    public function getEngine(): string
    {
        return $this->engine;
    }

    /**
     * @return mixed
     */
    public function getRows(): ?string
    {
        return $this->rows;
    }

    /**
     * Returns the fully qualified name of this table
     * @param bool $escaped If true, ticks (`) will be added to schema and name
     * @return string
     */
    public function getFullName(bool $escaped = false): string
    {
        if ($escaped) {
            return "`" . $this->schema . "`.`" . $this->name . "`";
        } else {
            return $this->schema . "." . $this->name;
        }
    }

    /**
     * Convenience method: List Columns of this table
     * @return string[]
     * @throws DatabaseException
     */
    public function listColumns(): array
    {
        return $this->database->getColumnList($this->schema, $this->name);
    }

    /**
     * Convenience method: count Columns of this table
     * @return int
     * @throws DatabaseException
     */
    public function countColumns(): int
    {
        return $this->database->getColumnCount($this->schema, $this->name);
    }
}
