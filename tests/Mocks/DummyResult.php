<?php
namespace Divae\DbConnectors\Test\Mocks;

class DummyResult
{
    /**
     * Rows to return
     * @var array
     */
    private array $rows;

    public function __construct(array $rows)
    {
        $this->rows = $rows;
    }

    public function free_result(): void
    {
        // No-op
    }

    public function fetch_assoc()
    {
        $result = current($this->rows);
        if ($result !== false) {
            next($this->rows);
        }

        return $result;
    }
}
