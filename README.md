# diva-e database abstraction layer

This component provides implementations for MySQL and ClickHouse.

## Installation

1. Add the repository to your `composer.json`
2. Require the project `composer require diva-e/php-db-connectors`

To add the repository to your `composer.json` the following changes have to be made:

```json
{
    "repositories": [  
        {
            "name": "diva-e/php-db-connectors",
            "type": "vcs",
            "url": "https://www.github.com/diva-e/php-db-connectors"
        }
    ]
}
```

## Usage

### Connect manually to a database

```php
$DBConnection = new \Divae\DbConnectors\MySQL();
$DBConnection->setCredentials([
    \Divae\DbConnectors\MySQL::CREDENTIALS_HOSTNAME => "database.diva-e.com",
    \Divae\DbConnectors\MySQL::CREDENTIALS_USERNAME => "user",
    \Divae\DbConnectors\MySQL::CREDENTIALS_PASSWORD => "supersecret",
    \Divae\DbConnectors\MySQL::CREDENTIALS_DATABASE => "test", // Optional
    \Divae\DbConnectors\MySQL::CREDENTIALS_PORT => 3306 // Optional
]);
// Assign a logger (optional)
$DBConnection->setLogger($logger);

// Connect to the database (there is no auto-connect)
$DBConnection->connect();
```

### Connect to a database with ConnFactory
`ConnFactory` allows you to reuse existing database connections. To identify a connection, an identifier is supposed to be passed.

```php
\Divae\DbConnectors\ConnFactory::setLogger($logger);
\Divae\DbConnectors\ConnFactory::setMySQLCredentials(
    'db-type-a',
    'database.diva-e.com',
    'user',
    'supersecret',
    'test' // Optional
);

$DBConnection = \Divae\DbConnectors\ConnFactory::getMySQLConnection('db-type-a');

// You need to connect to the database yourself (there is no auto-connect)
$DBConnection->connect();
```

## Testing TableHandler

The Testing TableHandler allows you to write TestCases for functions that transform data in a database. You
will get your own schema so that you can work with your own custom dataset.

To enable the Testing TableHandler for a database connection, use the following:

```php
\Divae\DbConnectors\Testing\TableHandler::createSchema($DBConnection);
```

Now you have to name every table that will be controlled by the Testing TableHandler:

```php
\Divae\DbConnectors\Testing\TableHandler::addTable(\Divae\DbConnectors\MySQL::class, 'Schema', 'TableName1');
\Divae\DbConnectors\Testing\TableHandler::addTable(\Divae\DbConnectors\MySQL::class, 'Schema', 'TableName2');
```

If there are views queried that need to be based on your test data, add them:

```php
\Divae\DbConnectors\Testing\TableHandler::addView(\Divae\DbConnectors\MySQL::class, 'Schema', 'ViewName');
```

Since the Testing TableHandler is always enabled, just pass the query to the database classes and then
TableHandler will rewrite all statements to use your temporary testing schema. After the test is done,
Testing TableHandler cleans up for you.

## TempTableHandler

This repository comes with a flexible TempTableHandler. To use it, you need a class that extends from
`\Divae\DbConnectors\TempTableHandler\TempTableHandler`. To create a table, call
`$this->createTempTable('<Name>' [<Field Configuration>])`. It returns the name of the newly created table.

A minimum working example may look like the following:

```php
use Divae\DbConnectors\DatabaseException;
use Divae\DbConnectors\DatabaseSchemaAware;

class MWETempTableHandler extends Divae\DbConnectors\TempTableHandler\TempTableHandler
{
    /**
     * @var string
     */
    public $myTempTableName;

    public function __construct(DatabaseSchemaAware $database, string $ttlTempTable, bool $useTemporaryTables = false)
    {
        parent::__construct($database, $ttlTempTable, $useTemporaryTables);
        $this->classUniqueKey = 'MWE';
    }

    /**
     * create all temporary tables with unique names
     * @throws DatabaseException
     */
    public function createTables(): void
    {
        $this->createMyTempTable();
    }

    /**
     * Creates the Table behind myTempTable
     * @throws DatabaseException
     */
    public function createMyTempTable(): void
    {
        $this->myTempTableName = $this->createTempTable('myTempTable', [
            'col1 Int',
            'col2 VARCHAR(10)'
        ]);
    }
}
```

After you are finished with all the temp tables, simply call `dropAllTables()` and everything is gone. If you want to
cleanup leaked tables from previous executions of any TempTableHander descendant, call
`runGC()` (or `dropZombieTables()`).