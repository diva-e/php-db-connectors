<?php
declare(strict_types=1);

namespace Divae\DbConnectors;

use Psr\Log\LoggerInterface;

class ConnFactory
{
    /**
     * List of open MySQL Connections
     * @var MySQL[]
     */
    private static array $openMySQLConnections = [];

    /**
     * List of open Clickhouse Connections
     * @var ClickHouse[]
     */
    private static array $openClickHouseConnections = [];

    private static array $MySQLCredentials = [];
    private static array $ClickHouseCredentials = [];

    private static ?LoggerInterface $logger = null;

    /**
     * Adds credentials for a MySQL connection to the credentials store
     *
     * @param string $identifier
     * @param string $host
     * @param string $username
     * @param string $password
     * @param string $database
     */
    public static function setMySQLCredentials(
        string $identifier,
        string $host,
        string $username,
        string $password,
        string $database = ''
    ): void {
        self::$MySQLCredentials[$identifier] = [
            AbstractConnector::CREDENTIALS_HOSTNAME => $host,
            AbstractConnector::CREDENTIALS_USERNAME => $username,
            MySQL::CREDENTIALS_PASSWORD             => $password,
            MySQL::CREDENTIALS_DATABASE             => $database
        ];
    }

    /**
     * Adds credentials for a ClickHouse connection to the credentials store
     *
     * @param string $identifier
     * @param string $host
     * @param string $username
     * @param string $password
     */
    public static function setClickHouseCredentials(
        string $identifier,
        string $host,
        string $username,
        string $password
    ): void {
        self::$ClickHouseCredentials[$identifier] = [
            AbstractConnector::CREDENTIALS_USERNAME => $username,
            ClickHouse::CREDENTIALS_PASSWORD        => $password,
            AbstractConnector::CREDENTIALS_HOSTNAME => $host,
        ];
    }

    /**
     * Set the logger to use; will apply this logger to all open connections
     *
     * @param LoggerInterface $logger
     */
    public static function setLogger(LoggerInterface $logger): void
    {
        self::$logger = $logger;

        // Now set the logger to all open connections
        foreach (self::$openMySQLConnections as $connection) {
            $connection->setLogger($logger);
        }

        foreach (self::$openClickHouseConnections as $connection) {
            $connection->setLogger($logger);
        }
    }


    /**
     * Returns a MySQL connection. If there is none matching the $identifier parameter a new one is created
     *
     * @param string $identifier GLOBAL or LOCAL
     * @return MySQL
     * @throws DatabaseException
     */
    public static function getMySQLConnection(string $identifier): MySQL
    {
        if (isset(self::$openMySQLConnections[$identifier])) {
            return self::$openMySQLConnections[$identifier];
        }

        // Do we have credentials?
        if (!isset(self::$MySQLCredentials[$identifier])) {
            throw new DatabaseException("No credentials for Connection Type {$identifier} found. Cannot connect to database");
        }

        // Connection does not yet exist
        $connection = new MySQL();
        $connection->setCredentials(self::$MySQLCredentials[$identifier]);
        if (!is_null(self::$logger)) {
            $connection->setLogger(self::$logger);
        }

        self::$openMySQLConnections[$identifier] = $connection;
        return $connection;
    }

    /**
     * Returns a ClickHouse connection. If there is none matching the $identifier parameter a new one is created
     *
     * @param $identifier
     * @return Clickhouse
     * @throws DatabaseException
     */
    public static function getClickHouseConnection($identifier): Clickhouse
    {
        // Do we have credentials?
        if (!isset(self::$ClickHouseCredentials[$identifier])) {
            throw new DatabaseException("No credentials for Connection Type {$identifier} found. Cannot connect to database");
        }

        // Connection does not yet exist
        $connection = new Clickhouse();
        $connection->setCredentials(self::$ClickHouseCredentials[$identifier]);
        if (!is_null(self::$logger)) {
            $connection->setLogger(self::$logger);
        }

        self::$openClickHouseConnections[$identifier] = $connection;
        return $connection;
    }

    /**
     * Resets the state of this static class back to its original state - for unit tests
     */
    public static function reset()
    {
        self::$openMySQLConnections = [];
        self::$openClickHouseConnections = [];
        self::$MySQLCredentials = [];
        self::$ClickHouseCredentials = [];
        self::$logger = null;
    }
}
