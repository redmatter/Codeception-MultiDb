<?php
/**
 * @copyright 2009-2014 Red Matter Ltd (UK)
 */

namespace Codeception\Extension;

use Codeception\Exception\TestRuntimeException;
use Codeception\Extension\MultiDb\Utils\AsIs;
use Codeception\Extension\MultiDb\Utils\CleanupAction;
use Codeception\Lib\Driver\Db as Driver;
use Codeception\Exception\ModuleException;
use Codeception\Exception\ModuleConfigException;
use Codeception\Module;
use Codeception\TestCase;

/**
 * MultiDb - Module that allows tests to perform setup queries and assertions across multiple databases.
 */
class MultiDb extends Module
{
    const ASIS_PREFIX = '@asis ';

    const CLEANUP_NEVER = 0;
    const CLEANUP_AFTER_TEST = 1;
    const CLEANUP_AFTER_SUITE = 2;

    protected $dbh;

    protected $config = ['connectors' => false, 'timezone' => 'UTC'];

    protected $requiredFields = ['connectors'];

    /** @var  Driver[] */
    protected $drivers = [];

    /** @var  Driver */
    protected $chosenDriver = null;

    protected $chosenConnector;

    /** @var CleanupAction[] */
    protected $test_cleanup_actions = [];
    /** @var CleanupAction[] */
    protected $suite_cleanup_actions = [];

    protected $connectorRequiredFields = ['dsn', 'user', 'password'];

    /** @var string */
    private $timezone;

    /** @var int */
    private $transaction_level = 0;
    /** @var string */
    private $transaction_connector = null;

    // HOOK: used after configuration is loaded
    // @codingStandardsIgnoreLine overridden function from \Codeception\Module
    public function _initialize()
    {
        $configOk = false;

        if (is_array($this->config['connectors'])) {
            foreach ($this->config['connectors'] as $connector => $connectorConfig) {
                if (is_array($connectorConfig)) {
                    $fields = array_keys($connectorConfig);
                    $configOk = (
                        array_intersect($this->connectorRequiredFields, $fields) == $this->connectorRequiredFields
                    );
                    if (!$configOk) {
                        break;
                    }
                }
            }
        }

        if (!$configOk) {
            throw new ModuleConfigException(
                __CLASS__,
                "\nOptions: " . implode(', ', $this->connectorRequiredFields) . " are required\n
                        Please, update the configuration and set all the required fields\n\n"
            );
        }

        $this->timezone = $this->config['timezone'];

        parent::_initialize();
    }

    // HOOK: before scenario
    // @codingStandardsIgnoreLine overridden function from \Codeception\Module
    public function _before(TestCase $test)
    {
        if ($this->transaction_level > 0) {
            $this->rollbackTransaction();
            $this->fail("Unfinished transaction was found; rolled back (before test '{$test->getName(false)}')");
        }
    }

    // HOOK: after scenario
    // @codingStandardsIgnoreLine overridden function from \Codeception\Module
    public function _after(TestCase $test)
    {
        $this->debug(__CLASS__.'::'.__FUNCTION__.'()');

        $unfinished_transaction = ($this->transaction_level > 0);
        if ($unfinished_transaction) {
            $this->debug("Unfinished transaction was found; rolling back (after test '{$test->getName(false)}')");
            // wrap up the transaction so that the clean-up below can succeed.
            // it is not possible to switch connectors mid-transaction.
            $this->rollbackTransaction();
        }

        foreach ($this->test_cleanup_actions as $cleanup_action) {
            $this->debugSection('cleanup', $cleanup_action->getDefinition());
            call_user_func($cleanup_action, $this);
        }

        if ($unfinished_transaction) {
            $this->fail("Unfinished transaction was found (after test '{$test->getName(false)}')");
        }

        $this->test_cleanup_actions = [];

        parent::_after($test);
    }

    // @codingStandardsIgnoreLine overridden function from \Codeception\Module
    public function _afterSuite()
    {
        $this->debug(__CLASS__.'::'.__FUNCTION__.'()');

        foreach ($this->suite_cleanup_actions as $cleanup_action) {
            $this->debugSection('cleanup(after-suite)', $cleanup_action->getDefinition());
            call_user_func($cleanup_action, $this);
        }

        $this->suite_cleanup_actions = [];

        parent::_afterSuite();
    }

    // @codingStandardsIgnoreLine overridden function from \Codeception\Module
    public function _failed(TestCase $test, $fail)
    {
        /** @var \PHPUnit_Framework_Exception $fail */

        $this->debugSection(__CLASS__.'::'.__FUNCTION__.'()', $fail->getMessage());

        // rollback any transaction that are yet to finish
        if ($this->transaction_level > 0) {
            $this->rollbackTransaction();
        }
    }

    /**
     * @see MultiDb::amConnectedToDb
     *
     * @param string $connector
     *
     * @throws ModuleException
     * @return \Codeception\Lib\Driver\Db
     */
    private function getDriver($connector)
    {
        if (!(isset($this->drivers[$connector]) && is_object($this->drivers[$connector]))) {
            if (!isset($this->config['connectors'][$connector])) {
                throw new ModuleException(
                    __CLASS__,
                    "The specified connector, {$connector}, does not exist in the configuration"
                );
            }
            $config = $this->config['connectors'][$connector];

            try {
                $this->drivers[$connector] = Driver::create($config['dsn'], $config['user'], $config['password']);
                $this->executeSqlAgainstDriver(
                    $this->drivers[$connector]->getDbh(),
                    "SET time_zone = '{$this->timezone}'"
                );
            } catch (\PDOException $e) {
                throw new ModuleException(
                    __CLASS__,
                    $e->getMessage() . ' while creating PDO connection ['.get_class($e).']'
                );
            } catch (\Exception $e) {
                throw new ModuleException(
                    __CLASS__,
                    $e->getMessage() . ' while creating PDO connection ['.get_class($e).']'
                );
            }
        }

        return $this->drivers[$connector];
    }

    /**
     * Get the chosen driver or throw!
     *
     * @return Driver
     */
    private function getChosenDriver()
    {
        if (null === $this->chosenDriver) {
            throw new TestRuntimeException("No connector was chosen before interactions with Db");
        }

        return $this->chosenDriver;
    }

    /**
     * Create database and setup cleanup
     *
     * Example:
     *   $I->createDatabase('BlogDb', ['character_set'=>'utf8', 'collation'=>'utf8_bin']);
     *
     * @param string $database      database name
     * @param array  $options       options for character set and collation options
     * @param int    $cleanup_after defines whether the database should be cleaned up at the end of the test, suite,
     *                              or not at all
     */
    public function createDatabase($database, $options = null, $cleanup_after = self::CLEANUP_AFTER_TEST)
    {
        $options = $options ?: [];
        if (!is_array($options)) {
            throw new TestRuntimeException('Invalid options given for '.__METHOD__);
        }
        $sql = "CREATE DATABASE {$database}";
        if (isset($options['character_set'])) {
            $sql .= " CHARACTER SET {$options['character_set']}";
        }
        if (isset($options['collation'])) {
            $sql .= " COLLATE {$options['character_set']}";
        }

        $this->executeSql($sql);
        $this->setupDbCleanup(CleanupAction::runSql("DROP DATABASE {$database}"), $cleanup_after);
    }

    /**
     * Create a table like another
     *
     * Example:
     *   $I->createTableLike('Posts', 'Posts_Template');
     *
     * @param string $template_table template table to make a replica of
     * @param string $table          table to be created
     * @param int    $cleanup_after  Defines whether the table should be cleaned up after the test, suite, or not at all
     */
    public function createTableLike($template_table, $table, $cleanup_after = self::CLEANUP_AFTER_TEST)
    {
        $this->executeSql("CREATE TABLE {$table} LIKE {$template_table}");
        $this->setupDbCleanup(CleanupAction::runSql("DROP TABLE {$table}"), $cleanup_after);
    }

    /**
     * Connects the Guy to a database described by the named connector
     * See configuration for connector names
     *
     * @param string $connector
     *
     * @throws ModuleException
     *
     * @return string The previously chosen connector, or the new connector, if no connector was previously chosen
     */
    public function amConnectedToDb($connector)
    {
        if ($this->transaction_level > 0 && $this->transaction_connector !== $connector) {
            //@codingStandardsIgnoreLine
            throw new TestRuntimeException("Cannot switch connector while a transaction is in progress on another connector '{$this->transaction_connector}'");
        }

        $previous_connector = empty($this->chosenConnector) ? $connector : $this->chosenConnector;
        $this->chosenDriver = $this->getDriver($connector);
        $this->chosenConnector = $connector;
        return $previous_connector;
    }

    /**
     * Execute the callable after switching to the specified connector; when finished, revert to the old
     * If the callable throws or fails, the connector is left in the switched state
     *
     * @param string   $connector
     * @param callable $callable
     *
     * @return mixed what ever the callable returns
     */
    public function connectToDbAndExecute($connector, callable $callable)
    {
        $old_connector = $this->amConnectedToDb($connector);
        $result = call_user_func($callable);
        $this->amConnectedToDb($old_connector);

        return $result;
    }

    /**
     * Get the current connector name
     *
     * @return string
     */
    public function getCurrentConnector()
    {
        return $this->chosenConnector;
    }

    /**
     * Insert a record into the given table
     *
     * @param string       $table                Table name, preferably Database.TableName
     * @param array        $field_values         An array of field values of the form ['Field1'=>Value, 'Field2'=>Value]
     * @param string|array $pk_field             Field name to match the last-insert-id or primaryKeyValue
     * @param mixed        $pk_value_for_cleanup A value other than 0 or null which can identify the row to be
     *                                           cleaned-up
     * @param int          $cleanup_after        Defines whether the table should be cleaned up after the test, suite,
     *                                           or not at all
     * @param bool|array   $ignore_dup_key       boolean true to ignore duplicate-key error from database by using
     *                                           INSERT INTO ... ON DUPLICATE KEY UPDATE Field1=Value1, ...
     *                                           If you need more control, use an array to specify a list of fields
     *                                           from $field_values
     *
     * @return int|array|null int if it is a single field primary key Last Insert ID was known from database.
     *                        array of compound primary key values, if compound primary key was specified, and their
     *                            values were found to be not NULL in $field_values.
     *                        null otherwise.
     */
    public function haveInDb(
        $table,
        $field_values,
        $pk_field = 'ID',
        $pk_value_for_cleanup = null,
        $cleanup_after = self::CLEANUP_AFTER_TEST,
        $ignore_dup_key = true
    ) {
        $driver = $this->getChosenDriver();

        $pk_field_was_array = is_array($pk_field);
        if (!$pk_field_was_array) {
            $pk_field = [$pk_field];
        }
        if ($pk_value_for_cleanup !== null) {
            if ((is_array($pk_value_for_cleanup) && count($pk_field) != count($pk_value_for_cleanup))
                || (!is_array($pk_value_for_cleanup) && count($pk_field) != 1)
            ) {
                // @codingStandardsIgnoreLine No line breaks in error messages
                throw new \RuntimeException('Incompatible primary key field and value; single field primary keys should specify non array value, and compound primary keys should specify compound values in an array of the same size');
            }
        }

        list($query, $params) = $this->formSqlInsertSingle($table, $field_values, $pk_field, $ignore_dup_key);
        $this->debugSection('Query', $query);
        $this->debugSection('Params', $params);

        $statement = $driver->getDbh()->prepare($query);
        if (!$statement) {
            $this->fail("Query '$query' can't be executed.");
        }

        $res = $statement->execute($params);
        if (!$res) {
            $this->fail(sprintf("Record with %s couldn't be inserted into %s", json_encode($field_values), $table));
        }

        $last_insert_id = null;
        $multi_field_pk_values = null;

        // if there is only one field, try and retrieve the LAST_INSERT_ID
        if (count($pk_field) == 1) {
            try {
                $last_insert_id = (int)$driver->lastInsertId($table);
            } catch (\PDOException $e) {
                // ignore errors due to uncommon DB structure, such as tables without auto-inc
            }
        }

        // mapper function that maps each field from $pk_field to one or zero
        // 1 if the field is set in the given $field_values and is not NULL; 0 otherwise
        $fnIssetFieldAndNotNull = function ($a_pk_field) use ($field_values) {
            return isset($field_values[$a_pk_field]) ? 1 : 0;
        };

        // is empty or $pk_value_for_cleanup is an array with all null values
        if (empty($pk_value_for_cleanup)
            || (is_array($pk_value_for_cleanup) && count(array_diff($pk_value_for_cleanup, [null])) == 0)
        ) {
            if (count($pk_field) == 1 && $last_insert_id) {
                $pk_value_for_cleanup = [reset($pk_field) => $last_insert_id];

                // if the fields in pk_field are present in $field_values and none of them are NULL,
                // then we can take values from there
            } elseif (0 !== array_product(array_map($fnIssetFieldAndNotNull, $pk_field))) {
                // filter the $field_values using $pk_field as keys
                $pk_value_for_cleanup = array_intersect_key($field_values, array_flip($pk_field));
                $multi_field_pk_values = $pk_value_for_cleanup;
            }
            // else
            // auto-cleanup cannot be setup
        } else {
            $pk_value_for_cleanup = array_combine(
                $pk_field,
                is_array($pk_value_for_cleanup) ? $pk_value_for_cleanup : [$pk_value_for_cleanup]
            );
        }

        if ($cleanup_after && $pk_value_for_cleanup) {
            $this->setupDbCleanup(CleanupAction::delete($table, $pk_value_for_cleanup), $cleanup_after);
        }

        if ($last_insert_id !== null) {
            return $pk_field_was_array ? array_combine($pk_field, [$last_insert_id]) : $last_insert_id;
        }

        if ($multi_field_pk_values !== null) {
            return $multi_field_pk_values;
        }

        return null;
    }

    /**
     * Inserts multiple records into the given table
     *
     * @param string     $table            Table name, preferably Database.TableName
     * @param array      $field_value_rows An array of array of field values of the form ['Field1'=>Value,
     *                                     'Field2'=>Value]
     * @param array      $cleanup_criteria an array of field values of the form ['Field1'=>Value, 'Field2'=>Value], to
     *                                     form the delete criteria during cleanup
     * @param int        $cleanup_after    Defines whether the table should be cleaned up after the test, suite, or not
     *                                     at all
     * @param bool|array $ignore_dup_key   boolean true to ignore duplicate-key error from database by using
     *                                     INSERT INTO ... ON DUPLICATE KEY UPDATE Field1=Value1, ...
     *                                     If you need more control, use an array to specify a list of fields
     *                                     from $field_values
     */
    public function haveInDbMultipleRows(
        $table,
        array $field_value_rows,
        $cleanup_criteria = null,
        $cleanup_after = self::CLEANUP_AFTER_TEST,
        $ignore_dup_key = true
    ) {
        if ($cleanup_criteria && !is_array($cleanup_criteria)) {
            throw new TestRuntimeException('Invalid clean-up criteria given to method:'.__METHOD__);
        }

        $driver = $this->getChosenDriver();

        list($query, $params) = $this->formSqlInsert($table, $field_value_rows, $ignore_dup_key);
        $this->debugSection('Query', $query);
        $this->debugSection('Params', $params);

        $statement = $driver->getDbh()->prepare($query);
        if (!$statement) {
            $this->fail("Query '$query' can't be executed.");
        }

        $res = $statement->execute($params);
        if (!$res) {
            $this->fail(sprintf("Record with %s couldn't be inserted into %s", json_encode($field_value_rows), $table));
        }

        if ($cleanup_criteria && $cleanup_after) {
            $this->setupDbCleanup(CleanupAction::delete($table, $cleanup_criteria), $cleanup_after);
        }
    }

    /**
     * Update a table with values for rows matching the given where clause
     *
     * @param string $table         Table name, preferably Database.TableName
     * @param array  $criteria      and array of field values of the form ['Field1'=>Value, 'Field2'=>Value] which gets
     *                              converted to "Field1=Value AND Field2=Value"
     * @param array  $field_updates and array of field values of the form ['Field1'=>Value, 'Field2'=>Value] which will
     *                              describe the new values for the given fields
     *
     * @return int number of rows updated
     */
    public function haveUpdatedDb($table, $field_updates, $criteria)
    {
        $driver = $this->getChosenDriver();

        list($query, $params) = $this->formSqlUpdate($table, $criteria, $field_updates);
        $this->debugSection('Query', $query);
        $this->debugSection('Params', $params);

        $statement = $driver->getDbh()->prepare($query);
        if (!$statement) {
            $this->fail("Query '$query' can't be executed.");
        }

        $res = $statement->execute($params);
        if (!$res) {
            $this->fail(
                sprintf(
                    "Record selected with %s couldn't be updated with %s into %s",
                    json_encode($criteria),
                    json_encode($field_updates),
                    $table
                )
            );
        }

        return $statement->rowCount();
    }

    /**
     * See in Db if there are records that match the given criteria in the given table.
     *
     * @param string $table          Table name, preferably Database.TableName
     * @param array  $criteria       Row selection criteria of the form ['Field1'=>Value, 'Field2'=>Value] which gets
     *                               converted to "Field1=Value AND Field2=Value"
     * @param int $count_expected    Expected record count; You can also specify the number of records that you expect
     *                               to see or you can use the default value of -1 to specify "any number of records".
     *                               Use -1 if you do not care how many records are present in the table, that matches
     *                               the given criteria, as long as at least one is found.
     */
    public function seeInDb($table, $criteria, $count_expected = -1)
    {
        $driver = $this->getChosenDriver();

        list($query, $params) = $this->formSqlSelect($table, $criteria, [new AsIs('COUNT(*)')]);
        $this->debugSection('Query', $query);
        $this->debugSection('Params', $params);

        $statement = $driver->getDbh()->prepare($query);
        if (!$statement) {
            $this->fail("Query '$query' can't be executed.");
        }

        $res = $statement->execute($params);
        if (!$res) {
            $this->fail(
                sprintf(
                    "Record selected with %s couldn't be counted from table %s",
                    json_encode($criteria),
                    $table
                )
            );
        }

        $count = $statement->fetchColumn(0);
        if ($count_expected < 0) {
            $this->assertGreaterThan(0, $count, 'No matching records found');
        } elseif ($count_expected == 0) {
            $this->assertLessThan(1, $count, 'Matching records were found');
        } else {
            $this->assertEquals($count_expected, $count, 'No given number of matching records found');
        }
    }

    /**
     * Same as @see seeInDb except that the count specified here is 0
     *
     * @param string $table    Table name, preferably Database.TableName
     * @param array  $criteria Row selection criteria of the form ['Field1'=>Value, 'Field2'=>Value] which gets
     *                         converted to "Field1=Value AND Field2=Value"
     */
    public function dontSeeInDb($table, $criteria)
    {
        $this->seeInDb($table, $criteria, 0);
    }

    /**
     * Get records from the table that match the criteria
     *
     * @param string       $table        Table name, preferably Database.TableName
     * @param array        $criteria     Row selection criteria of the form ['Field1'=>Value, 'Field2'=>Value] which
     *                                   gets converted to "Field1=Value AND Field2=Value"
     * @param array|string $fields       It can be a free formed SQL fragment to describe the values to select or an
     *                                   array of the form ['Field1', 'Field2']
     * @param array        $fetchPdoArgs Options to be passed to PDOStatement::fetchAll,
     *                                   see http://php.net/manual/en/pdostatement.fetchall.php
     *
     * @return array an array of rows ( depending on the $fetchPdoArgs given )
     */
    public function getFromDb($table, $criteria, $limit = 1, $fields = null, $fetchPdoArgs = array(\PDO::FETCH_ASSOC))
    {
        $driver = $this->getChosenDriver();

        list($query, $params) = $this->formSqlSelect($table, $criteria, $fields, $limit);
        $this->debugSection('Query', $query);
        $this->debugSection('Params', $params);

        $statement = $driver->getDbh()->prepare($query);
        if (!$statement) {
            $this->fail("Query '$query' can't be executed.");
        }

        $res = $statement->execute($params);
        if (!$res) {
            $this->fail(
                sprintf(
                    "Record with %s columns couldn't be selected with %s from table %s",
                    json_encode($fields),
                    json_encode($criteria),
                    $table
                )
            );
        }

        return call_user_func_array([$statement, 'fetchAll'], $fetchPdoArgs);
    }

    /**
     * Delete from a table with values for rows matching the given criteria
     *
     * @param string $table    Table name, preferably Database.TableName
     * @param array  $criteria Array of field values of the form ['Field1'=>Value, 'Field2'=>Value] which gets
     *                         converted to "Field1=Value AND Field2=Value"
     *
     * @return void
     */
    public function haveDeletedFromDb($table, $criteria)
    {
        $driver = $this->getChosenDriver();

        list($query, $params) = $this->formSqlDelete($table, $criteria);
        $this->debugSection('Query', $query);
        $this->debugSection('Params', $params);

        $statement = $driver->getDbh()->prepare($query);
        if (!$statement) {
            $this->fail("Query '$query' can't be executed.");
        }

        $res = $statement->execute($params);
        if (!$res) {
            $this->fail(
                sprintf(
                    "Record couldn't be deleted with %s from table %s",
                    json_encode($criteria),
                    $table
                )
            );
        }
    }

    /**
     * executes the given SQL
     *
     * @param string $query        an SQL optionally with ? for parameters specified in $params
     * @param array  $params       If $query is parametrised with ?, then this array should have the values for them
     * @param array  $fetchPdoArgs Options to be passed to PDOStatement::fetchAll,
     *                             see http://php.net/manual/en/pdostatement.fetchall.php
     *
     * @return mixed row count for non-SELECT query and an array of rows ( depending on the $fetchPdoArgs given )
     */
    public function executeSql($query, array $params = array(), array $fetchPdoArgs = array(\PDO::FETCH_ASSOC))
    {
        $this->debugSection('Query', $query);
        $this->debugSection('Params', $params);

        return $this->executeSqlAgainstDriver($this->getChosenDriver()->getDbh(), $query, $params, $fetchPdoArgs);
    }

    /**
     * @throws \LogicException if an inconsistent state is found
     *
     * @return void
     */
    private function assertSaneTransactionState()
    {
        if (($this->transaction_connector
                && ($this->transaction_connector != $this->getCurrentConnector() || $this->transaction_level == 0))
            || (!$this->transaction_connector && $this->transaction_level != 0)
        ) {
            // @codingStandardsIgnoreLine Error messages are not to be wrapped
            throw new \LogicException("Invalid transaction state (level:[{$this->transaction_level}] connector:[{$this->transaction_connector}])");
        }
    }

    /**
     * Begin a transaction or adjust nesting level
     *
     * Nesting level is so that an ongoing transaction is not committed till a commit from level 1 is invoked; all other
     * commits will only de-nest the transaction
     */
    public function startTransaction()
    {
        $this->assertSaneTransactionState();

        if ($this->transaction_level === 0) {
            $this->transaction_level = 1;
            $this->transaction_connector = $this->getCurrentConnector();
            $this->executeSql('BEGIN');
        } else {
            $this->transaction_level++;
        }
    }

    /**
     * Commit the ongoing transaction or de-nest the current level
     */
    public function commitTransaction()
    {
        $this->assertSaneTransactionState();

        switch (true) {
            case $this->transaction_level > 1:
                $this->transaction_level--;
                break;
            case $this->transaction_level === 1:
                $this->transaction_level = 0;
                $this->transaction_connector = null;
                $this->executeSql('COMMIT');
                break;
            default:
            case $this->transaction_level == 0:
                throw new \LogicException('Invalid call sequence; no transaction in progress');
                break;
        }
    }

    /**
     * Roll back the ongoing transaction
     */
    public function rollbackTransaction()
    {
        $this->assertSaneTransactionState();

        if ($this->transaction_level == 0) {
            throw new \LogicException('Invalid call sequence; no transaction in progress');
        }
        $this->transaction_level = 0;
        $this->transaction_connector = null;
        $this->executeSql('ROLLBACK');
    }

    /**
     * Executes the callable within a transaction block.
     *
     * Within the callable, you can perform any operation and if no exception results from the operation, the
     * transaction is commit when the callable returns
     *
     * @param callable $callable
     *
     * @return bool|mixed false on failure, otherwise what ever is returned by the callable
     *
     * @internal Use this only from other modules, as it does not generate a readable step output
     */
    public function transaction(callable $callable)
    {
        try {
            $this->debug('Current Connector is '.$this->getCurrentConnector());

            $this->startTransaction();
            $result = call_user_func($callable);
            $this->commitTransaction();

            return $result;
        } catch (\Exception $e) {
            $this->rollbackTransaction();
            $this->debugSection('Exception', (string)$e);
            $this->fail('Transaction failed; exception:'.$e->getMessage());
            return false;
        }
    }

    /**
     * @param \PDO   $dbh
     * @param string $query
     * @param array  $params
     * @param array  $fetchPdoArgs
     *
     * @return mixed
     */
    private function executeSqlAgainstDriver(
        \PDO $dbh,
        $query,
        array $params = array(),
        array $fetchPdoArgs = array(\PDO::FETCH_ASSOC)
    ) {
        $statement = $dbh->prepare($query);
        if (!$statement) {
            $this->fail("Query '$query' can't be executed.");
        }

        $res = $statement->execute($params);
        if (!$res) {
            $this->fail(
                sprintf(
                    "Query %s couldn't be run with the params %s",
                    $query,
                    json_encode($params)
                )
            );
        }

        // if not a SELECT query, then return the affected rows
        if (0 == $statement->columnCount()) {
            return $statement->rowCount();
        }

        return call_user_func_array([$statement, 'fetchAll'], $fetchPdoArgs);
    }

    /**
     * Setup cleanup
     *
     * @param \Codeception\Extension\MultiDb\Utils\CleanupAction $cleanup_action
     * @param int $cleanup_event Defines when the cleanup action should take place, the MultiDb::CLEANUP_* constants
     *                           should be used here.
     *
     * @deprecated changing the name to symbolise ONLY database cleanup; use setupDbCleanup() instead
     *
     * @see setupDbCleanup()
     */
    public function setupCleanup(CleanupAction $cleanup_action, $cleanup_event = self::CLEANUP_AFTER_TEST)
    {
        $this->setupDbCleanup($cleanup_action, $cleanup_event);
    }

    /**
     * Setup cleanup
     *
     * @param \Codeception\Extension\MultiDb\Utils\CleanupAction $cleanup_action
     * @param int $cleanup_event Defines when the cleanup action should take place, the MultiDb::CLEANUP_* constants
     *                           should be used here.
     */
    public function setupDbCleanup(CleanupAction $cleanup_action, $cleanup_event = self::CLEANUP_AFTER_TEST)
    {
        $cleanup_action->setConnector($this->chosenConnector);
        switch ($cleanup_event) {
            case self::CLEANUP_NEVER:
                break;
            case self::CLEANUP_AFTER_TEST:
                array_unshift($this->test_cleanup_actions, $cleanup_action);
                break;
            case self::CLEANUP_AFTER_SUITE:
                array_unshift($this->suite_cleanup_actions, $cleanup_action);
                break;
            default:
                throw new TestRuntimeException('Unexpected value for $cleanup_event: ' . $cleanup_event);

        }
    }

    /**
     * This function uses the information schema to get the latest auto-increment ID for the specified table.
     * This is useful for determining non-existent IDs.
     *
     * @param string      $table    The table from which to get the latest auto-increment ID
     * @param string|null $database Database where the above table resides, null to use the currently connected database
     *
     * @return int
     */
    public function getLatestAutoIncrementId($table, $database = null)
    {
        $result = $this->getFromDb(
            'information_schema.TABLES',
            array(
                'TABLE_NAME' => $table,
                'TABLE_SCHEMA' => $database === null ? new AsIs('DATABASE()') : $database,
            ),
            array('AUTO_INCREMENT')
        );

        if (empty($result) || isset($result[0]['AUTO_INCREMENT']) === false) {
            throw new TestRuntimeException(
                'Failed to retrieve the latest auto-increment ID for `' .
                ($database === null ? '<Current Database>' : $database) . "`.`{$table}`"
            );
        }

        return (int)$result[0]['AUTO_INCREMENT'];
    }

    /**
     * if the value starts with "@asis " it will be interpreted as AsIs
     *
     * @param string|AsIs &$value value to be normalised
     *
     * @return bool true if it was normalised
     */
    private static function normaliseAsIs(&$value)
    {
        if (is_scalar($value) && 0 === stripos($value, self::ASIS_PREFIX)) {
            $value = new AsIs(substr($value, strlen(self::ASIS_PREFIX)));
            return true;
        }

        return false;
    }

    /**
     * Normalise params list for easy processing later on
     *
     * @param array $params params array from one of the public functions
     *
     * @return array [ [ field, placeholder, value ], ... ]
     */
    protected static function normaliseParameterList($params)
    {
        $toScalar = function ($value) {
            return (null === $value) ? $value : (string)$value;
        };

        array_walk(
            $params,
            function (&$value, $field) use ($toScalar) {
                self::normaliseAsIs($value);

                // Check if no field was specified (so the array index will be an integer).
                if (is_numeric($field)) {
                    $value = ($value instanceof AsIs) ?
                        array(null, null, $toScalar($value)) : array(null, '?', $toScalar($value));
                } else {
                    $value = ($value instanceof AsIs)?
                        array($field, null, $toScalar($value)) : array($field, '?', $toScalar($value));
                }
            }
        );

        return array_values($params);
    }

    /**
     * Forms the INSERT SQL string to feed the database
     *
     * @param string     $table                table name
     * @param array      $data_rows            data to insert
     * @param bool|array $ignore_duplicate_key true to append the ON DUPLICATE KEY syntax
     *
     * @return array of the sql query and params list
     */
    private function formSqlInsert($table, array $data_rows, $ignore_duplicate_key)
    {
        if (!count($data_rows) || !count($data_rows[0])) {
            throw new TestRuntimeException('Invalid data rows given to '.__METHOD__);
        }

        $driver = $this->getChosenDriver();

        $columns = array_map(array($driver, 'getQuotedName'), array_keys($data_rows[0]));

        $ignore_duplicate_key_sql = null;
        // either true or an array containing field names
        if ($ignore_duplicate_key && (!is_array($ignore_duplicate_key) || count($ignore_duplicate_key))) {
            $update_fields = is_array($ignore_duplicate_key) ? $ignore_duplicate_key : array_keys($data_rows[0]);
            $ignore_duplicate_key_sql = ' ON DUPLICATE KEY UPDATE '.
                implode(
                    ', ',
                    array_map(
                        function ($field) use ($driver) {
                            return $driver->getQuotedName($field).'=VALUES('.$driver->getQuotedName($field).')';
                        },
                        $update_fields
                    )
                );
        }

        $param_list = [];
        $sql_values = [];
        foreach ($data_rows as $data) {
            $params = self::normaliseParameterList($data);
            $sql_values[] = implode(
                ', ',
                array_map(
                    function ($value) use (&$param_list) {
                        /** @noinspection PhpUnusedLocalVariableInspection */
                        list($field, $placeholder, $param_value) = $value;
                        if ($placeholder !== null) {
                            $param_list[] = $param_value;
                            return $placeholder;
                        }

                        return $param_value;
                    },
                    $params
                )
            );
        }

        $sql = sprintf(
            'INSERT INTO %s (%s) VALUES (%s) %s',
            $driver->getQuotedName($table),
            implode(', ', $columns),
            implode('), (', $sql_values),
            $ignore_duplicate_key_sql
        );

        return [$sql, $param_list];
    }

    /**
     * Forms the INSERT SQL string to feed the database
     *
     * @param string     $table                table name
     * @param array      $data                 data to insert
     * @param array      $pk_field             primary key field
     * @param bool|array $ignore_duplicate_key true to append the ON DUPLICATE KEY syntax
     *
     * @return array of the sql query and params list
     */
    private function formSqlInsertSingle($table, array $data, array $pk_field, $ignore_duplicate_key)
    {
        $driver = $this->getChosenDriver();

        // assumes that no one would want some crazy SQL formed from the given $data
        $columns = array_map(array($driver, 'getQuotedName'), array_keys($data));

        $ignore_duplicate_key_sql = null;
        // either true or an array containing field names
        if ($ignore_duplicate_key && (!is_array($ignore_duplicate_key) || count($ignore_duplicate_key))) {
            $update_fields = array_filter(
                is_array($ignore_duplicate_key) ? $ignore_duplicate_key : array_keys($data),
                function ($field) use ($pk_field) {
                    return !in_array($field, $pk_field);
                }
            );

            $ignore_duplicate_key_sql_fragments = [];
            if (count($pk_field) === 1) {
                $ignore_duplicate_key_sql_fragments[] = sprintf(
                    '%s=LAST_INSERT_ID(%s)',
                    $driver->getQuotedName(reset($pk_field)),
                    $driver->getQuotedName(reset($pk_field))
                );
            }
            $ignore_duplicate_key_sql_fragments += array_map(
                function ($field) use ($driver) {
                    return $driver->getQuotedName($field).'=VALUES('.$driver->getQuotedName($field).')';
                },
                $update_fields
            );
            $ignore_duplicate_key_sql = ' ON DUPLICATE KEY UPDATE '.implode(', ', $ignore_duplicate_key_sql_fragments);
        }

        $params = self::normaliseParameterList($data);

        $param_list = array();
        $sql = sprintf(
            "INSERT INTO %s (%s) VALUES (%s) %s",
            $driver->getQuotedName($table),
            implode(', ', $columns),
            implode(
                ', ',
                array_map(
                    function ($value) use (&$param_list) {

                        // $field = $value[0];
                        $placeholder = $value[1];
                        $value = $value[2];

                        if ($placeholder !== null) {
                            $param_list[] = $value;
                            return $placeholder;
                        }

                        return $value;
                    },
                    $params
                )
            ),
            $ignore_duplicate_key_sql
        );

        return [$sql, $param_list];
    }

    /**
     * Prepares a fragment of SQL to be used within a query.
     * This supports both [field] = [value]|[placeholder] or just [value]|[placeholder]
     *
     * @param string $field        The field name to be used, set to null if it's not needed
     * @param string $placeholder  The placeholder name to be used, set to null if a value is specified instead
     * @param string $value        The value to be used, set to null if a placeholder is specified instead
     * @param Driver $driver       Database driver, used to quote field names
     * @param bool   $is_for_nulls if true, we treat NULL different, otherwise use '=' only ( for UPDATE )
     *
     * @return string
     */
    private static function prepareClause($field, $placeholder, $value, Driver $driver, $is_for_nulls = true)
    {
        $rhs = ($placeholder === null) ? $value : $placeholder;

        if ($field === null) {
            return $rhs;
        }

        // If the value is NULL, we need to use IS NULL, rather than =.
        $operator = (($is_for_nulls && ($value === null)) ? 'IS' : '=');
        return "{$driver->getQuotedName($field)} {$operator} {$rhs}";
    }

    /**
     * Forms the UPDATE SQL string to feed the database
     *
     * @param string $table         table name
     * @param array  $criteria      criteria for the update
     * @param array  $fields_update updates for the records that match criteria
     *
     * @return array of the sql query and params list
     */
    private function formSqlUpdate($table, array $criteria, array $fields_update)
    {
        $driver = $this->getChosenDriver();

        $where_params = self::normaliseParameterList($criteria);
        $update_params = self::normaliseParameterList($fields_update);

        $param_list = array();
        $sql = sprintf(
            "UPDATE %s SET %s WHERE %s",
            $driver->getQuotedName($table),
            implode(
                ', ',
                array_map(
                    function ($value) use (&$param_list, $driver) {

                        $field = $value[0];
                        $placeholder = $value[1];
                        $value = $value[2];

                        if ($placeholder !== null) {
                            $param_list[] = $value;
                        }

                        return self::prepareClause($field, $placeholder, $value, $driver, false);
                    },
                    $update_params
                )
            ),
            implode(
                ' AND ',
                array_map(
                    function ($value) use (&$param_list, $driver) {

                        $field = $value[0];
                        $placeholder = $value[1];
                        $value = $value[2];

                        if ($placeholder !== null) {
                            $param_list[] = $value;
                        }

                        return self::prepareClause($field, $placeholder, $value, $driver);
                    },
                    $where_params
                )
            )
        );

        return [$sql, $param_list];
    }

    /**
     * Forms the SELECT SQL string to feed the database
     *
     * @param string $table    table name
     * @param array  $criteria criteria for selecting records
     * @param array  $columns  columns ( or As Is expression ) to select
     *
     * @return array of the sql query and params list
     */
    private function formSqlSelect($table, $criteria, $columns = null, $limit)
    {
        $driver = $this->getChosenDriver();

        $criteriaParams = self::normaliseParameterList($criteria);

        if (!$columns) {
            $columns = [new AsIs('*')];
        } elseif (is_scalar($columns)) {
            $columns = [new AsIs($columns)];
        }

        $param_list = array();
        $sql = sprintf(
            "SELECT %s FROM %s WHERE %s",
            implode(
                ', ',
                array_map(
                    function ($column) use (&$param_list, $driver) {
                        self::normaliseAsIs($column);
                        if ($column instanceof AsIs) {
                            return (string)$column;
                        }

                        return $driver->getQuotedName($column);
                    },
                    $columns
                )
            ),
            $driver->getQuotedName($table),
            implode(
                ' AND ',
                array_map(
                    function ($value) use (&$param_list, $driver) {

                        $field = $value[0];
                        $placeholder = $value[1];
                        $value = $value[2];

                        if ($placeholder !== null) {
                            $param_list[] = $value;
                        }

                        return self::prepareClause($field, $placeholder, $value, $driver);
                    },
                    $criteriaParams
                )
            ), 
            $limit
        );

        return [$sql, $param_list];
    }

    /**
     * Forms the DELETE SQL string to feed the database
     *
     * @param string $table    table name
     * @param array  $criteria criteria for deleting records
     *
     * @return array of the sql query and params list
     */
    private function formSqlDelete($table, $criteria)
    {
        $driver = $this->getChosenDriver();

        $where_params = self::normaliseParameterList($criteria);

        $param_list = array();
        $sql = sprintf(
            "DELETE FROM %s WHERE %s",
            $driver->getQuotedName($table),
            implode(
                ' AND ',
                array_map(
                    function ($value) use (&$param_list, $driver) {

                        $field = $value[0];
                        $placeholder = $value[1];
                        $value = $value[2];

                        if ($placeholder !== null) {
                            $param_list[] = $value;
                        }

                        return self::prepareClause($field, $placeholder, $value, $driver);
                    },
                    $where_params
                )
            )
        );

        return [$sql, $param_list];
    }

    /**
     * Get driver specific encoded table and field names; eg. back-tick for database, table and field names in MySQL
     *
     * @param string $name
     *
     * @return string
     *
     * @internal for use from other modules due to incompatibility with readable steps output
     *
     */
    public function getQuotedName($name)
    {
        return $this->getChosenDriver()->getQuotedName($name);
    }
}
