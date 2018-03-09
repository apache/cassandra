/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.cql3.validation.entities;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.exceptions.InvalidRequestException;

public class CountersTest extends CQLTester
{
    /**
     * Test for the validation bug of #4706,
     * migrated from cql_tests.py:TestCQL.validate_counter_regular_test()
     */
    @Test
    public void testRegularCounters() throws Throwable
    {
        assertInvalidThrowMessage("Cannot mix counter and non counter columns in the same table",
                                  InvalidRequestException.class,
                                  String.format("CREATE TABLE %s.%s (id bigint PRIMARY KEY, count counter, things set<text>)", KEYSPACE, createTableName()));
    }

    /**
     * Migrated from cql_tests.py:TestCQL.collection_counter_test()
     */
    @Test
    public void testCountersOnCollections() throws Throwable
    {
        String tableName = KEYSPACE + "." + createTableName();
        assertInvalidThrow(InvalidRequestException.class,
                           String.format("CREATE TABLE %s (k int PRIMARY KEY, l list<counter>)", tableName));

        tableName = KEYSPACE + "." + createTableName();
        assertInvalidThrow(InvalidRequestException.class,
                           String.format("CREATE TABLE %s (k int PRIMARY KEY, s set<counter>)", tableName));

        tableName = KEYSPACE + "." + createTableName();
        assertInvalidThrow(InvalidRequestException.class,
                           String.format("CREATE TABLE %s (k int PRIMARY KEY, m map<text, counter>)", tableName));
    }

    @Test
    public void testCounterUpdatesWithUnset() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, c counter)");

        // set up
        execute("UPDATE %s SET c = c + 1 WHERE k = 10");
        assertRows(execute("SELECT c FROM %s WHERE k = 10"),
                   row(1L)
        );
        // increment
        execute("UPDATE %s SET c = c + ? WHERE k = 10", 1L);
        assertRows(execute("SELECT c FROM %s WHERE k = 10"),
                   row(2L)
        );
        execute("UPDATE %s SET c = c + ? WHERE k = 10", unset());
        assertRows(execute("SELECT c FROM %s WHERE k = 10"),
                   row(2L) // no change to the counter value
        );
        // decrement
        execute("UPDATE %s SET c = c - ? WHERE k = 10", 1L);
        assertRows(execute("SELECT c FROM %s WHERE k = 10"),
                   row(1L)
        );
        execute("UPDATE %s SET c = c - ? WHERE k = 10", unset());
        assertRows(execute("SELECT c FROM %s WHERE k = 10"),
                   row(1L) // no change to the counter value
        );
    }

    @Test
    public void testCounterFiltering() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, a counter)");

        for (int i = 0; i < 10; i++)
            execute("UPDATE %s SET a = a + ? WHERE k = ?", (long) i, i);

        execute("UPDATE %s SET a = a + ? WHERE k = ?", 6L, 10);

        // GT
        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE a > ? ALLOW FILTERING", 5L),
                                row(6, 6L),
                                row(7, 7L),
                                row(8, 8L),
                                row(9, 9L),
                                row(10, 6L));

        // GTE
        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE a >= ? ALLOW FILTERING", 6L),
                                row(6, 6L),
                                row(7, 7L),
                                row(8, 8L),
                                row(9, 9L),
                                row(10, 6L));

        // LT
        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE a < ? ALLOW FILTERING", 3L),
                                row(0, 0L),
                                row(1, 1L),
                                row(2, 2L));

        // LTE
        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE a <= ? ALLOW FILTERING", 3L),
                                row(0, 0L),
                                row(1, 1L),
                                row(2, 2L),
                                row(3, 3L));

        // EQ
        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE a = ? ALLOW FILTERING", 6L),
                                row(6, 6L),
                                row(10, 6L));
    }

    @Test
    public void testCounterFilteringWithNull() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, a counter, b counter)");
        execute("UPDATE %s SET a = a + ? WHERE k = ?", 1L, 1);

        assertRows(execute("SELECT * FROM %s WHERE a > ? ALLOW FILTERING", 0L),
                   row(1, 1L, null));
        // GT
        assertEmpty(execute("SELECT * FROM %s WHERE b > ? ALLOW FILTERING", 1L));
        // GTE
        assertEmpty(execute("SELECT * FROM %s WHERE b >= ? ALLOW FILTERING", 1L));
        // LT
        assertEmpty(execute("SELECT * FROM %s WHERE b < ? ALLOW FILTERING", 1L));
        // LTE
        assertEmpty(execute("SELECT * FROM %s WHERE b <= ? ALLOW FILTERING", 1L));
        // EQ
        assertEmpty(execute("SELECT * FROM %s WHERE b = ? ALLOW FILTERING", 1L));
        // with null
        assertInvalidMessage("Invalid null value for counter increment/decrement",
                             "SELECT * FROM %s WHERE b = null ALLOW FILTERING");
    }

    /**
     * Test for the validation bug of #9395.
     */
    @Test
    public void testProhibitReversedCounterAsPartOfPrimaryKey() throws Throwable
    {
        assertInvalidThrowMessage("counter type is not supported for PRIMARY KEY part a",
                                  InvalidRequestException.class, String.format("CREATE TABLE %s.%s (a counter, b int, PRIMARY KEY (b, a)) WITH CLUSTERING ORDER BY (a desc);", KEYSPACE, createTableName()));
    }
}
