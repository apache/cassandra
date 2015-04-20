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
package org.apache.cassandra.cql3;

import org.junit.Test;

public class ModificationTest extends CQLTester
{
    @Test
    public void testModificationWithUnset() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, s text, i int)");

        // insert using nulls
        execute("INSERT INTO %s (k, s, i) VALUES (10, ?, ?)", "text", 10);
        execute("INSERT INTO %s (k, s, i) VALUES (10, ?, ?)", null, null);
        assertRows(execute("SELECT s, i FROM %s WHERE k = 10"),
                row(null, null) // sending null deletes the data
        );
        // insert using UNSET
        execute("INSERT INTO %s (k, s, i) VALUES (11, ?, ?)", "text", 10);
        execute("INSERT INTO %s (k, s, i) VALUES (11, ?, ?)", unset(), unset());
        assertRows(execute("SELECT s, i FROM %s WHERE k=11"),
                row("text", 10) // unset columns does not delete the existing data
        );

        assertInvalidMessage("Invalid unset value for column k", "UPDATE %s SET i = 0 WHERE k = ?", unset());
        assertInvalidMessage("Invalid unset value for column k", "DELETE FROM %s WHERE k = ?", unset());
        assertInvalidMessage("Invalid unset value for argument in call to function blobasint", "SELECT * FROM %s WHERE k = blobAsInt(?)", unset());
    }

    @Test
    public void testTtlWithUnset() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, i int)");
        execute("INSERT INTO %s (k, i) VALUES (1, 1) USING TTL ?", unset()); // treat as 'unlimited'
        assertRows(execute("SELECT ttl(i) FROM %s"),
                row(new Object[] {null})
        );
    }

    @Test
    public void testTimestampWithUnset() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, i int)");
        execute("INSERT INTO %s (k, i) VALUES (1, 1) USING TIMESTAMP ?", unset()); // treat as 'now'
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
    public void testBatchWithUnset() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, s text, i int)");

        // test batch and update
        String qualifiedTable = keyspace() + "." + currentTable();
        execute("BEGIN BATCH " +
                 "INSERT INTO %s (k, s, i) VALUES (100, 'batchtext', 7); " +
                 "INSERT INTO " + qualifiedTable + " (k, s, i) VALUES (111, 'batchtext', 7); " +
             "UPDATE " + qualifiedTable + " SET s=?, i=? WHERE k = 100; " +
                 "UPDATE " + qualifiedTable + " SET s=?, i=? WHERE k=111; " +
                 "APPLY BATCH;", null, unset(), unset(), null);
        assertRows(execute("SELECT k, s, i FROM %s where k in (100,111)"),
            row(100, null, 7),
            row(111, "batchtext", null)
        );
    }
}
