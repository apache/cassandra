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

package org.apache.cassandra.cql3.validation.operations;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;

public class BatchTest extends CQLTester
{
    /**
     * Test batch statements
     * migrated from cql_tests.py:TestCQL.batch_test()
     */
    @Test
    public void testBatch() throws Throwable
    {
        createTable("CREATE TABLE %s (userid text PRIMARY KEY, name text, password text)");

        String query = "BEGIN BATCH\n"
                       + "INSERT INTO %1$s (userid, password, name) VALUES ('user2', 'ch@ngem3b', 'second user');\n"
                       + "UPDATE %1$s SET password = 'ps22dhds' WHERE userid = 'user3';\n"
                       + "INSERT INTO %1$s (userid, password) VALUES ('user4', 'ch@ngem3c');\n"
                       + "DELETE name FROM %1$s WHERE userid = 'user1';\n"
                       + "APPLY BATCH;";

        execute(query);
    }

    /**
     * Migrated from cql_tests.py:TestCQL.batch_and_list_test()
     */
    @Test
    public void testBatchAndList() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, l list<int>)");

        execute("BEGIN BATCH " +
                "UPDATE %1$s SET l = l +[ 1 ] WHERE k = 0; " +
                "UPDATE %1$s SET l = l + [ 2 ] WHERE k = 0; " +
                "UPDATE %1$s SET l = l + [ 3 ] WHERE k = 0; " +
                "APPLY BATCH");

        assertRows(execute("SELECT l FROM %s WHERE k = 0"),
                   row(list(1, 2, 3)));

        execute("BEGIN BATCH " +
                "UPDATE %1$s SET l =[ 1 ] + l WHERE k = 1; " +
                "UPDATE %1$s SET l = [ 2 ] + l WHERE k = 1; " +
                "UPDATE %1$s SET l = [ 3 ] + l WHERE k = 1; " +
                "APPLY BATCH ");

        assertRows(execute("SELECT l FROM %s WHERE k = 1"),
                   row(list(3, 2, 1)));
    }

    /**
     * Migrated from cql_tests.py:TestCQL.bug_6115_test()
     */
    @Test
    public void testBatchDeleteInsert() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, v int, PRIMARY KEY (k, v))");

        execute("INSERT INTO %s (k, v) VALUES (0, 1)");
        execute("BEGIN BATCH DELETE FROM %1$s WHERE k=0 AND v=1; INSERT INTO %1$s (k, v) VALUES (0, 2); APPLY BATCH");

        assertRows(execute("SELECT * FROM %s"),
                   row(0, 2));
    }

    @Test
    public void testBatchWithUnset() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, s text, i int)");

        // test batch and update
        execute("BEGIN BATCH " +
                "INSERT INTO %1$s (k, s, i) VALUES (100, 'batchtext', 7); " +
                "INSERT INTO %1$s (k, s, i) VALUES (111, 'batchtext', 7); " +
                "UPDATE %1$s SET s=?, i=? WHERE k = 100; " +
                "UPDATE %1$s SET s=?, i=? WHERE k=111; " +
                "APPLY BATCH;", null, unset(), unset(), null);
        assertRows(execute("SELECT k, s, i FROM %s where k in (100,111)"),
                   row(100, null, 7),
                   row(111, "batchtext", null)
        );
    }

    @Test
    public void testBatchRangeDelete() throws Throwable
    {
        createTable("CREATE TABLE %s (partitionKey int," +
                "clustering int," +
                "value int," +
                " PRIMARY KEY (partitionKey, clustering)) WITH COMPACT STORAGE");

        int value = 0;
        for (int partitionKey = 0; partitionKey < 4; partitionKey++)
            for (int clustering1 = 0; clustering1 < 5; clustering1++)
                execute("INSERT INTO %s (partitionKey, clustering, value) VALUES (?, ?, ?)",
                        partitionKey, clustering1, value++);

        execute("BEGIN BATCH " +
                "DELETE FROM %1$s WHERE partitionKey = 1;" +
                "DELETE FROM %1$s WHERE partitionKey = 0 AND  clustering >= 4;" +
                "DELETE FROM %1$s WHERE partitionKey = 0 AND clustering <= 0;" +
                "DELETE FROM %1$s WHERE partitionKey = 2 AND clustering >= 0 AND clustering <= 3;" +
                "DELETE FROM %1$s WHERE partitionKey = 2 AND clustering <= 3 AND clustering >= 4;" +
                "DELETE FROM %1$s WHERE partitionKey = 3 AND (clustering) >= (3) AND (clustering) <= (6);" +
                "APPLY BATCH;");

        assertRows(execute("SELECT * FROM %s"),
                   row(0, 1, 1),
                   row(0, 2, 2),
                   row(0, 3, 3),
                   row(2, 4, 14),
                   row(3, 0, 15),
                   row(3, 1, 16),
                   row(3, 2, 17));
    }

    @Test
    public void testBatchUpdate() throws Throwable
    {
        createTable("CREATE TABLE %s (partitionKey int," +
                "clustering_1 int," +
                "value int," +
                " PRIMARY KEY (partitionKey, clustering_1))");

        execute("INSERT INTO %s (partitionKey, clustering_1, value) VALUES (0, 0, 0)");
        execute("INSERT INTO %s (partitionKey, clustering_1, value) VALUES (0, 1, 1)");
        execute("INSERT INTO %s (partitionKey, clustering_1, value) VALUES (0, 2, 2)");
        execute("INSERT INTO %s (partitionKey, clustering_1, value) VALUES (0, 3, 3)");
        execute("INSERT INTO %s (partitionKey, clustering_1, value) VALUES (0, 4, 4)");
        execute("INSERT INTO %s (partitionKey, clustering_1, value) VALUES (0, 5, 5)");
        execute("INSERT INTO %s (partitionKey, clustering_1, value) VALUES (0, 6, 6)");

        execute("BEGIN BATCH " +
                "UPDATE %1$s SET value = 7 WHERE partitionKey = 0 AND clustering_1 = 1" +
                "UPDATE %1$s SET value = 8 WHERE partitionKey = 0 AND (clustering_1) = (2)" +
                "UPDATE %1$s SET value = 10 WHERE partitionKey = 0 AND clustering_1 IN (3, 4)" +
                "UPDATE %1$s SET value = 20 WHERE partitionKey = 0 AND (clustering_1) IN ((5), (6))" +
                "APPLY BATCH;");

        assertRows(execute("SELECT * FROM %s"),
                   row(0, 0, 0),
                   row(0, 1, 7),
                   row(0, 2, 8),
                   row(0, 3, 10),
                   row(0, 4, 10),
                   row(0, 5, 20),
                   row(0, 6, 20));
    }

    @Test
    public void testBatchEmpty() throws Throwable
    {
        assertEmpty(execute("BEGIN BATCH APPLY BATCH;"));
    }

    @Test
    public void testBatchMultipleTable() throws Throwable
    {
        String tbl1 = KEYSPACE + "." + createTableName();
        String tbl2 = KEYSPACE + "." + createTableName();

        schemaChange(String.format("CREATE TABLE %s (k1 int PRIMARY KEY, v11 int, v12 int)", tbl1));
        schemaChange(String.format("CREATE TABLE %s (k2 int PRIMARY KEY, v21 int, v22 int)", tbl2));

        execute("BEGIN BATCH " +
                String.format("UPDATE %s SET v11 = 1 WHERE k1 = 0;", tbl1) +
                String.format("UPDATE %s SET v12 = 2 WHERE k1 = 0;", tbl1) +
                String.format("UPDATE %s SET v21 = 3 WHERE k2 = 0;", tbl2) +
                String.format("UPDATE %s SET v22 = 4 WHERE k2 = 0;", tbl2) +
                "APPLY BATCH;");

        assertRows(execute(String.format("SELECT * FROM %s", tbl1)), row(0, 1, 2));
        assertRows(execute(String.format("SELECT * FROM %s", tbl2)), row(0, 3, 4));

        flush();

        assertRows(execute(String.format("SELECT * FROM %s", tbl1)), row(0, 1, 2));
        assertRows(execute(String.format("SELECT * FROM %s", tbl2)), row(0, 3, 4));
    }
}
