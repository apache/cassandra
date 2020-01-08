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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;

import static org.apache.cassandra.utils.ByteBufferUtil.EMPTY_BYTE_BUFFER;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DeleteTest extends CQLTester
{
    /** Test for cassandra 8558 */
    @Test
    public void testRangeDeletion() throws Throwable
    {
        testRangeDeletion(true, true);
        testRangeDeletion(false, true);
        testRangeDeletion(true, false);
        testRangeDeletion(false, false);
    }

    private void testRangeDeletion(boolean flushData, boolean flushTombstone) throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY (a, b, c))");
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 1, 1, 1);
        flush(flushData);
        execute("DELETE FROM %s WHERE a=? AND b=?", 1, 1);
        flush(flushTombstone);
        assertEmpty(execute("SELECT * FROM %s WHERE a=? AND b=? AND c=?", 1, 1, 1));
    }

    @Test
    public void testDeleteRange() throws Throwable
    {
        testDeleteRange(true, true);
        testDeleteRange(false, true);
        testDeleteRange(true, false);
        testDeleteRange(false, false);
    }

    private void testDeleteRange(boolean flushData, boolean flushTombstone) throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a, b))");

        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 1, 1, 1);
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 2, 1, 2);
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 2, 2, 3);
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 2, 3, 4);
        flush(flushData);

        execute("DELETE FROM %s WHERE a = ? AND b >= ?", 2, 2);
        flush(flushTombstone);

        assertRowsIgnoringOrder(execute("SELECT * FROM %s"),
                                row(1, 1, 1),
                                row(2, 1, 2));

        assertRows(execute("SELECT * FROM %s WHERE a = ? AND b = ?", 2, 1),
                   row(2, 1, 2));
        assertEmpty(execute("SELECT * FROM %s WHERE a = ? AND b = ?", 2, 2));
        assertEmpty(execute("SELECT * FROM %s WHERE a = ? AND b = ?", 2, 3));
    }

    @Test
    public void testCrossMemSSTableMultiColumn() throws Throwable
    {
        testCrossMemSSTableMultiColumn(true, true);
        testCrossMemSSTableMultiColumn(false, true);
        testCrossMemSSTableMultiColumn(true, false);
        testCrossMemSSTableMultiColumn(false, false);
    }

    private void testCrossMemSSTableMultiColumn(boolean flushData, boolean flushTombstone) throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a, b))");

        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 1, 1, 1);
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 2, 1, 2);
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 2, 2, 2);
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 2, 3, 3);
        flush(flushData);

        execute("DELETE FROM %s WHERE a = ? AND (b) = (?)", 2, 2);
        execute("DELETE FROM %s WHERE a = ? AND (b) = (?)", 2, 3);

        flush(flushTombstone);

        assertRowsIgnoringOrder(execute("SELECT * FROM %s"),
                                row(1, 1, 1),
                                row(2, 1, 2));

        assertRows(execute("SELECT * FROM %s WHERE a = ? AND b = ?", 2, 1),
                   row(2, 1, 2));
        assertEmpty(execute("SELECT * FROM %s WHERE a = ? AND b = ?", 2, 2));
        assertEmpty(execute("SELECT * FROM %s WHERE a = ? AND b = ?", 2, 3));
    }


    /**
     * Test simple deletion and in particular check for #4193 bug
     * migrated from cql_tests.py:TestCQL.deletion_test()
     */
    @Test
    public void testDeletion() throws Throwable
    {
        createTable("CREATE TABLE %s (username varchar, id int, name varchar, stuff varchar, PRIMARY KEY(username, id))");

        execute("INSERT INTO %s (username, id, name, stuff) VALUES (?, ?, ?, ?)", "abc", 2, "rst", "some value");
        execute("INSERT INTO %s (username, id, name, stuff) VALUES (?, ?, ?, ?)", "abc", 4, "xyz", "some other value");

        assertRows(execute("SELECT * FROM %s"),
                   row("abc", 2, "rst", "some value"),
                   row("abc", 4, "xyz", "some other value"));

        execute("DELETE FROM %s WHERE username='abc' AND id=2");

        assertRows(execute("SELECT * FROM %s"),
                   row("abc", 4, "xyz", "some other value"));

        createTable("CREATE TABLE %s (username varchar, id int, name varchar, stuff varchar, PRIMARY KEY(username, id, name)) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (username, id, name, stuff) VALUES (?, ?, ?, ?)", "abc", 2, "rst", "some value");
        execute("INSERT INTO %s (username, id, name, stuff) VALUES (?, ?, ?, ?)", "abc", 4, "xyz", "some other value");

        assertRows(execute("SELECT * FROM %s"),
                   row("abc", 2, "rst", "some value"),
                   row("abc", 4, "xyz", "some other value"));

        execute("DELETE FROM %s WHERE username='abc' AND id=2");

        assertRows(execute("SELECT * FROM %s"),
                   row("abc", 4, "xyz", "some other value"));
    }

    /**
     * Test deletion by 'composite prefix' (range tombstones)
     * migrated from cql_tests.py:TestCQL.range_tombstones_test()
     */
    @Test
    public void testDeleteByCompositePrefix() throws Throwable
    { // This test used 3 nodes just to make sure RowMutation are correctly serialized

        createTable("CREATE TABLE %s ( k int, c1 int, c2 int, v1 int, v2 int, PRIMARY KEY (k, c1, c2))");

        int numRows = 5;
        int col1 = 2;
        int col2 = 2;
        int cpr = col1 * col2;

        for (int i = 0; i < numRows; i++)
            for (int j = 0; j < col1; j++)
                for (int k = 0; k < col2; k++)
                {
                    int n = (i * cpr) + (j * col2) + k;
                    execute("INSERT INTO %s (k, c1, c2, v1, v2) VALUES (?, ?, ?, ?, ?)", i, j, k, n, n);
                }

        for (int i = 0; i < numRows; i++)
        {
            Object[][] rows = getRows(execute("SELECT v1, v2 FROM %s where k = ?", i));
            for (int x = i * cpr; x < (i + 1) * cpr; x++)
            {
                assertEquals(x, rows[x - i * cpr][0]);
                assertEquals(x, rows[x - i * cpr][1]);
            }
        }

        for (int i = 0; i < numRows; i++)
            execute("DELETE FROM %s WHERE k = ? AND c1 = 0", i);

        for (int i = 0; i < numRows; i++)
        {
            Object[][] rows = getRows(execute("SELECT v1, v2 FROM %s WHERE k = ?", i));
            for (int x = i * cpr + col1; x < (i + 1) * cpr; x++)
            {
                assertEquals(x, rows[x - i * cpr - col1][0]);
                assertEquals(x, rows[x - i * cpr - col1][1]);
            }
        }

        for (int i = 0; i < numRows; i++)
        {
            Object[][] rows = getRows(execute("SELECT v1, v2 FROM %s WHERE k = ?", i));
            for (int x = i * cpr + col1; x < (i + 1) * cpr; x++)
            {
                assertEquals(x, rows[x - i * cpr - col1][0]);
                assertEquals(x, rows[x - i * cpr - col1][1]);
            }
        }
    }

    /**
     * Test deletion by 'composite prefix' (range tombstones) with compaction
     * migrated from cql_tests.py:TestCQL.range_tombstones_compaction_test()
     */
    @Test
    public void testDeleteByCompositePrefixWithCompaction() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c1 int, c2 int, v1 text, PRIMARY KEY (k, c1, c2))");

        for (int c1 = 0; c1 < 4; c1++)
            for (int c2 = 0; c2 < 2; c2++)
                execute("INSERT INTO %s (k, c1, c2, v1) VALUES (0, ?, ?, ?)", c1, c2, String.format("%d%d", c1, c2));

        flush();

        execute("DELETE FROM %s WHERE k = 0 AND c1 = 1");

        flush();
        compact();

        Object[][] rows = getRows(execute("SELECT v1 FROM %s WHERE k = 0"));

        int idx = 0;
        for (int c1 = 0; c1 < 4; c1++)
            for (int c2 = 0; c2 < 2; c2++)
                if (c1 != 1)
                    assertEquals(String.format("%d%d", c1, c2), rows[idx++][0]);
    }

    /**
     * Test deletion of rows
     * migrated from cql_tests.py:TestCQL.delete_row_test()
     */
    @Test
    public void testRowDeletion() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c1 int, c2 int, v1 int, v2 int, PRIMARY KEY (k, c1, c2))");

        execute("INSERT INTO %s (k, c1, c2, v1, v2) VALUES (?, ?, ?, ?, ?)", 0, 0, 0, 0, 0);
        execute("INSERT INTO %s (k, c1, c2, v1, v2) VALUES (?, ?, ?, ?, ?)", 0, 0, 1, 1, 1);
        execute("INSERT INTO %s (k, c1, c2, v1, v2) VALUES (?, ?, ?, ?, ?)", 0, 0, 2, 2, 2);
        execute("INSERT INTO %s (k, c1, c2, v1, v2) VALUES (?, ?, ?, ?, ?)", 0, 1, 0, 3, 3);

        execute("DELETE FROM %s WHERE k = 0 AND c1 = 0 AND c2 = 0");

        assertRowCount(execute("SELECT * FROM %s"), 3);
    }

    /**
     * Check the semantic of CQL row existence (part of #4361),
     * migrated from cql_tests.py:TestCQL.row_existence_test()
     */
    @Test
    public void testRowExistence() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v1 int, v2 int, PRIMARY KEY (k, c))");

        execute("INSERT INTO %s (k, c, v1, v2) VALUES (1, 1, 1, 1)");
        assertRows(execute("SELECT * FROM %s"),
                   row(1, 1, 1, 1));

        assertInvalid("DELETE c FROM %s WHERE k = 1 AND c = 1");

        execute("DELETE v2 FROM %s WHERE k = 1 AND c = 1");
        assertRows(execute("SELECT * FROM %s"),
                   row(1, 1, 1, null));

        execute("DELETE v1 FROM %s WHERE k = 1 AND c = 1");
        assertRows(execute("SELECT * FROM %s"),
                   row(1, 1, null, null));

        execute("DELETE FROM %s WHERE k = 1 AND c = 1");
        assertEmpty(execute("SELECT * FROM %s"));

        execute("INSERT INTO %s (k, c) VALUES (2, 2)");
        assertRows(execute("SELECT * FROM %s"),
                   row(2, 2, null, null));
    }

    /**
     * Migrated from cql_tests.py:TestCQL.remove_range_slice_test()
     */
    @Test
    public void testRemoveRangeSlice() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");

        for (int i = 0; i < 3; i++)
            execute("INSERT INTO %s (k, v) VALUES (?, ?)", i, i);

        execute("DELETE FROM %s WHERE k = 1");
        assertRows(execute("SELECT * FROM %s"),
                   row(0, 0),
                   row(2, 2));
    }

    /**
     * Test deletions
     * migrated from cql_tests.py:TestCQL.no_range_ghost_test()
     */
    @Test
    public void testNoRangeGhost() throws Throwable
    {
        createTable("CREATE TABLE %s ( k int PRIMARY KEY, v int ) ");

        for (int k = 0; k < 5; k++)
            execute("INSERT INTO %s (k, v) VALUES (?, 0)", k);

        Object[][] rows = getRows(execute("SELECT k FROM %s"));

        int[] ordered = sortIntRows(rows);
        for (int k = 0; k < 5; k++)
            assertEquals(k, ordered[k]);

        execute("DELETE FROM %s WHERE k=2");

        rows = getRows(execute("SELECT k FROM %s"));
        ordered = sortIntRows(rows);

        int idx = 0;
        for (int k = 0; k < 5; k++)
            if (k != 2)
                assertEquals(k, ordered[idx++]);

        // Example from #3505
        createTable("CREATE TABLE %s ( KEY varchar PRIMARY KEY, password varchar, gender varchar, birth_year bigint)");
        execute("INSERT INTO %s (KEY, password) VALUES ('user1', 'ch@ngem3a')");
        execute("UPDATE %s SET gender = 'm', birth_year = 1980 WHERE KEY = 'user1'");

        assertRows(execute("SELECT * FROM %s WHERE KEY='user1'"),
                   row("user1", 1980L, "m", "ch@ngem3a"));

        execute("TRUNCATE %s");
        assertEmpty(execute("SELECT * FROM %s"));

        assertEmpty(execute("SELECT * FROM %s WHERE KEY='user1'"));
    }

    private int[] sortIntRows(Object[][] rows)
    {
        int[] ret = new int[rows.length];
        for (int i = 0; i < ret.length; i++)
            ret[i] = rows[i][0] == null ? Integer.MIN_VALUE : (Integer) rows[i][0];
        Arrays.sort(ret);
        return ret;
    }

    /**
     * Migrated from cql_tests.py:TestCQL.range_with_deletes_test()
     */
    @Test
    public void testRandomDeletions() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int,)");

        int nb_keys = 30;
        int nb_deletes = 5;

        List<Integer> deletions = new ArrayList<>(nb_keys);
        for (int i = 0; i < nb_keys; i++)
        {
            execute("INSERT INTO %s (k, v) VALUES (?, ?)", i, i);
            deletions.add(i);
        }

        Collections.shuffle(deletions);

        for (int i = 0; i < nb_deletes; i++)
            execute("DELETE FROM %s WHERE k = ?", deletions.get(i));

        assertRowCount(execute("SELECT * FROM %s LIMIT ?", (nb_keys / 2)), nb_keys / 2);
    }

    /**
     * Test for CASSANDRA-8558, deleted row still can be selected out
     * migrated from cql_tests.py:TestCQL.bug_8558_test()
     */
    @Test
    public void testDeletedRowCannotBeSelected() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c text,primary key(a,b))");
        execute("INSERT INTO %s (a,b,c) VALUES(1,1,'1')");
        flush();

        execute("DELETE FROM %s  where a=1 and b=1");
        flush();

        assertEmpty(execute("select * from %s  where a=1 and b=1"));
    }

    /** Test that two deleted rows for the same partition but on different sstables do not resurface */
    @Test
    public void testDeletedRowsDoNotResurface() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c text, primary key (a, b))");
        execute("INSERT INTO %s (a, b, c) VALUES(1, 1, '1')");
        execute("INSERT INTO %s (a, b, c) VALUES(1, 2, '2')");
        execute("INSERT INTO %s (a, b, c) VALUES(1, 3, '3')");
        flush();

        execute("DELETE FROM %s where a=1 and b = 1");
        flush();

        execute("DELETE FROM %s where a=1 and b = 2");
        flush();

        assertRows(execute("SELECT * FROM %s WHERE a = ?", 1),
                   row(1, 3, "3"));
    }

    @Test
    public void testDeleteWithNoClusteringColumns() throws Throwable
    {
        testDeleteWithNoClusteringColumns(false);
        testDeleteWithNoClusteringColumns(true);
    }

    private void testDeleteWithNoClusteringColumns(boolean forceFlush) throws Throwable
    {
        for (String compactOption : new String[] {"", " WITH COMPACT STORAGE" })
        {
            createTable("CREATE TABLE %s (partitionKey int PRIMARY KEY," +
                                      "value int)" + compactOption);

            execute("INSERT INTO %s (partitionKey, value) VALUES (0, 0)");
            execute("INSERT INTO %s (partitionKey, value) VALUES (1, 1)");
            execute("INSERT INTO %s (partitionKey, value) VALUES (2, 2)");
            execute("INSERT INTO %s (partitionKey, value) VALUES (3, 3)");
            flush(forceFlush);

            execute("DELETE value FROM %s WHERE partitionKey = ?", 0);
            flush(forceFlush);

            if (isEmpty(compactOption))
            {
                assertRows(execute("SELECT * FROM %s WHERE partitionKey = ?", 0),
                           row(0, null));
            }
            else
            {
                assertEmpty(execute("SELECT * FROM %s WHERE partitionKey = ?", 0));
            }

            execute("DELETE FROM %s WHERE partitionKey IN (?, ?)", 0, 1);
            flush(forceFlush);
            assertRows(execute("SELECT * FROM %s"),
                       row(2, 2),
                       row(3, 3));

            // test invalid queries

            // token function
            assertInvalidMessage("The token function cannot be used in WHERE clauses for DELETE statements",
                                 "DELETE FROM %s WHERE token(partitionKey) = token(?)", 0);

            // multiple time same primary key element in WHERE clause
            assertInvalidMessage("partitionkey cannot be restricted by more than one relation if it includes an Equal",
                                 "DELETE FROM %s WHERE partitionKey = ? AND partitionKey = ?", 0, 1);

            // unknown identifiers
            assertInvalidMessage("Undefined column name unknown",
                                 "DELETE unknown FROM %s WHERE partitionKey = ?", 0);

            assertInvalidMessage("Undefined column name partitionkey1",
                                 "DELETE FROM %s WHERE partitionKey1 = ?", 0);

            // Invalid operator in the where clause
            assertInvalidMessage("Only EQ and IN relation are supported on the partition key (unless you use the token() function)",
                                 "DELETE FROM %s WHERE partitionKey > ? ", 0);

            assertInvalidMessage("Cannot use CONTAINS on non-collection column partitionkey",
                                 "DELETE FROM %s WHERE partitionKey CONTAINS ?", 0);

            // Non primary key in the where clause
            assertInvalidMessage("Non PRIMARY KEY columns found in where clause: value",
                                 "DELETE FROM %s WHERE partitionKey = ? AND value = ?", 0, 1);
        }
    }

    @Test
    public void testDeleteWithOneClusteringColumns() throws Throwable
    {
        testDeleteWithOneClusteringColumns(false);
        testDeleteWithOneClusteringColumns(true);
    }

    private void testDeleteWithOneClusteringColumns(boolean forceFlush) throws Throwable
    {
        for (String compactOption : new String[] {"", " WITH COMPACT STORAGE" })
        {
            createTable("CREATE TABLE %s (partitionKey int," +
                                      "clustering int," +
                                      "value int," +
                                      " PRIMARY KEY (partitionKey, clustering))" + compactOption);

            execute("INSERT INTO %s (partitionKey, clustering, value) VALUES (0, 0, 0)");
            execute("INSERT INTO %s (partitionKey, clustering, value) VALUES (0, 1, 1)");
            execute("INSERT INTO %s (partitionKey, clustering, value) VALUES (0, 2, 2)");
            execute("INSERT INTO %s (partitionKey, clustering, value) VALUES (0, 3, 3)");
            execute("INSERT INTO %s (partitionKey, clustering, value) VALUES (0, 4, 4)");
            execute("INSERT INTO %s (partitionKey, clustering, value) VALUES (0, 5, 5)");
            execute("INSERT INTO %s (partitionKey, clustering, value) VALUES (1, 0, 6)");
            flush(forceFlush);

            execute("DELETE value FROM %s WHERE partitionKey = ? AND clustering = ?", 0, 1);
            flush(forceFlush);
            if (isEmpty(compactOption))
            {
                assertRows(execute("SELECT * FROM %s WHERE partitionKey = ? AND clustering = ?", 0, 1),
                           row(0, 1, null));
            }
            else
            {
                assertEmpty(execute("SELECT * FROM %s WHERE partitionKey = ? AND clustering = ?", 0, 1));
            }

            execute("DELETE FROM %s WHERE partitionKey = ? AND clustering = ?", 0, 1);
            flush(forceFlush);
            assertEmpty(execute("SELECT value FROM %s WHERE partitionKey = ? AND clustering = ?", 0, 1));

            execute("DELETE FROM %s WHERE partitionKey IN (?, ?) AND clustering = ?", 0, 1, 0);
            flush(forceFlush);
            assertRows(execute("SELECT * FROM %s WHERE partitionKey IN (?, ?)", 0, 1),
                       row(0, 2, 2),
                       row(0, 3, 3),
                       row(0, 4, 4),
                       row(0, 5, 5));

            execute("DELETE FROM %s WHERE partitionKey = ? AND (clustering) IN ((?), (?))", 0, 4, 5);
            flush(forceFlush);
            assertRows(execute("SELECT * FROM %s WHERE partitionKey IN (?, ?)", 0, 1),
                       row(0, 2, 2),
                       row(0, 3, 3));

            // test invalid queries

            // missing primary key element
            assertInvalidMessage("Some partition key parts are missing: partitionkey",
                                 "DELETE FROM %s WHERE clustering = ?", 1);

            // token function
            assertInvalidMessage("The token function cannot be used in WHERE clauses for DELETE statements",
                                 "DELETE FROM %s WHERE token(partitionKey) = token(?) AND clustering = ? ", 0, 1);

            // multiple time same primary key element in WHERE clause
            assertInvalidMessage("clustering cannot be restricted by more than one relation if it includes an Equal",
                                 "DELETE FROM %s WHERE partitionKey = ? AND clustering = ? AND clustering = ?", 0, 1, 1);

            // unknown identifiers
            assertInvalidMessage("Undefined column name value1",
                                 "DELETE value1 FROM %s WHERE partitionKey = ? AND clustering = ?", 0, 1);

            assertInvalidMessage("Undefined column name partitionkey1",
                                 "DELETE FROM %s WHERE partitionKey1 = ? AND clustering = ?", 0, 1);

            assertInvalidMessage("Undefined column name clustering_3",
                                 "DELETE FROM %s WHERE partitionKey = ? AND clustering_3 = ?", 0, 1);

            // Invalid operator in the where clause
            assertInvalidMessage("Only EQ and IN relation are supported on the partition key (unless you use the token() function)",
                                 "DELETE FROM %s WHERE partitionKey > ? AND clustering = ?", 0, 1);

            assertInvalidMessage("Cannot use CONTAINS on non-collection column partitionkey",
                                 "DELETE FROM %s WHERE partitionKey CONTAINS ? AND clustering = ?", 0, 1);

            // Non primary key in the where clause
            assertInvalidMessage("Non PRIMARY KEY columns found in where clause: value",
                                 "DELETE FROM %s WHERE partitionKey = ? AND clustering = ? AND value = ?", 0, 1, 3);
        }
    }

    @Test
    public void testDeleteWithTwoClusteringColumns() throws Throwable
    {
        testDeleteWithTwoClusteringColumns(false);
        testDeleteWithTwoClusteringColumns(true);
    }

    private void testDeleteWithTwoClusteringColumns(boolean forceFlush) throws Throwable
    {
        for (String compactOption : new String[] { "", " WITH COMPACT STORAGE" })
        {
            createTable("CREATE TABLE %s (partitionKey int," +
                                      "clustering_1 int," +
                                      "clustering_2 int," +
                                      "value int," +
                                      " PRIMARY KEY (partitionKey, clustering_1, clustering_2))" + compactOption);

            execute("INSERT INTO %s (partitionKey, clustering_1, clustering_2, value) VALUES (0, 0, 0, 0)");
            execute("INSERT INTO %s (partitionKey, clustering_1, clustering_2, value) VALUES (0, 0, 1, 1)");
            execute("INSERT INTO %s (partitionKey, clustering_1, clustering_2, value) VALUES (0, 0, 2, 2)");
            execute("INSERT INTO %s (partitionKey, clustering_1, clustering_2, value) VALUES (0, 0, 3, 3)");
            execute("INSERT INTO %s (partitionKey, clustering_1, clustering_2, value) VALUES (0, 1, 1, 4)");
            execute("INSERT INTO %s (partitionKey, clustering_1, clustering_2, value) VALUES (0, 1, 2, 5)");
            execute("INSERT INTO %s (partitionKey, clustering_1, clustering_2, value) VALUES (1, 0, 0, 6)");
            flush(forceFlush);

            execute("DELETE value FROM %s WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ?", 0, 1, 1);
            flush(forceFlush);

            if (isEmpty(compactOption))
            {
                assertRows(execute("SELECT * FROM %s WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ?",
                                   0, 1, 1),
                           row(0, 1, 1, null));
            }
            else
            {
                assertEmpty(execute("SELECT * FROM %s WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ?",
                                   0, 1, 1));
            }

            execute("DELETE FROM %s WHERE partitionKey = ? AND (clustering_1, clustering_2) = (?, ?)", 0, 1, 1);
            flush(forceFlush);
            assertEmpty(execute("SELECT value FROM %s WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ?",
                                0, 1, 1));

            execute("DELETE FROM %s WHERE partitionKey IN (?, ?) AND clustering_1 = ? AND clustering_2 = ?", 0, 1, 0, 0);
            flush(forceFlush);
            assertRows(execute("SELECT * FROM %s WHERE partitionKey IN (?, ?)", 0, 1),
                       row(0, 0, 1, 1),
                       row(0, 0, 2, 2),
                       row(0, 0, 3, 3),
                       row(0, 1, 2, 5));

            Object[][] rows;
            if (isEmpty(compactOption))
            {
                rows = new Object[][]{row(0, 0, 1, 1),
                                      row(0, 0, 2, null),
                                      row(0, 0, 3, null),
                                      row(0, 1, 2, 5)};
            }
            else
            {
                rows = new Object[][]{row(0, 0, 1, 1), row(0, 1, 2, 5)};
            }

            execute("DELETE value FROM %s WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 IN (?, ?)", 0, 0, 2, 3);
            flush(forceFlush);
            assertRows(execute("SELECT * FROM %s WHERE partitionKey IN (?, ?)", 0, 1), rows);

            if (isEmpty(compactOption))
            {
                rows = new Object[][]{row(0, 0, 1, 1),
                                      row(0, 0, 3, null)};
            }
            else
            {
                rows = new Object[][]{row(0, 0, 1, 1)};
            }

            execute("DELETE FROM %s WHERE partitionKey = ? AND (clustering_1, clustering_2) IN ((?, ?), (?, ?))", 0, 0, 2, 1, 2);
            flush(forceFlush);
            assertRows(execute("SELECT * FROM %s WHERE partitionKey IN (?, ?)", 0, 1), rows);

            execute("DELETE FROM %s WHERE partitionKey = ? AND (clustering_1) IN ((?), (?)) AND clustering_2 = ?", 0, 0, 2, 3);
            flush(forceFlush);
            assertRows(execute("SELECT * FROM %s WHERE partitionKey IN (?, ?)", 0, 1),
                       row(0, 0, 1, 1));

            // test invalid queries

            // missing primary key element
            assertInvalidMessage("Some partition key parts are missing: partitionkey",
                                 "DELETE FROM %s WHERE clustering_1 = ? AND clustering_2 = ?", 1, 1);

            assertInvalidMessage("PRIMARY KEY column \"clustering_2\" cannot be restricted as preceding column \"clustering_1\" is not restricted",
                                 "DELETE FROM %s WHERE partitionKey = ? AND clustering_2 = ?", 0, 1);

            // token function
            assertInvalidMessage("The token function cannot be used in WHERE clauses for DELETE statements",
                                 "DELETE FROM %s WHERE token(partitionKey) = token(?) AND clustering_1 = ? AND clustering_2 = ?", 0, 1, 1);

            // multiple time same primary key element in WHERE clause
            assertInvalidMessage("clustering_1 cannot be restricted by more than one relation if it includes an Equal",
                                 "DELETE FROM %s WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ? AND clustering_1 = ?", 0, 1, 1, 1);

            // unknown identifiers
            assertInvalidMessage("Undefined column name value1",
                                 "DELETE value1 FROM %s WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ?", 0, 1, 1);

            assertInvalidMessage("Undefined column name partitionkey1",
                                 "DELETE FROM %s WHERE partitionKey1 = ? AND clustering_1 = ? AND clustering_2 = ?", 0, 1, 1);

            assertInvalidMessage("Undefined column name clustering_3",
                                 "DELETE FROM %s WHERE partitionKey = ? AND clustering_1 = ? AND clustering_3 = ?", 0, 1, 1);

            // Invalid operator in the where clause
            assertInvalidMessage("Only EQ and IN relation are supported on the partition key (unless you use the token() function)",
                                 "DELETE FROM %s WHERE partitionKey > ? AND clustering_1 = ? AND clustering_2 = ?", 0, 1, 1);

            assertInvalidMessage("Cannot use CONTAINS on non-collection column partitionkey",
                                 "DELETE FROM %s WHERE partitionKey CONTAINS ? AND clustering_1 = ? AND clustering_2 = ?", 0, 1, 1);

            // Non primary key in the where clause
            assertInvalidMessage("Non PRIMARY KEY columns found in where clause: value",
                                 "DELETE FROM %s WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ? AND value = ?", 0, 1, 1, 3);
        }
    }

    @Test
    public void testDeleteWithNonoverlappingRange() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c text, primary key (a, b))");

        for (int i = 0; i < 10; i++)
            execute("INSERT INTO %s (a, b, c) VALUES(1, ?, 'abc')", i);
        flush();

        execute("DELETE FROM %s WHERE a=1 and b <= 3");
        flush();

        // this query does not overlap the tombstone range above and caused the rows to be resurrected
        assertEmpty(execute("SELECT * FROM %s WHERE a=1 and b <= 2"));
    }

    @Test
    public void testDeleteWithIntermediateRangeAndOneClusteringColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c text, primary key (a, b))");
        execute("INSERT INTO %s (a, b, c) VALUES(1, 1, '1')");
        execute("INSERT INTO %s (a, b, c) VALUES(1, 3, '3')");
        execute("DELETE FROM %s where a=1 and b >= 2 and b <= 3");
        execute("INSERT INTO %s (a, b, c) VALUES(1, 2, '2')");
        flush();

        execute("DELETE FROM %s where a=1 and b >= 2 and b <= 3");
        flush();

        assertRows(execute("SELECT * FROM %s WHERE a = ?", 1),
                   row(1, 1, "1"));
    }

    @Test
    public void testDeleteWithRangeAndOneClusteringColumn() throws Throwable
    {
        testDeleteWithRangeAndOneClusteringColumn(false);
        testDeleteWithRangeAndOneClusteringColumn(true);
    }

    private void testDeleteWithRangeAndOneClusteringColumn(boolean forceFlush) throws Throwable
    {
        for (String compactOption : new String[] { "", " WITH COMPACT STORAGE" })
        {
            createTable("CREATE TABLE %s (partitionKey int," +
                                          "clustering int," +
                                          "value int," +
                                          " PRIMARY KEY (partitionKey, clustering))" + compactOption);

            int value = 0;
            for (int partitionKey = 0; partitionKey < 5; partitionKey++)
                for (int clustering1 = 0; clustering1 < 5; clustering1++)
                        execute("INSERT INTO %s (partitionKey, clustering, value) VALUES (?, ?, ?)",
                                partitionKey, clustering1, value++);

            flush(forceFlush);

            // test delete partition
            execute("DELETE FROM %s WHERE partitionKey = ?", 1);
            flush(forceFlush);
            assertEmpty(execute("SELECT * FROM %s WHERE partitionKey = ?", 1));

            // test slices on the first clustering column

            execute("DELETE FROM %s WHERE partitionKey = ? AND  clustering >= ?", 0, 4);
            flush(forceFlush);
            assertRows(execute("SELECT * FROM %s WHERE partitionKey = ?", 0),
                       row(0, 0, 0),
                       row(0, 1, 1),
                       row(0, 2, 2),
                       row(0, 3, 3));

            execute("DELETE FROM %s WHERE partitionKey = ? AND  clustering > ?", 0, 2);
            flush(forceFlush);
            assertRows(execute("SELECT * FROM %s WHERE partitionKey = ?", 0),
                       row(0, 0, 0),
                       row(0, 1, 1),
                       row(0, 2, 2));

            execute("DELETE FROM %s WHERE partitionKey = ? AND clustering <= ?", 0, 0);
            flush(forceFlush);
            assertRows(execute("SELECT * FROM %s WHERE partitionKey = ?", 0),
                       row(0, 1, 1),
                       row(0, 2, 2));

            execute("DELETE FROM %s WHERE partitionKey = ? AND clustering < ?", 0, 2);
            flush(forceFlush);
            assertRows(execute("SELECT * FROM %s WHERE partitionKey = ?", 0),
                       row(0, 2, 2));

            execute("DELETE FROM %s WHERE partitionKey = ? AND clustering >= ? AND clustering < ?", 2, 0, 3);
            flush(forceFlush);
            assertRows(execute("SELECT * FROM %s WHERE partitionKey = ?", 2),
                       row(2, 3, 13),
                       row(2, 4, 14));

            execute("DELETE FROM %s WHERE partitionKey = ? AND clustering > ? AND clustering <= ?", 2, 3, 5);
            flush(forceFlush);
            assertRows(execute("SELECT * FROM %s WHERE partitionKey = ?", 2),
                       row(2, 3, 13));

            execute("DELETE FROM %s WHERE partitionKey = ? AND clustering < ? AND clustering > ?", 2, 3, 5);
            flush(forceFlush);
            assertRows(execute("SELECT * FROM %s WHERE partitionKey = ?", 2),
                       row(2, 3, 13));

            // test multi-column slices
            execute("DELETE FROM %s WHERE partitionKey = ? AND (clustering) > (?)", 3, 2);
            flush(forceFlush);
            assertRows(execute("SELECT * FROM %s WHERE partitionKey = ?", 3),
                       row(3, 0, 15),
                       row(3, 1, 16),
                       row(3, 2, 17));

            execute("DELETE FROM %s WHERE partitionKey = ? AND (clustering) < (?)", 3, 1);
            flush(forceFlush);
            assertRows(execute("SELECT * FROM %s WHERE partitionKey = ?", 3),
                       row(3, 1, 16),
                       row(3, 2, 17));

            execute("DELETE FROM %s WHERE partitionKey = ? AND (clustering) >= (?) AND (clustering) <= (?)", 3, 0, 1);
            flush(forceFlush);
            assertRows(execute("SELECT * FROM %s WHERE partitionKey = ?", 3),
                       row(3, 2, 17));

            // Test invalid queries
            assertInvalidMessage("Range deletions are not supported for specific columns",
                                 "DELETE value FROM %s WHERE partitionKey = ? AND clustering >= ?", 2, 1);
            assertInvalidMessage("Range deletions are not supported for specific columns",
                                 "DELETE value FROM %s WHERE partitionKey = ?", 2);
        }
    }

    @Test
    public void testDeleteWithRangeAndTwoClusteringColumns() throws Throwable
    {
        testDeleteWithRangeAndTwoClusteringColumns(false);
        testDeleteWithRangeAndTwoClusteringColumns(true);
    }

    private void testDeleteWithRangeAndTwoClusteringColumns(boolean forceFlush) throws Throwable
    {
        for (String compactOption : new String[] { "", " WITH COMPACT STORAGE" })
        {
            createTable("CREATE TABLE %s (partitionKey int," +
                    "clustering_1 int," +
                    "clustering_2 int," +
                    "value int," +
                    " PRIMARY KEY (partitionKey, clustering_1, clustering_2))" + compactOption);

            int value = 0;
            for (int partitionKey = 0; partitionKey < 5; partitionKey++)
                for (int clustering1 = 0; clustering1 < 5; clustering1++)
                    for (int clustering2 = 0; clustering2 < 5; clustering2++) {
                        execute("INSERT INTO %s (partitionKey, clustering_1, clustering_2, value) VALUES (?, ?, ?, ?)",
                                partitionKey, clustering1, clustering2, value++);}
            flush(forceFlush);

            // test unspecified second clustering column
            execute("DELETE FROM %s WHERE partitionKey = ? AND clustering_1 = ?", 0, 1);
            flush(forceFlush);
            assertRows(execute("SELECT * FROM %s WHERE partitionKey = ? AND clustering_1 < ?", 0, 2),
                       row(0, 0, 0, 0),
                       row(0, 0, 1, 1),
                       row(0, 0, 2, 2),
                       row(0, 0, 3, 3),
                       row(0, 0, 4, 4));

            // test delete partition
            execute("DELETE FROM %s WHERE partitionKey = ?", 1);
            flush(forceFlush);
            assertEmpty(execute("SELECT * FROM %s WHERE partitionKey = ?", 1));

            // test slices on the second clustering column

            execute("DELETE FROM %s WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 < ?", 0, 0, 2);
            flush(forceFlush);
            assertRows(execute("SELECT * FROM %s WHERE partitionKey = ? AND clustering_1 < ?", 0, 2),
                       row(0, 0, 2, 2),
                       row(0, 0, 3, 3),
                       row(0, 0, 4, 4));

            execute("DELETE FROM %s WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 <= ?", 0, 0, 3);
            flush(forceFlush);
            assertRows(execute("SELECT * FROM %s WHERE partitionKey = ? AND clustering_1 < ?", 0, 2),
                       row(0, 0, 4, 4));

            execute("DELETE FROM %s WHERE partitionKey = ? AND  clustering_1 = ? AND clustering_2 > ? ", 0, 2, 2);
            flush(forceFlush);
            assertRows(execute("SELECT * FROM %s WHERE partitionKey = ? AND  clustering_1 = ?", 0, 2),
                       row(0, 2, 0, 10),
                       row(0, 2, 1, 11),
                       row(0, 2, 2, 12));

            execute("DELETE FROM %s WHERE partitionKey = ? AND  clustering_1 = ? AND clustering_2 >= ? ", 0, 2, 1);
            flush(forceFlush);
            assertRows(execute("SELECT * FROM %s WHERE partitionKey = ? AND  clustering_1 = ?", 0, 2),
                       row(0, 2, 0, 10));

            execute("DELETE FROM %s WHERE partitionKey = ? AND  clustering_1 = ? AND clustering_2 > ? AND clustering_2 < ? ",
                    0, 3, 1, 4);
            flush(forceFlush);
            assertRows(execute("SELECT * FROM %s WHERE partitionKey = ? AND  clustering_1 = ?", 0, 3),
                       row(0, 3, 0, 15),
                       row(0, 3, 1, 16),
                       row(0, 3, 4, 19));

            execute("DELETE FROM %s WHERE partitionKey = ? AND  clustering_1 = ? AND clustering_2 > ? AND clustering_2 < ? ",
                    0, 3, 4, 1);
            flush(forceFlush);
            assertRows(execute("SELECT * FROM %s WHERE partitionKey = ? AND  clustering_1 = ?", 0, 3),
                       row(0, 3, 0, 15),
                       row(0, 3, 1, 16),
                       row(0, 3, 4, 19));

            execute("DELETE FROM %s WHERE partitionKey = ? AND  clustering_1 = ? AND clustering_2 >= ? AND clustering_2 <= ? ",
                    0, 3, 1, 4);
            flush(forceFlush);
            assertRows(execute("SELECT * FROM %s WHERE partitionKey = ? AND  clustering_1 = ?", 0, 3),
                       row(0, 3, 0, 15));

            // test slices on the first clustering column

            execute("DELETE FROM %s WHERE partitionKey = ? AND  clustering_1 >= ?", 0, 4);
            flush(forceFlush);
            assertRows(execute("SELECT * FROM %s WHERE partitionKey = ?", 0),
                       row(0, 0, 4, 4),
                       row(0, 2, 0, 10),
                       row(0, 3, 0, 15));

            execute("DELETE FROM %s WHERE partitionKey = ? AND  clustering_1 > ?", 0, 3);
            flush(forceFlush);
            assertRows(execute("SELECT * FROM %s WHERE partitionKey = ?", 0),
                       row(0, 0, 4, 4),
                       row(0, 2, 0, 10),
                       row(0, 3, 0, 15));

            execute("DELETE FROM %s WHERE partitionKey = ? AND clustering_1 < ?", 0, 3);
            flush(forceFlush);
            assertRows(execute("SELECT * FROM %s WHERE partitionKey = ?", 0),
                       row(0, 3, 0, 15));

            execute("DELETE FROM %s WHERE partitionKey = ? AND clustering_1 >= ? AND clustering_1 < ?", 2, 0, 3);
            flush(forceFlush);
            assertRows(execute("SELECT * FROM %s WHERE partitionKey = ?", 2),
                       row(2, 3, 0, 65),
                       row(2, 3, 1, 66),
                       row(2, 3, 2, 67),
                       row(2, 3, 3, 68),
                       row(2, 3, 4, 69),
                       row(2, 4, 0, 70),
                       row(2, 4, 1, 71),
                       row(2, 4, 2, 72),
                       row(2, 4, 3, 73),
                       row(2, 4, 4, 74));

            execute("DELETE FROM %s WHERE partitionKey = ? AND clustering_1 > ? AND clustering_1 <= ?", 2, 3, 5);
            flush(forceFlush);
            assertRows(execute("SELECT * FROM %s WHERE partitionKey = ?", 2),
                       row(2, 3, 0, 65),
                       row(2, 3, 1, 66),
                       row(2, 3, 2, 67),
                       row(2, 3, 3, 68),
                       row(2, 3, 4, 69));

            execute("DELETE FROM %s WHERE partitionKey = ? AND clustering_1 < ? AND clustering_1 > ?", 2, 3, 5);
            flush(forceFlush);
            assertRows(execute("SELECT * FROM %s WHERE partitionKey = ?", 2),
                       row(2, 3, 0, 65),
                       row(2, 3, 1, 66),
                       row(2, 3, 2, 67),
                       row(2, 3, 3, 68),
                       row(2, 3, 4, 69));

            // test multi-column slices
            execute("DELETE FROM %s WHERE partitionKey = ? AND (clustering_1, clustering_2) > (?, ?)", 2, 3, 3);
            flush(forceFlush);
            assertRows(execute("SELECT * FROM %s WHERE partitionKey = ?", 2),
                       row(2, 3, 0, 65),
                       row(2, 3, 1, 66),
                       row(2, 3, 2, 67),
                       row(2, 3, 3, 68));

            execute("DELETE FROM %s WHERE partitionKey = ? AND (clustering_1, clustering_2) < (?, ?)", 2, 3, 1);
            flush(forceFlush);
            assertRows(execute("SELECT * FROM %s WHERE partitionKey = ?", 2),
                       row(2, 3, 1, 66),
                       row(2, 3, 2, 67),
                       row(2, 3, 3, 68));

            execute("DELETE FROM %s WHERE partitionKey = ? AND (clustering_1, clustering_2) >= (?, ?) AND (clustering_1) <= (?)", 2, 3, 2, 4);
            flush(forceFlush);
            assertRows(execute("SELECT * FROM %s WHERE partitionKey = ?", 2),
                       row(2, 3, 1, 66));

            // Test with a mix of single column and multi-column restrictions
            execute("DELETE FROM %s WHERE partitionKey = ? AND clustering_1 = ? AND (clustering_2) < (?)", 3, 0, 3);
            flush(forceFlush);
            assertRows(execute("SELECT * FROM %s WHERE partitionKey = ? AND clustering_1 = ?", 3, 0),
                       row(3, 0, 3, 78),
                       row(3, 0, 4, 79));

            execute("DELETE FROM %s WHERE partitionKey = ? AND clustering_1 IN (?, ?) AND (clustering_2) >= (?)", 3, 0, 1, 3);
            flush(forceFlush);
            assertRows(execute("SELECT * FROM %s WHERE partitionKey = ? AND clustering_1 IN (?, ?)", 3, 0, 1),
                       row(3, 1, 0, 80),
                       row(3, 1, 1, 81),
                       row(3, 1, 2, 82));

            execute("DELETE FROM %s WHERE partitionKey = ? AND (clustering_1) IN ((?), (?)) AND clustering_2 < ?", 3, 0, 1, 1);
            flush(forceFlush);
            assertRows(execute("SELECT * FROM %s WHERE partitionKey = ? AND clustering_1 IN (?, ?)", 3, 0, 1),
                       row(3, 1, 1, 81),
                       row(3, 1, 2, 82));

            execute("DELETE FROM %s WHERE partitionKey = ? AND (clustering_1) = (?) AND clustering_2 >= ?", 3, 1, 2);
            flush(forceFlush);
            assertRows(execute("SELECT * FROM %s WHERE partitionKey = ? AND clustering_1 IN (?, ?)", 3, 0, 1),
                       row(3, 1, 1, 81));

            // Test invalid queries
            assertInvalidMessage("Range deletions are not supported for specific columns",
                                 "DELETE value FROM %s WHERE partitionKey = ? AND (clustering_1, clustering_2) >= (?, ?)", 2, 3, 1);
            assertInvalidMessage("Range deletions are not supported for specific columns",
                                 "DELETE value FROM %s WHERE partitionKey = ? AND clustering_1 >= ?", 2, 3);
            assertInvalidMessage("Range deletions are not supported for specific columns",
                                 "DELETE value FROM %s WHERE partitionKey = ? AND clustering_1 = ?", 2, 3);
            assertInvalidMessage("Range deletions are not supported for specific columns",
                                 "DELETE value FROM %s WHERE partitionKey = ?", 2);
        }
    }

    @Test
    public void testDeleteWithAStaticColumn() throws Throwable
    {
        testDeleteWithAStaticColumn(false);
        testDeleteWithAStaticColumn(true);
    }

    private void testDeleteWithAStaticColumn(boolean forceFlush) throws Throwable
    {
        createTable("CREATE TABLE %s (partitionKey int," +
                                      "clustering_1 int," +
                                      "clustering_2 int," +
                                      "value int," +
                                      "staticValue text static," +
                                      " PRIMARY KEY (partitionKey, clustering_1, clustering_2))");

        execute("INSERT INTO %s (partitionKey, clustering_1, clustering_2, value, staticValue) VALUES (0, 0, 0, 0, 'A')");
        execute("INSERT INTO %s (partitionKey, clustering_1, clustering_2, value) VALUES (0, 0, 1, 1)");
        execute("INSERT INTO %s (partitionKey, clustering_1, clustering_2, value, staticValue) VALUES (1, 0, 0, 6, 'B')");
        flush(forceFlush);

        execute("DELETE staticValue FROM %s WHERE partitionKey = ?", 0);
        flush(forceFlush);
        assertRows(execute("SELECT DISTINCT staticValue FROM %s WHERE partitionKey IN (?, ?)", 0, 1),
                   row(new Object[1]), row("B"));

        execute("DELETE staticValue, value FROM %s WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ?",
                1, 0, 0);
        flush(forceFlush);
        assertRows(execute("SELECT * FROM %s"),
                   row(1, 0, 0, null, null),
                   row(0, 0, 0, null, 0),
                   row(0, 0, 1, null, 1));

        assertInvalidMessage("Invalid restrictions on clustering columns since the DELETE statement modifies only static columns",
                             "DELETE staticValue FROM %s WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ?",
                             0, 0, 1);

        assertInvalidMessage("Invalid restrictions on clustering columns since the DELETE statement modifies only static columns",
                             "DELETE staticValue FROM %s WHERE partitionKey = ? AND (clustering_1, clustering_2) >= (?, ?)",
                             0, 0, 1);
    }

    @Test
    public void testDeleteWithSecondaryIndices() throws Throwable
    {
        testDeleteWithSecondaryIndices(false);
        testDeleteWithSecondaryIndices(true);
    }

    private void testDeleteWithSecondaryIndices(boolean forceFlush) throws Throwable
    {
        createTable("CREATE TABLE %s (partitionKey int," +
                "clustering_1 int," +
                "value int," +
                "values set<int>," +
                " PRIMARY KEY (partitionKey, clustering_1))");

        createIndex("CREATE INDEX ON %s (value)");
        createIndex("CREATE INDEX ON %s (clustering_1)");
        createIndex("CREATE INDEX ON %s (values)");

        execute("INSERT INTO %s (partitionKey, clustering_1, value, values) VALUES (0, 0, 0, {0})");
        execute("INSERT INTO %s (partitionKey, clustering_1, value, values) VALUES (0, 1, 1, {0, 1})");
        execute("INSERT INTO %s (partitionKey, clustering_1, value, values) VALUES (0, 2, 2, {0, 1, 2})");
        execute("INSERT INTO %s (partitionKey, clustering_1, value, values) VALUES (0, 3, 3, {0, 1, 2, 3})");
        execute("INSERT INTO %s (partitionKey, clustering_1, value, values) VALUES (1, 0, 4, {0, 1, 2, 3, 4})");

        flush(forceFlush);

        assertInvalidMessage("Non PRIMARY KEY columns found in where clause: value",
                             "DELETE FROM %s WHERE partitionKey = ? AND clustering_1 = ? AND value = ?", 3, 3, 3);
        assertInvalidMessage("Non PRIMARY KEY columns found in where clause: values",
                             "DELETE FROM %s WHERE partitionKey = ? AND clustering_1 = ? AND values CONTAINS ?", 3, 3, 3);
        assertInvalidMessage("Non PRIMARY KEY columns found in where clause: value",
                             "DELETE FROM %s WHERE partitionKey = ? AND value = ?", 3, 3);
        assertInvalidMessage("Non PRIMARY KEY columns found in where clause: values",
                             "DELETE FROM %s WHERE partitionKey = ? AND values CONTAINS ?", 3, 3);
        assertInvalidMessage("Some partition key parts are missing: partitionkey",
                             "DELETE FROM %s WHERE clustering_1 = ?", 3);
        assertInvalidMessage("Some partition key parts are missing: partitionkey",
                             "DELETE FROM %s WHERE value = ?", 3);
        assertInvalidMessage("Some partition key parts are missing: partitionkey",
                             "DELETE FROM %s WHERE values CONTAINS ?", 3);
    }

    @Test
    public void testDeleteWithOnlyPK() throws Throwable
    {
        // This is a regression test for CASSANDRA-11102

        createTable("CREATE TABLE %s (k int, v int, PRIMARY KEY (k, v)) WITH gc_grace_seconds=1");

        execute("INSERT INTO %s(k, v) VALUES (?, ?)", 1, 2);

        execute("DELETE FROM %s WHERE k = ? AND v = ?", 1, 2);
        execute("INSERT INTO %s(k, v) VALUES (?, ?)", 2, 3);

        Thread.sleep(500);

        execute("DELETE FROM %s WHERE k = ? AND v = ?", 2, 3);
        execute("INSERT INTO %s(k, v) VALUES (?, ?)", 1, 2);

        Thread.sleep(500);

        flush();

        assertRows(execute("SELECT * FROM %s"), row(1, 2));

        Thread.sleep(1000);
        compact();

        assertRows(execute("SELECT * FROM %s"), row(1, 2));
    }

    @Test
    public void testDeleteColumnNoClustering() throws Throwable
    {
        // This is a regression test for CASSANDRA-11068 (and ultimately another test for CASSANDRA-11102)
        // Creates a table without clustering, insert a row (with a column) and only remove the column.
        // We should still have a row (with a null column value) even post-compaction.

        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int) WITH gc_grace_seconds=0");

        execute("INSERT INTO %s(k, v) VALUES (?, ?)", 0, 0);
        execute("DELETE v FROM %s WHERE k=?", 0);

        assertRows(execute("SELECT * FROM %s"), row(0, null));

        flush();
        assertRows(execute("SELECT * FROM %s"), row(0, null));

        compact();
        assertRows(execute("SELECT * FROM %s"), row(0, null));
    }

    @Test
    public void testDeleteAndReverseQueries() throws Throwable
    {
        // This test insert rows in one sstable and a range tombstone covering some of those rows in another, and it
        // validates we correctly get only the non-removed rows when doing reverse queries.

        createTable("CREATE TABLE %s (k text, i int, PRIMARY KEY (k, i))");

        for (int i = 0; i < 10; i++)
            execute("INSERT INTO %s(k, i) values (?, ?)", "a", i);

        flush();

        execute("DELETE FROM %s WHERE k = ? AND i >= ? AND i <= ?", "a", 2, 7);

        assertRows(execute("SELECT i FROM %s WHERE k = ? ORDER BY i DESC", "a"),
            row(9), row(8), row(1), row(0)
        );

        flush();

        assertRows(execute("SELECT i FROM %s WHERE k = ? ORDER BY i DESC", "a"),
            row(9), row(8), row(1), row(0)
        );
    }

    @Test
    public void testDeleteWithEmptyRestrictionValue() throws Throwable
    {
        for (String options : new String[] { "", " WITH COMPACT STORAGE" })
        {
            createTable("CREATE TABLE %s (pk blob, c blob, v blob, PRIMARY KEY (pk, c))" + options);

            if (StringUtils.isEmpty(options))
            {
                execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)", bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("1"));
                execute("DELETE FROM %s WHERE pk = textAsBlob('foo123') AND c = textAsBlob('');");

                assertEmpty(execute("SELECT * FROM %s"));

                execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)", bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("1"));
                execute("DELETE FROM %s WHERE pk = textAsBlob('foo123') AND c IN (textAsBlob(''), textAsBlob('1'));");

                assertEmpty(execute("SELECT * FROM %s"));

                execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)", bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("1"));
                execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)", bytes("foo123"), bytes("1"), bytes("1"));
                execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)", bytes("foo123"), bytes("2"), bytes("2"));

                execute("DELETE FROM %s WHERE pk = textAsBlob('foo123') AND c > textAsBlob('')");

                assertRows(execute("SELECT * FROM %s"),
                           row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("1")));

                execute("DELETE FROM %s WHERE pk = textAsBlob('foo123') AND c >= textAsBlob('')");

                assertEmpty(execute("SELECT * FROM %s"));
            }
            else
            {
                assertInvalid("Invalid empty or null value for column c",
                              "DELETE FROM %s WHERE pk = textAsBlob('foo123') AND c = textAsBlob('')");
                assertInvalid("Invalid empty or null value for column c",
                              "DELETE FROM %s WHERE pk = textAsBlob('foo123') AND c IN (textAsBlob(''), textAsBlob('1'))");
            }

            execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)", bytes("foo123"), bytes("1"), bytes("1"));
            execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)", bytes("foo123"), bytes("2"), bytes("2"));

            execute("DELETE FROM %s WHERE pk = textAsBlob('foo123') AND c > textAsBlob('')");

            assertEmpty(execute("SELECT * FROM %s"));

            execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)", bytes("foo123"), bytes("1"), bytes("1"));
            execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)", bytes("foo123"), bytes("2"), bytes("2"));

            execute("DELETE FROM %s WHERE pk = textAsBlob('foo123') AND c <= textAsBlob('')");
            execute("DELETE FROM %s WHERE pk = textAsBlob('foo123') AND c < textAsBlob('')");

            assertRows(execute("SELECT * FROM %s"),
                       row(bytes("foo123"), bytes("1"), bytes("1")),
                       row(bytes("foo123"), bytes("2"), bytes("2")));
        }
    }

    @Test
    public void testDeleteWithMultipleClusteringColumnsAndEmptyRestrictionValue() throws Throwable
    {
        for (String options : new String[] { "", " WITH COMPACT STORAGE" })
        {
            createTable("CREATE TABLE %s (pk blob, c1 blob, c2 blob, v blob, PRIMARY KEY (pk, c1, c2))" + options);

            execute("INSERT INTO %s (pk, c1, c2, v) VALUES (?, ?, ?, ?)", bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("1"), bytes("1"));
            execute("DELETE FROM %s WHERE pk = textAsBlob('foo123') AND c1 = textAsBlob('');");

            assertEmpty(execute("SELECT * FROM %s"));

            execute("INSERT INTO %s (pk, c1, c2, v) VALUES (?, ?, ?, ?)", bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("1"), bytes("1"));
            execute("DELETE FROM %s WHERE pk = textAsBlob('foo123') AND c1 IN (textAsBlob(''), textAsBlob('1')) AND c2 = textAsBlob('1');");

            assertEmpty(execute("SELECT * FROM %s"));

            execute("INSERT INTO %s (pk, c1, c2, v) VALUES (?, ?, ?, ?)", bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("1"), bytes("0"));
            execute("INSERT INTO %s (pk, c1, c2, v) VALUES (?, ?, ?, ?)", bytes("foo123"), bytes("1"), bytes("1"), bytes("1"));
            execute("INSERT INTO %s (pk, c1, c2, v) VALUES (?, ?, ?, ?)", bytes("foo123"), bytes("1"), bytes("2"), bytes("3"));

            execute("DELETE FROM %s WHERE pk = textAsBlob('foo123') AND c1 > textAsBlob('')");

            assertRows(execute("SELECT * FROM %s"),
                       row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("1"), bytes("0")));

            execute("DELETE FROM %s WHERE pk = textAsBlob('foo123') AND c1 >= textAsBlob('')");

            assertEmpty(execute("SELECT * FROM %s"));

            execute("INSERT INTO %s (pk, c1, c2, v) VALUES (?, ?, ?, ?)", bytes("foo123"), bytes("1"), bytes("1"), bytes("1"));
            execute("INSERT INTO %s (pk, c1, c2, v) VALUES (?, ?, ?, ?)", bytes("foo123"), bytes("1"), bytes("2"), bytes("3"));

            execute("DELETE FROM %s WHERE pk = textAsBlob('foo123') AND c1 > textAsBlob('')");

            assertEmpty(execute("SELECT * FROM %s"));

            execute("INSERT INTO %s (pk, c1, c2, v) VALUES (?, ?, ?, ?)", bytes("foo123"), bytes("1"), bytes("1"), bytes("1"));
            execute("INSERT INTO %s (pk, c1, c2, v) VALUES (?, ?, ?, ?)", bytes("foo123"), bytes("1"), bytes("2"), bytes("3"));

            execute("DELETE FROM %s WHERE pk = textAsBlob('foo123') AND c1 <= textAsBlob('')");
            execute("DELETE FROM %s WHERE pk = textAsBlob('foo123') AND c1 < textAsBlob('')");

            assertRows(execute("SELECT * FROM %s"),
                       row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")),
                       row(bytes("foo123"), bytes("1"), bytes("2"), bytes("3")));
        }
    }

    /**
     * Test for CASSANDRA-12829
     */
    @Test
    public void testDeleteWithEmptyInRestriction() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a,b))");
        execute("INSERT INTO %s (a,b,c) VALUES (?,?,?)", 1, 1, 1);
        execute("INSERT INTO %s (a,b,c) VALUES (?,?,?)", 1, 2, 2);
        execute("INSERT INTO %s (a,b,c) VALUES (?,?,?)", 1, 3, 3);

        execute("DELETE FROM %s WHERE a IN ();");
        execute("DELETE FROM %s WHERE a IN () AND b IN ();");
        execute("DELETE FROM %s WHERE a IN () AND b = 1;");
        execute("DELETE FROM %s WHERE a = 1 AND b IN ();");
        assertRows(execute("SELECT * FROM %s"),
                   row(1, 1, 1),
                   row(1, 2, 2),
                   row(1, 3, 3));

        createTable("CREATE TABLE %s (a int, b int, c int, d int, s int static, PRIMARY KEY ((a,b), c))");
        execute("INSERT INTO %s (a,b,c,d,s) VALUES (?,?,?,?,?)", 1, 1, 1, 1, 1);
        execute("INSERT INTO %s (a,b,c,d,s) VALUES (?,?,?,?,?)", 1, 1, 2, 2, 1);
        execute("INSERT INTO %s (a,b,c,d,s) VALUES (?,?,?,?,?)", 1, 1, 3, 3, 1);

        execute("DELETE FROM %s WHERE a = 1 AND b = 1 AND c IN ();");
        execute("DELETE FROM %s WHERE a = 1 AND b IN () AND c IN ();");
        execute("DELETE FROM %s WHERE a IN () AND b IN () AND c IN ();");
        execute("DELETE FROM %s WHERE a IN () AND b = 1 AND c IN ();");
        execute("DELETE FROM %s WHERE a IN () AND b IN () AND c = 1;");
        assertRows(execute("SELECT * FROM %s"),
                   row(1, 1, 1, 1, 1),
                   row(1, 1, 2, 1, 2),
                   row(1, 1, 3, 1, 3));

        createTable("CREATE TABLE %s (a int, b int, c int, d int, e int, PRIMARY KEY ((a,b), c, d))");
        execute("INSERT INTO %s (a,b,c,d,e) VALUES (?,?,?,?,?)", 1, 1, 1, 1, 1);
        execute("INSERT INTO %s (a,b,c,d,e) VALUES (?,?,?,?,?)", 1, 1, 1, 2, 2);
        execute("INSERT INTO %s (a,b,c,d,e) VALUES (?,?,?,?,?)", 1, 1, 1, 3, 3);
        execute("INSERT INTO %s (a,b,c,d,e) VALUES (?,?,?,?,?)", 1, 1, 1, 4, 4);

        execute("DELETE FROM %s WHERE a = 1 AND b = 1 AND c IN ();");
        execute("DELETE FROM %s WHERE a = 1 AND b = 1 AND c = 1 AND d IN ();");
        execute("DELETE FROM %s WHERE a = 1 AND b = 1 AND c IN () AND d IN ();");
        execute("DELETE FROM %s WHERE a = 1 AND b IN () AND c IN () AND d IN ();");
        execute("DELETE FROM %s WHERE a IN () AND b IN () AND c IN () AND d IN ();");
        execute("DELETE FROM %s WHERE a IN () AND b IN () AND c IN () AND d = 1;");
        execute("DELETE FROM %s WHERE a IN () AND b IN () AND c = 1 AND d = 1;");
        execute("DELETE FROM %s WHERE a IN () AND b IN () AND c = 1 AND d IN ();");
        execute("DELETE FROM %s WHERE a IN () AND b = 1");
        assertRows(execute("SELECT * FROM %s"),
                   row(1, 1, 1, 1, 1),
                   row(1, 1, 1, 2, 2),
                   row(1, 1, 1, 3, 3),
                   row(1, 1, 1, 4, 4));
    }

    /**
     * Test for CASSANDRA-13152
     */
    @Test
    public void testThatDeletesWithEmptyInRestrictionDoNotCreateMutations() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a,b))");

        execute("DELETE FROM %s WHERE a IN ();");
        execute("DELETE FROM %s WHERE a IN () AND b IN ();");
        execute("DELETE FROM %s WHERE a IN () AND b = 1;");
        execute("DELETE FROM %s WHERE a = 1 AND b IN ();");

        assertTrue("The memtable should be empty but is not", isMemtableEmpty());

        createTable("CREATE TABLE %s (a int, b int, c int, d int, s int static, PRIMARY KEY ((a,b), c))");

        execute("DELETE FROM %s WHERE a = 1 AND b = 1 AND c IN ();");
        execute("DELETE FROM %s WHERE a = 1 AND b IN () AND c IN ();");
        execute("DELETE FROM %s WHERE a IN () AND b IN () AND c IN ();");
        execute("DELETE FROM %s WHERE a IN () AND b = 1 AND c IN ();");
        execute("DELETE FROM %s WHERE a IN () AND b IN () AND c = 1;");

        assertTrue("The memtable should be empty but is not", isMemtableEmpty());

        createTable("CREATE TABLE %s (a int, b int, c int, d int, e int, PRIMARY KEY ((a,b), c, d))");

        execute("DELETE FROM %s WHERE a = 1 AND b = 1 AND c IN ();");
        execute("DELETE FROM %s WHERE a = 1 AND b = 1 AND c = 1 AND d IN ();");
        execute("DELETE FROM %s WHERE a = 1 AND b = 1 AND c IN () AND d IN ();");
        execute("DELETE FROM %s WHERE a = 1 AND b IN () AND c IN () AND d IN ();");
        execute("DELETE FROM %s WHERE a IN () AND b IN () AND c IN () AND d IN ();");
        execute("DELETE FROM %s WHERE a IN () AND b IN () AND c IN () AND d = 1;");
        execute("DELETE FROM %s WHERE a IN () AND b IN () AND c = 1 AND d = 1;");
        execute("DELETE FROM %s WHERE a IN () AND b IN () AND c = 1 AND d IN ();");
        execute("DELETE FROM %s WHERE a IN () AND b = 1");

        assertTrue("The memtable should be empty but is not", isMemtableEmpty());
    }

    @Test
    public void testQueryingOnRangeTombstoneBoundForward() throws Throwable
    {
        createTable("CREATE TABLE %s (k text, i int, PRIMARY KEY (k, i))");

        execute("INSERT INTO %s (k, i) VALUES (?, ?)", "a", 0);

        execute("DELETE FROM %s WHERE k = ? AND i > ? AND i <= ?", "a", 0, 1);
        execute("DELETE FROM %s WHERE k = ? AND i > ?", "a", 1);

        flush();

        assertEmpty(execute("SELECT i FROM %s WHERE k = ? AND i = ?", "a", 1));
    }

    @Test
    public void testQueryingOnRangeTombstoneBoundReverse() throws Throwable
    {
        createTable("CREATE TABLE %s (k text, i int, PRIMARY KEY (k, i))");

        execute("INSERT INTO %s (k, i) VALUES (?, ?)", "a", 0);

        execute("DELETE FROM %s WHERE k = ? AND i > ? AND i <= ?", "a", 0, 1);
        execute("DELETE FROM %s WHERE k = ? AND i > ?", "a", 1);

        flush();

        assertRows(execute("SELECT i FROM %s WHERE k = ? AND i <= ? ORDER BY i DESC", "a", 1), row(0));
    }

    @Test
    public void testReverseQueryWithRangeTombstoneOnMultipleBlocks() throws Throwable
    {
        createTable("CREATE TABLE %s (k text, i int, v text, PRIMARY KEY (k, i))");

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 1200; i++)
            sb.append('a');
        String longText = sb.toString();

        for (int i = 0; i < 10; i++)
            execute("INSERT INTO %s(k, i, v) VALUES (?, ?, ?) USING TIMESTAMP 3", "a", i*2, longText);

        execute("DELETE FROM %s USING TIMESTAMP 1 WHERE k = ? AND i >= ? AND i <= ?", "a", 12, 16);

        flush();

        execute("INSERT INTO %s(k, i, v) VALUES (?, ?, ?) USING TIMESTAMP 0", "a", 3, longText);
        execute("INSERT INTO %s(k, i, v) VALUES (?, ?, ?) USING TIMESTAMP 3", "a", 11, longText);
        execute("INSERT INTO %s(k, i, v) VALUES (?, ?, ?) USING TIMESTAMP 0", "a", 15, longText);
        execute("INSERT INTO %s(k, i, v) VALUES (?, ?, ?) USING TIMESTAMP 0", "a", 17, longText);

        flush();

        assertRows(execute("SELECT i FROM %s WHERE k = ? ORDER BY i DESC", "a"),
                   row(18),
                   row(17),
                   row(16),
                   row(14),
                   row(12),
                   row(11),
                   row(10),
                   row(8),
                   row(6),
                   row(4),
                   row(3),
                   row(2),
                   row(0));
    }

    /**
     * Test for CASSANDRA-13305
     */
    @Test
    public void testWithEmptyRange() throws Throwable
    {
        createTable("CREATE TABLE %s (k text, a int, b int, PRIMARY KEY (k, a, b))");

        // Both of the following should be doing nothing, but before #13305 this inserted broken ranges. We do it twice
        // and the follow-up delete mainly as a way to show the bug as the combination of this will trigger an assertion
        // in RangeTombstoneList pre-#13305 showing that something wrong happened.
        execute("DELETE FROM %s WHERE k = ? AND a >= ? AND a < ?", "a", 1, 1);
        execute("DELETE FROM %s WHERE k = ? AND a >= ? AND a < ?", "a", 1, 1);

        execute("DELETE FROM %s WHERE k = ? AND a >= ? AND a < ?", "a", 0, 2);
    }

    /**
     * Test for CASSANDRA-13917
    */
    @Test
    public void testWithCompactStaticFormat() throws Throwable
    {
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b int, c int) WITH COMPACT STORAGE");
        testWithCompactFormat();

        // if column1 is present, hidden column is called column2
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b int, c int, column1 int) WITH COMPACT STORAGE");
        assertInvalidMessage("Undefined column name column2",
                             "DELETE FROM %s WHERE a = 1 AND column2= 1");
        assertInvalidMessage("Undefined column name column2",
                             "DELETE FROM %s WHERE a = 1 AND column2 = 1 AND value1 = 1");
        assertInvalidMessage("Undefined column name column2",
                             "DELETE column2 FROM %s WHERE a = 1");

        // if value is present, hidden column is called value1
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b int, c int, value int) WITH COMPACT STORAGE");
        assertInvalidMessage("Undefined column name value1",
                             "DELETE FROM %s WHERE a = 1 AND value1 = 1");
        assertInvalidMessage("Undefined column name value1",
                             "DELETE FROM %s WHERE a = 1 AND value1 = 1 AND column1 = 1");
        assertInvalidMessage("Undefined column name value1",
                             "DELETE value1 FROM %s WHERE a = 1");
    }

    /**
     * Test for CASSANDRA-13917
     */
    @Test
    public void testWithCompactNonStaticFormat() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, PRIMARY KEY (a, b)) WITH COMPACT STORAGE");
        execute("INSERT INTO %s (a, b) VALUES (1, 1)");
        execute("INSERT INTO %s (a, b) VALUES (2, 1)");
        assertRows(execute("SELECT a, b FROM %s"),
                   row(1, 1),
                   row(2, 1));
        testWithCompactFormat();

        createTable("CREATE TABLE %s (a int, b int, v int, PRIMARY KEY (a, b)) WITH COMPACT STORAGE");
        execute("INSERT INTO %s (a, b, v) VALUES (1, 1, 3)");
        execute("INSERT INTO %s (a, b, v) VALUES (2, 1, 4)");
        assertRows(execute("SELECT a, b, v FROM %s"),
                   row(1, 1, 3),
                   row(2, 1, 4));
        testWithCompactFormat();
    }

    private void testWithCompactFormat() throws Throwable
    {
        assertInvalidMessage("Undefined column name value",
                             "DELETE FROM %s WHERE a = 1 AND value = 1");
        assertInvalidMessage("Undefined column name column1",
                             "DELETE FROM %s WHERE a = 1 AND column1= 1");
        assertInvalidMessage("Undefined column name value",
                             "DELETE FROM %s WHERE a = 1 AND value = 1 AND column1 = 1");
        assertInvalidMessage("Undefined column name value",
                             "DELETE value FROM %s WHERE a = 1");
        assertInvalidMessage("Undefined column name column1",
                             "DELETE column1 FROM %s WHERE a = 1");
    }

    /**
     * Checks if the memtable is empty or not
     * @return {@code true} if the memtable is empty, {@code false} otherwise.
     */
    private boolean isMemtableEmpty()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(currentTable());
        return cfs.metric.allMemtablesLiveDataSize.getValue() == 0;
    }
}
