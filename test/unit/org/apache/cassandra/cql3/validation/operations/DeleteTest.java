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

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;

import static org.junit.Assert.assertEquals;

public class DeleteTest extends CQLTester
{
    /** Test for cassandra 8558 */
    @Test
    public void testRangeDeletion() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY (a, b, c))");

        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 1, 1, 1);
        flush();
        execute("DELETE FROM %s WHERE a=? AND b=?", 1, 1);
        flush();
        assertEmpty(execute("SELECT * FROM %s WHERE a=? AND b=? AND c=?", 1, 1, 1));
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
}
