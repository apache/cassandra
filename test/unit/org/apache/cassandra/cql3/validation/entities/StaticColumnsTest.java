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

import java.util.Arrays;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;

import static junit.framework.Assert.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StaticColumnsTest extends CQLTester
{

    /**
     * Migrated from cql_tests.py:TestCQL.static_columns_test()
     */
    @Test
    public void testStaticColumns() throws Throwable
    {
        testStaticColumns(false);
        testStaticColumns(true);
    }

    private void testStaticColumns(boolean forceFlush) throws Throwable
    {
        createTable("CREATE TABLE %s ( k int, p int, s int static, v int, PRIMARY KEY (k, p))");

        execute("INSERT INTO %s(k, s) VALUES (0, 42)");
        flush(forceFlush);

        assertRows(execute("SELECT * FROM %s"), row(0, null, 42, null));

        // Check that writetime works (//7081) -- we can't predict the exact value easily so
        // we just check that it's non zero
        Object[][] row = getRows(execute("SELECT s, writetime(s) FROM %s WHERE k=0"));
        assertEquals(42, row[0][0]);
        assertTrue((Long)row[0][1] > 0);

        execute("INSERT INTO %s (k, p, s, v) VALUES (0, 0, 12, 0)");
        execute("INSERT INTO %s (k, p, s, v) VALUES (0, 1, 24, 1)");
        flush(forceFlush);

        // Check the static columns in indeed "static"
        assertRows(execute("SELECT * FROM %s"), row(0, 0, 24, 0), row(0, 1, 24, 1));

        // Check we do correctly get the static column value with a SELECT *, even
        // if we're only slicing part of the partition
        assertRows(execute("SELECT * FROM %s WHERE k=0 AND p=0"), row(0, 0, 24, 0));
        assertRows(execute("SELECT * FROM %s WHERE k=0 AND p=1"), row(0, 1, 24, 1));

        // Test for IN on the clustering key (//6769)
        assertRows(execute("SELECT * FROM %s WHERE k=0 AND p IN (0, 1)"), row(0, 0, 24, 0), row(0, 1, 24, 1));

        // Check things still work if we don't select the static column. We also want
        // this to not request the static columns internally at all, though that part
        // require debugging to assert
        assertRows(execute("SELECT p, v FROM %s WHERE k=0 AND p=1"), row(1, 1));

        // Check selecting only a static column with distinct only yield one value
        // (as we only query the static columns)
        assertRows(execute("SELECT DISTINCT s FROM %s WHERE k=0"), row(24));
        // But without DISTINCT, we still get one result per row
        assertRows(execute("SELECT s FROM %s WHERE k=0"),row(24),row(24));
        // but that querying other columns does correctly yield the full partition
        assertRows(execute("SELECT s, v FROM %s WHERE k=0"),row(24, 0),row(24, 1));
        assertRows(execute("SELECT s, v FROM %s WHERE k=0 AND p=1"),row(24, 1));
        assertRows(execute("SELECT p, s FROM %s WHERE k=0 AND p=1"), row(1, 24));
        assertRows(execute("SELECT k, p, s FROM %s WHERE k=0 AND p=1"),row(0, 1, 24));

        // Check that deleting a row don't implicitely deletes statics
        execute("DELETE FROM %s WHERE k=0 AND p=0");
        flush(forceFlush);
        assertRows(execute("SELECT * FROM %s"),row(0, 1, 24, 1));

        // But that explicitely deleting the static column does remove it
        execute("DELETE s FROM %s WHERE k=0");
        flush(forceFlush);
        assertRows(execute("SELECT * FROM %s"), row(0, 1, null, 1));

        // Check we can add a static column ...
        execute("ALTER TABLE %s ADD s2 int static");
        assertRows(execute("SELECT * FROM %s"), row(0, 1, null, null, 1));
        execute("INSERT INTO %s (k, p, s2, v) VALUES(0, 2, 42, 2)");
        assertRows(execute("SELECT * FROM %s"), row(0, 1, null, 42, 1), row(0, 2, null, 42, 2));
        // ... and that we can drop it
        execute("ALTER TABLE %s DROP s2");
        assertRows(execute("SELECT * FROM %s"), row(0, 1, null, 1), row(0, 2, null, 2));
    }

    /**
     * Migrated from cql_tests.py:TestCQL.static_columns_with_2i_test()
     */
    @Test
    public void testStaticColumnsWithSecondaryIndex() throws Throwable
    {
        createTable(" CREATE TABLE %s (k int, p int, s int static, v int, PRIMARY KEY (k, p) ) ");

        createIndex("CREATE INDEX ON %s (v)");

        execute("INSERT INTO %s (k, p, s, v) VALUES (0, 0, 42, 1)");
        execute("INSERT INTO %s (k, p, v) VALUES (0, 1, 1)");
        execute("INSERT INTO %s (k, p, v) VALUES (0, 2, 2)");

        assertRows(execute("SELECT * FROM %s WHERE v = 1"), row(0, 0, 42, 1), row(0, 1, 42, 1));
        assertRows(execute("SELECT p, s FROM %s WHERE v = 1"), row(0, 42), row(1, 42));
        assertRows(execute("SELECT p FROM %s WHERE v = 1"), row(0), row(1));
        // We don't support that
        assertInvalid("SELECT s FROM %s WHERE v = 1");
    }

    /**
     * Migrated from cql_tests.py:TestCQL.static_columns_with_distinct_test()
     */
    @Test
    public void testStaticColumnsWithDistinct() throws Throwable
    {
        createTable("CREATE TABLE %s( k int, p int, s int static, PRIMARY KEY (k, p) ) ");

        execute("INSERT INTO %s (k, p) VALUES (1, 1)");
        execute("INSERT INTO %s (k, p) VALUES (1, 2)");

        assertRows(execute("SELECT k, s FROM %s"), row(1, null), row(1, null));
        assertRows(execute("SELECT DISTINCT k, s FROM %s"), row(1, null));

        Object[][] rows = getRows(execute("SELECT DISTINCT s FROM %s WHERE k=1"));
        assertNull(rows[0][0]);

        assertEmpty(execute("SELECT DISTINCT s FROM %s WHERE k=2"));

        execute("INSERT INTO %s (k, p, s) VALUES (2, 1, 3)");
        execute("INSERT INTO %s (k, p) VALUES (2, 2)");

        assertRows(execute("SELECT k, s FROM %s"), row(1, null), row(1, null), row(2, 3), row(2, 3));
        assertRows(execute("SELECT DISTINCT k, s FROM %s"), row(1, null), row(2, 3));
        rows = getRows(execute("SELECT DISTINCT s FROM %s WHERE k=1"));
        assertNull(rows[0][0]);
        assertRows(execute("SELECT DISTINCT s FROM %s WHERE k=2"), row(3));

        assertInvalid("SELECT DISTINCT s FROM %s");

        // paging to test for CASSANDRA-8108
        execute("TRUNCATE %s");
        for (int i = 0; i < 10; i++)
            for (int j = 0; j < 10; j++)
                execute("INSERT INTO %s (k, p, s) VALUES (?, ?, ?)", i, j, i);

        rows = getRows(execute("SELECT DISTINCT k, s FROM %s"));
        checkDistinctRows(rows, true, 0, 10, 0, 10);

        String keys = "0, 1, 2, 3, 4, 5, 6, 7, 8, 9";
        rows = getRows(execute("SELECT DISTINCT k, s FROM %s WHERE k IN (" + keys + ")"));
        checkDistinctRows(rows, false, 0, 10, 0, 10);

        // additional testing for CASSANRA-8087
        createTable("CREATE TABLE %s( k int, c1 int, c2 int, s1 int static, s2 int static, PRIMARY KEY (k, c1, c2))");

        for (int i = 0; i < 10; i++)
            for (int j = 0; j < 5; j++)
                for (int k = 0; k < 5; k++)
                    execute("INSERT INTO %s (k, c1, c2, s1, s2) VALUES (?, ?, ?, ?, ?)", i, j, k, i, i + 1);

        rows = getRows(execute("SELECT DISTINCT k, s1 FROM %s"));
        checkDistinctRows(rows, true, 0, 10, 0, 10);

        rows = getRows(execute("SELECT DISTINCT k, s2 FROM %s"));
        checkDistinctRows(rows, true, 0, 10, 1, 11);

        rows = getRows(execute("SELECT DISTINCT k, s1 FROM %s LIMIT 10"));
        checkDistinctRows(rows, true, 0, 10, 0, 10);

        rows = getRows(execute("SELECT DISTINCT k, s1 FROM %s WHERE k IN (" + keys + ")"));
        checkDistinctRows(rows, false, 0, 10, 0, 10);

        rows = getRows(execute("SELECT DISTINCT k, s2 FROM %s WHERE k IN (" + keys + ")"));
        checkDistinctRows(rows, false, 0, 10, 1, 11);

        rows = getRows(execute("SELECT DISTINCT k, s1 FROM %s WHERE k IN (" + keys + ")"));
        checkDistinctRows(rows, true, 0, 10, 0, 10);
    }

    void checkDistinctRows(Object[][] rows, boolean sort, int... ranges)
    {
        assertTrue(ranges.length % 2 == 0);

        int numdim = ranges.length / 2;
        int[] from = new int[numdim];
        int[] to = new int[numdim];

        for (int i = 0, j = 0; i < ranges.length && j < numdim; i+= 2, j++)
        {
            from[j] = ranges[i];
            to[j] = ranges[i+1];
        }

        //sort the rows
        for (int i = 0; i < numdim; i++)
        {
            int[] vals = new int[rows.length];
            for (int j = 0; j < rows.length; j++)
                vals[j] = (Integer)rows[j][i];

            if (sort)
                Arrays.sort(vals);

            for (int j = from[i]; j < to[i]; j++)
                assertEquals(j, vals[j - from[i]]);
        }
    }

    /**
     * Test LIMIT when static columns are present (#6956),
     * migrated from cql_tests.py:TestCQL.static_with_limit_test()
     */
    @Test
    public void testStaticColumnsWithLimit() throws Throwable
    {
        createTable(" CREATE TABLE %s (k int, s int static, v int, PRIMARY KEY (k, v))");

        execute("INSERT INTO %s (k, s) VALUES(0, 42)");
        for (int i = 0; i < 4; i++)
            execute("INSERT INTO %s(k, v) VALUES(0, ?)", i);

        assertRows(execute("SELECT * FROM %s WHERE k = 0 LIMIT 1"),
                   row(0, 0, 42));
        assertRows(execute("SELECT * FROM %s WHERE k = 0 LIMIT 2"),
                   row(0, 0, 42),
                   row(0, 1, 42));
        assertRows(execute("SELECT * FROM %s WHERE k = 0 LIMIT 3"),
                   row(0, 0, 42),
                   row(0, 1, 42),
                   row(0, 2, 42));
    }

    /**
     * Test for bug of #7455,
     * migrated from cql_tests.py:TestCQL.static_with_empty_clustering_test()
     */
    @Test
    public void testStaticColumnsWithEmptyClustering() throws Throwable
    {
        createTable("CREATE TABLE %s (pkey text, ckey text, value text, static_value text static, PRIMARY KEY(pkey, ckey))");

        execute("INSERT INTO %s (pkey, static_value) VALUES ('partition1', 'static value')");
        execute("INSERT INTO %s (pkey, ckey, value) VALUES('partition1', '', 'value')");

        assertRows(execute("SELECT * FROM %s"),
                   row("partition1", "", "static value", "value"));
    }

    /**
     * Migrated from cql_tests.py:TestCQL.alter_clustering_and_static_test()
     */
    @Test
    public void testAlterClusteringAndStatic() throws Throwable
    {
        createTable("CREATE TABLE %s (bar int, PRIMARY KEY (bar))");

        // We shouldn 't allow static when there is not clustering columns
        assertInvalid("ALTER TABLE %s ADD bar2 text static");
    }
}
