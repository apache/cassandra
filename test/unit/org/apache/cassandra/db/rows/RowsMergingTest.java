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
package org.apache.cassandra.db.rows;

import java.util.Arrays;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.commons.lang3.StringUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Joiner;

import static org.apache.cassandra.config.CassandraRelevantProperties.BTREE_BRANCH_SHIFT;

public class RowsMergingTest extends CQLTester
{
    @BeforeClass
    public static void setUpClass()
    {
        BTREE_BRANCH_SHIFT.setInt(2);
        CQLTester.setUpClass();
    }

    @Test
    public void testInsertion() throws Throwable
    {
        checker().schema("CREATE TABLE %s (pk int, ck int, a int, b int, c int, d int, e int, f int, PRIMARY KEY (pk, ck))")
                 .queries("INSERT INTO %s (pk, ck, a, b, c, d, e, f) VALUES (?, 1, 1, 1, 1, 1, 1, 1) USING TIMESTAMP 1001",
                          "INSERT INTO %s (pk, ck, a, b, c, d) VALUES (?, 1, 2, 2, 2, 2) USING TIMESTAMP 1002")
                 .expectedRow(1, 2, 2, 2, 2, 1, 1)
                 .check();
    }

    @Test
    public void testRowDeletions() throws Throwable
    {
        checker().schema("CREATE TABLE %s (pk int, ck int, a int, b int, c int, d int, e int, f int, PRIMARY KEY (pk, ck))")
                 .queries("INSERT INTO %s (pk, ck, a, b, c, d, e, f) VALUES (?, 1, 1, 1, 1, 1, 1, 1) USING TIMESTAMP 1001",
                          "DELETE FROM %s USING TIMESTAMP 1002 WHERE pk = ? AND ck = 1",
                          "INSERT INTO %s (pk, ck, a) VALUES (?, 1, 2) USING TIMESTAMP 1003")
                 .expectedRow(1, 2, null, null, null, null, null)
                 .check();
    }

    @Test
    public void testRowDeletionWithUpdate() throws Throwable
    {
        checker().schema("CREATE TABLE %s (pk int, ck int, a int, b int, c int, d int, e int, f int, PRIMARY KEY (pk, ck))")
                 .queries("INSERT INTO %s (pk, ck, a, b, c, d, e, f) VALUES (?, 1, 1, 1, 1, 1, 1, 1) USING TIMESTAMP 1001",
                          "DELETE FROM %s USING TIMESTAMP 1002 WHERE pk = ? AND ck = 1",
                          "UPDATE %s USING TIMESTAMP 1003 SET a = 2 WHERE pk = ? AND ck = 1")
                 .expectedRow(1, 2, null, null, null, null, null)
                 .check();
    }

    @Test
    public void testRowDeletionWithOnlyUpdates() throws Throwable
    {
        checker().schema("CREATE TABLE %s (pk int, ck int, a int, b int, c int, d int, e int, f int, PRIMARY KEY (pk, ck))")
                 .queries("UPDATE %s USING TIMESTAMP 1001 SET a = 1, b = 1, c = 1, d = 1, e = 1, f = 1 WHERE pk = ? AND ck = 1",
                          "DELETE FROM %s USING TIMESTAMP 1002 WHERE pk = ? AND ck = 1",
                          "UPDATE %s USING TIMESTAMP 1003 SET a = 2 WHERE pk = ? AND ck = 1")
                 .expectedRow(1, 2, null, null, null, null, null)
                 .check();
    }

    @Test
    public void testRowWithMultipleDeletions() throws Throwable
    {
        checker().schema("CREATE TABLE %s (pk int, ck int, a int, b int, c int, d int, e int, f int, PRIMARY KEY (pk, ck))")
                 .queries("INSERT INTO %s (pk, ck, a, b, c, d, e, f) VALUES (?, 1, 1, 1, 1, 1, 1, 1) USING TIMESTAMP 1001",
                          "DELETE FROM %s USING TIMESTAMP 1002 WHERE pk = ? AND ck = 1",
                          "UPDATE %s USING TIMESTAMP 1003 SET a = 2 WHERE pk = ? AND ck = 1",
                          "DELETE FROM %s USING TIMESTAMP 1004 WHERE pk = ? AND ck = 1")
                 .check();
    }

    @Test
    public void testRowWithComplexCollectionOverride() throws Throwable
    {
        checker().schema("CREATE TABLE %s (pk int, ck int, s set<text>, PRIMARY KEY (pk, ck))")
                 .queries("INSERT INTO %s (pk, ck, s) VALUES (?, 1, {'a', 'b', 'c'}) USING TIMESTAMP 1001",
                          "UPDATE %s USING TIMESTAMP 1002 SET s = {'m', 'n'} WHERE pk = ? AND ck = 1")
                 .expectedRow(1, set("m", "n"))
                 .check();
    }

    @Test
    public void testRowWithDeletionAndComplexCollection() throws Throwable
    {
        checker().schema("CREATE TABLE %s (pk int, ck int, s set<text>, PRIMARY KEY (pk, ck))")
                 .queries("INSERT INTO %s (pk, ck, s) VALUES (?, 1, {'a', 'b', 'c'}) USING TIMESTAMP 1001",
                          "DELETE FROM %s USING TIMESTAMP 1002 WHERE pk = ? AND ck = 1")
                 .check();
    }

    @Test
    public void testRowWithDeletionAndComplexCollectionOverride() throws Throwable
    {
        checker().schema("CREATE TABLE %s (pk int, ck int, s set<text>, PRIMARY KEY (pk, ck))")
                 .queries("INSERT INTO %s (pk, ck, s) VALUES (?, 1, {'a', 'b', 'c'}) USING TIMESTAMP 1001",
                          "DELETE FROM %s USING TIMESTAMP 1002 WHERE pk = ? AND ck = 1",
                          "UPDATE %s USING TIMESTAMP 1003 SET s = {'m', 'n'} WHERE pk = ? AND ck = 1")
                 .expectedRow(1, set("m", "n"))
                 .check();
    }

    @Test
    public void testRowWithComplexDeletionAfterRowDeletion() throws Throwable
    {
        checker().schema("CREATE TABLE %s (pk int, ck int, s set<text>, PRIMARY KEY (pk, ck))")
                 .queries("DELETE FROM %s USING TIMESTAMP 1001 WHERE pk = ? AND ck = 1",
                         "INSERT INTO %s (pk, ck, s) VALUES (?, 1, {'a', 'b', 'c'}) USING TIMESTAMP 1002",
                         "DELETE s FROM %s USING TIMESTAMP 1003 WHERE pk = ? AND ck = 1")
                 .expectedRow(1, null)
                 .check();
    }

    @Test
    public void testRowDeletionsWithBatchWithBatchTimestamp() throws Throwable
    {
        checker().schema("CREATE TABLE %s (pk int, ck int, a int, b int, c int, d int, e int, f int, PRIMARY KEY (pk, ck))")
                 .queries("BEGIN BATCH USING TIMESTAMP 1001 \n" +
                          "INSERT INTO %s (pk, ck, a, b, c, d, e, f) VALUES (?, 1, 1, 1, 1, 1, 1, 1); \n" +
                          "DELETE FROM %s WHERE pk = ? AND ck = 1; \n" +
                          "APPLY BATCH;",
                          "INSERT INTO %s (pk, ck, a) VALUES (?, 1, 2) USING TIMESTAMP 1002")
                 .expectedRow(1, 2, null, null, null, null, null)
                 .check();
    }

    @Test
    public void testRowDeletionsWithBatchWithTimestampPerOperation() throws Throwable
    {
        checker().schema("CREATE TABLE %s (pk int, ck int, a int, b int, c int, d int, e int, f int, PRIMARY KEY (pk, ck))")
                 .queries("BEGIN BATCH \n" +
                          "INSERT INTO %s (pk, ck, a, b, c, d, e, f) VALUES (?, 1, 1, 1, 1, 1, 1, 1) USING TIMESTAMP 1001 ; \n" +
                          "DELETE FROM %s USING TIMESTAMP 1002 WHERE pk = ? AND ck = 1; \n" +
                          "APPLY BATCH;",
                          "INSERT INTO %s (pk, ck, a) VALUES (?, 1, 2) USING TIMESTAMP 1003")
                 .expectedRow(1, 2, null, null, null, null, null)
                 .check();
    }

    public MergeChecker checker()
    {
        return new MergeChecker();
    }

    /**
     * Utility class to check that a merge result in the expected row not no matter in which order the operation are applied.
     */
    private class MergeChecker
    {
        private int pk;

        private String schema;

        private String[] queries;

        private Object[] expectedRow;

        public MergeChecker schema(String schema)
        {
            this.schema = schema;
            return this;
        }

        public MergeChecker queries(String... queries)
        {
            this.queries = queries;
            return this;
        }

        public MergeChecker expectedRow(Object... columnValues)
        {
            this.expectedRow = new Object[columnValues.length + 1];
            System.arraycopy(columnValues, 0, this.expectedRow, 1, columnValues.length);
            return this;
        }

        public void check() throws Throwable
        {
            createTable(schema);
            checkAllPermutations(queries.length, queries);
        }

        private void check(String[] queries) throws Throwable
        {
            for (String query : queries)
            {
                try
                {
                    int count = StringUtils.countMatches(query, "%s");
                    Object[] parameters1 = new Object[count];
                    Arrays.fill(parameters1, pk);
                    Object[] parameters = parameters1;
                    executeFormattedQuery(formatQueries(query, count), parameters);
                }
                catch (Throwable e)
                {
                    throw new AssertionError("Executing the following queries did not lead to the expected result: \n"
                                + Joiner.on("; \n").join(queries)
                                + "\n when executing: \n" + query, e);
                }
            }

            try
            {
                if (expectedRow != null)
                {
                    expectedRow[0] = pk;
                    assertRows(execute("SELECT * FROM %s WHERE pk = ?" , pk),
                               expectedRow);
                }
                else
                {
                    assertEmpty(execute("SELECT * FROM %s WHERE pk = ?" , pk));
                }
            }
            catch (Throwable e)
            {
                throw new AssertionError("Executing the following queries did not lead to the expected result: \n" + Joiner.on("; \n").join(queries), e);
            }
            pk++;
        }

        private String formatQueries(String query, int numberOfqueries)
        {
            String table = keyspace() + '.' + currentTable();
            return String.format(query, createFilledArray(numberOfqueries, table));
        }

        private Object[] createFilledArray(int length, Object fillingValue)
        {
            Object[] tables = new Object[length];
            Arrays.fill(tables, fillingValue);
            return tables;
        }

        private void checkAllPermutations(int n, String[] queries) throws Throwable
        {
            if (n == 1)
            {
                check(queries);
            }
            else
            {
                for (int i = 0, m = n - 1; i < m; i++)
                {
                    checkAllPermutations(n - 1, queries);
                    if ((i & 1) == 0)
                    {
                        swap(queries, i, n - 1);
                    }
                    else
                    {
                        swap(queries, 0, n - 1);
                    }
                }
                checkAllPermutations(n - 1, queries);
            }
        }

        private void swap(String[] queries, int i, int j)
        {
            String tmp = queries[i];
            queries[i] = queries[j];
            queries[j] = tmp;
        }
    }
}
