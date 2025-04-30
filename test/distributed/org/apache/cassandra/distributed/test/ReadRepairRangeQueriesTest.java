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

package org.apache.cassandra.distributed.test;

import java.util.Arrays;
import java.util.Comparator;

import org.junit.Assume;
import org.junit.Test;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.distributed.shared.AssertUtils.row;

/**
 * {@link ReadRepairQueryTester} for range queries.
 *
 */
public class ReadRepairRangeQueriesTest extends ReadRepairQueryTester
{
    /**
     * Test range queries on a table without clustering columns.
     */
    @Test
    public void testRangeQueryOnSkinnyTable()
    {
        tester("WHERE token(k) >= token(2)")
        .createTable("CREATE TABLE %s (k int PRIMARY KEY, a int, b int)")
        .mutate("INSERT INTO %s (k, a, b) VALUES (1, 10, 100)",
                "INSERT INTO %s (k, a, b) VALUES (2, 20, 200)",
                "INSERT INTO %s (k, a, b) VALUES (3, 30, 300)")
        .queryColumns("a", 2, 2,
                      rows(row(20), row(30)),
                      rows(row(2, 20, 200), row(3, 30, 300)),
                      rows(row(2, 20, null), row(3, 30, null)))
        .deleteColumn("DELETE a FROM %s WHERE k=2", "b", 0, 1,
                      rows(row(200), row(300)),
                      rows(row(2, null, 200), row(3, 30, 300)),
                      rows(row(2, 20, 200), row(3, 30, 300)))
        .deleteRows("DELETE FROM %s WHERE k=3", 1,
                    rows(row(2, null, 200)),
                    rows(row(2, null, 200), row(3, 30, 300)))
        .tearDown(1,
                  rows(row(1, 10, 100), row(2, null, 200)),
                  rows(row(2, null, 200)),
                  true);
    }

    /**
     * Test range queries on a table with clustering columns.
     */
    @Test
    public void testRangeQueryOnWideTable()
    {
        tester("WHERE token(k) >= token(2)")
        .createTable("CREATE TABLE %s (k int, c int, a int, b int, PRIMARY KEY(k, c))")
        .mutate("INSERT INTO %s (k, c, a, b) VALUES (1, 10, 100, 1000)",
                "INSERT INTO %s (k, c, a, b) VALUES (2, 20, 200, 2000)",
                "INSERT INTO %s (k, c, a, b) VALUES (2, 21, 201, 2001)",
                "INSERT INTO %s (k, c, a, b) VALUES (3, 30, 300, 3000)")
        .queryColumns("a",
                      paging ? 3 : 2,
                      paging ? 3 : 2,
                      rows(row(200), row(201), row(300)),
                      rows(row(2, 20, 200, 2000), row(2, 21, 201, 2001), row(3, 30, 300, 3000)),
                      rows(row(2, 20, 200, null), row(2, 21, 201, null), row(3, 30, 300, null)))
        .deleteColumn("DELETE a FROM %s WHERE k=2 AND c=21", "b", 0, 1,
                      rows(row(2000), row(2001), row(3000)),
                      rows(row(2, 20, 200, 2000), row(2, 21, null, 2001), row(3, 30, 300, 3000)),
                      rows(row(2, 20, 200, 2000), row(2, 21, 201, 2001), row(3, 30, 300, 3000)))
        .deleteRows("DELETE FROM %s WHERE k=2 AND c=21", 1,
                    rows(row(2, 20, 200, 2000), row(3, 30, 300, 3000)),
                    rows(row(2, 20, 200, 2000), row(2, 21, null, 2001), row(3, 30, 300, 3000)))
        .deleteRows("DELETE FROM %s WHERE k=2", 1,
                    rows(row(3, 30, 300, 3000)),
                    rows(row(2, 20, 200, 2000), row(3, 30, 300, 3000)))
        .tearDown(1,
                  rows(row(1, 10, 100, 1000), row(3, 30, 300, 3000)),
                  rows(row(3, 30, 300, 3000)),
                  true);
    }

    /**
     * Test range queries without restrictions but with a limit on a table without clustering columns.
     */
    @Test
    public void testRangeQueryWithLimitOnSkinnyTable()
    {
        tester("WHERE token(k) >= token(1) LIMIT 2")
        .createTable("CREATE TABLE %s (k int PRIMARY KEY, a int, b int)")
        .mutate("INSERT INTO %s (k, a, b) VALUES (1, 10, 100)",
                "INSERT INTO %s (k, a, b) VALUES (2, 20, 200)",
                "INSERT INTO %s (k, a, b) VALUES (3, 30, 300)")
        .queryColumns("a", 2, 2,
                      rows(row(10), row(20)),
                      rows(row(1, 10, 100), row(2, 20, 200)),
                      rows(row(1, 10, null), row(2, 20, null)))
        .deleteColumn("DELETE a FROM %s WHERE k=2", "b", 0, 1,
                      rows(row(100), row(200)),
                      rows(row(1, 10, 100), row(2, null, 200)),
                      rows(row(1, 10, 100), row(2, 20, 200)))
        .deleteRows("DELETE FROM %s WHERE k=1", 2,
                    rows(row(2, null, 200), row(3, 30, 300)),
                    rows(row(1, 10, 100), row(2, null, 200)))
        .tearDown(0,
                  rows(row(2, null, 200), row(3, 30, 300)),
                  rows(row(2, null, 200), row(3, 30, 300)));
    }

    /**
     * Test range queries without restrictions but with a limit on a table with clustering columns.
     */
    @Test
    public void testRangeQueryWithLimitOnWideTable()
    {
        tester("WHERE token(k) >= token(1) LIMIT 2")
        .createTable("CREATE TABLE %s (k int, c int, a int, b int, PRIMARY KEY(k, c))")
        .mutate("INSERT INTO %s (k, c, a, b) VALUES (1, 10, 100, 1000)",
                "INSERT INTO %s (k, c, a, b) VALUES (2, 20, 200, 2000)",
                "INSERT INTO %s (k, c, a, b) VALUES (2, 21, 201, 2001)",
                "INSERT INTO %s (k, c, a, b) VALUES (3, 30, 300, 3000)")
        .queryColumns("a", 2, 2,
                      rows(row(100), row(200)),
                      rows(row(1, 10, 100, 1000), row(2, 20, 200, 2000)),
                      rows(row(1, 10, 100, null), row(2, 20, 200, null)))
        .deleteColumn("DELETE a FROM %s WHERE k=2 AND c=20", "b", 0, 1,
                      rows(row(1000), row(2000)),
                      rows(row(1, 10, 100, 1000), row(2, 20, null, 2000)),
                      rows(row(1, 10, 100, 1000), row(2, 20, 200, 2000)))
        .deleteRows("DELETE FROM %s WHERE k=2 AND c=20", 1,
                    rows(row(1, 10, 100, 1000), row(2, 21, 201, 2001)),
                    rows(row(1, 10, 100, 1000), row(2, 20, null, 2000)))
        .deleteRows("DELETE FROM %s WHERE k=2", 2,
                    rows(row(1, 10, 100, 1000), row(3, 30, 300, 3000)),
                    rows(row(1, 10, 100, 1000), row(2, 21, 201, 2001)))
        .tearDown(0,
                  rows(row(1, 10, 100, 1000), row(3, 30, 300, 3000)),
                  rows(row(1, 10, 100, 1000), row(3, 30, 300, 3000)));
    }

    /**
     * Test range queries using filtering on a selected column on a table without clustering columns.
     */
    @Test
    public void testRangeQueryWithFilterOnSelectedColumnOnSkinnyTable()
    {
        tester("WHERE a=2 ALLOW FILTERING")
        .createTable("CREATE TABLE %s (k int PRIMARY KEY, a int, b int)")
        .mutate("INSERT INTO %s (k, a, b) VALUES (1, 2, 3)",
                "INSERT INTO %s (k, a, b) VALUES (10, 20, 30)")
        .queryColumns("a", 1, 1,
                      rows(row(2)),
                      rows(row(1, 2, 3)),
                      rows(row(1, 2, null)))
        .deleteColumn("DELETE b FROM %s WHERE k=1", "a", 0, 1,
                      rows(row(2)),
                      rows(row(1, 2, null)),
                      rows(row(1, 2, 3)))
        .deleteRows("DELETE FROM %s WHERE k=1", 1,
                    rows(),
                    rows(row(1, 2, null)))
        .tearDown(1,
                  rows(row(10, 20, 30)),
                  rows());
    }

    /**
     * Test range queries using filtering on an selected column on a table with clustering columns.
     */
    @Test
    public void testRangeQueryWithFilterOnSelectedColumnOnWideTable()
    {
        tester("WHERE a=1 ALLOW FILTERING")
        .createTable("CREATE TABLE %s (k int, c int, a int, b int, PRIMARY KEY(k, c))")
        .mutate("INSERT INTO %s (k, c, a, b) VALUES (1, 1, 1, 1)",
                "INSERT INTO %s (k, c, a, b) VALUES (1, 2, 2, 2)",
                "INSERT INTO %s (k, c, a, b) VALUES (2, 1, 1, 1)",
                "INSERT INTO %s (k, c, a, b) VALUES (2, 2, 2, 2)")
        .queryColumns("a", 2, 2,
                      rows(row(1), row(1)),
                      rows(row(1, 1, 1, 1), row(2, 1, 1, 1)),
                      rows(row(1, 1, 1, null), row(2, 1, 1, null)))
        .deleteColumn("DELETE b FROM %s WHERE k=1 AND c=1", "a", 0, 1,
                      rows(row(1), row(1)),
                      rows(row(1, 1, 1, null), row(2, 1, 1, 1)),
                      rows(row(1, 1, 1, 1), row(2, 1, 1, 1)))
        .deleteRows("DELETE FROM %s WHERE k=1 AND c=1", 1,
                    rows(row(2, 1, 1, 1)),
                    rows(row(1, 1, 1, null), row(2, 1, 1, 1)))
        .deleteRows("DELETE FROM %s WHERE k=2", 1,
                    rows(),
                    rows(row(2, 1, 1, 1)))
        .tearDown(1,
                  rows(row(1, 2, 2, 2)),
                  rows());
    }

    /**
     * Test range queries using filtering on an unselected column on a table without clustering columns.
     */
    @Test
    public void testRangeQueryWithFilterOnUnselectedColumnOnSkinnyTable()
    {
        tester("WHERE b=3 ALLOW FILTERING")
        .createTable("CREATE TABLE %s (k int PRIMARY KEY, a int, b int)")
        .mutate("INSERT INTO %s (k, a, b) VALUES (1, 2, 3)",
                "INSERT INTO %s (k, a, b) VALUES (10, 20, 30)")
        .queryColumns("a", 1, 0,
                      rows(row(2)),
                      rows(row(1, 2, 3)),
                      rows(row(1, 2, 3))) // the filtered column is repaired even if it isn't selected
        .deleteColumn("DELETE a FROM %s WHERE k=1", "b", 0, 1,
                      rows(row(3)),
                      rows(row(1, null, 3)),
                      rows(row(1, 2, 3)))
        .deleteRows("DELETE FROM %s WHERE k=1", 1,
                    rows(),
                    rows(row(1, null, 3)))
        .tearDown(1,
                  rows(row(10, 20, 30)),
                  rows());
    }

    /**
     * Test range queries using filtering on an unselected column on a table with clustering columns.
     */
    @Test
    public void testRangeQueryWithFilterOnUnselectedColumnOnWideTable()
    {
        tester("WHERE b=2 ALLOW FILTERING")
        .createTable("CREATE TABLE %s (k int, c int, a int, b int, PRIMARY KEY(k, c))")
        .mutate("INSERT INTO %s (k, c, a, b) VALUES (1, 1, 1, 1)",
                "INSERT INTO %s (k, c, a, b) VALUES (1, 2, 2, 2)",
                "INSERT INTO %s (k, c, a, b) VALUES (2, 1, 1, 1)",
                "INSERT INTO %s (k, c, a, b) VALUES (2, 2, 2, 2)")
        .queryColumns("a", 2, 0,
                      rows(row(2), row(2)),
                      rows(row(1, 2, 2, 2), row(2, 2, 2, 2)),
                      rows(row(1, 2, 2, 2), row(2, 2, 2, 2))) // the filtered column is repaired even if it isn't selected
        .deleteColumn("DELETE a FROM %s WHERE k=1 AND c=2", "b", 0, 1,
                      rows(row(2), row(2)),
                      rows(row(1, 2, null, 2), row(2, 2, 2, 2)),
                      rows(row(1, 2, 2, 2), row(2, 2, 2, 2)))
        .deleteRows("DELETE FROM %s WHERE k=2 AND c=2", 1,
                    rows(row(1, 2, null, 2)),
                    rows(row(1, 2, null, 2), row(2, 2, 2, 2)))
        .deleteRows("DELETE FROM %s WHERE k=1", 1,
                    rows(),
                    rows(row(1, 2, null, 2)))
        .tearDown(1,
                  rows(row(2, 1, 1, 1)),
                  rows());
    }

    /**
     * Test range queries using filtering on an selected column on a table with clustering columns.
     */
    @Test
    public void testRangeQueryWithFilterOnSelectedColumnConflictingUpdates()
    {
        Assume.assumeTrue(replicationType.isTracked());  // flaky for untracked replication
        tester("WHERE a=1 ALLOW FILTERING")
        .createTable("CREATE TABLE %s (k int, c int, a int, b int, PRIMARY KEY(k, c))")
        .mutate(1, "INSERT INTO %s (k, c, a, b) VALUES (1, 1, 1, 1)",
                "INSERT INTO %s (k, c, a, b) VALUES (1, 2, 2, 2)",
                "INSERT INTO %s (k, c, a, b) VALUES (2, 1, 2, 1)",
                "INSERT INTO %s (k, c, a, b) VALUES (2, 2, 2, 2)")
        .mutate(2, "INSERT INTO %s (k, c, a, b) VALUES (2, 1, 1, 1)")
        .mutate(1)  // updates the last coordinator for mutation tracking
        .queryColumns("k, c, a", 2, 2,
                      rows(row(1, 1, 1), row(2, 1, 1)),
                      rows(row(1, 1, 1, 1), row(2, 1, 1, 1)),
                      rows(row(1, 1, 1, null), row(2, 1, 1, 1)))
        .tearDown(2,
                  rows(row(1, 1, 1, 1), row(1, 2, 2, 2), row(2, 1, 1, 1), row(2, 2, 2, 2)),
                  rows(row(1, 1, 1, 1), row(2, 1, 1, 1)));
    }

    /**
     * Helper class for creating rows in token sorted order
     */
    private static class TokenSortedKey
    {
        final int key;

        public TokenSortedKey(int key)
        {
            this.key = key;
        }

        Token token()
        {
            return Murmur3Partitioner.instance.getToken(ByteBufferUtil.bytes(key));
        }

        public Object[] row(Object... values)
        {
            Object[] row = new Object[values.length + 1];
            row[0] = key;
            System.arraycopy(values, 0, row, 1, values.length);
            return row;
        }

        public static TokenSortedKey[] create(int numKeys)
        {
            TokenSortedKey[] keys = new TokenSortedKey[numKeys];
            for (int i = 0; i < numKeys; i++)
                keys[i] = new TokenSortedKey(i);
            Arrays.sort(keys, Comparator.comparing(TokenSortedKey::token));
            return keys;
        }

    }

    /**
     * Tests the allow filtering case where the local coordinator filters out a partition that hashes to a token
     * between 2 valid matches and another replica reports a mutation that causes it to be re-added to the result
     */
    @Test
    public void testFilteredInterleavedShortRead()
    {
        Assume.assumeTrue(replicationType.isTracked());  // flaky for untracked replication
        TokenSortedKey[] keys = TokenSortedKey.create(4);

        tester("WHERE a=1 ALLOW FILTERING")
        .createTable("CREATE TABLE %s (k int, c int, a int, PRIMARY KEY(k, c))")
        .mutate(1, "INSERT INTO %s (k, c, a) VALUES (" + keys[0].key + ", 1, 1)",
                "INSERT INTO %s (k, c, a) VALUES (" + keys[0].key + ", 2, 2)",
                "INSERT INTO %s (k, c, a) VALUES (" + keys[1].key + ", 1, 2)",
                "INSERT INTO %s (k, c, a) VALUES (" + keys[2].key + ", 1, 1)")
        .mutate(2, "INSERT INTO %s (k, c, a) VALUES (" + keys[1].key + ", 1, 1)") // make keys[1] match the filter
        .mutate(1)  // updates the last coordinator for mutation tracking
        .queryColumns("k, c, a", 3, 0,
                      rows(keys[0].row(1, 1), keys[1].row(1, 1), keys[2].row(1, 1)),
                      rows(keys[0].row(1, 1), keys[1].row(1, 1), keys[2].row(1, 1)),
                      rows(keys[0].row(1, 1), keys[1].row(1, 1), keys[2].row(1, 1)))
        .tearDown(1,
                  rows(keys[0].row(1, 1), keys[0].row(2, 2), keys[1].row(1, 1), keys[2].row(1, 1)),
                  rows(keys[0].row(1, 1), keys[1].row(1, 1), keys[2].row(1, 1)));
    }

    @Test
    public void testNonFilteredInterleavedShortRead()
    {
        Assume.assumeTrue(replicationType.isTracked());  // flaky for untracked replication
        TokenSortedKey[] keys = TokenSortedKey.create(4);

        tester("WHERE a=1 ALLOW FILTERING")
        .createTable("CREATE TABLE %s (k int, c int, a int, PRIMARY KEY(k, c))")
        .mutate(1, "INSERT INTO %s (k, c, a) VALUES (" + keys[0].key + ", 1, 1)",
                "INSERT INTO %s (k, c, a) VALUES (" + keys[0].key + ", 2, 2)",
                "INSERT INTO %s (k, c, a) VALUES (" + keys[2].key + ", 1, 1)")
        .mutate(2, "INSERT INTO %s (k, c, a) VALUES (" + keys[1].key + ", 1, 1)") // make keys[1] match the filter
        .mutate(1)  // updates the last coordinator for mutation tracking
        .queryColumns("k, c, a", 3, 0,
                      rows(keys[0].row(1, 1), keys[1].row(1, 1), keys[2].row(1, 1)),
                      rows(keys[0].row(1, 1), keys[1].row(1, 1), keys[2].row(1, 1)),
                      rows(keys[0].row(1, 1), keys[1].row(1, 1), keys[2].row(1, 1)))
        .tearDown(1,
                  rows(keys[0].row(1, 1), keys[0].row(2, 2), keys[1].row(1, 1), keys[2].row(1, 1)),
                  rows(keys[0].row(1, 1), keys[1].row(1, 1), keys[2].row(1, 1)));
    }
}
