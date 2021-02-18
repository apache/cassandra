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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.cql3.validation.entities.SecondaryIndexTest;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.SchemaCQLHelper;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static junit.framework.TestCase.assertTrue;
import static org.apache.cassandra.utils.ByteBufferUtil.EMPTY_BYTE_BUFFER;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;
import static org.reflections.util.Utils.isEmpty;

public class CompactStorageTest extends CQLTester
{
    private static final String compactOption = " WITH COMPACT STORAGE";

    @Test
    public void testSparseCompactTableIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (key ascii PRIMARY KEY, val ascii) WITH COMPACT STORAGE");

        // Indexes are allowed only on the sparse compact tables
        createIndex("CREATE INDEX ON %s(val)");
        for (int i = 0; i < 10; i++)
            execute("INSERT INTO %s (key, val) VALUES (?, ?)", Integer.toString(i), Integer.toString(i * 10));

        alterTable("ALTER TABLE %s DROP COMPACT STORAGE");

        assertRows(execute("SELECT * FROM %s WHERE val = '50'"),
                   row("5", null, "50", null));
        assertRows(execute("SELECT * FROM %s WHERE key = '5'"),
                   row("5", null, "50", null));
    }

    @Test
    public void before() throws Throwable
    {
        createTable("CREATE TABLE %s (key TEXT, column TEXT, value BLOB, PRIMARY KEY (key, column)) WITH COMPACT STORAGE");

        ByteBuffer largeBytes = ByteBuffer.wrap(new byte[100000]);
        execute("INSERT INTO %s (key, column, value) VALUES (?, ?, ?)", "test", "a", largeBytes);
        ByteBuffer smallBytes = ByteBuffer.wrap(new byte[10]);
        execute("INSERT INTO %s (key, column, value) VALUES (?, ?, ?)", "test", "c", smallBytes);

        flush();

        assertRows(execute("SELECT column FROM %s WHERE key = ? AND column IN (?, ?, ?)", "test", "c", "a", "b"),
                   row("a"),
                   row("c"));
    }

    @Test
    public void testStaticCompactTables() throws Throwable
    {
        createTable("CREATE TABLE %s (k text PRIMARY KEY, v1 int, v2 text) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (k, v1, v2) values (?, ?, ?)", "first", 1, "value1");
        execute("INSERT INTO %s (k, v1, v2) values (?, ?, ?)", "second", 2, "value2");
        execute("INSERT INTO %s (k, v1, v2) values (?, ?, ?)", "third", 3, "value3");

        assertRows(execute("SELECT * FROM %s WHERE k = ?", "first"),
                   row("first", 1, "value1")
        );

        assertRows(execute("SELECT v2 FROM %s WHERE k = ?", "second"),
                   row("value2")
        );

        // Murmur3 order
        assertRows(execute("SELECT * FROM %s"),
                   row("third", 3, "value3"),
                   row("second", 2, "value2"),
                   row("first", 1, "value1")
        );
    }

    @Test
    public void testCompactStorageUpdateWithNull() throws Throwable
    {
        createTable("CREATE TABLE %s (partitionKey int," +
                    "clustering_1 int," +
                    "value int," +
                    " PRIMARY KEY (partitionKey, clustering_1)) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (partitionKey, clustering_1, value) VALUES (0, 0, 0)");
        execute("INSERT INTO %s (partitionKey, clustering_1, value) VALUES (0, 1, 1)");

        flush();

        execute("UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 = ?", null, 0, 0);

        assertRows(execute("SELECT * FROM %s WHERE partitionKey = ? AND (clustering_1) IN ((?), (?))", 0, 0, 1),
                   row(0, 1, 1)
        );
    }

    /**
     * Migrated from cql_tests.py:TestCQL.collection_compact_test()
     */
    @Test
    public void testCompactCollections() throws Throwable
    {
        String tableName = KEYSPACE + "." + createTableName();
        assertInvalid(String.format("CREATE TABLE %s (user ascii PRIMARY KEY, mails list < text >) WITH COMPACT STORAGE;", tableName));
    }

    /**
     * Check for a table with counters,
     * migrated from cql_tests.py:TestCQL.counters_test()
     */
    @Test
    public void testCounters() throws Throwable
    {
        createTable("CREATE TABLE %s (userid int, url text, total counter, PRIMARY KEY (userid, url)) WITH COMPACT STORAGE");

        execute("UPDATE %s SET total = total + 1 WHERE userid = 1 AND url = 'http://foo.com'");
        assertRows(execute("SELECT total FROM %s WHERE userid = 1 AND url = 'http://foo.com'"),
                   row(1L));

        execute("UPDATE %s SET total = total - 4 WHERE userid = 1 AND url = 'http://foo.com'");
        assertRows(execute("SELECT total FROM %s WHERE userid = 1 AND url = 'http://foo.com'"),
                   row(-3L));

        execute("UPDATE %s SET total = total+1 WHERE userid = 1 AND url = 'http://foo.com'");
        assertRows(execute("SELECT total FROM %s WHERE userid = 1 AND url = 'http://foo.com'"),
                   row(-2L));

        execute("UPDATE %s SET total = total -2 WHERE userid = 1 AND url = 'http://foo.com'");
        assertRows(execute("SELECT total FROM %s WHERE userid = 1 AND url = 'http://foo.com'"),
                   row(-4L));

        execute("UPDATE %s SET total += 6 WHERE userid = 1 AND url = 'http://foo.com'");
        assertRows(execute("SELECT total FROM %s WHERE userid = 1 AND url = 'http://foo.com'"),
                   row(2L));

        execute("UPDATE %s SET total -= 1 WHERE userid = 1 AND url = 'http://foo.com'");
        assertRows(execute("SELECT total FROM %s WHERE userid = 1 AND url = 'http://foo.com'"),
                   row(1L));

        execute("UPDATE %s SET total += -2 WHERE userid = 1 AND url = 'http://foo.com'");
        assertRows(execute("SELECT total FROM %s WHERE userid = 1 AND url = 'http://foo.com'"),
                   row(-1L));

        execute("UPDATE %s SET total -= -2 WHERE userid = 1 AND url = 'http://foo.com'");
        assertRows(execute("SELECT total FROM %s WHERE userid = 1 AND url = 'http://foo.com'"),
                   row(1L));
    }


    @Test
    public void testCounterFiltering() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, a counter) WITH COMPACT STORAGE");

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

    /**
     * Test for the bug of #11726.
     */
    @Test
    public void testCounterAndColumnSelection() throws Throwable
    {
        for (String compactStorageClause : new String[]{ "", " WITH COMPACT STORAGE" })
        {
            createTable("CREATE TABLE %s (k int PRIMARY KEY, c counter)" + compactStorageClause);

            // Flush 2 updates in different sstable so that the following select does a merge, which is what triggers
            // the problem from #11726

            execute("UPDATE %s SET c = c + ? WHERE k = ?", 1L, 0);

            flush();

            execute("UPDATE %s SET c = c + ? WHERE k = ?", 1L, 0);

            flush();

            // Querying, but not including the counter. Pre-CASSANDRA-11726, this made us query the counter but include
            // it's value, which broke at merge (post-CASSANDRA-11726 are special cases to never skip values).
            assertRows(execute("SELECT k FROM %s"), row(0));
        }
    }

    /*
     * Check that a counter batch works as intended
     */
    @Test
    public void testCounterBatch() throws Throwable
    {
        createTable("CREATE TABLE %s (userid int, url text, total counter, PRIMARY KEY (userid, url)) WITH COMPACT STORAGE");

        // Ensure we handle updates to the same CQL row in the same partition properly
        execute("BEGIN UNLOGGED BATCH " +
                "UPDATE %1$s SET total = total + 1 WHERE userid = 1 AND url = 'http://foo.com'; " +
                "UPDATE %1$s SET total = total + 1 WHERE userid = 1 AND url = 'http://foo.com'; " +
                "UPDATE %1$s SET total = total + 1 WHERE userid = 1 AND url = 'http://foo.com'; " +
                "APPLY BATCH; ");
        assertRows(execute("SELECT total FROM %s WHERE userid = 1 AND url = 'http://foo.com'"),
                   row(3L));

        // Ensure we handle different CQL rows in the same partition properly
        execute("BEGIN UNLOGGED BATCH " +
                "UPDATE %1$s SET total = total + 1 WHERE userid = 1 AND url = 'http://bar.com'; " +
                "UPDATE %1$s SET total = total + 1 WHERE userid = 1 AND url = 'http://baz.com'; " +
                "UPDATE %1$s SET total = total + 1 WHERE userid = 1 AND url = 'http://bad.com'; " +
                "APPLY BATCH; ");
        assertRows(execute("SELECT url, total FROM %s WHERE userid = 1"),
                   row("http://bad.com", 1L),
                   row("http://bar.com", 1L),
                   row("http://baz.com", 1L),
                   row("http://foo.com", 3L)); // from previous batch

        // Different counters in the same CQL Row
        createTable("CREATE TABLE %s (userid int, url text, first counter, second counter, third counter, PRIMARY KEY (userid, url))");
        execute("BEGIN UNLOGGED BATCH " +
                "UPDATE %1$s SET first = first + 1 WHERE userid = 1 AND url = 'http://foo.com'; " +
                "UPDATE %1$s SET first = first + 1 WHERE userid = 1 AND url = 'http://foo.com'; " +
                "UPDATE %1$s SET second = second + 1 WHERE userid = 1 AND url = 'http://foo.com'; " +
                "APPLY BATCH; ");
        assertRows(execute("SELECT first, second, third FROM %s WHERE userid = 1 AND url = 'http://foo.com'"),
                   row(2L, 1L, null));

        // Different counters in different CQL Rows
        execute("BEGIN UNLOGGED BATCH " +
                "UPDATE %1$s SET first = first + 1 WHERE userid = 1 AND url = 'http://bad.com'; " +
                "UPDATE %1$s SET first = first + 1, second = second + 1 WHERE userid = 1 AND url = 'http://bar.com'; " +
                "UPDATE %1$s SET first = first - 1, second = second - 1 WHERE userid = 1 AND url = 'http://bar.com'; " +
                "UPDATE %1$s SET second = second + 1 WHERE userid = 1 AND url = 'http://baz.com'; " +
                "APPLY BATCH; ");
        assertRows(execute("SELECT url, first, second, third FROM %s WHERE userid = 1"),
                   row("http://bad.com", 1L, null, null),
                   row("http://bar.com", 0L, 0L, null),
                   row("http://baz.com", null, 1L, null),
                   row("http://foo.com", 2L, 1L, null)); // from previous batch


        // Different counters in different partitions
        execute("BEGIN UNLOGGED BATCH " +
                "UPDATE %1$s SET first = first + 1 WHERE userid = 2 AND url = 'http://bad.com'; " +
                "UPDATE %1$s SET first = first + 1, second = second + 1 WHERE userid = 3 AND url = 'http://bar.com'; " +
                "UPDATE %1$s SET first = first - 1, second = second - 1 WHERE userid = 4 AND url = 'http://bar.com'; " +
                "UPDATE %1$s SET second = second + 1 WHERE userid = 5 AND url = 'http://baz.com'; " +
                "APPLY BATCH; ");
        assertRowsIgnoringOrder(execute("SELECT userid, url, first, second, third FROM %s WHERE userid IN (2, 3, 4, 5)"),
                                row(2, "http://bad.com", 1L, null, null),
                                row(3, "http://bar.com", 1L, 1L, null),
                                row(4, "http://bar.com", -1L, -1L, null),
                                row(5, "http://baz.com", null, 1L, null));
    }

    /**
     * from FrozenCollectionsTest
     */

    @Test
    public void testClusteringKeyUsageSet() throws Throwable
    {
        testClusteringKeyUsage("set<int>",
                               set(),
                               set(1, 2, 3),
                               set(4, 5, 6),
                               set(7, 8, 9));
    }

    @Test
    public void testClusteringKeyUsageList() throws Throwable
    {
        testClusteringKeyUsage("list<int>",
                               list(),
                               list(1, 2, 3),
                               list(4, 5, 6),
                               list(7, 8, 9));
    }

    @Test
    public void testClusteringKeyUsageMap() throws Throwable
    {
        testClusteringKeyUsage("map<int, int>",
                               map(),
                               map(1, 10, 2, 20, 3, 30),
                               map(4, 40, 5, 50, 6, 60),
                               map(7, 70, 8, 80, 9, 90));
    }

    private void testClusteringKeyUsage(String type, Object v1, Object v2, Object v3, Object v4) throws Throwable
    {
        createTable(String.format("CREATE TABLE %%s (a int, b frozen<%s>, c int, PRIMARY KEY (a, b)) WITH COMPACT STORAGE",
                                  type));

        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, v1, 1);
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, v2, 1);
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, v3, 0);
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, v4, 0);

        // overwrite with an update
        execute("UPDATE %s SET c=? WHERE a=? AND b=?", 0, 0, v1);
        execute("UPDATE %s SET c=? WHERE a=? AND b=?", 0, 0, v2);

        assertRows(execute("SELECT * FROM %s"),
                   row(0, v1, 0),
                   row(0, v2, 0),
                   row(0, v3, 0),
                   row(0, v4, 0)
        );

        assertRows(execute("SELECT b FROM %s"),
                   row(v1),
                   row(v2),
                   row(v3),
                   row(v4)
        );

        assertRows(execute("SELECT * FROM %s LIMIT 2"),
                   row(0, v1, 0),
                   row(0, v2, 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b=?", 0, v3),
                   row(0, v3, 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b=?", 0, v1),
                   row(0, v1, 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b IN ?", 0, list(v3, v1)),
                   row(0, v1, 0),
                   row(0, v3, 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b > ?", 0, v3),
                   row(0, v4, 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b >= ?", 0, v3),
                   row(0, v3, 0),
                   row(0, v4, 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b < ?", 0, v3),
                   row(0, v1, 0),
                   row(0, v2, 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b <= ?", 0, v3),
                   row(0, v1, 0),
                   row(0, v2, 0),
                   row(0, v3, 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b > ? AND b <= ?", 0, v2, v3),
                   row(0, v3, 0)
        );

        execute("DELETE FROM %s WHERE a=? AND b=?", 0, v1);
        execute("DELETE FROM %s WHERE a=? AND b=?", 0, v3);
        assertRows(execute("SELECT * FROM %s"),
                   row(0, v2, 0),
                   row(0, v4, 0)
        );
    }

    @Test
    public void testNestedClusteringKeyUsage() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b frozen<map<set<int>, list<int>>>, c frozen<set<int>>, d int, PRIMARY KEY (a, b, c)) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, map(), set(), 0);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, map(set(), list(1, 2, 3)), set(), 0);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, map(set(1, 2, 3), list(1, 2, 3)), set(1, 2, 3), 0);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, map(set(4, 5, 6), list(1, 2, 3)), set(1, 2, 3), 0);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, map(set(7, 8, 9), list(1, 2, 3)), set(1, 2, 3), 0);

        assertRows(execute("SELECT * FROM %s"),
                   row(0, map(), set(), 0),
                   row(0, map(set(), list(1, 2, 3)), set(), 0),
                   row(0, map(set(1, 2, 3), list(1, 2, 3)), set(1, 2, 3), 0),
                   row(0, map(set(4, 5, 6), list(1, 2, 3)), set(1, 2, 3), 0),
                   row(0, map(set(7, 8, 9), list(1, 2, 3)), set(1, 2, 3), 0)
        );

        assertRows(execute("SELECT b FROM %s"),
                   row(map()),
                   row(map(set(), list(1, 2, 3))),
                   row(map(set(1, 2, 3), list(1, 2, 3))),
                   row(map(set(4, 5, 6), list(1, 2, 3))),
                   row(map(set(7, 8, 9), list(1, 2, 3)))
        );

        assertRows(execute("SELECT c FROM %s"),
                   row(set()),
                   row(set()),
                   row(set(1, 2, 3)),
                   row(set(1, 2, 3)),
                   row(set(1, 2, 3))
        );

        assertRows(execute("SELECT * FROM %s LIMIT 3"),
                   row(0, map(), set(), 0),
                   row(0, map(set(), list(1, 2, 3)), set(), 0),
                   row(0, map(set(1, 2, 3), list(1, 2, 3)), set(1, 2, 3), 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=0 ORDER BY b DESC LIMIT 4"),
                   row(0, map(set(7, 8, 9), list(1, 2, 3)), set(1, 2, 3), 0),
                   row(0, map(set(4, 5, 6), list(1, 2, 3)), set(1, 2, 3), 0),
                   row(0, map(set(1, 2, 3), list(1, 2, 3)), set(1, 2, 3), 0),
                   row(0, map(set(), list(1, 2, 3)), set(), 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b=?", 0, map()),
                   row(0, map(), set(), 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b=?", 0, map(set(), list(1, 2, 3))),
                   row(0, map(set(), list(1, 2, 3)), set(), 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b=?", 0, map(set(1, 2, 3), list(1, 2, 3))),
                   row(0, map(set(1, 2, 3), list(1, 2, 3)), set(1, 2, 3), 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b=? AND c=?", 0, map(set(), list(1, 2, 3)), set()),
                   row(0, map(set(), list(1, 2, 3)), set(), 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND (b, c) IN ?", 0, list(tuple(map(set(4, 5, 6), list(1, 2, 3)), set(1, 2, 3)),
                                                                                 tuple(map(), set()))),
                   row(0, map(), set(), 0),
                   row(0, map(set(4, 5, 6), list(1, 2, 3)), set(1, 2, 3), 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b > ?", 0, map(set(4, 5, 6), list(1, 2, 3))),
                   row(0, map(set(7, 8, 9), list(1, 2, 3)), set(1, 2, 3), 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b >= ?", 0, map(set(4, 5, 6), list(1, 2, 3))),
                   row(0, map(set(4, 5, 6), list(1, 2, 3)), set(1, 2, 3), 0),
                   row(0, map(set(7, 8, 9), list(1, 2, 3)), set(1, 2, 3), 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b < ?", 0, map(set(4, 5, 6), list(1, 2, 3))),
                   row(0, map(), set(), 0),
                   row(0, map(set(), list(1, 2, 3)), set(), 0),
                   row(0, map(set(1, 2, 3), list(1, 2, 3)), set(1, 2, 3), 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b <= ?", 0, map(set(4, 5, 6), list(1, 2, 3))),
                   row(0, map(), set(), 0),
                   row(0, map(set(), list(1, 2, 3)), set(), 0),
                   row(0, map(set(1, 2, 3), list(1, 2, 3)), set(1, 2, 3), 0),
                   row(0, map(set(4, 5, 6), list(1, 2, 3)), set(1, 2, 3), 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b > ? AND b <= ?", 0, map(set(1, 2, 3), list(1, 2, 3)), map(set(4, 5, 6), list(1, 2, 3))),
                   row(0, map(set(4, 5, 6), list(1, 2, 3)), set(1, 2, 3), 0)
        );

        execute("DELETE FROM %s WHERE a=? AND b=? AND c=?", 0, map(), set());
        assertEmpty(execute("SELECT * FROM %s WHERE a=? AND b=? AND c=?", 0, map(), set()));

        execute("DELETE FROM %s WHERE a=? AND b=? AND c=?", 0, map(set(), list(1, 2, 3)), set());
        assertEmpty(execute("SELECT * FROM %s WHERE a=? AND b=? AND c=?", 0, map(set(), list(1, 2, 3)), set()));

        execute("DELETE FROM %s WHERE a=? AND b=? AND c=?", 0, map(set(4, 5, 6), list(1, 2, 3)), set(1, 2, 3));
        assertEmpty(execute("SELECT * FROM %s WHERE a=? AND b=? AND c=?", 0, map(set(4, 5, 6), list(1, 2, 3)), set(1, 2, 3)));

        assertRows(execute("SELECT * FROM %s"),
                   row(0, map(set(1, 2, 3), list(1, 2, 3)), set(1, 2, 3), 0),
                   row(0, map(set(7, 8, 9), list(1, 2, 3)), set(1, 2, 3), 0)
        );
    }

    @Test
    public void testNestedClusteringKeyUsageWithReverseOrder() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b frozen<map<set<int>, list<int>>>, c frozen<set<int>>, d int, " +
                    "PRIMARY KEY (a, b, c)) WITH COMPACT STORAGE AND CLUSTERING ORDER BY (b DESC)");

        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, map(), set(), 0);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, map(set(), list(1, 2, 3)), set(), 0);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, map(set(1, 2, 3), list(1, 2, 3)), set(1, 2, 3), 0);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, map(set(4, 5, 6), list(1, 2, 3)), set(1, 2, 3), 0);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, map(set(7, 8, 9), list(1, 2, 3)), set(1, 2, 3), 0);

        assertRows(execute("SELECT * FROM %s"),
                   row(0, map(set(7, 8, 9), list(1, 2, 3)), set(1, 2, 3), 0),
                   row(0, map(set(4, 5, 6), list(1, 2, 3)), set(1, 2, 3), 0),
                   row(0, map(set(1, 2, 3), list(1, 2, 3)), set(1, 2, 3), 0),
                   row(0, map(set(), list(1, 2, 3)), set(), 0),
                   row(0, map(), set(), 0)
        );

        assertRows(execute("SELECT b FROM %s"),
                   row(map(set(7, 8, 9), list(1, 2, 3))),
                   row(map(set(4, 5, 6), list(1, 2, 3))),
                   row(map(set(1, 2, 3), list(1, 2, 3))),
                   row(map(set(), list(1, 2, 3))),
                   row(map())
        );

        assertRows(execute("SELECT c FROM %s"),
                   row(set(1, 2, 3)),
                   row(set(1, 2, 3)),
                   row(set(1, 2, 3)),
                   row(set()),
                   row(set())
        );

        assertRows(execute("SELECT * FROM %s LIMIT 3"),
                   row(0, map(set(7, 8, 9), list(1, 2, 3)), set(1, 2, 3), 0),
                   row(0, map(set(4, 5, 6), list(1, 2, 3)), set(1, 2, 3), 0),
                   row(0, map(set(1, 2, 3), list(1, 2, 3)), set(1, 2, 3), 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=0 ORDER BY b DESC LIMIT 4"),
                   row(0, map(set(7, 8, 9), list(1, 2, 3)), set(1, 2, 3), 0),
                   row(0, map(set(4, 5, 6), list(1, 2, 3)), set(1, 2, 3), 0),
                   row(0, map(set(1, 2, 3), list(1, 2, 3)), set(1, 2, 3), 0),
                   row(0, map(set(), list(1, 2, 3)), set(), 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b=?", 0, map()),
                   row(0, map(), set(), 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b=?", 0, map(set(), list(1, 2, 3))),
                   row(0, map(set(), list(1, 2, 3)), set(), 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b=?", 0, map(set(1, 2, 3), list(1, 2, 3))),
                   row(0, map(set(1, 2, 3), list(1, 2, 3)), set(1, 2, 3), 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b=? AND c=?", 0, map(set(), list(1, 2, 3)), set()),
                   row(0, map(set(), list(1, 2, 3)), set(), 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND (b, c) IN ?", 0, list(tuple(map(set(4, 5, 6), list(1, 2, 3)), set(1, 2, 3)),
                                                                                 tuple(map(), set()))),
                   row(0, map(set(4, 5, 6), list(1, 2, 3)), set(1, 2, 3), 0),
                   row(0, map(), set(), 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b > ?", 0, map(set(4, 5, 6), list(1, 2, 3))),
                   row(0, map(set(7, 8, 9), list(1, 2, 3)), set(1, 2, 3), 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b >= ?", 0, map(set(4, 5, 6), list(1, 2, 3))),
                   row(0, map(set(7, 8, 9), list(1, 2, 3)), set(1, 2, 3), 0),
                   row(0, map(set(4, 5, 6), list(1, 2, 3)), set(1, 2, 3), 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b < ?", 0, map(set(4, 5, 6), list(1, 2, 3))),
                   row(0, map(set(1, 2, 3), list(1, 2, 3)), set(1, 2, 3), 0),
                   row(0, map(set(), list(1, 2, 3)), set(), 0),
                   row(0, map(), set(), 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b <= ?", 0, map(set(4, 5, 6), list(1, 2, 3))),
                   row(0, map(set(4, 5, 6), list(1, 2, 3)), set(1, 2, 3), 0),
                   row(0, map(set(1, 2, 3), list(1, 2, 3)), set(1, 2, 3), 0),
                   row(0, map(set(), list(1, 2, 3)), set(), 0),
                   row(0, map(), set(), 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b > ? AND b <= ?", 0, map(set(1, 2, 3), list(1, 2, 3)), map(set(4, 5, 6), list(1, 2, 3))),
                   row(0, map(set(4, 5, 6), list(1, 2, 3)), set(1, 2, 3), 0)
        );

        execute("DELETE FROM %s WHERE a=? AND b=? AND c=?", 0, map(), set());
        assertEmpty(execute("SELECT * FROM %s WHERE a=? AND b=? AND c=?", 0, map(), set()));

        execute("DELETE FROM %s WHERE a=? AND b=? AND c=?", 0, map(set(), list(1, 2, 3)), set());
        assertEmpty(execute("SELECT * FROM %s WHERE a=? AND b=? AND c=?", 0, map(set(), list(1, 2, 3)), set()));

        execute("DELETE FROM %s WHERE a=? AND b=? AND c=?", 0, map(set(4, 5, 6), list(1, 2, 3)), set(1, 2, 3));
        assertEmpty(execute("SELECT * FROM %s WHERE a=? AND b=? AND c=?", 0, map(set(4, 5, 6), list(1, 2, 3)), set(1, 2, 3)));

        assertRows(execute("SELECT * FROM %s"),
                   row(0, map(set(7, 8, 9), list(1, 2, 3)), set(1, 2, 3), 0),
                   row(0, map(set(1, 2, 3), list(1, 2, 3)), set(1, 2, 3), 0)
        );
    }

    @Test
    public void testNormalColumnUsage() throws Throwable
    {
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b frozen<map<set<int>, list<int>>>, c frozen<set<int>>)" + compactOption);

        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, map(), set());
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 1, map(set(), list(99999, 999999, 99999)), set());
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 2, map(set(1, 2, 3), list(1, 2, 3)), set(1, 2, 3));
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 3, map(set(4, 5, 6), list(1, 2, 3)), set(1, 2, 3));
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 4, map(set(7, 8, 9), list(1, 2, 3)), set(1, 2, 3));

        // overwrite with update
        execute("UPDATE %s SET b=? WHERE a=?", map(set(), list(1, 2, 3)), 1);

        assertRowsIgnoringOrder(execute("SELECT * FROM %s"),
                                row(0, map(), set()),
                                row(1, map(set(), list(1, 2, 3)), set()),
                                row(2, map(set(1, 2, 3), list(1, 2, 3)), set(1, 2, 3)),
                                row(3, map(set(4, 5, 6), list(1, 2, 3)), set(1, 2, 3)),
                                row(4, map(set(7, 8, 9), list(1, 2, 3)), set(1, 2, 3))
        );

        assertRowsIgnoringOrder(execute("SELECT b FROM %s"),
                                row(map()),
                                row(map(set(), list(1, 2, 3))),
                                row(map(set(1, 2, 3), list(1, 2, 3))),
                                row(map(set(4, 5, 6), list(1, 2, 3))),
                                row(map(set(7, 8, 9), list(1, 2, 3)))
        );

        assertRowsIgnoringOrder(execute("SELECT c FROM %s"),
                                row(set()),
                                row(set()),
                                row(set(1, 2, 3)),
                                row(set(1, 2, 3)),
                                row(set(1, 2, 3))
        );

        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE a=?", 3),
                                row(3, map(set(4, 5, 6), list(1, 2, 3)), set(1, 2, 3))
        );

        execute("UPDATE %s SET b=? WHERE a=?", null, 1);
        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE a=?", 1),
                                row(1, null, set())
        );

        execute("UPDATE %s SET b=? WHERE a=?", map(), 1);
        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE a=?", 1),
                                row(1, map(), set())
        );

        execute("UPDATE %s SET c=? WHERE a=?", null, 2);
        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE a=?", 2),
                                row(2, map(set(1, 2, 3), list(1, 2, 3)), null)
        );

        execute("UPDATE %s SET c=? WHERE a=?", set(), 2);
        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE a=?", 2),
                                row(2, map(set(1, 2, 3), list(1, 2, 3)), set())
        );

        execute("DELETE b FROM %s WHERE a=?", 3);
        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE a=?", 3),
                                row(3, null, set(1, 2, 3))
        );

        execute("DELETE c FROM %s WHERE a=?", 4);
        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE a=?", 4),
                                row(4, map(set(7, 8, 9), list(1, 2, 3)), null)
        );
    }

    /**
     * from SecondaryIndexTest
     */
    @Test
    public void testCompactTableWithValueOver64k() throws Throwable
    {
        createTable("CREATE TABLE %s(a int, b blob, PRIMARY KEY (a)) WITH COMPACT STORAGE");
        createIndex("CREATE INDEX ON %s(b)");
        failInsert("INSERT INTO %s (a, b) VALUES (0, ?)", ByteBuffer.allocate(SecondaryIndexTest.TOO_BIG));
        failInsert("INSERT INTO %s (a, b) VALUES (0, ?) IF NOT EXISTS", ByteBuffer.allocate(SecondaryIndexTest.TOO_BIG));
        failInsert("BEGIN BATCH\n" +
                   "INSERT INTO %s (a, b) VALUES (0, ?);\n" +
                   "APPLY BATCH",
                   ByteBuffer.allocate(SecondaryIndexTest.TOO_BIG));
        failInsert("BEGIN BATCH\n" +
                   "INSERT INTO %s (a, b) VALUES (0, ?) IF NOT EXISTS;\n" +
                   "APPLY BATCH",
                   ByteBuffer.allocate(SecondaryIndexTest.TOO_BIG));
    }

    public void failInsert(String insertCQL, Object... args) throws Throwable
    {
        try
        {
            execute(insertCQL, args);
            fail("Expected statement to fail validation");
        }
        catch (Exception e)
        {
            // as expected
        }
    }

    /**
     * Migrated from cql_tests.py:TestCQL.invalid_clustering_indexing_test()
     */
    @Test
    public void testIndexesOnClusteringInvalid() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY ((a, b))) WITH COMPACT STORAGE");
        assertInvalid("CREATE INDEX ON %s (a)");
        assertInvalid("CREATE INDEX ON %s (b)");

        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a, b)) WITH COMPACT STORAGE");
        assertInvalid("CREATE INDEX ON %s (a)");
        assertInvalid("CREATE INDEX ON %s (b)");
        assertInvalid("CREATE INDEX ON %s (c)");
    }

    @Test
    public void testEmptyRestrictionValueWithSecondaryIndexAndCompactTables() throws Throwable
    {
        createTable("CREATE TABLE %s (pk blob, c blob, v blob, PRIMARY KEY ((pk), c)) WITH COMPACT STORAGE");
        assertInvalidMessage("Secondary indexes are not supported on PRIMARY KEY columns in COMPACT STORAGE tables",
                             "CREATE INDEX on %s(c)");

        createTable("CREATE TABLE %s (pk blob PRIMARY KEY, v blob) WITH COMPACT STORAGE");
        createIndex("CREATE INDEX on %s(v)");

        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", bytes("foo123"), bytes("1"));

        // Test restrictions on non-primary key value
        assertEmpty(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND v = textAsBlob('');"));

        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", bytes("foo124"), EMPTY_BYTE_BUFFER);

        assertRows(execute("SELECT * FROM %s WHERE v = textAsBlob('');"),
                   row(bytes("foo124"), EMPTY_BYTE_BUFFER));
    }

    @Test
    public void testIndicesOnCompactTable() throws Throwable
    {
        assertInvalidMessage("COMPACT STORAGE with composite PRIMARY KEY allows no more than one column not part of the PRIMARY KEY (got: v1, v2)",
                             "CREATE TABLE " + KEYSPACE + ".test (pk int, c int, v1 int, v2 int, PRIMARY KEY(pk, c)) WITH COMPACT STORAGE");

        createTable("CREATE TABLE %s (pk int, c int, v int, PRIMARY KEY(pk, c)) WITH COMPACT STORAGE");
        assertInvalidMessage("Secondary indexes are not supported on compact value column of COMPACT STORAGE tables",
                             "CREATE INDEX ON %s(v)");

        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v int) WITH COMPACT STORAGE");
        createIndex("CREATE INDEX ON %s(v)");

        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 1, 1);
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 2, 1);
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 3, 3);

        assertRows(execute("SELECT pk, v FROM %s WHERE v = 1"),
                   row(1, 1),
                   row(2, 1));

        assertRows(execute("SELECT pk, v FROM %s WHERE v = 3"),
                   row(3, 3));

        assertEmpty(execute("SELECT pk, v FROM %s WHERE v = 5"));

        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v1 int, v2 int) WITH COMPACT STORAGE");
        createIndex("CREATE INDEX ON %s(v1)");

        execute("INSERT INTO %s (pk, v1, v2) VALUES (?, ?, ?)", 1, 1, 1);
        execute("INSERT INTO %s (pk, v1, v2) VALUES (?, ?, ?)", 2, 1, 2);
        execute("INSERT INTO %s (pk, v1, v2) VALUES (?, ?, ?)", 3, 3, 3);

        assertRows(execute("SELECT pk, v2 FROM %s WHERE v1 = 1"),
                   row(1, 1),
                   row(2, 2));

        assertRows(execute("SELECT pk, v2 FROM %s WHERE v1 = 3"),
                   row(3, 3));

        assertEmpty(execute("SELECT pk, v2 FROM %s WHERE v1 = 5"));
    }

    /**
     * OverflowTest
     */

    /**
     * Test regression from #5189,
     * migrated from cql_tests.py:TestCQL.compact_metadata_test()
     */
    @Test
    public void testCompactMetadata() throws Throwable
    {
        createTable("CREATE TABLE %s (id int primary key, i int ) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (id, i) VALUES (1, 2)");
        assertRows(execute("SELECT * FROM %s"),
                   row(1, 2));
    }

    @Test
    public void testEmpty() throws Throwable
    {
        // Same test, but for compact
        createTable("CREATE TABLE %s (k1 int, k2 int, v int, PRIMARY KEY (k1, k2)) WITH COMPACT STORAGE");

        // Inserts a few rows to make sure we don 't actually query something
        Object[][] rows = fill();

        assertEmpty(execute("SELECT v FROM %s WHERE k1 IN ()"));
        assertEmpty(execute("SELECT v FROM %s WHERE k1 = 0 AND k2 IN ()"));

        // Test empty IN() in DELETE
        execute("DELETE FROM %s WHERE k1 IN ()");
        assertArrayEquals(rows, getRows(execute("SELECT * FROM %s")));

        // Test empty IN() in UPDATE
        execute("UPDATE %s SET v = 3 WHERE k1 IN () AND k2 = 2");
        assertArrayEquals(rows, getRows(execute("SELECT * FROM %s")));
    }

    private Object[][] fill() throws Throwable
    {
        for (int i = 0; i < 2; i++)
            for (int j = 0; j < 2; j++)
                execute("INSERT INTO %s (k1, k2, v) VALUES (?, ?, ?)", i, j, i + j);

        return getRows(execute("SELECT * FROM %s"));
    }

    /**
     * AggregationTest
     */

    @Test
    public void testFunctionsWithCompactStorage() throws Throwable
    {
        createTable("CREATE TABLE %s (a int , b int, c double, primary key(a, b) ) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (a, b, c) VALUES (1, 1, 11.5)");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 2, 9.5)");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 3, 9.0)");

        assertRows(execute("SELECT max(b), min(b), sum(b), avg(b) , max(c), sum(c), avg(c) FROM %s"),
                   row(3, 1, 6, 2, 11.5, 30.0, 10.0));

        assertRows(execute("SELECT COUNT(*) FROM %s"), row(3L));
        assertRows(execute("SELECT COUNT(1) FROM %s"), row(3L));
        assertRows(execute("SELECT COUNT(*) FROM %s WHERE a = 1 AND b > 1"), row(2L));
        assertRows(execute("SELECT COUNT(1) FROM %s WHERE a = 1 AND b > 1"), row(2L));
        assertRows(execute("SELECT max(b), min(b), sum(b), avg(b) , max(c), sum(c), avg(c) FROM %s WHERE a = 1 AND b > 1"),
                   row(3, 2, 5, 2, 9.5, 18.5, 9.25));
    }

    /**
     * BatchTest
     */
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

    /**
     * CreateTest
     */
    /**
     * /**
     * Creation and basic operations on a static table with compact storage,
     * migrated from cql_tests.py:TestCQL.noncomposite_static_cf_test()
     */
    @Test
    public void testDenseStaticTable() throws Throwable
    {
        createTable("CREATE TABLE %s (userid uuid PRIMARY KEY, firstname text, lastname text, age int) WITH COMPACT STORAGE");

        UUID id1 = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
        UUID id2 = UUID.fromString("f47ac10b-58cc-4372-a567-0e02b2c3d479");

        execute("INSERT INTO %s (userid, firstname, lastname, age) VALUES (?, ?, ?, ?)", id1, "Frodo", "Baggins", 32);
        execute("UPDATE %s SET firstname = ?, lastname = ?, age = ? WHERE userid = ?", "Samwise", "Gamgee", 33, id2);

        assertRows(execute("SELECT firstname, lastname FROM %s WHERE userid = ?", id1),
                   row("Frodo", "Baggins"));

        assertRows(execute("SELECT * FROM %s WHERE userid = ?", id1),
                   row(id1, 32, "Frodo", "Baggins"));

        assertRows(execute("SELECT * FROM %s"),
                   row(id2, 33, "Samwise", "Gamgee"),
                   row(id1, 32, "Frodo", "Baggins")
        );

        String batch = "BEGIN BATCH "
                       + "INSERT INTO %1$s (userid, age) VALUES (?, ?) "
                       + "UPDATE %1$s SET age = ? WHERE userid = ? "
                       + "DELETE firstname, lastname FROM %1$s WHERE userid = ? "
                       + "DELETE firstname, lastname FROM %1$s WHERE userid = ? "
                       + "APPLY BATCH";

        execute(batch, id1, 36, 37, id2, id1, id2);

        assertRows(execute("SELECT * FROM %s"),
                   row(id2, 37, null, null),
                   row(id1, 36, null, null));
    }

    /**
     * Creation and basic operations on a non-composite table with compact storage,
     * migrated from cql_tests.py:TestCQL.dynamic_cf_test()
     */
    @Test
    public void testDenseNonCompositeTable() throws Throwable
    {
        createTable("CREATE TABLE %s (userid uuid, url text, time bigint, PRIMARY KEY (userid, url)) WITH COMPACT STORAGE");

        UUID id1 = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
        UUID id2 = UUID.fromString("f47ac10b-58cc-4372-a567-0e02b2c3d479");
        UUID id3 = UUID.fromString("810e8500-e29b-41d4-a716-446655440000");

        execute("INSERT INTO %s (userid, url, time) VALUES (?, ?, ?)", id1, "http://foo.bar", 42L);
        execute("INSERT INTO %s (userid, url, time) VALUES (?, ?, ?)", id1, "http://foo-2.bar", 24L);
        execute("INSERT INTO %s (userid, url, time) VALUES (?, ?, ?)", id1, "http://bar.bar", 128L);
        execute("UPDATE %s SET time = 24 WHERE userid = ? and url = 'http://bar.foo'", id2);
        execute("UPDATE %s SET time = 12 WHERE userid IN (?, ?) and url = 'http://foo-3'", id2, id1);

        assertRows(execute("SELECT url, time FROM %s WHERE userid = ?", id1),
                   row("http://bar.bar", 128L),
                   row("http://foo-2.bar", 24L),
                   row("http://foo-3", 12L),
                   row("http://foo.bar", 42L));

        assertRows(execute("SELECT * FROM %s WHERE userid = ?", id2),
                   row(id2, "http://bar.foo", 24L),
                   row(id2, "http://foo-3", 12L));

        assertRows(execute("SELECT time FROM %s"),
                   row(24L), // id2
                   row(12L),
                   row(128L), // id1
                   row(24L),
                   row(12L),
                   row(42L)
        );

        // Check we don't allow empty values for url since this is the full underlying cell name (#6152)
        assertInvalid("INSERT INTO %s (userid, url, time) VALUES (?, '', 42)", id3);
    }

    /**
     * Creation and basic operations on a composite table with compact storage,
     * migrated from cql_tests.py:TestCQL.dense_cf_test()
     */
    @Test
    public void testDenseCompositeTable() throws Throwable
    {
        createTable("CREATE TABLE %s (userid uuid, ip text, port int, time bigint, PRIMARY KEY (userid, ip, port)) WITH COMPACT STORAGE");

        UUID id1 = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
        UUID id2 = UUID.fromString("f47ac10b-58cc-4372-a567-0e02b2c3d479");

        execute("INSERT INTO %s (userid, ip, port, time) VALUES (?, '192.168.0.1', 80, 42)", id1);
        execute("INSERT INTO %s (userid, ip, port, time) VALUES (?, '192.168.0.2', 80, 24)", id1);
        execute("INSERT INTO %s (userid, ip, port, time) VALUES (?, '192.168.0.2', 90, 42)", id1);
        execute("UPDATE %s SET time = 24 WHERE userid = ? AND ip = '192.168.0.2' AND port = 80", id2);

        // we don't have to include all of the clustering columns (see CASSANDRA-7990)
        execute("INSERT INTO %s (userid, ip, time) VALUES (?, '192.168.0.3', 42)", id2);
        execute("UPDATE %s SET time = 42 WHERE userid = ? AND ip = '192.168.0.4'", id2);

        assertRows(execute("SELECT ip, port, time FROM %s WHERE userid = ?", id1),
                   row("192.168.0.1", 80, 42L),
                   row("192.168.0.2", 80, 24L),
                   row("192.168.0.2", 90, 42L));

        assertRows(execute("SELECT ip, port, time FROM %s WHERE userid = ? and ip >= '192.168.0.2'", id1),
                   row("192.168.0.2", 80, 24L),
                   row("192.168.0.2", 90, 42L));

        assertRows(execute("SELECT ip, port, time FROM %s WHERE userid = ? and ip = '192.168.0.2'", id1),
                   row("192.168.0.2", 80, 24L),
                   row("192.168.0.2", 90, 42L));

        assertEmpty(execute("SELECT ip, port, time FROM %s WHERE userid = ? and ip > '192.168.0.2'", id1));

        assertRows(execute("SELECT ip, port, time FROM %s WHERE userid = ? AND ip = '192.168.0.3'", id2),
                   row("192.168.0.3", null, 42L));

        assertRows(execute("SELECT ip, port, time FROM %s WHERE userid = ? AND ip = '192.168.0.4'", id2),
                   row("192.168.0.4", null, 42L));

        execute("DELETE time FROM %s WHERE userid = ? AND ip = '192.168.0.2' AND port = 80", id1);

        assertRowCount(execute("SELECT * FROM %s WHERE userid = ?", id1), 2);

        execute("DELETE FROM %s WHERE userid = ?", id1);
        assertEmpty(execute("SELECT * FROM %s WHERE userid = ?", id1));

        execute("DELETE FROM %s WHERE userid = ? AND ip = '192.168.0.3'", id2);
        assertEmpty(execute("SELECT * FROM %s WHERE userid = ? AND ip = '192.168.0.3'", id2));
    }

    @Test
    public void testCreateIndexOnCompactTableWithClusteringColumns() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int , c int, PRIMARY KEY (a, b)) WITH COMPACT STORAGE;");

        assertInvalidMessage("Secondary indexes are not supported on PRIMARY KEY columns in COMPACT STORAGE tables",
                             "CREATE INDEX ON %s (a);");

        assertInvalidMessage("Secondary indexes are not supported on PRIMARY KEY columns in COMPACT STORAGE tables",
                             "CREATE INDEX ON %s (b);");

        assertInvalidMessage("Secondary indexes are not supported on compact value column of COMPACT STORAGE tables",
                             "CREATE INDEX ON %s (c);");
    }

    @Test
    public void testCreateIndexOnCompactTableWithoutClusteringColumns() throws Throwable
    {
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b int) WITH COMPACT STORAGE;");

        assertInvalidMessage("Secondary indexes are not supported on PRIMARY KEY columns in COMPACT STORAGE tables",
                             "CREATE INDEX ON %s (a);");

        createIndex("CREATE INDEX ON %s (b);");

        execute("INSERT INTO %s (a, b) values (1, 1)");
        execute("INSERT INTO %s (a, b) values (2, 4)");
        execute("INSERT INTO %s (a, b) values (3, 6)");

        assertRows(execute("SELECT * FROM %s WHERE b = ?", 4), row(2, 4));
    }

    /**
     * DeleteTest
     */

    @Test
    public void testDeleteWithNoClusteringColumns() throws Throwable
    {
        testDeleteWithNoClusteringColumns(false);
        testDeleteWithNoClusteringColumns(true);
    }

    private void testDeleteWithNoClusteringColumns(boolean forceFlush) throws Throwable
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

        assertEmpty(execute("SELECT * FROM %s WHERE partitionKey = ?", 0));

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

        // Undefined column names
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


    @Test
    public void testDeleteWithOneClusteringColumns() throws Throwable
    {
        testDeleteWithOneClusteringColumns(false);
        testDeleteWithOneClusteringColumns(true);
    }

    private void testDeleteWithOneClusteringColumns(boolean forceFlush) throws Throwable
    {
        String compactOption = " WITH COMPACT STORAGE";

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

        assertEmpty(execute("SELECT * FROM %s WHERE partitionKey = ? AND clustering = ?", 0, 1));

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

        // Undefined column names
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

    @Test
    public void testDeleteWithTwoClusteringColumns() throws Throwable
    {
        testDeleteWithTwoClusteringColumns(false);
        testDeleteWithTwoClusteringColumns(true);
    }

    private void testDeleteWithTwoClusteringColumns(boolean forceFlush) throws Throwable
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

        assertEmpty(execute("SELECT * FROM %s WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ?",
                            0, 1, 1));

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

        Object[][] rows = new Object[][]{ row(0, 0, 1, 1), row(0, 1, 2, 5) };

        execute("DELETE value FROM %s WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 IN (?, ?)", 0, 0, 2, 3);
        flush(forceFlush);
        assertRows(execute("SELECT * FROM %s WHERE partitionKey IN (?, ?)", 0, 1), rows);

        rows = new Object[][]{ row(0, 0, 1, 1) };

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

        // Undefined column names
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

    /**
     * InsertTest
     */

    @Test
    public void testInsertWithCompactFormat() throws Throwable
    {
        testInsertWithCompactFormat(false);
        testInsertWithCompactFormat(true);
    }

    private void testInsertWithCompactFormat(boolean forceFlush) throws Throwable
    {
        createTable("CREATE TABLE %s (partitionKey int," +
                    "clustering int," +
                    "value int," +
                    " PRIMARY KEY (partitionKey, clustering)) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (partitionKey, clustering, value) VALUES (0, 0, 0)");
        execute("INSERT INTO %s (partitionKey, clustering, value) VALUES (0, 1, 1)");
        flush(forceFlush);

        assertRows(execute("SELECT * FROM %s"),
                   row(0, 0, 0),
                   row(0, 1, 1));

        // Invalid Null values for the clustering key or the regular column
        assertInvalidMessage("Some clustering keys are missing: clustering",
                             "INSERT INTO %s (partitionKey, value) VALUES (0, 0)");
        assertInvalidMessage("Column value is mandatory for this COMPACT STORAGE table",
                             "INSERT INTO %s (partitionKey, clustering) VALUES (0, 0)");

        // Missing primary key columns
        assertInvalidMessage("Some partition key parts are missing: partitionkey",
                             "INSERT INTO %s (clustering, value) VALUES (0, 1)");

        // multiple time the same value
        assertInvalidMessage("The column names contains duplicates",
                             "INSERT INTO %s (partitionKey, clustering, value, value) VALUES (0, 0, 2, 2)");

        // multiple time same primary key element in WHERE clause
        assertInvalidMessage("The column names contains duplicates",
                             "INSERT INTO %s (partitionKey, clustering, clustering, value) VALUES (0, 0, 0, 2)");

        // Undefined column names
        assertInvalidMessage("Undefined column name clusteringx",
                             "INSERT INTO %s (partitionKey, clusteringx, value) VALUES (0, 0, 2)");

        assertInvalidMessage("Undefined column name valuex",
                             "INSERT INTO %s (partitionKey, clustering, valuex) VALUES (0, 0, 2)");
    }

    @Test
    public void testInsertWithCompactStorageAndTwoClusteringColumns() throws Throwable
    {
        testInsertWithCompactStorageAndTwoClusteringColumns(false);
        testInsertWithCompactStorageAndTwoClusteringColumns(true);
    }

    private void testInsertWithCompactStorageAndTwoClusteringColumns(boolean forceFlush) throws Throwable
    {
        createTable("CREATE TABLE %s (partitionKey int," +
                    "clustering_1 int," +
                    "clustering_2 int," +
                    "value int," +
                    " PRIMARY KEY (partitionKey, clustering_1, clustering_2)) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (partitionKey, clustering_1, value) VALUES (0, 0, 0)");
        execute("INSERT INTO %s (partitionKey, clustering_1, clustering_2, value) VALUES (0, 0, 0, 0)");
        execute("INSERT INTO %s (partitionKey, clustering_1, clustering_2, value) VALUES (0, 0, 1, 1)");
        flush(forceFlush);

        assertRows(execute("SELECT * FROM %s"),
                   row(0, 0, null, 0),
                   row(0, 0, 0, 0),
                   row(0, 0, 1, 1));

        // Invalid Null values for the clustering key or the regular column
        assertInvalidMessage("PRIMARY KEY column \"clustering_2\" cannot be restricted as preceding column \"clustering_1\" is not restricted",
                             "INSERT INTO %s (partitionKey, clustering_2, value) VALUES (0, 0, 0)");
        assertInvalidMessage("Column value is mandatory for this COMPACT STORAGE table",
                             "INSERT INTO %s (partitionKey, clustering_1, clustering_2) VALUES (0, 0, 0)");

        // Missing primary key columns
        assertInvalidMessage("Some partition key parts are missing: partitionkey",
                             "INSERT INTO %s (clustering_1, clustering_2, value) VALUES (0, 0, 1)");
        assertInvalidMessage("PRIMARY KEY column \"clustering_2\" cannot be restricted as preceding column \"clustering_1\" is not restricted",
                             "INSERT INTO %s (partitionKey, clustering_2, value) VALUES (0, 0, 2)");

        // multiple time the same value
        assertInvalidMessage("The column names contains duplicates",
                             "INSERT INTO %s (partitionKey, clustering_1, value, clustering_2, value) VALUES (0, 0, 2, 0, 2)");

        // multiple time same primary key element in WHERE clause
        assertInvalidMessage("The column names contains duplicates",
                             "INSERT INTO %s (partitionKey, clustering_1, clustering_1, clustering_2, value) VALUES (0, 0, 0, 0, 2)");

        // Undefined column names
        assertInvalidMessage("Undefined column name clustering_1x",
                             "INSERT INTO %s (partitionKey, clustering_1x, clustering_2, value) VALUES (0, 0, 0, 2)");

        assertInvalidMessage("Undefined column name valuex",
                             "INSERT INTO %s (partitionKey, clustering_1, clustering_2, valuex) VALUES (0, 0, 0, 2)");
    }

    /**
     * InsertUpdateIfConditionTest
     */
    /**
     * Test for CAS with compact storage table, and #6813 in particular,
     * migrated from cql_tests.py:TestCQL.cas_and_compact_test()
     */
    @Test
    public void testCompactStorage() throws Throwable
    {
        createTable("CREATE TABLE %s (partition text, key text, owner text, PRIMARY KEY (partition, key) ) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (partition, key, owner) VALUES ('a', 'b', null)");
        assertRows(execute("UPDATE %s SET owner='z' WHERE partition='a' AND key='b' IF owner=null"), row(true));

        assertRows(execute("UPDATE %s SET owner='b' WHERE partition='a' AND key='b' IF owner='a'"), row(false, "z"));
        assertRows(execute("UPDATE %s SET owner='b' WHERE partition='a' AND key='b' IF owner='z'"), row(true));

        assertRows(execute("INSERT INTO %s (partition, key, owner) VALUES ('a', 'c', 'x') IF NOT EXISTS"), row(true));
    }

    /**
     * SelectGroupByTest
     */

    @Test
    public void testGroupByWithoutPaging() throws Throwable
    {

        createTable("CREATE TABLE %s (a int, b int, c int, d int, e int, primary key (a, b, c, d))"
                    + compactOption);

        execute("INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 1, 3, 6)");
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 2, 6, 12)");
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (1, 3, 2, 12, 24)");
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (1, 4, 2, 12, 24)");
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (1, 4, 2, 6, 12)");
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (2, 2, 3, 3, 6)");
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (2, 4, 3, 6, 12)");
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (3, 3, 2, 12, 24)");
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (4, 8, 2, 12, 24)");

        // Makes sure that we have some tombstones
        execute("DELETE FROM %s WHERE a = 1 AND b = 3 AND c = 2 AND d = 12");
        execute("DELETE FROM %s WHERE a = 3");

        // Range queries
        assertRows(execute("SELECT a, b, e, count(b), max(e) FROM %s GROUP BY a"),
                   row(1, 2, 6, 4L, 24),
                   row(2, 2, 6, 2L, 12),
                   row(4, 8, 24, 1L, 24));

        assertRows(execute("SELECT a, b, e, count(b), max(e) FROM %s GROUP BY a, b"),
                   row(1, 2, 6, 2L, 12),
                   row(1, 4, 12, 2L, 24),
                   row(2, 2, 6, 1L, 6),
                   row(2, 4, 12, 1L, 12),
                   row(4, 8, 24, 1L, 24));

        assertRows(execute("SELECT a, b, e, count(b), max(e) FROM %s WHERE b = 2 GROUP BY a, b ALLOW FILTERING"),
                   row(1, 2, 6, 2L, 12),
                   row(2, 2, 6, 1L, 6));

        assertEmpty(execute("SELECT a, b, e, count(b), max(e) FROM %s WHERE b IN () GROUP BY a, b ALLOW FILTERING"));

        // Range queries without aggregates
        assertRows(execute("SELECT a, b, c, d FROM %s GROUP BY a, b, c"),
                   row(1, 2, 1, 3),
                   row(1, 2, 2, 6),
                   row(1, 4, 2, 6),
                   row(2, 2, 3, 3),
                   row(2, 4, 3, 6),
                   row(4, 8, 2, 12));

        assertRows(execute("SELECT a, b, c, d FROM %s GROUP BY a, b"),
                   row(1, 2, 1, 3),
                   row(1, 4, 2, 6),
                   row(2, 2, 3, 3),
                   row(2, 4, 3, 6),
                   row(4, 8, 2, 12));

        // Range queries with wildcard
        assertRows(execute("SELECT * FROM %s GROUP BY a, b, c"),
                   row(1, 2, 1, 3, 6),
                   row(1, 2, 2, 6, 12),
                   row(1, 4, 2, 6, 12),
                   row(2, 2, 3, 3, 6),
                   row(2, 4, 3, 6, 12),
                   row(4, 8, 2, 12, 24));

        assertRows(execute("SELECT * FROM %s GROUP BY a, b"),
                   row(1, 2, 1, 3, 6),
                   row(1, 4, 2, 6, 12),
                   row(2, 2, 3, 3, 6),
                   row(2, 4, 3, 6, 12),
                   row(4, 8, 2, 12, 24));

        // Range query with LIMIT
        assertRows(execute("SELECT a, b, e, count(b), max(e) FROM %s GROUP BY a, b LIMIT 2"),
                   row(1, 2, 6, 2L, 12),
                   row(1, 4, 12, 2L, 24));

        // Range queries with PER PARTITION LIMIT
        assertRows(execute("SELECT a, b, e, count(b), max(e) FROM %s GROUP BY a, b PER PARTITION LIMIT 1"),
                   row(1, 2, 6, 2L, 12),
                   row(2, 2, 6, 1L, 6),
                   row(4, 8, 24, 1L, 24));

        assertRows(execute("SELECT a, b, e, count(b), max(e) FROM %s GROUP BY a PER PARTITION LIMIT 2"),
                   row(1, 2, 6, 4L, 24),
                   row(2, 2, 6, 2L, 12),
                   row(4, 8, 24, 1L, 24));

        // Range query with PER PARTITION LIMIT and LIMIT
        assertRows(execute("SELECT a, b, e, count(b), max(e) FROM %s GROUP BY a, b PER PARTITION LIMIT 1 LIMIT 2"),
                   row(1, 2, 6, 2L, 12),
                   row(2, 2, 6, 1L, 6));

        assertRows(execute("SELECT a, b, e, count(b), max(e) FROM %s GROUP BY a PER PARTITION LIMIT 2"),
                   row(1, 2, 6, 4L, 24),
                   row(2, 2, 6, 2L, 12),
                   row(4, 8, 24, 1L, 24));

        // Range queries without aggregates and with LIMIT
        assertRows(execute("SELECT a, b, c, d FROM %s GROUP BY a, b, c LIMIT 3"),
                   row(1, 2, 1, 3),
                   row(1, 2, 2, 6),
                   row(1, 4, 2, 6));

        assertRows(execute("SELECT a, b, c, d FROM %s GROUP BY a, b LIMIT 3"),
                   row(1, 2, 1, 3),
                   row(1, 4, 2, 6),
                   row(2, 2, 3, 3));

        // Range queries with wildcard and with LIMIT
        assertRows(execute("SELECT * FROM %s GROUP BY a, b, c LIMIT 3"),
                   row(1, 2, 1, 3, 6),
                   row(1, 2, 2, 6, 12),
                   row(1, 4, 2, 6, 12));

        assertRows(execute("SELECT * FROM %s GROUP BY a, b LIMIT 3"),
                   row(1, 2, 1, 3, 6),
                   row(1, 4, 2, 6, 12),
                   row(2, 2, 3, 3, 6));

        // Range queries without aggregates and with PER PARTITION LIMIT
        assertRows(execute("SELECT a, b, c, d FROM %s GROUP BY a, b, c PER PARTITION LIMIT 2"),
                   row(1, 2, 1, 3),
                   row(1, 2, 2, 6),
                   row(2, 2, 3, 3),
                   row(2, 4, 3, 6),
                   row(4, 8, 2, 12));

        assertRows(execute("SELECT a, b, c, d FROM %s GROUP BY a, b PER PARTITION LIMIT 1"),
                   row(1, 2, 1, 3),
                   row(2, 2, 3, 3),
                   row(4, 8, 2, 12));

        // Range queries with wildcard and with PER PARTITION LIMIT
        assertRows(execute("SELECT * FROM %s GROUP BY a, b, c PER PARTITION LIMIT 2"),
                   row(1, 2, 1, 3, 6),
                   row(1, 2, 2, 6, 12),
                   row(2, 2, 3, 3, 6),
                   row(2, 4, 3, 6, 12),
                   row(4, 8, 2, 12, 24));

        assertRows(execute("SELECT * FROM %s GROUP BY a, b PER PARTITION LIMIT 1"),
                   row(1, 2, 1, 3, 6),
                   row(2, 2, 3, 3, 6),
                   row(4, 8, 2, 12, 24));

        // Range queries without aggregates, with PER PARTITION LIMIT and LIMIT
        assertRows(execute("SELECT a, b, c, d FROM %s GROUP BY a, b, c PER PARTITION LIMIT 2 LIMIT 3"),
                   row(1, 2, 1, 3),
                   row(1, 2, 2, 6),
                   row(2, 2, 3, 3));

        // Range queries with wildcard, with PER PARTITION LIMIT and LIMIT
        assertRows(execute("SELECT * FROM %s GROUP BY a, b, c PER PARTITION LIMIT 2 LIMIT 3"),
                   row(1, 2, 1, 3, 6),
                   row(1, 2, 2, 6, 12),
                   row(2, 2, 3, 3, 6));

        // Range query with DISTINCT
        assertRows(execute("SELECT DISTINCT a, count(a)FROM %s GROUP BY a"),
                   row(1, 1L),
                   row(2, 1L),
                   row(4, 1L));

        assertInvalidMessage("Grouping on clustering columns is not allowed for SELECT DISTINCT queries",
                             "SELECT DISTINCT a, count(a)FROM %s GROUP BY a, b");

        // Range query with DISTINCT and LIMIT
        assertRows(execute("SELECT DISTINCT a, count(a)FROM %s GROUP BY a LIMIT 2"),
                   row(1, 1L),
                   row(2, 1L));

        assertInvalidMessage("Grouping on clustering columns is not allowed for SELECT DISTINCT queries",
                             "SELECT DISTINCT a, count(a)FROM %s GROUP BY a, b LIMIT 2");

        // Range query with ORDER BY
        assertInvalidMessage("ORDER BY is only supported when the partition key is restricted by an EQ or an IN",
                             "SELECT a, b, c, count(b), max(e) FROM %s GROUP BY a, b ORDER BY b DESC, c DESC");

        // Single partition queries
        assertRows(execute("SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 GROUP BY a, b, c"),
                   row(1, 2, 6, 1L, 6),
                   row(1, 2, 12, 1L, 12),
                   row(1, 4, 12, 2L, 24));

        assertRows(execute("SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 GROUP BY b, c"),
                   row(1, 2, 6, 1L, 6),
                   row(1, 2, 12, 1L, 12),
                   row(1, 4, 12, 2L, 24));

        assertRows(execute("SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 AND b = 2 GROUP BY a, b, c"),
                   row(1, 2, 6, 1L, 6),
                   row(1, 2, 12, 1L, 12));

        assertRows(execute("SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 AND b = 2 GROUP BY a, c"),
                   row(1, 2, 6, 1L, 6),
                   row(1, 2, 12, 1L, 12));

        assertRows(execute("SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 AND b = 2 GROUP BY c"),
                   row(1, 2, 6, 1L, 6),
                   row(1, 2, 12, 1L, 12));

        // Single partition queries without aggregates
        assertRows(execute("SELECT a, b, c, d FROM %s WHERE a = 1 GROUP BY a, b"),
                   row(1, 2, 1, 3),
                   row(1, 4, 2, 6));

        assertRows(execute("SELECT a, b, c, d FROM %s WHERE a = 1 GROUP BY a, b, c"),
                   row(1, 2, 1, 3),
                   row(1, 2, 2, 6),
                   row(1, 4, 2, 6));

        assertRows(execute("SELECT a, b, c, d FROM %s WHERE a = 1 GROUP BY b, c"),
                   row(1, 2, 1, 3),
                   row(1, 2, 2, 6),
                   row(1, 4, 2, 6));

        assertRows(execute("SELECT a, b, c, d FROM %s WHERE a = 1 and token(a) = token(1) GROUP BY b, c"),
                   row(1, 2, 1, 3),
                   row(1, 2, 2, 6),
                   row(1, 4, 2, 6));

        // Single partition queries with wildcard
        assertRows(execute("SELECT * FROM %s WHERE a = 1 GROUP BY a, b, c"),
                   row(1, 2, 1, 3, 6),
                   row(1, 2, 2, 6, 12),
                   row(1, 4, 2, 6, 12));

        assertRows(execute("SELECT * FROM %s WHERE a = 1 GROUP BY a, b"),
                   row(1, 2, 1, 3, 6),
                   row(1, 4, 2, 6, 12));

        // Single partition queries with DISTINCT
        assertRows(execute("SELECT DISTINCT a, count(a)FROM %s WHERE a = 1 GROUP BY a"),
                   row(1, 1L));

        assertInvalidMessage("Grouping on clustering columns is not allowed for SELECT DISTINCT queries",
                             "SELECT DISTINCT a, count(a)FROM %s WHERE a = 1 GROUP BY a, b");

        // Single partition queries with LIMIT
        assertRows(execute("SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 GROUP BY a, b, c LIMIT 10"),
                   row(1, 2, 6, 1L, 6),
                   row(1, 2, 12, 1L, 12),
                   row(1, 4, 12, 2L, 24));

        assertRows(execute("SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 GROUP BY a, b, c LIMIT 2"),
                   row(1, 2, 6, 1L, 6),
                   row(1, 2, 12, 1L, 12));

        assertRows(execute("SELECT count(b), max(e) FROM %s WHERE a = 1 GROUP BY a, b, c LIMIT 1"),
                   row(1L, 6));

        // Single partition queries with PER PARTITION LIMIT
        assertRows(execute("SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 GROUP BY a, b, c PER PARTITION LIMIT 10"),
                   row(1, 2, 6, 1L, 6),
                   row(1, 2, 12, 1L, 12),
                   row(1, 4, 12, 2L, 24));

        assertRows(execute("SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 GROUP BY a, b, c PER PARTITION LIMIT 2"),
                   row(1, 2, 6, 1L, 6),
                   row(1, 2, 12, 1L, 12));

        assertRows(execute("SELECT count(b), max(e) FROM %s WHERE a = 1 GROUP BY a, b, c PER PARTITION LIMIT 1"),
                   row(1L, 6));

        // Single partition queries without aggregates and with LIMIT
        assertRows(execute("SELECT a, b, c, d FROM %s WHERE a = 1 GROUP BY a, b LIMIT 2"),
                   row(1, 2, 1, 3),
                   row(1, 4, 2, 6));

        assertRows(execute("SELECT a, b, c, d FROM %s WHERE a = 1 GROUP BY a, b LIMIT 1"),
                   row(1, 2, 1, 3));

        assertRows(execute("SELECT a, b, c, d FROM %s WHERE a = 1 GROUP BY a, b, c LIMIT 2"),
                   row(1, 2, 1, 3),
                   row(1, 2, 2, 6));

        // Single partition queries with wildcard and with LIMIT
        assertRows(execute("SELECT * FROM %s WHERE a = 1 GROUP BY a, b, c LIMIT 2"),
                   row(1, 2, 1, 3, 6),
                   row(1, 2, 2, 6, 12));

        assertRows(execute("SELECT * FROM %s WHERE a = 1 GROUP BY a, b LIMIT 1"),
                   row(1, 2, 1, 3, 6));

        // Single partition queries without aggregates and with PER PARTITION LIMIT
        assertRows(execute("SELECT a, b, c, d FROM %s WHERE a = 1 GROUP BY a, b PER PARTITION LIMIT 2"),
                   row(1, 2, 1, 3),
                   row(1, 4, 2, 6));

        assertRows(execute("SELECT a, b, c, d FROM %s WHERE a = 1 GROUP BY a, b PER PARTITION LIMIT 1"),
                   row(1, 2, 1, 3));

        assertRows(execute("SELECT a, b, c, d FROM %s WHERE a = 1 GROUP BY a, b, c PER PARTITION LIMIT 2"),
                   row(1, 2, 1, 3),
                   row(1, 2, 2, 6));

        // Single partition queries with wildcard and with PER PARTITION LIMIT
        assertRows(execute("SELECT * FROM %s WHERE a = 1 GROUP BY a, b, c PER PARTITION LIMIT 2"),
                   row(1, 2, 1, 3, 6),
                   row(1, 2, 2, 6, 12));

        assertRows(execute("SELECT * FROM %s WHERE a = 1 GROUP BY a, b PER PARTITION LIMIT 1"),
                   row(1, 2, 1, 3, 6));

        // Single partition queries with ORDER BY
        assertRows(execute("SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 GROUP BY a, b, c ORDER BY b DESC, c DESC"),
                   row(1, 4, 24, 2L, 24),
                   row(1, 2, 12, 1L, 12),
                   row(1, 2, 6, 1L, 6));

        // Single partition queries with ORDER BY and PER PARTITION LIMIT
        assertRows(execute("SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 GROUP BY a, b, c ORDER BY b DESC, c DESC PER PARTITION LIMIT 1"),
                   row(1, 4, 24, 2L, 24));

        // Single partition queries with ORDER BY and LIMIT
        assertRows(execute("SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 GROUP BY a, b, c ORDER BY b DESC, c DESC LIMIT 2"),
                   row(1, 4, 24, 2L, 24),
                   row(1, 2, 12, 1L, 12));

        // Multi-partitions queries
        assertRows(execute("SELECT a, b, e, count(b), max(e) FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b, c"),
                   row(1, 2, 6, 1L, 6),
                   row(1, 2, 12, 1L, 12),
                   row(1, 4, 12, 2L, 24),
                   row(2, 2, 6, 1L, 6),
                   row(2, 4, 12, 1L, 12),
                   row(4, 8, 24, 1L, 24));

        assertRows(execute("SELECT a, b, e, count(b), max(e) FROM %s WHERE a IN (1, 2, 4) AND b = 2 GROUP BY a, b, c"),
                   row(1, 2, 6, 1L, 6),
                   row(1, 2, 12, 1L, 12),
                   row(2, 2, 6, 1L, 6));

        // Multi-partitions queries without aggregates
        assertRows(execute("SELECT a, b, c, d FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b"),
                   row(1, 2, 1, 3),
                   row(1, 4, 2, 6),
                   row(2, 2, 3, 3),
                   row(2, 4, 3, 6),
                   row(4, 8, 2, 12));

        assertRows(execute("SELECT a, b, c, d FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b, c"),
                   row(1, 2, 1, 3),
                   row(1, 2, 2, 6),
                   row(1, 4, 2, 6),
                   row(2, 2, 3, 3),
                   row(2, 4, 3, 6),
                   row(4, 8, 2, 12));

        // Multi-partitions with wildcard
        assertRows(execute("SELECT * FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b, c"),
                   row(1, 2, 1, 3, 6),
                   row(1, 2, 2, 6, 12),
                   row(1, 4, 2, 6, 12),
                   row(2, 2, 3, 3, 6),
                   row(2, 4, 3, 6, 12),
                   row(4, 8, 2, 12, 24));

        assertRows(execute("SELECT * FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b"),
                   row(1, 2, 1, 3, 6),
                   row(1, 4, 2, 6, 12),
                   row(2, 2, 3, 3, 6),
                   row(2, 4, 3, 6, 12),
                   row(4, 8, 2, 12, 24));

        // Multi-partitions query with DISTINCT
        assertRows(execute("SELECT DISTINCT a, count(a)FROM %s WHERE a IN (1, 2, 4) GROUP BY a"),
                   row(1, 1L),
                   row(2, 1L),
                   row(4, 1L));

        assertInvalidMessage("Grouping on clustering columns is not allowed for SELECT DISTINCT queries",
                             "SELECT DISTINCT a, count(a)FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b");

        // Multi-partitions query with DISTINCT and LIMIT
        assertRows(execute("SELECT DISTINCT a, count(a)FROM %s WHERE a IN (1, 2, 4) GROUP BY a LIMIT 2"),
                   row(1, 1L),
                   row(2, 1L));

        // Multi-partitions queries with PER PARTITION LIMIT
        assertRows(execute("SELECT a, b, e, count(b), max(e) FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b, c PER PARTITION LIMIT 1"),
                   row(1, 2, 6, 1L, 6),
                   row(2, 2, 6, 1L, 6),
                   row(4, 8, 24, 1L, 24));

        assertRows(execute("SELECT a, b, e, count(b), max(e) FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b, c PER PARTITION LIMIT 2"),
                   row(1, 2, 6, 1L, 6),
                   row(1, 2, 12, 1L, 12),
                   row(2, 2, 6, 1L, 6),
                   row(2, 4, 12, 1L, 12),
                   row(4, 8, 24, 1L, 24));

        // Multi-partitions with wildcard and PER PARTITION LIMIT
        assertRows(execute("SELECT * FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b, c PER PARTITION LIMIT 2"),
                   row(1, 2, 1, 3, 6),
                   row(1, 2, 2, 6, 12),
                   row(2, 2, 3, 3, 6),
                   row(2, 4, 3, 6, 12),
                   row(4, 8, 2, 12, 24));

        assertRows(execute("SELECT * FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b PER PARTITION LIMIT 1"),
                   row(1, 2, 1, 3, 6),
                   row(2, 2, 3, 3, 6),
                   row(4, 8, 2, 12, 24));

        // Multi-partitions queries with ORDER BY
        assertRows(execute("SELECT a, b, c, count(b), max(e) FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b ORDER BY b DESC, c DESC"),
                   row(4, 8, 2, 1L, 24),
                   row(2, 4, 3, 1L, 12),
                   row(1, 4, 2, 2L, 24),
                   row(2, 2, 3, 1L, 6),
                   row(1, 2, 2, 2L, 12));

        assertRows(execute("SELECT a, b, c, d FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b, c ORDER BY b DESC, c DESC"),
                   row(4, 8, 2, 12),
                   row(2, 4, 3, 6),
                   row(1, 4, 2, 12),
                   row(2, 2, 3, 3),
                   row(1, 2, 2, 6),
                   row(1, 2, 1, 3));

        // Multi-partitions queries with ORDER BY and LIMIT
        assertRows(execute("SELECT a, b, c, d FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b ORDER BY b DESC, c DESC LIMIT 3"),
                   row(4, 8, 2, 12),
                   row(2, 4, 3, 6),
                   row(1, 4, 2, 12));

        // Multi-partitions with wildcard, ORDER BY and LIMIT
        assertRows(execute("SELECT * FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b, c ORDER BY b DESC, c DESC LIMIT 3"),
                   row(4, 8, 2, 12, 24),
                   row(2, 4, 3, 6, 12),
                   row(1, 4, 2, 12, 24));

        // Invalid queries
        assertInvalidMessage("Group by is currently only supported on the columns of the PRIMARY KEY, got e",
                             "SELECT a, b, d, count(b), max(c) FROM %s WHERE a = 1 GROUP BY a, e");

        assertInvalidMessage("Group by currently only support groups of columns following their declared order in the PRIMARY KEY",
                             "SELECT a, b, d, count(b), max(c) FROM %s WHERE a = 1 GROUP BY c");

        assertInvalidMessage("Group by currently only support groups of columns following their declared order in the PRIMARY KEY",
                             "SELECT a, b, d, count(b), max(c) FROM %s WHERE a = 1 GROUP BY a, c, b");

        assertInvalidMessage("Group by currently only support groups of columns following their declared order in the PRIMARY KEY",
                             "SELECT a, b, d, count(b), max(c) FROM %s WHERE a = 1 GROUP BY a, a");

        assertInvalidMessage("Group by currently only support groups of columns following their declared order in the PRIMARY KEY",
                             "SELECT a, b, c, d FROM %s WHERE token(a) = token(1) GROUP BY b, c");

        assertInvalidMessage("Undefined column name clustering1",
                             "SELECT a, b as clustering1, max(c) FROM %s WHERE a = 1 GROUP BY a, clustering1");

        assertInvalidMessage("Undefined column name z",
                             "SELECT a, b, max(c) FROM %s WHERE a = 1 GROUP BY a, b, z");

        // Test with composite partition key
        createTable("CREATE TABLE %s (a int, b int, c int, d int, e int, primary key ((a, b), c, d))" + compactOption);

        execute("INSERT INTO %s (a, b, c, d, e) VALUES (1, 1, 1, 3, 6)");
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (1, 1, 2, 6, 12)");
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (1, 1, 3, 12, 24)");
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 1, 12, 24)");
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 2, 6, 12)");

        assertInvalidMessage("Group by is not supported on only a part of the partition key",
                             "SELECT a, b, max(d) FROM %s GROUP BY a");

        assertRows(execute("SELECT a, b, max(d) FROM %s GROUP BY a, b"),
                   row(1, 2, 12),
                   row(1, 1, 12));

        assertRows(execute("SELECT a, b, max(d) FROM %s WHERE a = 1 AND b = 1 GROUP BY b"),
                   row(1, 1, 12));

        // Test with table without clustering key
        createTable("CREATE TABLE %s (a int primary key, b int, c int)" + compactOption);

        execute("INSERT INTO %s (a, b, c) VALUES (1, 3, 6)");
        execute("INSERT INTO %s (a, b, c) VALUES (2, 6, 12)");
        execute("INSERT INTO %s (a, b, c) VALUES (3, 12, 24)");

        assertInvalidMessage("Group by currently only support groups of columns following their declared order in the PRIMARY KEY",
                             "SELECT a, max(c) FROM %s WHERE a = 1 GROUP BY a, a");
    }

    @Test
    public void testGroupByWithoutPagingWithDeletions() throws Throwable
    {

        createTable("CREATE TABLE %s (a int, b int, c int, d int, e int, primary key (a, b, c, d))"
                    + compactOption);

        execute("INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 1, 3, 6)");
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 1, 6, 12)");
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 1, 9, 18)");
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 1, 12, 24)");
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 2, 3, 6)");
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 2, 6, 12)");
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 2, 9, 18)");
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 2, 12, 24)");
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 3, 3, 6)");
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 3, 6, 12)");
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 3, 9, 18)");
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 3, 12, 24)");

        execute("DELETE FROM %s WHERE a = 1 AND b = 2 AND c = 1 AND d = 12");
        execute("DELETE FROM %s WHERE a = 1 AND b = 2 AND c = 2 AND d = 9");

        assertRows(execute("SELECT a, b, c, count(b), max(d) FROM %s GROUP BY a, b, c"),
                   row(1, 2, 1, 3L, 9),
                   row(1, 2, 2, 3L, 12),
                   row(1, 2, 3, 4L, 12));
    }

    @Test
    public void testGroupByWithRangeNamesQueryWithoutPaging() throws Throwable
    {

        createTable("CREATE TABLE %s (a int, b int, c int, d int, primary key (a, b, c))"
                    + compactOption);

        for (int i = 1; i < 5; i++)
            for (int j = 1; j < 5; j++)
                for (int k = 1; k < 5; k++)
                    execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", i, j, k, i + j);

        // Makes sure that we have some tombstones
        execute("DELETE FROM %s WHERE a = 3");

        // Range queries
        assertRows(execute("SELECT a, b, d, count(b), max(d) FROM %s WHERE b = 1 and c IN (1, 2) GROUP BY a ALLOW FILTERING"),
                   row(1, 1, 2, 2L, 2),
                   row(2, 1, 3, 2L, 3),
                   row(4, 1, 5, 2L, 5));

        assertRows(execute("SELECT a, b, d, count(b), max(d) FROM %s WHERE b = 1 and c IN (1, 2) GROUP BY a, b ALLOW FILTERING"),
                   row(1, 1, 2, 2L, 2),
                   row(2, 1, 3, 2L, 3),
                   row(4, 1, 5, 2L, 5));

        assertRows(execute("SELECT a, b, d, count(b), max(d) FROM %s WHERE b IN (1, 2) and c IN (1, 2) GROUP BY a, b ALLOW FILTERING"),
                   row(1, 1, 2, 2L, 2),
                   row(1, 2, 3, 2L, 3),
                   row(2, 1, 3, 2L, 3),
                   row(2, 2, 4, 2L, 4),
                   row(4, 1, 5, 2L, 5),
                   row(4, 2, 6, 2L, 6));

        // Range queries with LIMIT
        assertRows(execute("SELECT a, b, d, count(b), max(d) FROM %s WHERE b = 1 and c IN (1, 2) GROUP BY a LIMIT 5 ALLOW FILTERING"),
                   row(1, 1, 2, 2L, 2),
                   row(2, 1, 3, 2L, 3),
                   row(4, 1, 5, 2L, 5));

        assertRows(execute("SELECT a, b, d, count(b), max(d) FROM %s WHERE b = 1 and c IN (1, 2) GROUP BY a, b LIMIT 3 ALLOW FILTERING"),
                   row(1, 1, 2, 2L, 2),
                   row(2, 1, 3, 2L, 3),
                   row(4, 1, 5, 2L, 5));

        assertRows(execute("SELECT a, b, d, count(b), max(d) FROM %s WHERE b IN (1, 2) and c IN (1, 2) GROUP BY a, b LIMIT 3 ALLOW FILTERING"),
                   row(1, 1, 2, 2L, 2),
                   row(1, 2, 3, 2L, 3),
                   row(2, 1, 3, 2L, 3));

        // Range queries with PER PARTITION LIMIT
        assertRows(execute("SELECT a, b, d, count(b), max(d) FROM %s WHERE b = 1 and c IN (1, 2) GROUP BY a, b PER PARTITION LIMIT 2 ALLOW FILTERING"),
                   row(1, 1, 2, 2L, 2),
                   row(2, 1, 3, 2L, 3),
                   row(4, 1, 5, 2L, 5));

        assertRows(execute("SELECT a, b, d, count(b), max(d) FROM %s WHERE b IN (1, 2) and c IN (1, 2) GROUP BY a, b PER PARTITION LIMIT 1 ALLOW FILTERING"),
                   row(1, 1, 2, 2L, 2),
                   row(2, 1, 3, 2L, 3),
                   row(4, 1, 5, 2L, 5));

        // Range queries with PER PARTITION LIMIT and LIMIT
        assertRows(execute("SELECT a, b, d, count(b), max(d) FROM %s WHERE b = 1 and c IN (1, 2) GROUP BY a, b PER PARTITION LIMIT 2 LIMIT 5 ALLOW FILTERING"),
                   row(1, 1, 2, 2L, 2),
                   row(2, 1, 3, 2L, 3),
                   row(4, 1, 5, 2L, 5));

        assertRows(execute("SELECT a, b, d, count(b), max(d) FROM %s WHERE b IN (1, 2) and c IN (1, 2) GROUP BY a, b PER PARTITION LIMIT 1 LIMIT 2 ALLOW FILTERING"),
                   row(1, 1, 2, 2L, 2),
                   row(2, 1, 3, 2L, 3));
    }

    /**
     * SelectSingleColumn
     */
    @Test
    public void testClusteringColumnRelationsWithCompactStorage() throws Throwable
    {
        createTable("CREATE TABLE %s (a text, b int, c int, d int, primary key(a, b, c)) WITH COMPACT STORAGE;");
        execute("insert into %s (a, b, c, d) values (?, ?, ?, ?)", "first", 1, 5, 1);
        execute("insert into %s (a, b, c, d) values (?, ?, ?, ?)", "first", 2, 6, 2);
        execute("insert into %s (a, b, c, d) values (?, ?, ?, ?)", "first", 3, 7, 3);
        execute("insert into %s (a, b, c, d) values (?, ?, ?, ?)", "second", 4, 8, 4);

        assertRows(execute("select * from %s where a in (?, ?)", "first", "second"),
                   row("first", 1, 5, 1),
                   row("first", 2, 6, 2),
                   row("first", 3, 7, 3),
                   row("second", 4, 8, 4));

        assertRows(execute("select * from %s where a = ? and b = ? and c in (?, ?)", "first", 2, 6, 7),
                   row("first", 2, 6, 2));

        assertRows(execute("select * from %s where a = ? and b in (?, ?) and c in (?, ?)", "first", 2, 3, 6, 7),
                   row("first", 2, 6, 2),
                   row("first", 3, 7, 3));

        assertRows(execute("select * from %s where a = ? and b in (?, ?) and c in (?, ?)", "first", 3, 2, 7, 6),
                   row("first", 2, 6, 2),
                   row("first", 3, 7, 3));

        assertRows(execute("select * from %s where a = ? and c in (?, ?) and b in (?, ?)", "first", 7, 6, 3, 2),
                   row("first", 2, 6, 2),
                   row("first", 3, 7, 3));

        assertRows(execute("select c, d from %s where a = ? and c in (?, ?) and b in (?, ?)", "first", 7, 6, 3, 2),
                   row(6, 2),
                   row(7, 3));

        assertRows(execute("select c, d from %s where a = ? and c in (?, ?) and b in (?, ?, ?)", "first", 7, 6, 3, 2, 3),
                   row(6, 2),
                   row(7, 3));

        assertRows(execute("select * from %s where a = ? and b in (?, ?) and c = ?", "first", 3, 2, 7),
                   row("first", 3, 7, 3));

        assertRows(execute("select * from %s where a = ? and b in ? and c in ?",
                           "first", Arrays.asList(3, 2), Arrays.asList(7, 6)),
                   row("first", 2, 6, 2),
                   row("first", 3, 7, 3));

        assertInvalidMessage("Invalid null value for column b",
                             "select * from %s where a = ? and b in ? and c in ?", "first", null, Arrays.asList(7, 6));

        assertRows(execute("select * from %s where a = ? and c >= ? and b in (?, ?)", "first", 6, 3, 2),
                   row("first", 2, 6, 2),
                   row("first", 3, 7, 3));

        assertRows(execute("select * from %s where a = ? and c > ? and b in (?, ?)", "first", 6, 3, 2),
                   row("first", 3, 7, 3));

        assertRows(execute("select * from %s where a = ? and c <= ? and b in (?, ?)", "first", 6, 3, 2),
                   row("first", 2, 6, 2));

        assertRows(execute("select * from %s where a = ? and c < ? and b in (?, ?)", "first", 7, 3, 2),
                   row("first", 2, 6, 2));

        assertRows(execute("select * from %s where a = ? and c >= ? and c <= ? and b in (?, ?)", "first", 6, 7, 3, 2),
                   row("first", 2, 6, 2),
                   row("first", 3, 7, 3));

        assertRows(execute("select * from %s where a = ? and c > ? and c <= ? and b in (?, ?)", "first", 6, 7, 3, 2),
                   row("first", 3, 7, 3));

        assertEmpty(execute("select * from %s where a = ? and c > ? and c < ? and b in (?, ?)", "first", 6, 7, 3, 2));

        assertInvalidMessage("Column \"c\" cannot be restricted by both an equality and an inequality relation",
                             "select * from %s where a = ? and c > ? and c = ? and b in (?, ?)", "first", 6, 7, 3, 2);

        assertInvalidMessage("c cannot be restricted by more than one relation if it includes an Equal",
                             "select * from %s where a = ? and c = ? and c > ?  and b in (?, ?)", "first", 6, 7, 3, 2);

        assertRows(execute("select * from %s where a = ? and c in (?, ?) and b in (?, ?) order by b DESC",
                           "first", 7, 6, 3, 2),
                   row("first", 3, 7, 3),
                   row("first", 2, 6, 2));

        assertInvalidMessage("More than one restriction was found for the start bound on b",
                             "select * from %s where a = ? and b > ? and b > ?", "first", 6, 3, 2);

        assertInvalidMessage("More than one restriction was found for the end bound on b",
                             "select * from %s where a = ? and b < ? and b <= ?", "first", 6, 3, 2);
    }

    /**
     * SelectTest
     */
    /**
     * Check query with KEY IN clause for wide row tables
     * migrated from cql_tests.py:TestCQL.in_clause_wide_rows_test()
     */
    @Test
    public void testSelectKeyInForWideRows() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c)) WITH COMPACT STORAGE");

        for (int i = 0; i < 10; i++)
            execute("INSERT INTO %s (k, c, v) VALUES (0, ?, ?)", i, i);

        assertRows(execute("SELECT v FROM %s WHERE k = 0 AND c IN (5, 2, 8)"),
                   row(2), row(5), row(8));

        createTable("CREATE TABLE %s (k int, c1 int, c2 int, v int, PRIMARY KEY (k, c1, c2)) WITH COMPACT STORAGE");

        for (int i = 0; i < 10; i++)
            execute("INSERT INTO %s (k, c1, c2, v) VALUES (0, 0, ?, ?)", i, i);

        assertEmpty(execute("SELECT v FROM %s WHERE k = 0 AND c1 IN (5, 2, 8) AND c2 = 3"));

        assertRows(execute("SELECT v FROM %s WHERE k = 0 AND c1 = 0 AND c2 IN (5, 2, 8)"),
                   row(2), row(5), row(8));
    }

    /**
     * Check SELECT respects inclusive and exclusive bounds
     * migrated from cql_tests.py:TestCQL.exclusive_slice_test()
     */
    @Test
    public void testSelectBounds() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c)) WITH COMPACT STORAGE");

        for (int i = 0; i < 10; i++)
            execute("INSERT INTO %s (k, c, v) VALUES (0, ?, ?)", i, i);

        assertRowCount(execute("SELECT v FROM %s WHERE k = 0"), 10);

        assertRows(execute("SELECT v FROM %s WHERE k = 0 AND c >= 2 AND c <= 6"),
                   row(2), row(3), row(4), row(5), row(6));

        assertRows(execute("SELECT v FROM %s WHERE k = 0 AND c > 2 AND c <= 6"),
                   row(3), row(4), row(5), row(6));

        assertRows(execute("SELECT v FROM %s WHERE k = 0 AND c >= 2 AND c < 6"),
                   row(2), row(3), row(4), row(5));

        assertRows(execute("SELECT v FROM %s WHERE k = 0 AND c > 2 AND c < 6"),
                   row(3), row(4), row(5));

        assertRows(execute("SELECT v FROM %s WHERE k = 0 AND c > 2 AND c <= 6 LIMIT 2"),
                   row(3), row(4));

        assertRows(execute("SELECT v FROM %s WHERE k = 0 AND c >= 2 AND c < 6 ORDER BY c DESC LIMIT 2"),
                   row(5), row(4));
    }

    /**
     * Test for #4716 bug and more generally for good behavior of ordering,
     * migrated from cql_tests.py:TestCQL.reversed_compact_test()
     */
    @Test
    public void testReverseCompact() throws Throwable
    {
        createTable("CREATE TABLE %s ( k text, c int, v int, PRIMARY KEY (k, c) ) WITH COMPACT STORAGE AND CLUSTERING ORDER BY (c DESC)");

        for (int i = 0; i < 10; i++)
            execute("INSERT INTO %s (k, c, v) VALUES ('foo', ?, ?)", i, i);

        assertRows(execute("SELECT c FROM %s WHERE c > 2 AND c < 6 AND k = 'foo'"),
                   row(5), row(4), row(3));

        assertRows(execute("SELECT c FROM %s WHERE c >= 2 AND c <= 6 AND k = 'foo'"),
                   row(6), row(5), row(4), row(3), row(2));

        assertRows(execute("SELECT c FROM %s WHERE c > 2 AND c < 6 AND k = 'foo' ORDER BY c ASC"),
                   row(3), row(4), row(5));

        assertRows(execute("SELECT c FROM %s WHERE c >= 2 AND c <= 6 AND k = 'foo' ORDER BY c ASC"),
                   row(2), row(3), row(4), row(5), row(6));

        assertRows(execute("SELECT c FROM %s WHERE c > 2 AND c < 6 AND k = 'foo' ORDER BY c DESC"),
                   row(5), row(4), row(3));

        assertRows(execute("SELECT c FROM %s WHERE c >= 2 AND c <= 6 AND k = 'foo' ORDER BY c DESC"),
                   row(6), row(5), row(4), row(3), row(2));

        createTable("CREATE TABLE %s ( k text, c int, v int, PRIMARY KEY (k, c) ) WITH COMPACT STORAGE");

        for (int i = 0; i < 10; i++)
            execute("INSERT INTO %s(k, c, v) VALUES ('foo', ?, ?)", i, i);

        assertRows(execute("SELECT c FROM %s WHERE c > 2 AND c < 6 AND k = 'foo'"),
                   row(3), row(4), row(5));

        assertRows(execute("SELECT c FROM %s WHERE c >= 2 AND c <= 6 AND k = 'foo'"),
                   row(2), row(3), row(4), row(5), row(6));

        assertRows(execute("SELECT c FROM %s WHERE c > 2 AND c < 6 AND k = 'foo' ORDER BY c ASC"),
                   row(3), row(4), row(5));

        assertRows(execute("SELECT c FROM %s WHERE c >= 2 AND c <= 6 AND k = 'foo' ORDER BY c ASC"),
                   row(2), row(3), row(4), row(5), row(6));

        assertRows(execute("SELECT c FROM %s WHERE c > 2 AND c < 6 AND k = 'foo' ORDER BY c DESC"),
                   row(5), row(4), row(3));

        assertRows(execute("SELECT c FROM %s WHERE c >= 2 AND c <= 6 AND k = 'foo' ORDER BY c DESC"),
                   row(6), row(5), row(4), row(3), row(2));
    }

    /**
     * Test for the bug from #4760 and #4759,
     * migrated from cql_tests.py:TestCQL.reversed_compact_multikey_test()
     */
    @Test
    public void testReversedCompactMultikey() throws Throwable
    {
        createTable("CREATE TABLE %s (key text, c1 int, c2 int, value text, PRIMARY KEY(key, c1, c2) ) WITH COMPACT STORAGE AND CLUSTERING ORDER BY(c1 DESC, c2 DESC)");

        for (int i = 0; i < 3; i++)
            for (int j = 0; j < 3; j++)
                execute("INSERT INTO %s (key, c1, c2, value) VALUES ('foo', ?, ?, 'bar')", i, j);

        // Equalities
        assertRows(execute("SELECT c1, c2 FROM %s WHERE key='foo' AND c1 = 1"),
                   row(1, 2), row(1, 1), row(1, 0));

        assertRows(execute("SELECT c1, c2 FROM %s WHERE key='foo' AND c1 = 1 ORDER BY c1 ASC, c2 ASC"),
                   row(1, 0), row(1, 1), row(1, 2));

        assertRows(execute("SELECT c1, c2 FROM %s WHERE key='foo' AND c1 = 1 ORDER BY c1 DESC, c2 DESC"),
                   row(1, 2), row(1, 1), row(1, 0));

        // GT
        assertRows(execute("SELECT c1, c2 FROM %s WHERE key='foo' AND c1 > 1"),
                   row(2, 2), row(2, 1), row(2, 0));

        assertRows(execute("SELECT c1, c2 FROM %s WHERE key='foo' AND c1 > 1 ORDER BY c1 ASC, c2 ASC"),
                   row(2, 0), row(2, 1), row(2, 2));

        assertRows(execute("SELECT c1, c2 FROM %s WHERE key='foo' AND c1 > 1 ORDER BY c1 DESC, c2 DESC"),
                   row(2, 2), row(2, 1), row(2, 0));

        assertRows(execute("SELECT c1, c2 FROM %s WHERE key='foo' AND c1 >= 1"),
                   row(2, 2), row(2, 1), row(2, 0), row(1, 2), row(1, 1), row(1, 0));

        assertRows(execute("SELECT c1, c2 FROM %s WHERE key='foo' AND c1 >= 1 ORDER BY c1 ASC, c2 ASC"),
                   row(1, 0), row(1, 1), row(1, 2), row(2, 0), row(2, 1), row(2, 2));

        assertRows(execute("SELECT c1, c2 FROM %s WHERE key='foo' AND c1 >= 1 ORDER BY c1 ASC"),
                   row(1, 0), row(1, 1), row(1, 2), row(2, 0), row(2, 1), row(2, 2));

        assertRows(execute("SELECT c1, c2 FROM %s WHERE key='foo' AND c1 >= 1 ORDER BY c1 DESC, c2 DESC"),
                   row(2, 2), row(2, 1), row(2, 0), row(1, 2), row(1, 1), row(1, 0));

        // LT
        assertRows(execute("SELECT c1, c2 FROM %s WHERE key='foo' AND c1 < 1"),
                   row(0, 2), row(0, 1), row(0, 0));

        assertRows(execute("SELECT c1, c2 FROM %s WHERE key='foo' AND c1 < 1 ORDER BY c1 ASC, c2 ASC"),
                   row(0, 0), row(0, 1), row(0, 2));

        assertRows(execute("SELECT c1, c2 FROM %s WHERE key='foo' AND c1 < 1 ORDER BY c1 DESC, c2 DESC"),
                   row(0, 2), row(0, 1), row(0, 0));

        assertRows(execute("SELECT c1, c2 FROM %s WHERE key='foo' AND c1 <= 1"),
                   row(1, 2), row(1, 1), row(1, 0), row(0, 2), row(0, 1), row(0, 0));

        assertRows(execute("SELECT c1, c2 FROM %s WHERE key='foo' AND c1 <= 1 ORDER BY c1 ASC, c2 ASC"),
                   row(0, 0), row(0, 1), row(0, 2), row(1, 0), row(1, 1), row(1, 2));

        assertRows(execute("SELECT c1, c2 FROM %s WHERE key='foo' AND c1 <= 1 ORDER BY c1 ASC"),
                   row(0, 0), row(0, 1), row(0, 2), row(1, 0), row(1, 1), row(1, 2));

        assertRows(execute("SELECT c1, c2 FROM %s WHERE key='foo' AND c1 <= 1 ORDER BY c1 DESC, c2 DESC"),
                   row(1, 2), row(1, 1), row(1, 0), row(0, 2), row(0, 1), row(0, 0));
    }

    /**
     * Migrated from cql_tests.py:TestCQL.multi_in_compact_non_composite_test()
     */
    @Test
    public void testMultiSelectsNonCompositeCompactStorage() throws Throwable
    {
        createTable("CREATE TABLE %s (key int, c int, v int, PRIMARY KEY (key, c)) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (key, c, v) VALUES (0, 0, 0)");
        execute("INSERT INTO %s (key, c, v) VALUES (0, 1, 1)");
        execute("INSERT INTO %s (key, c, v) VALUES (0, 2, 2)");

        assertRows(execute("SELECT * FROM %s WHERE key=0 AND c IN (0, 2)"),
                   row(0, 0, 0), row(0, 2, 2));
    }

    @Test
    public void testSelectDistinct() throws Throwable
    {
        //Test a 'compact storage' table.
        createTable("CREATE TABLE %s (pk0 int, pk1 int, val int, PRIMARY KEY((pk0, pk1))) WITH COMPACT STORAGE");

        for (int i = 0; i < 3; i++)
            execute("INSERT INTO %s (pk0, pk1, val) VALUES (?, ?, ?)", i, i, i);

        assertRows(execute("SELECT DISTINCT pk0, pk1 FROM %s LIMIT 1"),
                   row(0, 0));

        assertRows(execute("SELECT DISTINCT pk0, pk1 FROM %s LIMIT 3"),
                   row(0, 0),
                   row(2, 2),
                   row(1, 1));

        // Test a 'wide row' thrift table.
        createTable("CREATE TABLE %s (pk int, name text, val int, PRIMARY KEY(pk, name)) WITH COMPACT STORAGE");

        for (int i = 0; i < 3; i++)
        {
            execute("INSERT INTO %s (pk, name, val) VALUES (?, 'name0', 0)", i);
            execute("INSERT INTO %s (pk, name, val) VALUES (?, 'name1', 1)", i);
        }

        assertRows(execute("SELECT DISTINCT pk FROM %s LIMIT 1"),
                   row(1));

        assertRows(execute("SELECT DISTINCT pk FROM %s LIMIT 3"),
                   row(1),
                   row(0),
                   row(2));
    }

    @Test
    public void testFilteringOnCompactTablesWithoutIndices() throws Throwable
    {
        //----------------------------------------------
        // Test COMPACT table with clustering columns
        //----------------------------------------------
        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a, b)) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (a, b, c) VALUES (1, 2, 4)");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 3, 6)");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 4, 4)");
        execute("INSERT INTO %s (a, b, c) VALUES (2, 3, 7)");

        // Adds tomstones
        execute("INSERT INTO %s (a, b, c) VALUES (1, 1, 4)");
        execute("INSERT INTO %s (a, b, c) VALUES (2, 2, 7)");
        execute("DELETE FROM %s WHERE a = 1 AND b = 1");
        execute("DELETE FROM %s WHERE a = 2 AND b = 2");

        beforeAndAfterFlush(() -> {

            // Checks filtering
            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a = 1 AND b = 4 AND c = 4");

            assertRows(execute("SELECT * FROM %s WHERE a = 1 AND b = 4 AND c = 4 ALLOW FILTERING"),
                       row(1, 4, 4));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a IN (1, 2) AND c IN (6, 7)");

            assertRows(execute("SELECT * FROM %s WHERE a IN (1, 2) AND c IN (6, 7) ALLOW FILTERING"),
                       row(1, 3, 6),
                       row(2, 3, 7));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c > 4");

            assertRows(execute("SELECT * FROM %s WHERE c > 4 ALLOW FILTERING"),
                       row(1, 3, 6),
                       row(2, 3, 7));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE b < 3 AND c <= 4");

            assertRows(execute("SELECT * FROM %s WHERE b < 3 AND c <= 4 ALLOW FILTERING"),
                       row(1, 2, 4));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c >= 3 AND c <= 6");

            assertRows(execute("SELECT * FROM %s WHERE c >= 3 AND c <= 6 ALLOW FILTERING"),
                       row(1, 2, 4),
                       row(1, 3, 6),
                       row(1, 4, 4));
        });

        // Checks filtering with null
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c = null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c = null ALLOW FILTERING");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c > null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c > null ALLOW FILTERING");

        // Checks filtering with unset
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c = ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c > ? ALLOW FILTERING",
                             unset());

        //----------------------------------------------
        // Test COMPACT table without clustering columns
        //----------------------------------------------
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b int, c int) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (a, b, c) VALUES (1, 2, 4)");
        execute("INSERT INTO %s (a, b, c) VALUES (2, 1, 6)");
        execute("INSERT INTO %s (a, b, c) VALUES (3, 2, 4)");
        execute("INSERT INTO %s (a, b, c) VALUES (4, 1, 7)");

        // Adds tomstones
        execute("INSERT INTO %s (a, b, c) VALUES (0, 1, 4)");
        execute("INSERT INTO %s (a, b, c) VALUES (5, 2, 7)");
        execute("DELETE FROM %s WHERE a = 0");
        execute("DELETE FROM %s WHERE a = 5");

        beforeAndAfterFlush(() -> {

            // Checks filtering
            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a = 1 AND b = 4 AND c = 4");

            assertRows(execute("SELECT * FROM %s WHERE a = 1 AND b = 2 AND c = 4 ALLOW FILTERING"),
                       row(1, 2, 4));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a IN (1, 2) AND c IN (6, 7)");

            assertRows(execute("SELECT * FROM %s WHERE a IN (1, 2) AND c IN (6, 7) ALLOW FILTERING"),
                       row(2, 1, 6));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c > 4");

            assertRows(execute("SELECT * FROM %s WHERE c > 4 ALLOW FILTERING"),
                       row(2, 1, 6),
                       row(4, 1, 7));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE b < 3 AND c <= 4");

            assertRows(execute("SELECT * FROM %s WHERE b < 3 AND c <= 4 ALLOW FILTERING"),
                       row(1, 2, 4),
                       row(3, 2, 4));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c >= 3 AND c <= 6");

            assertRows(execute("SELECT * FROM %s WHERE c >= 3 AND c <= 6 ALLOW FILTERING"),
                       row(1, 2, 4),
                       row(2, 1, 6),
                       row(3, 2, 4));
        });

        // Checks filtering with null
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c = null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c = null ALLOW FILTERING");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c > null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c > null ALLOW FILTERING");

        // // Checks filtering with unset
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c = ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c > ? ALLOW FILTERING",
                             unset());
    }

    @Test
    public void testFilteringOnCompactTablesWithoutIndicesAndWithLists() throws Throwable
    {
        //----------------------------------------------
        // Test COMPACT table with clustering columns
        //----------------------------------------------
        createTable("CREATE TABLE %s (a int, b int, c frozen<list<int>>, PRIMARY KEY (a, b)) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (a, b, c) VALUES (1, 2, [4, 2])");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 3, [6, 2])");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 4, [4, 1])");
        execute("INSERT INTO %s (a, b, c) VALUES (2, 3, [7, 1])");

        beforeAndAfterFlush(() -> {

            // Checks filtering
            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a = 1 AND b = 4 AND c = [4, 1]");

            assertRows(execute("SELECT * FROM %s WHERE a = 1 AND b = 4 AND c = [4, 1] ALLOW FILTERING"),
                       row(1, 4, list(4, 1)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c > [4, 2]");

            assertRows(execute("SELECT * FROM %s WHERE c > [4, 2] ALLOW FILTERING"),
                       row(1, 3, list(6, 2)),
                       row(2, 3, list(7, 1)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE b <= 3 AND c < [6, 2]");

            assertRows(execute("SELECT * FROM %s WHERE b <= 3 AND c < [6, 2] ALLOW FILTERING"),
                       row(1, 2, list(4, 2)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c >= [4, 2] AND c <= [6, 4]");

            assertRows(execute("SELECT * FROM %s WHERE c >= [4, 2] AND c <= [6, 4] ALLOW FILTERING"),
                       row(1, 2, list(4, 2)),
                       row(1, 3, list(6, 2)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c CONTAINS 2");

            assertRows(execute("SELECT * FROM %s WHERE c CONTAINS 2 ALLOW FILTERING"),
                       row(1, 2, list(4, 2)),
                       row(1, 3, list(6, 2)));

            assertInvalidMessage("Cannot use CONTAINS KEY on non-map column c",
                                 "SELECT * FROM %s WHERE c CONTAINS KEY 2 ALLOW FILTERING");

            assertRows(execute("SELECT * FROM %s WHERE c CONTAINS 2 AND c CONTAINS 6 ALLOW FILTERING"),
                       row(1, 3, list(6, 2)));
        });

        // Checks filtering with null
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c = null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c = null ALLOW FILTERING");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c > null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c > null ALLOW FILTERING");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c CONTAINS null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c CONTAINS null ALLOW FILTERING");

        // Checks filtering with unset
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c = ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c > ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c CONTAINS ? ALLOW FILTERING",
                             unset());

        //----------------------------------------------
        // Test COMPACT table without clustering columns
        //----------------------------------------------
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b int, c frozen<list<int>>) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (a, b, c) VALUES (1, 2, [4, 2])");
        execute("INSERT INTO %s (a, b, c) VALUES (2, 1, [6, 2])");
        execute("INSERT INTO %s (a, b, c) VALUES (3, 2, [4, 1])");
        execute("INSERT INTO %s (a, b, c) VALUES (4, 1, [7, 1])");

        beforeAndAfterFlush(() -> {

            // Checks filtering
            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a = 1 AND b = 2 AND c = [4, 2]");

            assertRows(execute("SELECT * FROM %s WHERE a = 1 AND b = 2 AND c = [4, 2] ALLOW FILTERING"),
                       row(1, 2, list(4, 2)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c > [4, 2]");

            assertRows(execute("SELECT * FROM %s WHERE c > [4, 2] ALLOW FILTERING"),
                       row(2, 1, list(6, 2)),
                       row(4, 1, list(7, 1)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE b < 3 AND c <= [4, 2]");

            assertRows(execute("SELECT * FROM %s WHERE b < 3 AND c <= [4, 2] ALLOW FILTERING"),
                       row(1, 2, list(4, 2)),
                       row(3, 2, list(4, 1)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c >= [4, 3] AND c <= [7]");

            assertRows(execute("SELECT * FROM %s WHERE c >= [4, 3] AND c <= [7] ALLOW FILTERING"),
                       row(2, 1, list(6, 2)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c CONTAINS 2");

            assertRows(execute("SELECT * FROM %s WHERE c CONTAINS 2 ALLOW FILTERING"),
                       row(1, 2, list(4, 2)),
                       row(2, 1, list(6, 2)));

            assertInvalidMessage("Cannot use CONTAINS KEY on non-map column c",
                                 "SELECT * FROM %s WHERE c CONTAINS KEY 2 ALLOW FILTERING");

            assertRows(execute("SELECT * FROM %s WHERE c CONTAINS 2 AND c CONTAINS 6 ALLOW FILTERING"),
                       row(2, 1, list(6, 2)));
        });

        // Checks filtering with null
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c = null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c = null ALLOW FILTERING");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c > null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c > null ALLOW FILTERING");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c CONTAINS null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c CONTAINS null ALLOW FILTERING");

        // Checks filtering with unset
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c = ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c > ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c CONTAINS ? ALLOW FILTERING",
                             unset());
    }

    @Test
    public void testFilteringOnCompactTablesWithoutIndicesAndWithSets() throws Throwable
    {
        //----------------------------------------------
        // Test COMPACT table with clustering columns
        //----------------------------------------------
        createTable("CREATE TABLE %s (a int, b int, c frozen<set<int>>, PRIMARY KEY (a, b)) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (a, b, c) VALUES (1, 2, {4, 2})");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 3, {6, 2})");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 4, {4, 1})");
        execute("INSERT INTO %s (a, b, c) VALUES (2, 3, {7, 1})");

        beforeAndAfterFlush(() -> {

            // Checks filtering
            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a = 1 AND b = 4 AND c = {4, 1}");

            assertRows(execute("SELECT * FROM %s WHERE a = 1 AND b = 4 AND c = {4, 1} ALLOW FILTERING"),
                       row(1, 4, set(4, 1)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c > {4, 2}");

            assertRows(execute("SELECT * FROM %s WHERE c > {4, 2} ALLOW FILTERING"),
                       row(1, 3, set(6, 2)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE b <= 3 AND c < {6, 2}");

            assertRows(execute("SELECT * FROM %s WHERE b <= 3 AND c < {6, 2} ALLOW FILTERING"),
                       row(1, 2, set(2, 4)),
                       row(2, 3, set(1, 7)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c >= {4, 2} AND c <= {6, 4}");

            assertRows(execute("SELECT * FROM %s WHERE c >= {4, 2} AND c <= {6, 4} ALLOW FILTERING"),
                       row(1, 2, set(4, 2)),
                       row(1, 3, set(6, 2)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c CONTAINS 2");

            assertRows(execute("SELECT * FROM %s WHERE c CONTAINS 2 ALLOW FILTERING"),
                       row(1, 2, set(4, 2)),
                       row(1, 3, set(6, 2)));

            assertInvalidMessage("Cannot use CONTAINS KEY on non-map column c",
                                 "SELECT * FROM %s WHERE c CONTAINS KEY 2 ALLOW FILTERING");

            assertRows(execute("SELECT * FROM %s WHERE c CONTAINS 2 AND c CONTAINS 6 ALLOW FILTERING"),
                       row(1, 3, set(6, 2)));
        });
        // Checks filtering with null
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c = null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c = null ALLOW FILTERING");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c > null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c > null ALLOW FILTERING");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c CONTAINS null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c CONTAINS null ALLOW FILTERING");

        // Checks filtering with unset
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c = ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c > ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c CONTAINS ? ALLOW FILTERING",
                             unset());

        //----------------------------------------------
        // Test COMPACT table without clustering columns
        //----------------------------------------------
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b int, c frozen<set<int>>) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (a, b, c) VALUES (1, 2, {4, 2})");
        execute("INSERT INTO %s (a, b, c) VALUES (2, 1, {6, 2})");
        execute("INSERT INTO %s (a, b, c) VALUES (3, 2, {4, 1})");
        execute("INSERT INTO %s (a, b, c) VALUES (4, 1, {7, 1})");

        beforeAndAfterFlush(() -> {

            // Checks filtering
            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a = 1 AND b = 2 AND c = {4, 2}");

            assertRows(execute("SELECT * FROM %s WHERE a = 1 AND b = 2 AND c = {4, 2} ALLOW FILTERING"),
                       row(1, 2, set(4, 2)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c > {4, 2}");

            assertRows(execute("SELECT * FROM %s WHERE c > {4, 2} ALLOW FILTERING"),
                       row(2, 1, set(6, 2)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE b < 3 AND c <= {4, 2}");

            assertRows(execute("SELECT * FROM %s WHERE b < 3 AND c <= {4, 2} ALLOW FILTERING"),
                       row(1, 2, set(4, 2)),
                       row(4, 1, set(1, 7)),
                       row(3, 2, set(4, 1)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c >= {4, 3} AND c <= {7}");

            assertRows(execute("SELECT * FROM %s WHERE c >= {5, 2} AND c <= {7} ALLOW FILTERING"),
                       row(2, 1, set(6, 2)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c CONTAINS 2");

            assertRows(execute("SELECT * FROM %s WHERE c CONTAINS 2 ALLOW FILTERING"),
                       row(1, 2, set(4, 2)),
                       row(2, 1, set(6, 2)));

            assertInvalidMessage("Cannot use CONTAINS KEY on non-map column c",
                                 "SELECT * FROM %s WHERE c CONTAINS KEY 2 ALLOW FILTERING");

            assertRows(execute("SELECT * FROM %s WHERE c CONTAINS 2 AND c CONTAINS 6 ALLOW FILTERING"),
                       row(2, 1, set(6, 2)));
        });

        // Checks filtering with null
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c = null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c = null ALLOW FILTERING");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c > null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c > null ALLOW FILTERING");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c CONTAINS null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c CONTAINS null ALLOW FILTERING");

        // Checks filtering with unset
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c = ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c > ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c CONTAINS ? ALLOW FILTERING",
                             unset());
    }

    @Test
    public void testAllowFilteringOnPartitionKeyWithDistinct() throws Throwable
    {
        // Test a 'compact storage' table.
        createTable("CREATE TABLE %s (pk0 int, pk1 int, val int, PRIMARY KEY((pk0, pk1))) WITH COMPACT STORAGE");

        for (int i = 0; i < 3; i++)
            execute("INSERT INTO %s (pk0, pk1, val) VALUES (?, ?, ?)", i, i, i);

        beforeAndAfterFlush(() -> {
            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT DISTINCT pk0, pk1 FROM %s WHERE pk1 = 1 LIMIT 3");

            assertRows(execute("SELECT DISTINCT pk0, pk1 FROM %s WHERE pk0 < 2 AND pk1 = 1 LIMIT 1 ALLOW FILTERING"),
                       row(1, 1));

            assertRows(execute("SELECT DISTINCT pk0, pk1 FROM %s WHERE pk1 > 1 LIMIT 3 ALLOW FILTERING"),
                       row(2, 2));
        });

        // Test a 'wide row' thrift table.
        createTable("CREATE TABLE %s (pk int, name text, val int, PRIMARY KEY(pk, name)) WITH COMPACT STORAGE");

        for (int i = 0; i < 3; i++)
        {
            execute("INSERT INTO %s (pk, name, val) VALUES (?, 'name0', 0)", i);
            execute("INSERT INTO %s (pk, name, val) VALUES (?, 'name1', 1)", i);
        }

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT DISTINCT pk FROM %s WHERE pk > 1 LIMIT 1 ALLOW FILTERING"),
                       row(2));

            assertRows(execute("SELECT DISTINCT pk FROM %s WHERE pk > 0 LIMIT 3 ALLOW FILTERING"),
                       row(1),
                       row(2));
        });
    }

    @Test
    public void testAllowFilteringOnPartitionKeyWithCounters() throws Throwable
    {
        for (String compactStorageClause : new String[]{ "", " WITH COMPACT STORAGE" })
        {
            createTable("CREATE TABLE %s (a int, b int, c int, cnt counter, PRIMARY KEY ((a, b), c))"
                        + compactStorageClause);

            execute("UPDATE %s SET cnt = cnt + ? WHERE a = ? AND b = ? AND c = ?", 14L, 11, 12, 13);
            execute("UPDATE %s SET cnt = cnt + ? WHERE a = ? AND b = ? AND c = ?", 24L, 21, 22, 23);
            execute("UPDATE %s SET cnt = cnt + ? WHERE a = ? AND b = ? AND c = ?", 27L, 21, 22, 26);
            execute("UPDATE %s SET cnt = cnt + ? WHERE a = ? AND b = ? AND c = ?", 34L, 31, 32, 33);
            execute("UPDATE %s SET cnt = cnt + ? WHERE a = ? AND b = ? AND c = ?", 24L, 41, 42, 43);

            beforeAndAfterFlush(() -> {

                assertRows(executeFilteringOnly("SELECT * FROM %s WHERE cnt = 24"),
                           row(41, 42, 43, 24L),
                           row(21, 22, 23, 24L));
                assertRows(executeFilteringOnly("SELECT * FROM %s WHERE b > 22 AND cnt = 24"),
                           row(41, 42, 43, 24L));
                assertRows(executeFilteringOnly("SELECT * FROM %s WHERE b > 10 AND b < 25 AND cnt = 24"),
                           row(21, 22, 23, 24L));
                assertRows(executeFilteringOnly("SELECT * FROM %s WHERE b > 10 AND c < 25 AND cnt = 24"),
                           row(21, 22, 23, 24L));

                assertInvalidMessage(
                "ORDER BY is only supported when the partition key is restricted by an EQ or an IN.",
                "SELECT * FROM %s WHERE a = 21 AND b > 10 AND cnt > 23 ORDER BY c DESC ALLOW FILTERING");

                assertRows(executeFilteringOnly("SELECT * FROM %s WHERE a = 21 AND b = 22 AND cnt > 23 ORDER BY c DESC"),
                           row(21, 22, 26, 27L),
                           row(21, 22, 23, 24L));

                assertRows(executeFilteringOnly("SELECT * FROM %s WHERE cnt > 20 AND cnt < 30"),
                           row(41, 42, 43, 24L),
                           row(21, 22, 23, 24L),
                           row(21, 22, 26, 27L));
            });
        }
    }

    @Test
    public void testAllowFilteringOnPartitionKeyOnCompactTablesWithoutIndicesAndWithLists() throws Throwable
    {
        // ----------------------------------------------
        // Test COMPACT table with clustering columns
        // ----------------------------------------------
        createTable("CREATE TABLE %s (a int, b int, c frozen<list<int>>, PRIMARY KEY (a, b)) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (a, b, c) VALUES (1, 2, [4, 2])");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 3, [6, 2])");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 4, [4, 1])");
        execute("INSERT INTO %s (a, b, c) VALUES (2, 3, [7, 1])");

        beforeAndAfterFlush(() -> {

            // Checks filtering
            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a >= 1 AND b = 4 AND c = [4, 1]");

            assertRows(execute("SELECT * FROM %s WHERE a >= 1 AND b >= 4 AND c = [4, 1] ALLOW FILTERING"),
                       row(1, 4, list(4, 1)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a > 0 AND c > [4, 2]");

            assertRows(execute("SELECT * FROM %s WHERE a >= 1 AND c > [4, 2] ALLOW FILTERING"),
                       row(1, 3, list(6, 2)),
                       row(2, 3, list(7, 1)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a > 1 AND b <= 3 AND c < [6, 2]");

            assertRows(execute("SELECT * FROM %s WHERE a <= 1 AND b <= 3 AND c < [6, 2] ALLOW FILTERING"),
                       row(1, 2, list(4, 2)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a <= 1 AND c >= [4, 2] AND c <= [6, 4]");

            assertRows(execute("SELECT * FROM %s WHERE a > 0 AND b <= 3 AND c >= [4, 2] AND c <= [6, 4] ALLOW FILTERING"),
                       row(1, 2, list(4, 2)),
                       row(1, 3, list(6, 2)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a > 1 AND c CONTAINS 2");

            assertRows(execute("SELECT * FROM %s WHERE a > 0 AND c CONTAINS 2 ALLOW FILTERING"),
                       row(1, 2, list(4, 2)),
                       row(1, 3, list(6, 2)));

            assertInvalidMessage("Cannot use CONTAINS KEY on non-map column c",
                                 "SELECT * FROM %s WHERE a > 1 AND c CONTAINS KEY 2 ALLOW FILTERING");

            assertRows(execute("SELECT * FROM %s WHERE a < 2 AND c CONTAINS 2 AND c CONTAINS 6 ALLOW FILTERING"),
                       row(1, 3, list(6, 2)));
        });
        // Checks filtering with null
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a > 1 AND c = null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE a > 1 AND c = null ALLOW FILTERING");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a > 1 AND c > null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE a > 1 AND c > null ALLOW FILTERING");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a > 1 AND c CONTAINS null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE a > 1 AND c CONTAINS null ALLOW FILTERING");

        // Checks filtering with unset
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE a > 1 AND c = ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE a > 1 AND c > ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE a > 1 AND c CONTAINS ? ALLOW FILTERING",
                             unset());

        // ----------------------------------------------
        // Test COMPACT table without clustering columns
        // ----------------------------------------------
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b int, c frozen<list<int>>) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (a, b, c) VALUES (1, 2, [4, 2])");
        execute("INSERT INTO %s (a, b, c) VALUES (2, 1, [6, 2])");
        execute("INSERT INTO %s (a, b, c) VALUES (3, 2, [4, 1])");
        execute("INSERT INTO %s (a, b, c) VALUES (4, 1, [7, 1])");

        beforeAndAfterFlush(() -> {

            // Checks filtering
            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a >= 1 AND b = 2 AND c = [4, 2]");

            assertRows(execute("SELECT * FROM %s WHERE a >= 1 AND b = 2 AND c = [4, 2] ALLOW FILTERING"),
                       row(1, 2, list(4, 2)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a > 1 AND c > [4, 2]");

            assertRows(execute("SELECT * FROM %s WHERE a > 3 AND c > [4, 2] ALLOW FILTERING"),
                       row(4, 1, list(7, 1)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a < 1 AND b < 3 AND c <= [4, 2]");

            assertRows(execute("SELECT * FROM %s WHERE a < 3 AND b < 3 AND c <= [4, 2] ALLOW FILTERING"),
                       row(1, 2, list(4, 2)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a > 1 AND c >= [4, 3] AND c <= [7]");

            assertRows(execute("SELECT * FROM %s WHERE a >= 2 AND c >= [4, 3] AND c <= [7] ALLOW FILTERING"),
                       row(2, 1, list(6, 2)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a > 3 AND c CONTAINS 2");

            assertRows(execute("SELECT * FROM %s WHERE a >= 1 AND c CONTAINS 2 ALLOW FILTERING"),
                       row(1, 2, list(4, 2)),
                       row(2, 1, list(6, 2)));

            assertInvalidMessage("Cannot use CONTAINS KEY on non-map column c",
                                 "SELECT * FROM %s WHERE a >=1 AND c CONTAINS KEY 2 ALLOW FILTERING");

            assertRows(execute("SELECT * FROM %s WHERE a < 3 AND c CONTAINS 2 AND c CONTAINS 6 ALLOW FILTERING"),
                       row(2, 1, list(6, 2)));
        });

        // Checks filtering with null
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a > 1 AND c = null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE a > 1 AND c = null ALLOW FILTERING");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a > 1 AND c > null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE a > 1 AND c > null ALLOW FILTERING");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a > 1 AND c CONTAINS null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE a > 1 AND c CONTAINS null ALLOW FILTERING");

        // Checks filtering with unset
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE a > 1 AND c = ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE a > 1 AND c > ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE a > 1 AND c CONTAINS ? ALLOW FILTERING",
                             unset());
    }


    @Test
    public void testAllowFilteringOnPartitionKeyOnCompactTablesWithoutIndicesAndWithMaps() throws Throwable
    {
        //----------------------------------------------
        // Test COMPACT table with clustering columns
        //----------------------------------------------
        createTable("CREATE TABLE %s (a int, b int, c frozen<map<int, int>>, PRIMARY KEY (a, b)) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (a, b, c) VALUES (1, 2, {4 : 2})");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 3, {6 : 2})");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 4, {4 : 1})");
        execute("INSERT INTO %s (a, b, c) VALUES (2, 3, {7 : 1})");

        beforeAndAfterFlush(() -> {

            // Checks filtering
            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a >= 1 AND b = 4 AND c = {4 : 1}");

            assertRows(execute("SELECT * FROM %s WHERE a <= 1 AND b = 4 AND c = {4 : 1} ALLOW FILTERING"),
                       row(1, 4, map(4, 1)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a > 1 AND c > {4 : 2}");

            assertRows(execute("SELECT * FROM %s WHERE a > 1 AND c > {4 : 2} ALLOW FILTERING"),
                       row(2, 3, map(7, 1)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a > 1 AND b <= 3 AND c < {6 : 2}");

            assertRows(execute("SELECT * FROM %s WHERE a >= 1 AND b <= 3 AND c < {6 : 2} ALLOW FILTERING"),
                       row(1, 2, map(4, 2)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a > 1 AND c >= {4 : 2} AND c <= {6 : 4}");

            assertRows(execute("SELECT * FROM %s WHERE a > 0 AND c >= {4 : 2} AND c <= {6 : 4} ALLOW FILTERING"),
                       row(1, 2, map(4, 2)),
                       row(1, 3, map(6, 2)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a > 10 AND c CONTAINS 2");

            assertRows(execute("SELECT * FROM %s WHERE a > 0 AND c CONTAINS 2 ALLOW FILTERING"),
                       row(1, 2, map(4, 2)),
                       row(1, 3, map(6, 2)));

            assertRows(execute("SELECT * FROM %s WHERE a < 2 AND c CONTAINS KEY 6 ALLOW FILTERING"),
                       row(1, 3, map(6, 2)));

            assertRows(execute("SELECT * FROM %s WHERE a >= 1 AND c CONTAINS 2 AND c CONTAINS KEY 6 ALLOW FILTERING"),
                       row(1, 3, map(6, 2)));
        });

        // Checks filtering with null
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a >= 1 AND c = null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c = null ALLOW FILTERING");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a >= 1 AND c > null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c > null ALLOW FILTERING");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS null ALLOW FILTERING");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS KEY null ALLOW FILTERING");

        // Checks filtering with unset
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c = ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c > ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS KEY ? ALLOW FILTERING",
                             unset());

        //----------------------------------------------
        // Test COMPACT table without clustering columns
        //----------------------------------------------
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b int, c frozen<map<int, int>>) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (a, b, c) VALUES (1, 2, {4 : 2})");
        execute("INSERT INTO %s (a, b, c) VALUES (2, 1, {6 : 2})");
        execute("INSERT INTO %s (a, b, c) VALUES (3, 2, {4 : 1})");
        execute("INSERT INTO %s (a, b, c) VALUES (4, 1, {7 : 1})");

        beforeAndAfterFlush(() -> {

            // Checks filtering
            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a >= 1 AND b = 2 AND c = {4 : 2}");

            assertRows(execute("SELECT * FROM %s WHERE a >= 1 AND b = 2 AND c = {4 : 2} ALLOW FILTERING"),
                       row(1, 2, map(4, 2)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a >= 1 AND c > {4 : 2}");

            assertRows(execute("SELECT * FROM %s WHERE a >= 1 AND c > {4 : 2} ALLOW FILTERING"),
                       row(2, 1, map(6, 2)),
                       row(4, 1, map(7, 1)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a >= 1 AND b < 3 AND c <= {4 : 2}");

            assertRows(execute("SELECT * FROM %s WHERE a >= 1 AND b < 3 AND c <= {4 : 2} ALLOW FILTERING"),
                       row(1, 2, map(4, 2)),
                       row(3, 2, map(4, 1)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a >= 1 AND c >= {4 : 3} AND c <= {7 : 1}");

            assertRows(execute("SELECT * FROM %s WHERE a >= 2 AND c >= {5 : 2} AND c <= {7 : 0} ALLOW FILTERING"),
                       row(2, 1, map(6, 2)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS 2");

            assertRows(execute("SELECT * FROM %s WHERE a >= 1 AND c CONTAINS 2 ALLOW FILTERING"),
                       row(1, 2, map(4, 2)),
                       row(2, 1, map(6, 2)));

            assertRows(execute("SELECT * FROM %s WHERE a > 0 AND c CONTAINS KEY 4 ALLOW FILTERING"),
                       row(1, 2, map(4, 2)),
                       row(3, 2, map(4, 1)));

            assertRows(execute("SELECT * FROM %s WHERE a >= 2 AND c CONTAINS 2 AND c CONTAINS KEY 6 ALLOW FILTERING"),
                       row(2, 1, map(6, 2)));
        });

        // Checks filtering with null
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a >= 1 AND c = null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c = null ALLOW FILTERING");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a >= 1 AND c > null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c > null ALLOW FILTERING");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS null ALLOW FILTERING");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS KEY null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS KEY null ALLOW FILTERING");

        // Checks filtering with unset
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c = ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c > ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS KEY ? ALLOW FILTERING",
                             unset());
    }

    @Test
    public void testAllowFilteringOnPartitionKeyOnCompactTablesWithoutIndicesAndWithSets() throws Throwable
    {
        //----------------------------------------------
        // Test COMPACT table with clustering columns
        //----------------------------------------------
        createTable("CREATE TABLE %s (a int, b int, c frozen<set<int>>, PRIMARY KEY (a, b)) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (a, b, c) VALUES (1, 2, {4, 2})");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 3, {6, 2})");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 4, {4, 1})");
        execute("INSERT INTO %s (a, b, c) VALUES (2, 3, {7, 1})");

        beforeAndAfterFlush(() -> {

            // Checks filtering
            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a >= 1 AND b = 4 AND c = {4, 1}");

            assertRows(execute("SELECT * FROM %s WHERE a >= 1 AND b = 4 AND c = {4, 1} ALLOW FILTERING"),
                       row(1, 4, set(4, 1)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a >= 1 AND c > {4, 2}");

            assertRows(execute("SELECT * FROM %s WHERE a >= 1 AND c > {4, 2} ALLOW FILTERING"),
                       row(1, 3, set(6, 2)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a >= 1 AND b <= 3 AND c < {6, 2}");

            assertRows(execute("SELECT * FROM %s WHERE a > 0 AND b <= 3 AND c < {6, 2} ALLOW FILTERING"),
                       row(1, 2, set(2, 4)),
                       row(2, 3, set(1, 7)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a >= 1 AND c >= {4, 2} AND c <= {6, 4}");

            assertRows(execute("SELECT * FROM %s WHERE a >= 0 AND c >= {4, 2} AND c <= {6, 4} ALLOW FILTERING"),
                       row(1, 2, set(4, 2)),
                       row(1, 3, set(6, 2)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS 2");

            assertRows(execute("SELECT * FROM %s WHERE a < 2 AND c CONTAINS 2 ALLOW FILTERING"),
                       row(1, 2, set(4, 2)),
                       row(1, 3, set(6, 2)));

            assertInvalidMessage("Cannot use CONTAINS KEY on non-map column c",
                                 "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS KEY 2 ALLOW FILTERING");

            assertRows(execute("SELECT * FROM %s WHERE a >= 1 AND c CONTAINS 2 AND c CONTAINS 6 ALLOW FILTERING"),
                       row(1, 3, set(6, 2)));
        });
        // Checks filtering with null
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a >= 1 AND c = null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c = null ALLOW FILTERING");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a >= 1 AND c > null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c > null ALLOW FILTERING");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS null ALLOW FILTERING");

        // Checks filtering with unset
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c = ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c > ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS ? ALLOW FILTERING",
                             unset());

        //----------------------------------------------
        // Test COMPACT table without clustering columns
        //----------------------------------------------
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b int, c frozen<set<int>>) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (a, b, c) VALUES (1, 2, {4, 2})");
        execute("INSERT INTO %s (a, b, c) VALUES (2, 1, {6, 2})");
        execute("INSERT INTO %s (a, b, c) VALUES (3, 2, {4, 1})");
        execute("INSERT INTO %s (a, b, c) VALUES (4, 1, {7, 1})");

        beforeAndAfterFlush(() -> {

            // Checks filtering
            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a >= 1 AND b = 2 AND c = {4, 2}");

            assertRows(execute("SELECT * FROM %s WHERE a >= 1 AND b = 2 AND c = {4, 2} ALLOW FILTERING"),
                       row(1, 2, set(4, 2)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a >= 1 AND c > {4, 2}");

            assertRows(execute("SELECT * FROM %s WHERE a >= 1 AND c > {4, 2} ALLOW FILTERING"),
                       row(2, 1, set(6, 2)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a >= 1 AND b < 3 AND c <= {4, 2}");

            assertRows(execute("SELECT * FROM %s WHERE a <= 4 AND b < 3 AND c <= {4, 2} ALLOW FILTERING"),
                       row(1, 2, set(4, 2)),
                       row(4, 1, set(1, 7)),
                       row(3, 2, set(4, 1)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a >= 1 AND c >= {4, 3} AND c <= {7}");

            assertRows(execute("SELECT * FROM %s WHERE a < 3 AND c >= {5, 2} AND c <= {7} ALLOW FILTERING"),
                       row(2, 1, set(6, 2)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS 2");

            assertRows(execute("SELECT * FROM %s WHERE a >= 0 AND c CONTAINS 2 ALLOW FILTERING"),
                       row(1, 2, set(4, 2)),
                       row(2, 1, set(6, 2)));

            assertInvalidMessage("Cannot use CONTAINS KEY on non-map column c",
                                 "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS KEY 2 ALLOW FILTERING");

            assertRows(execute("SELECT * FROM %s WHERE a >= 2 AND c CONTAINS 2 AND c CONTAINS 6 ALLOW FILTERING"),
                       row(2, 1, set(6, 2)));
        });

        // Checks filtering with null
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a >= 1 AND c = null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c = null ALLOW FILTERING");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a >= 1 AND c > null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c > null ALLOW FILTERING");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS null ALLOW FILTERING");

        // Checks filtering with unset
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c = ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c > ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS ? ALLOW FILTERING",
                             unset());
    }

    @Test
    public void testAllowFilteringOnPartitionKeyOnCompactTablesWithoutIndices() throws Throwable
    {
        // ----------------------------------------------
        // Test COMPACT table with clustering columns
        // ----------------------------------------------
        createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY ((a, b), c)) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (a, b, c, d) VALUES (1, 2, 4, 5)");
        execute("INSERT INTO %s (a, b, c, d) VALUES (1, 3, 6, 7)");
        execute("INSERT INTO %s (a, b, c, d) VALUES (1, 4, 4, 5)");
        execute("INSERT INTO %s (a, b, c, d) VALUES (2, 3, 7, 8)");

        // Adds tomstones
        execute("INSERT INTO %s (a, b, c, d) VALUES (1, 1, 4, 5)");
        execute("INSERT INTO %s (a, b, c, d) VALUES (2, 2, 7, 8)");
        execute("DELETE FROM %s WHERE a = 1 AND b = 1 AND c = 4");
        execute("DELETE FROM %s WHERE a = 2 AND b = 2 AND c = 7");

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT * FROM %s WHERE a = 1 AND b = 4 AND c = 4"),
                       row(1, 4, 4, 5));

            // Checks filtering
            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a = 1 AND b = 4 AND c = 4 AND d = 5");

            assertRows(execute("SELECT * FROM %s WHERE a = 1 AND b = 4 AND c = 4 ALLOW FILTERING"),
                       row(1, 4, 4, 5));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a IN (1, 2) AND b = 3 AND d IN (6, 7)");

            assertRows(execute("SELECT * FROM %s WHERE a IN (1, 2) AND b = 3 AND d IN (6, 7) ALLOW FILTERING"),
                       row(1, 3, 6, 7));

            assertRows(execute("SELECT * FROM %s WHERE a < 2 AND c > 4 AND c <= 6 ALLOW FILTERING"),
                       row(1, 3, 6, 7));

            assertRows(execute("SELECT * FROM %s WHERE a <= 1 AND b >= 2 AND c >= 4 AND d <= 8 ALLOW FILTERING"),
                       row(1, 3, 6, 7),
                       row(1, 4, 4, 5),
                       row(1, 2, 4, 5));

            assertRows(execute("SELECT * FROM %s WHERE a = 1 AND c >= 4 AND d <= 8 ALLOW FILTERING"),
                       row(1, 3, 6, 7),
                       row(1, 4, 4, 5),
                       row(1, 2, 4, 5));

            assertRows(execute("SELECT * FROM %s WHERE a >= 2 AND c >= 4 AND d <= 8 ALLOW FILTERING"),
                       row(2, 3, 7, 8));
        });

        // Checks filtering with null
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE d = null");
        assertInvalidMessage("Unsupported null value for column a",
                             "SELECT * FROM %s WHERE a = null ALLOW FILTERING");
        assertInvalidMessage("Unsupported null value for column a",
                             "SELECT * FROM %s WHERE a > null ALLOW FILTERING");

        // Checks filtering with unset
        assertInvalidMessage("Unsupported unset value for column a",
                             "SELECT * FROM %s WHERE a = ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column a",
                             "SELECT * FROM %s WHERE a > ? ALLOW FILTERING",
                             unset());

        //----------------------------------------------
        // Test COMPACT table without clustering columns
        //----------------------------------------------
        createTable("CREATE TABLE %s (a int primary key, b int, c int) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (a, b, c) VALUES (1, 2, 4)");
        execute("INSERT INTO %s (a, b, c) VALUES (2, 1, 6)");
        execute("INSERT INTO %s (a, b, c) VALUES (3, 2, 4)");
        execute("INSERT INTO %s (a, b, c) VALUES (4, 1, 7)");

        // Adds tomstones
        execute("INSERT INTO %s (a, b, c) VALUES (0, 1, 4)");
        execute("INSERT INTO %s (a, b, c) VALUES (5, 2, 7)");
        execute("DELETE FROM %s WHERE a = 0");
        execute("DELETE FROM %s WHERE a = 5");

        beforeAndAfterFlush(() -> {

            // Checks filtering
            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a = 1 AND b = 4 AND c = 4");

            assertRows(execute("SELECT * FROM %s WHERE a = 1 AND b = 2 AND c = 4 ALLOW FILTERING"),
                       row(1, 2, 4));

            assertRows(execute("SELECT * FROM %s WHERE a = 1 AND b = 2 ALLOW FILTERING"),
                       row(1, 2, 4));

            assertRows(execute("SELECT * FROM %s WHERE b >= 2 AND c <= 4 ALLOW FILTERING"),
                       row(1, 2, 4),
                       row(3, 2, 4));

            assertRows(execute("SELECT * FROM %s WHERE a = 1 ALLOW FILTERING"),
                       row(1, 2, 4));

            assertRows(execute("SELECT * FROM %s WHERE b >= 2 ALLOW FILTERING"),
                       row(1, 2, 4),
                       row(3, 2, 4));

            assertRows(execute("SELECT * FROM %s WHERE a >= 2 AND b <=1 ALLOW FILTERING"),
                       row(2, 1, 6),
                       row(4, 1, 7));

            assertRows(execute("SELECT * FROM %s WHERE a = 1 AND c >= 4 ALLOW FILTERING"),
                       row(1, 2, 4));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a = 1 AND b IN (1, 2) AND c IN (6, 7)");

            assertRows(execute("SELECT * FROM %s WHERE a IN (1, 2) AND c IN (6, 7) ALLOW FILTERING"),
                       row(2, 1, 6));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c > 4");

            assertRows(execute("SELECT * FROM %s WHERE c > 4 ALLOW FILTERING"),
                       row(2, 1, 6),
                       row(4, 1, 7));

            assertRows(execute("SELECT * FROM %s WHERE a >= 1 AND b >= 2 AND c <= 4 ALLOW FILTERING"),
                       row(1, 2, 4),
                       row(3, 2, 4));

            assertRows(execute("SELECT * FROM %s WHERE a < 3 AND c <= 4 ALLOW FILTERING"),
                       row(1, 2, 4));

            assertRows(execute("SELECT * FROM %s WHERE a < 3 AND b >= 2 AND c <= 4 ALLOW FILTERING"),
                       row(1, 2, 4));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c >= 3 AND c <= 6");

            assertRows(execute("SELECT * FROM %s WHERE c <=6 ALLOW FILTERING"),
                       row(1, 2, 4),
                       row(2, 1, 6),
                       row(3, 2, 4));

            assertRows(execute("SELECT * FROM %s WHERE token(a) >= token(2)"),
                       row(2, 1, 6),
                       row(4, 1, 7),
                       row(3, 2, 4));

            assertRows(execute("SELECT * FROM %s WHERE token(a) >= token(2) ALLOW FILTERING"),
                       row(2, 1, 6),
                       row(4, 1, 7),
                       row(3, 2, 4));

            assertRows(execute("SELECT * FROM %s WHERE token(a) >= token(2) AND b = 1 ALLOW FILTERING"),
                       row(2, 1, 6),
                       row(4, 1, 7));
        });
    }

    @Test
    public void testFilteringOnCompactTablesWithoutIndicesAndWithMaps() throws Throwable
    {
        //----------------------------------------------
        // Test COMPACT table with clustering columns
        //----------------------------------------------
        createTable("CREATE TABLE %s (a int, b int, c frozen<map<int, int>>, PRIMARY KEY (a, b)) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (a, b, c) VALUES (1, 2, {4 : 2})");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 3, {6 : 2})");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 4, {4 : 1})");
        execute("INSERT INTO %s (a, b, c) VALUES (2, 3, {7 : 1})");

        beforeAndAfterFlush(() -> {

            // Checks filtering
            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a = 1 AND b = 4 AND c = {4 : 1}");

            assertRows(execute("SELECT * FROM %s WHERE a = 1 AND b = 4 AND c = {4 : 1} ALLOW FILTERING"),
                       row(1, 4, map(4, 1)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c > {4 : 2}");

            assertRows(execute("SELECT * FROM %s WHERE c > {4 : 2} ALLOW FILTERING"),
                       row(1, 3, map(6, 2)),
                       row(2, 3, map(7, 1)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE b <= 3 AND c < {6 : 2}");

            assertRows(execute("SELECT * FROM %s WHERE b <= 3 AND c < {6 : 2} ALLOW FILTERING"),
                       row(1, 2, map(4, 2)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c >= {4 : 2} AND c <= {6 : 4}");

            assertRows(execute("SELECT * FROM %s WHERE c >= {4 : 2} AND c <= {6 : 4} ALLOW FILTERING"),
                       row(1, 2, map(4, 2)),
                       row(1, 3, map(6, 2)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c CONTAINS 2");

            assertRows(execute("SELECT * FROM %s WHERE c CONTAINS 2 ALLOW FILTERING"),
                       row(1, 2, map(4, 2)),
                       row(1, 3, map(6, 2)));

            assertRows(execute("SELECT * FROM %s WHERE c CONTAINS KEY 6 ALLOW FILTERING"),
                       row(1, 3, map(6, 2)));

            assertRows(execute("SELECT * FROM %s WHERE c CONTAINS 2 AND c CONTAINS KEY 6 ALLOW FILTERING"),
                       row(1, 3, map(6, 2)));
        });

        // Checks filtering with null
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c = null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c = null ALLOW FILTERING");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c > null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c > null ALLOW FILTERING");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c CONTAINS null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c CONTAINS null ALLOW FILTERING");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c CONTAINS KEY null ALLOW FILTERING");

        // Checks filtering with unset
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c = ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c > ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c CONTAINS ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c CONTAINS KEY ? ALLOW FILTERING",
                             unset());

        //----------------------------------------------
        // Test COMPACT table without clustering columns
        //----------------------------------------------
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b int, c frozen<map<int, int>>) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (a, b, c) VALUES (1, 2, {4 : 2})");
        execute("INSERT INTO %s (a, b, c) VALUES (2, 1, {6 : 2})");
        execute("INSERT INTO %s (a, b, c) VALUES (3, 2, {4 : 1})");
        execute("INSERT INTO %s (a, b, c) VALUES (4, 1, {7 : 1})");

        beforeAndAfterFlush(() -> {

            // Checks filtering
            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a = 1 AND b = 2 AND c = {4 : 2}");

            assertRows(execute("SELECT * FROM %s WHERE a = 1 AND b = 2 AND c = {4 : 2} ALLOW FILTERING"),
                       row(1, 2, map(4, 2)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c > {4 : 2}");

            assertRows(execute("SELECT * FROM %s WHERE c > {4 : 2} ALLOW FILTERING"),
                       row(2, 1, map(6, 2)),
                       row(4, 1, map(7, 1)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE b < 3 AND c <= {4 : 2}");

            assertRows(execute("SELECT * FROM %s WHERE b < 3 AND c <= {4 : 2} ALLOW FILTERING"),
                       row(1, 2, map(4, 2)),
                       row(3, 2, map(4, 1)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c >= {4 : 3} AND c <= {7 : 1}");

            assertRows(execute("SELECT * FROM %s WHERE c >= {5 : 2} AND c <= {7 : 0} ALLOW FILTERING"),
                       row(2, 1, map(6, 2)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c CONTAINS 2");

            assertRows(execute("SELECT * FROM %s WHERE c CONTAINS 2 ALLOW FILTERING"),
                       row(1, 2, map(4, 2)),
                       row(2, 1, map(6, 2)));

            assertRows(execute("SELECT * FROM %s WHERE c CONTAINS KEY 4 ALLOW FILTERING"),
                       row(1, 2, map(4, 2)),
                       row(3, 2, map(4, 1)));

            assertRows(execute("SELECT * FROM %s WHERE c CONTAINS 2 AND c CONTAINS KEY 6 ALLOW FILTERING"),
                       row(2, 1, map(6, 2)));
        });

        // Checks filtering with null
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c = null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c = null ALLOW FILTERING");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c > null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c > null ALLOW FILTERING");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c CONTAINS null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c CONTAINS null ALLOW FILTERING");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c CONTAINS KEY null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c CONTAINS KEY null ALLOW FILTERING");

        // Checks filtering with unset
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c = ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c > ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c CONTAINS ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c CONTAINS KEY ? ALLOW FILTERING",
                             unset());
    }

    @Test
    public void filteringOnCompactTable() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY (a, b, c)) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 11, 12, 13, 14);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 21, 22, 23, 24);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 21, 25, 26, 27);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 31, 32, 33, 34);

        beforeAndAfterFlush(() -> {

            assertRows(executeFilteringOnly("SELECT * FROM %s WHERE c > 13"),
                       row(21, 22, 23, 24),
                       row(21, 25, 26, 27),
                       row(31, 32, 33, 34));

            assertRows(executeFilteringOnly("SELECT * FROM %s WHERE c > 13 AND c < 33"),
                       row(21, 22, 23, 24),
                       row(21, 25, 26, 27));

            assertRows(executeFilteringOnly("SELECT * FROM %s WHERE c > 13 AND b < 32"),
                       row(21, 22, 23, 24),
                       row(21, 25, 26, 27));

            assertRows(executeFilteringOnly("SELECT * FROM %s WHERE a = 21 AND c > 13 AND b < 32 ORDER BY b DESC"),
                       row(21, 25, 26, 27),
                       row(21, 22, 23, 24));

            assertRows(executeFilteringOnly("SELECT * FROM %s WHERE a IN (21, 31) AND c > 13 ORDER BY b DESC"),
                       row(31, 32, 33, 34),
                       row(21, 25, 26, 27),
                       row(21, 22, 23, 24));

            assertRows(executeFilteringOnly("SELECT * FROM %s WHERE c > 13 AND d < 34"),
                       row(21, 22, 23, 24),
                       row(21, 25, 26, 27));

            assertRows(executeFilteringOnly("SELECT * FROM %s WHERE c > 13"),
                       row(21, 22, 23, 24),
                       row(21, 25, 26, 27),
                       row(31, 32, 33, 34));
        });

        // with frozen in clustering key
        createTable("CREATE TABLE %s (a int, b int, c frozen<list<int>>, d int, PRIMARY KEY (a, b, c)) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 11, 12, list(1, 3), 14);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 21, 22, list(2, 3), 24);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 21, 25, list(2, 6), 27);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 31, 32, list(3, 3), 34);

        beforeAndAfterFlush(() -> {

            assertRows(executeFilteringOnly("SELECT * FROM %s WHERE c CONTAINS 2"),
                       row(21, 22, list(2, 3), 24),
                       row(21, 25, list(2, 6), 27));

            assertRows(executeFilteringOnly("SELECT * FROM %s WHERE c CONTAINS 2 AND b < 25"),
                       row(21, 22, list(2, 3), 24));

            assertRows(executeFilteringOnly("SELECT * FROM %s WHERE c CONTAINS 2 AND c CONTAINS 3"),
                       row(21, 22, list(2, 3), 24));

            assertRows(executeFilteringOnly("SELECT * FROM %s WHERE b > 12 AND c CONTAINS 2 AND d < 27"),
                       row(21, 22, list(2, 3), 24));
        });

        // with frozen in value
        createTable("CREATE TABLE %s (a int, b int, c int, d frozen<list<int>>, PRIMARY KEY (a, b, c)) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 11, 12, 13, list(1, 4));
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 21, 22, 23, list(2, 4));
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 21, 25, 25, list(2, 6));
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 31, 32, 34, list(3, 4));

        beforeAndAfterFlush(() -> {

            assertRows(executeFilteringOnly("SELECT * FROM %s WHERE d CONTAINS 2"),
                       row(21, 22, 23, list(2, 4)),
                       row(21, 25, 25, list(2, 6)));

            assertRows(executeFilteringOnly("SELECT * FROM %s WHERE d CONTAINS 2 AND b < 25"),
                       row(21, 22, 23, list(2, 4)));

            assertRows(executeFilteringOnly("SELECT * FROM %s WHERE d CONTAINS 2 AND d CONTAINS 4"),
                       row(21, 22, 23, list(2, 4)));

            assertRows(executeFilteringOnly("SELECT * FROM %s WHERE b > 12 AND c < 25 AND d CONTAINS 2"),
                       row(21, 22, 23, list(2, 4)));
        });
    }

    private UntypedResultSet executeFilteringOnly(String statement) throws Throwable
    {
        assertInvalid(statement);
        return execute(statement + " ALLOW FILTERING");
    }

    @Test
    public void testFilteringWithCounters() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, cnt counter, PRIMARY KEY (a, b, c))" + compactOption);

        execute("UPDATE %s SET cnt = cnt + ? WHERE a = ? AND b = ? AND c = ?", 14L, 11, 12, 13);
        execute("UPDATE %s SET cnt = cnt + ? WHERE a = ? AND b = ? AND c = ?", 24L, 21, 22, 23);
        execute("UPDATE %s SET cnt = cnt + ? WHERE a = ? AND b = ? AND c = ?", 27L, 21, 25, 26);
        execute("UPDATE %s SET cnt = cnt + ? WHERE a = ? AND b = ? AND c = ?", 34L, 31, 32, 33);
        execute("UPDATE %s SET cnt = cnt + ? WHERE a = ? AND b = ? AND c = ?", 24L, 41, 42, 43);

        beforeAndAfterFlush(() -> {

            assertRows(executeFilteringOnly("SELECT * FROM %s WHERE cnt = 24"),
                       row(21, 22, 23, 24L),
                       row(41, 42, 43, 24L));
            assertRows(executeFilteringOnly("SELECT * FROM %s WHERE b > 22 AND cnt = 24"),
                       row(41, 42, 43, 24L));
            assertRows(executeFilteringOnly("SELECT * FROM %s WHERE b > 10 AND b < 25 AND cnt = 24"),
                       row(21, 22, 23, 24L));
            assertRows(executeFilteringOnly("SELECT * FROM %s WHERE b > 10 AND c < 25 AND cnt = 24"),
                       row(21, 22, 23, 24L));
            assertRows(executeFilteringOnly("SELECT * FROM %s WHERE a = 21 AND b > 10 AND cnt > 23 ORDER BY b DESC"),
                       row(21, 25, 26, 27L),
                       row(21, 22, 23, 24L));
            assertRows(executeFilteringOnly("SELECT * FROM %s WHERE cnt > 20 AND cnt < 30"),
                       row(21, 22, 23, 24L),
                       row(21, 25, 26, 27L),
                       row(41, 42, 43, 24L));
        });
    }

    /**
     * Check select with and without compact storage, with different column
     * order. See CASSANDRA-10988
     */
    @Test
    public void testClusteringOrderWithSlice() throws Throwable
    {
        final String compactOption = " WITH COMPACT STORAGE AND";

        // non-compound, ASC order
        createTable("CREATE TABLE %s (a text, b int, PRIMARY KEY (a, b)) " +
                    compactOption +
                    " CLUSTERING ORDER BY (b ASC)");

        execute("INSERT INTO %s (a, b) VALUES ('a', 2)");
        execute("INSERT INTO %s (a, b) VALUES ('a', 3)");
        assertRows(execute("SELECT * FROM %s WHERE a = 'a' AND b > 0"),
                   row("a", 2),
                   row("a", 3));

        assertRows(execute("SELECT * FROM %s WHERE a = 'a' AND b > 0 ORDER BY b DESC"),
                   row("a", 3),
                   row("a", 2));

        // non-compound, DESC order
        createTable("CREATE TABLE %s (a text, b int, PRIMARY KEY (a, b))" +
                    compactOption +
                    " CLUSTERING ORDER BY (b DESC)");

        execute("INSERT INTO %s (a, b) VALUES ('a', 2)");
        execute("INSERT INTO %s (a, b) VALUES ('a', 3)");
        assertRows(execute("SELECT * FROM %s WHERE a = 'a' AND b > 0"),
                   row("a", 3),
                   row("a", 2));

        assertRows(execute("SELECT * FROM %s WHERE a = 'a' AND b > 0 ORDER BY b ASC"),
                   row("a", 2),
                   row("a", 3));

        // compound, first column DESC order
        createTable("CREATE TABLE %s (a text, b int, c int, PRIMARY KEY (a, b, c)) " +
                    compactOption +
                    " CLUSTERING ORDER BY (b DESC)"
        );

        execute("INSERT INTO %s (a, b, c) VALUES ('a', 2, 4)");
        execute("INSERT INTO %s (a, b, c) VALUES ('a', 3, 5)");
        assertRows(execute("SELECT * FROM %s WHERE a = 'a' AND b > 0"),
                   row("a", 3, 5),
                   row("a", 2, 4));

        assertRows(execute("SELECT * FROM %s WHERE a = 'a' AND b > 0 ORDER BY b ASC"),
                   row("a", 2, 4),
                   row("a", 3, 5));

        // compound, mixed order
        createTable("CREATE TABLE %s (a text, b int, c int, PRIMARY KEY (a, b, c)) " +
                    compactOption +
                    " CLUSTERING ORDER BY (b ASC, c DESC)"
        );

        execute("INSERT INTO %s (a, b, c) VALUES ('a', 2, 4)");
        execute("INSERT INTO %s (a, b, c) VALUES ('a', 3, 5)");
        assertRows(execute("SELECT * FROM %s WHERE a = 'a' AND b > 0"),
                   row("a", 2, 4),
                   row("a", 3, 5));

        assertRows(execute("SELECT * FROM %s WHERE a = 'a' AND b > 0 ORDER BY b ASC"),
                   row("a", 2, 4),
                   row("a", 3, 5));
    }


    @Test
    public void testEmptyRestrictionValue() throws Throwable
    {
        for (String options : new String[]{ "", " WITH COMPACT STORAGE" })
        {
            createTable("CREATE TABLE %s (pk blob, c blob, v blob, PRIMARY KEY ((pk), c))" + options);
            execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)",
                    bytes("foo123"), bytes("1"), bytes("1"));
            execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)",
                    bytes("foo123"), bytes("2"), bytes("2"));

            beforeAndAfterFlush(() -> {

                assertInvalidMessage("Key may not be empty", "SELECT * FROM %s WHERE pk = textAsBlob('');");
                assertInvalidMessage("Key may not be empty", "SELECT * FROM %s WHERE pk IN (textAsBlob(''), textAsBlob('1'));");

                assertInvalidMessage("Key may not be empty",
                                     "INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)",
                                     EMPTY_BYTE_BUFFER, bytes("2"), bytes("2"));

                // Test clustering columns restrictions
                assertEmpty(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c = textAsBlob('');"));

                assertEmpty(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c) = (textAsBlob(''));"));

                assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c IN (textAsBlob(''), textAsBlob('1'));"),
                           row(bytes("foo123"), bytes("1"), bytes("1")));

                assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c) IN ((textAsBlob('')), (textAsBlob('1')));"),
                           row(bytes("foo123"), bytes("1"), bytes("1")));

                assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c > textAsBlob('');"),
                           row(bytes("foo123"), bytes("1"), bytes("1")),
                           row(bytes("foo123"), bytes("2"), bytes("2")));

                assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c) > (textAsBlob(''));"),
                           row(bytes("foo123"), bytes("1"), bytes("1")),
                           row(bytes("foo123"), bytes("2"), bytes("2")));

                assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c >= textAsBlob('');"),
                           row(bytes("foo123"), bytes("1"), bytes("1")),
                           row(bytes("foo123"), bytes("2"), bytes("2")));

                assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c) >= (textAsBlob(''));"),
                           row(bytes("foo123"), bytes("1"), bytes("1")),
                           row(bytes("foo123"), bytes("2"), bytes("2")));

                assertEmpty(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c <= textAsBlob('');"));

                assertEmpty(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c) <= (textAsBlob(''));"));

                assertEmpty(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c < textAsBlob('');"));

                assertEmpty(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c) < (textAsBlob(''));"));

                assertEmpty(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c > textAsBlob('') AND c < textAsBlob('');"));
            });

            if (options.contains("COMPACT"))
            {
                assertInvalidMessage("Invalid empty or null value for column c",
                                     "INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)",
                                     bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("4"));
            }
            else
            {
                execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)",
                        bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("4"));

                beforeAndAfterFlush(() -> {
                    assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c = textAsBlob('');"),
                               row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("4")));

                    assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c) = (textAsBlob(''));"),
                               row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("4")));

                    assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c IN (textAsBlob(''), textAsBlob('1'));"),
                               row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("4")),
                               row(bytes("foo123"), bytes("1"), bytes("1")));

                    assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c) IN ((textAsBlob('')), (textAsBlob('1')));"),
                               row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("4")),
                               row(bytes("foo123"), bytes("1"), bytes("1")));

                    assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c > textAsBlob('');"),
                               row(bytes("foo123"), bytes("1"), bytes("1")),
                               row(bytes("foo123"), bytes("2"), bytes("2")));

                    assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c) > (textAsBlob(''));"),
                               row(bytes("foo123"), bytes("1"), bytes("1")),
                               row(bytes("foo123"), bytes("2"), bytes("2")));

                    assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c >= textAsBlob('');"),
                               row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("4")),
                               row(bytes("foo123"), bytes("1"), bytes("1")),
                               row(bytes("foo123"), bytes("2"), bytes("2")));

                    assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c) >= (textAsBlob(''));"),
                               row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("4")),
                               row(bytes("foo123"), bytes("1"), bytes("1")),
                               row(bytes("foo123"), bytes("2"), bytes("2")));

                    assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c <= textAsBlob('');"),
                               row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("4")));

                    assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c) <= (textAsBlob(''));"),
                               row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("4")));

                    assertEmpty(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c < textAsBlob('');"));

                    assertEmpty(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c) < (textAsBlob(''));"));

                    assertEmpty(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c >= textAsBlob('') AND c < textAsBlob('');"));
                });
            }

            // Test restrictions on non-primary key value
            assertEmpty(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND v = textAsBlob('') ALLOW FILTERING;"));

            execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)",
                    bytes("foo123"), bytes("3"), EMPTY_BYTE_BUFFER);

            beforeAndAfterFlush(() -> {
                assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND v = textAsBlob('') ALLOW FILTERING;"),
                           row(bytes("foo123"), bytes("3"), EMPTY_BYTE_BUFFER));
            });
        }
    }

    @Test
    public void testEmptyRestrictionValueWithMultipleClusteringColumns() throws Throwable
    {
        createTable("CREATE TABLE %s (pk blob, c1 blob, c2 blob, v blob, PRIMARY KEY (pk, c1, c2))" + compactOption);
        execute("INSERT INTO %s (pk, c1, c2, v) VALUES (?, ?, ?, ?)", bytes("foo123"), bytes("1"), bytes("1"), bytes("1"));
        execute("INSERT INTO %s (pk, c1, c2, v) VALUES (?, ?, ?, ?)", bytes("foo123"), bytes("1"), bytes("2"), bytes("2"));

        beforeAndAfterFlush(() -> {

            assertEmpty(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c1 = textAsBlob('');"));

            assertEmpty(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c1 = textAsBlob('1') AND c2 = textAsBlob('');"));

            assertEmpty(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c1, c2) = (textAsBlob('1'), textAsBlob(''));"));

            assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c1 IN (textAsBlob(''), textAsBlob('1')) AND c2 = textAsBlob('1');"),
                       row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")));

            assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c1 = textAsBlob('1') AND c2 IN (textAsBlob(''), textAsBlob('1'));"),
                       row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")));

            assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c1, c2) IN ((textAsBlob(''), textAsBlob('1')), (textAsBlob('1'), textAsBlob('1')));"),
                       row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")));

            assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c1 > textAsBlob('');"),
                       row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")),
                       row(bytes("foo123"), bytes("1"), bytes("2"), bytes("2")));

            assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c1 = textAsBlob('1') AND c2 > textAsBlob('');"),
                       row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")),
                       row(bytes("foo123"), bytes("1"), bytes("2"), bytes("2")));

            assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c1, c2) > (textAsBlob(''), textAsBlob('1'));"),
                       row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")),
                       row(bytes("foo123"), bytes("1"), bytes("2"), bytes("2")));

            assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c1 = textAsBlob('1') AND c2 >= textAsBlob('');"),
                       row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")),
                       row(bytes("foo123"), bytes("1"), bytes("2"), bytes("2")));

            assertEmpty(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c1 = textAsBlob('1') AND c2 <= textAsBlob('');"));

            assertEmpty(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c1, c2) <= (textAsBlob('1'), textAsBlob(''));"));
        });

        execute("INSERT INTO %s (pk, c1, c2, v) VALUES (?, ?, ?, ?)",
                bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("1"), bytes("4"));

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c1 = textAsBlob('');"),
                       row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("1"), bytes("4")));

            assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c1 = textAsBlob('') AND c2 = textAsBlob('1');"),
                       row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("1"), bytes("4")));

            assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c1, c2) = (textAsBlob(''), textAsBlob('1'));"),
                       row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("1"), bytes("4")));

            assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c1 IN (textAsBlob(''), textAsBlob('1')) AND c2 = textAsBlob('1');"),
                       row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("1"), bytes("4")),
                       row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")));

            assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c1, c2) IN ((textAsBlob(''), textAsBlob('1')), (textAsBlob('1'), textAsBlob('1')));"),
                       row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("1"), bytes("4")),
                       row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")));

            assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c1, c2) > (textAsBlob(''), textAsBlob('1'));"),
                       row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")),
                       row(bytes("foo123"), bytes("1"), bytes("2"), bytes("2")));

            assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c1, c2) >= (textAsBlob(''), textAsBlob('1'));"),
                       row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("1"), bytes("4")),
                       row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")),
                       row(bytes("foo123"), bytes("1"), bytes("2"), bytes("2")));

            assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c1, c2) <= (textAsBlob(''), textAsBlob('1'));"),
                       row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("1"), bytes("4")));

            assertEmpty(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c1, c2) < (textAsBlob(''), textAsBlob('1'));"));
        });
    }

    @Test
    public void testEmptyRestrictionValueWithOrderBy() throws Throwable
    {
        for (String options : new String[]{ " WITH COMPACT STORAGE",
                                            " WITH COMPACT STORAGE AND CLUSTERING ORDER BY (c DESC)" })
        {
            String orderingClause = options.contains("ORDER") ? "" : "ORDER BY c DESC";

            createTable("CREATE TABLE %s (pk blob, c blob, v blob, PRIMARY KEY ((pk), c))" + options);
            execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)",
                    bytes("foo123"),
                    bytes("1"),
                    bytes("1"));
            execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)",
                    bytes("foo123"),
                    bytes("2"),
                    bytes("2"));

            beforeAndAfterFlush(() -> {

                assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c > textAsBlob('')" + orderingClause),
                           row(bytes("foo123"), bytes("2"), bytes("2")),
                           row(bytes("foo123"), bytes("1"), bytes("1")));

                assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c >= textAsBlob('')" + orderingClause),
                           row(bytes("foo123"), bytes("2"), bytes("2")),
                           row(bytes("foo123"), bytes("1"), bytes("1")));

                assertEmpty(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c < textAsBlob('')" + orderingClause));

                assertEmpty(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c <= textAsBlob('')" + orderingClause));
            });

            assertInvalidMessage("Invalid empty or null value for column c",
                                 "INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)",
                                 bytes("foo123"),
                                 EMPTY_BYTE_BUFFER,
                                 bytes("4"));
        }
    }

    @Test
    public void testEmptyRestrictionValueWithMultipleClusteringColumnsAndOrderBy() throws Throwable
    {
        for (String options : new String[]{ " WITH COMPACT STORAGE",
                                            " WITH COMPACT STORAGE AND CLUSTERING ORDER BY (c1 DESC, c2 DESC)" })
        {
            String orderingClause = options.contains("ORDER") ? "" : "ORDER BY c1 DESC, c2 DESC";

            createTable("CREATE TABLE %s (pk blob, c1 blob, c2 blob, v blob, PRIMARY KEY (pk, c1, c2))" + options);
            execute("INSERT INTO %s (pk, c1, c2, v) VALUES (?, ?, ?, ?)", bytes("foo123"), bytes("1"), bytes("1"), bytes("1"));
            execute("INSERT INTO %s (pk, c1, c2, v) VALUES (?, ?, ?, ?)", bytes("foo123"), bytes("1"), bytes("2"), bytes("2"));

            beforeAndAfterFlush(() -> {

                assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c1 > textAsBlob('')" + orderingClause),
                           row(bytes("foo123"), bytes("1"), bytes("2"), bytes("2")),
                           row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")));

                assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c1 = textAsBlob('1') AND c2 > textAsBlob('')" + orderingClause),
                           row(bytes("foo123"), bytes("1"), bytes("2"), bytes("2")),
                           row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")));

                assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c1, c2) > (textAsBlob(''), textAsBlob('1'))" + orderingClause),
                           row(bytes("foo123"), bytes("1"), bytes("2"), bytes("2")),
                           row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")));

                assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c1 = textAsBlob('1') AND c2 >= textAsBlob('')" + orderingClause),
                           row(bytes("foo123"), bytes("1"), bytes("2"), bytes("2")),
                           row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")));
            });

            execute("INSERT INTO %s (pk, c1, c2, v) VALUES (?, ?, ?, ?)",
                    bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("1"), bytes("4"));

            beforeAndAfterFlush(() -> {

                assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c1 IN (textAsBlob(''), textAsBlob('1')) AND c2 = textAsBlob('1')" + orderingClause),
                           row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")),
                           row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("1"), bytes("4")));

                assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c1, c2) IN ((textAsBlob(''), textAsBlob('1')), (textAsBlob('1'), textAsBlob('1')))" + orderingClause),
                           row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")),
                           row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("1"), bytes("4")));

                assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c1, c2) > (textAsBlob(''), textAsBlob('1'))" + orderingClause),
                           row(bytes("foo123"), bytes("1"), bytes("2"), bytes("2")),
                           row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")));

                assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c1, c2) >= (textAsBlob(''), textAsBlob('1'))" + orderingClause),
                           row(bytes("foo123"), bytes("1"), bytes("2"), bytes("2")),
                           row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")),
                           row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("1"), bytes("4")));
            });
        }
    }

    /**
     * UpdateTest
     */
    @Test
    public void testUpdate() throws Throwable
    {
        testUpdate(false);
        testUpdate(true);
    }

    private void testUpdate(boolean forceFlush) throws Throwable
    {
        createTable("CREATE TABLE %s (partitionKey int," +
                    "clustering_1 int," +
                    "value int," +
                    " PRIMARY KEY (partitionKey, clustering_1))" + compactOption);

        execute("INSERT INTO %s (partitionKey, clustering_1, value) VALUES (0, 0, 0)");
        execute("INSERT INTO %s (partitionKey, clustering_1, value) VALUES (0, 1, 1)");
        execute("INSERT INTO %s (partitionKey, clustering_1, value) VALUES (0, 2, 2)");
        execute("INSERT INTO %s (partitionKey, clustering_1, value) VALUES (0, 3, 3)");
        execute("INSERT INTO %s (partitionKey, clustering_1, value) VALUES (1, 0, 4)");

        flush(forceFlush);

        execute("UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 = ?", 7, 0, 1);
        flush(forceFlush);
        assertRows(execute("SELECT value FROM %s WHERE partitionKey = ? AND clustering_1 = ?",
                           0, 1),
                   row(7));

        execute("UPDATE %s SET value = ? WHERE partitionKey = ? AND (clustering_1) = (?)", 8, 0, 2);
        flush(forceFlush);
        assertRows(execute("SELECT value FROM %s WHERE partitionKey = ? AND clustering_1 = ?",
                           0, 2),
                   row(8));

        execute("UPDATE %s SET value = ? WHERE partitionKey IN (?, ?) AND clustering_1 = ?", 9, 0, 1, 0);
        flush(forceFlush);
        assertRows(execute("SELECT * FROM %s WHERE partitionKey IN (?, ?) AND clustering_1 = ?",
                           0, 1, 0),
                   row(0, 0, 9),
                   row(1, 0, 9));

        execute("UPDATE %s SET value = ? WHERE partitionKey IN ? AND clustering_1 = ?", 19, Arrays.asList(0, 1), 0);
        flush(forceFlush);
        assertRows(execute("SELECT * FROM %s WHERE partitionKey IN ? AND clustering_1 = ?",
                           Arrays.asList(0, 1), 0),
                   row(0, 0, 19),
                   row(1, 0, 19));

        execute("UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 IN (?, ?)", 10, 0, 1, 0);
        flush(forceFlush);
        assertRows(execute("SELECT * FROM %s WHERE partitionKey = ? AND clustering_1 IN (?, ?)",
                           0, 1, 0),
                   row(0, 0, 10),
                   row(0, 1, 10));

        execute("UPDATE %s SET value = ? WHERE partitionKey = ? AND (clustering_1) IN ((?), (?))", 20, 0, 0, 1);
        flush(forceFlush);
        assertRows(execute("SELECT * FROM %s WHERE partitionKey = ? AND (clustering_1) IN ((?), (?))",
                           0, 0, 1),
                   row(0, 0, 20),
                   row(0, 1, 20));

        execute("UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 = ?", null, 0, 0);
        flush(forceFlush);

        if (isEmpty(compactOption))
        {
            assertRows(execute("SELECT * FROM %s WHERE partitionKey = ? AND (clustering_1) IN ((?), (?))",
                               0, 0, 1),
                       row(0, 0, null),
                       row(0, 1, 20));
        }
        else
        {
            assertRows(execute("SELECT * FROM %s WHERE partitionKey = ? AND (clustering_1) IN ((?), (?))",
                               0, 0, 1),
                       row(0, 1, 20));
        }

        // test invalid queries

        // missing primary key element
        assertInvalidMessage("Some partition key parts are missing: partitionkey",
                             "UPDATE %s SET value = ? WHERE clustering_1 = ? ", 7, 1);

        assertInvalidMessage("Some clustering keys are missing: clustering_1",
                             "UPDATE %s SET value = ? WHERE partitionKey = ?", 7, 0);

        assertInvalidMessage("Some clustering keys are missing: clustering_1",
                             "UPDATE %s SET value = ? WHERE partitionKey = ?", 7, 0);

        // token function
        assertInvalidMessage("The token function cannot be used in WHERE clauses for UPDATE statements",
                             "UPDATE %s SET value = ? WHERE token(partitionKey) = token(?) AND clustering_1 = ?",
                             7, 0, 1);

        // multiple time the same value
        assertInvalidSyntax("UPDATE %s SET value = ?, value = ? WHERE partitionKey = ? AND clustering_1 = ?", 7, 0, 1);

        // multiple time same primary key element in WHERE clause
        assertInvalidMessage("clustering_1 cannot be restricted by more than one relation if it includes an Equal",
                             "UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 = ? AND clustering_1 = ?", 7, 0, 1, 1);

        // Undefined column names
        assertInvalidMessage("Undefined column name value1",
                             "UPDATE %s SET value1 = ? WHERE partitionKey = ? AND clustering_1 = ?", 7, 0, 1);

        assertInvalidMessage("Undefined column name partitionkey1",
                             "UPDATE %s SET value = ? WHERE partitionKey1 = ? AND clustering_1 = ?", 7, 0, 1);

        assertInvalidMessage("Undefined column name clustering_3",
                             "UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_3 = ?", 7, 0, 1);

        // Invalid operator in the where clause
        assertInvalidMessage("Only EQ and IN relation are supported on the partition key (unless you use the token() function)",
                             "UPDATE %s SET value = ? WHERE partitionKey > ? AND clustering_1 = ?", 7, 0, 1);

        assertInvalidMessage("Cannot use CONTAINS on non-collection column partitionkey",
                             "UPDATE %s SET value = ? WHERE partitionKey CONTAINS ? AND clustering_1 = ?", 7, 0, 1);

        assertInvalidMessage("Non PRIMARY KEY columns found in where clause: value",
                             "UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 = ? AND value = ?", 7, 0, 1, 3);

        assertInvalidMessage("Slice restrictions are not supported on the clustering columns in UPDATE statements",
                             "UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 > ?", 7, 0, 1);
    }

    @Test
    public void testUpdateWithTwoClusteringColumns() throws Throwable
    {
        testUpdateWithTwoClusteringColumns(false);
        testUpdateWithTwoClusteringColumns(true);
    }

    private void testUpdateWithTwoClusteringColumns(boolean forceFlush) throws Throwable
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

        execute("UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ?", 7, 0, 1, 1);
        flush(forceFlush);
        assertRows(execute("SELECT value FROM %s WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ?",
                           0, 1, 1),
                   row(7));

        execute("UPDATE %s SET value = ? WHERE partitionKey = ? AND (clustering_1, clustering_2) = (?, ?)", 8, 0, 1, 2);
        flush(forceFlush);
        assertRows(execute("SELECT value FROM %s WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ?",
                           0, 1, 2),
                   row(8));

        execute("UPDATE %s SET value = ? WHERE partitionKey IN (?, ?) AND clustering_1 = ? AND clustering_2 = ?", 9, 0, 1, 0, 0);
        flush(forceFlush);
        assertRows(execute("SELECT * FROM %s WHERE partitionKey IN (?, ?) AND clustering_1 = ? AND clustering_2 = ?",
                           0, 1, 0, 0),
                   row(0, 0, 0, 9),
                   row(1, 0, 0, 9));

        execute("UPDATE %s SET value = ? WHERE partitionKey IN ? AND clustering_1 = ? AND clustering_2 = ?", 9, Arrays.asList(0, 1), 0, 0);
        flush(forceFlush);
        assertRows(execute("SELECT * FROM %s WHERE partitionKey IN ? AND clustering_1 = ? AND clustering_2 = ?",
                           Arrays.asList(0, 1), 0, 0),
                   row(0, 0, 0, 9),
                   row(1, 0, 0, 9));

        execute("UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 IN (?, ?)", 12, 0, 1, 1, 2);
        flush(forceFlush);
        assertRows(execute("SELECT * FROM %s WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 IN (?, ?)",
                           0, 1, 1, 2),
                   row(0, 1, 1, 12),
                   row(0, 1, 2, 12));

        execute("UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 IN (?, ?) AND clustering_2 IN (?, ?)", 10, 0, 1, 0, 1, 2);
        flush(forceFlush);
        assertRows(execute("SELECT * FROM %s WHERE partitionKey = ? AND clustering_1 IN (?, ?) AND clustering_2 IN (?, ?)",
                           0, 1, 0, 1, 2),
                   row(0, 0, 1, 10),
                   row(0, 0, 2, 10),
                   row(0, 1, 1, 10),
                   row(0, 1, 2, 10));

        execute("UPDATE %s SET value = ? WHERE partitionKey = ? AND (clustering_1, clustering_2) IN ((?, ?), (?, ?))", 20, 0, 0, 2, 1, 2);
        flush(forceFlush);
        assertRows(execute("SELECT * FROM %s WHERE partitionKey = ? AND (clustering_1, clustering_2) IN ((?, ?), (?, ?))",
                           0, 0, 2, 1, 2),
                   row(0, 0, 2, 20),
                   row(0, 1, 2, 20));

        execute("UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ?", null, 0, 0, 2);
        flush(forceFlush);


        assertRows(execute("SELECT * FROM %s WHERE partitionKey = ? AND (clustering_1, clustering_2) IN ((?, ?), (?, ?))",
                           0, 0, 2, 1, 2),
                   row(0, 1, 2, 20));


        // test invalid queries

        // missing primary key element
        assertInvalidMessage("Some partition key parts are missing: partitionkey",
                             "UPDATE %s SET value = ? WHERE clustering_1 = ? AND clustering_2 = ?", 7, 1, 1);

        String errorMsg = "PRIMARY KEY column \"clustering_2\" cannot be restricted as preceding column \"clustering_1\" is not restricted";

        assertInvalidMessage(errorMsg,
                             "UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_2 = ?", 7, 0, 1);

        assertInvalidMessage("Some clustering keys are missing: clustering_1, clustering_2",
                             "UPDATE %s SET value = ? WHERE partitionKey = ?", 7, 0);

        // token function
        assertInvalidMessage("The token function cannot be used in WHERE clauses for UPDATE statements",
                             "UPDATE %s SET value = ? WHERE token(partitionKey) = token(?) AND clustering_1 = ? AND clustering_2 = ?",
                             7, 0, 1, 1);

        // multiple time the same value
        assertInvalidSyntax("UPDATE %s SET value = ?, value = ? WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ?", 7, 0, 1, 1);

        // multiple time same primary key element in WHERE clause
        assertInvalidMessage("clustering_1 cannot be restricted by more than one relation if it includes an Equal",
                             "UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ? AND clustering_1 = ?", 7, 0, 1, 1, 1);

        // Undefined column names
        assertInvalidMessage("Undefined column name value1",
                             "UPDATE %s SET value1 = ? WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ?", 7, 0, 1, 1);

        assertInvalidMessage("Undefined column name partitionkey1",
                             "UPDATE %s SET value = ? WHERE partitionKey1 = ? AND clustering_1 = ? AND clustering_2 = ?", 7, 0, 1, 1);

        assertInvalidMessage("Undefined column name clustering_3",
                             "UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 = ? AND clustering_3 = ?", 7, 0, 1, 1);

        // Invalid operator in the where clause
        assertInvalidMessage("Only EQ and IN relation are supported on the partition key (unless you use the token() function)",
                             "UPDATE %s SET value = ? WHERE partitionKey > ? AND clustering_1 = ? AND clustering_2 = ?", 7, 0, 1, 1);

        assertInvalidMessage("Cannot use CONTAINS on non-collection column partitionkey",
                             "UPDATE %s SET value = ? WHERE partitionKey CONTAINS ? AND clustering_1 = ? AND clustering_2 = ?", 7, 0, 1, 1);

        assertInvalidMessage("Non PRIMARY KEY columns found in where clause: value",
                             "UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ? AND value = ?", 7, 0, 1, 1, 3);

        assertInvalidMessage("Slice restrictions are not supported on the clustering columns in UPDATE statements",
                             "UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 > ?", 7, 0, 1);

        assertInvalidMessage("Slice restrictions are not supported on the clustering columns in UPDATE statements",
                             "UPDATE %s SET value = ? WHERE partitionKey = ? AND (clustering_1, clustering_2) > (?, ?)", 7, 0, 1, 1);
    }

    @Test
    public void testUpdateWithMultiplePartitionKeyComponents() throws Throwable
    {
        testUpdateWithMultiplePartitionKeyComponents(false);
        testUpdateWithMultiplePartitionKeyComponents(true);
    }

    public void testUpdateWithMultiplePartitionKeyComponents(boolean forceFlush) throws Throwable
    {
        createTable("CREATE TABLE %s (partitionKey_1 int," +
                    "partitionKey_2 int," +
                    "clustering_1 int," +
                    "clustering_2 int," +
                    "value int," +
                    " PRIMARY KEY ((partitionKey_1, partitionKey_2), clustering_1, clustering_2))" + compactOption);

        execute("INSERT INTO %s (partitionKey_1, partitionKey_2, clustering_1, clustering_2, value) VALUES (0, 0, 0, 0, 0)");
        execute("INSERT INTO %s (partitionKey_1, partitionKey_2, clustering_1, clustering_2, value) VALUES (0, 1, 0, 1, 1)");
        execute("INSERT INTO %s (partitionKey_1, partitionKey_2, clustering_1, clustering_2, value) VALUES (0, 1, 1, 1, 2)");
        execute("INSERT INTO %s (partitionKey_1, partitionKey_2, clustering_1, clustering_2, value) VALUES (1, 0, 0, 1, 3)");
        execute("INSERT INTO %s (partitionKey_1, partitionKey_2, clustering_1, clustering_2, value) VALUES (1, 1, 0, 1, 3)");
        flush(forceFlush);

        execute("UPDATE %s SET value = ? WHERE partitionKey_1 = ? AND partitionKey_2 = ? AND clustering_1 = ? AND clustering_2 = ?", 7, 0, 0, 0, 0);
        flush(forceFlush);
        assertRows(execute("SELECT value FROM %s WHERE partitionKey_1 = ? AND partitionKey_2 = ? AND clustering_1 = ? AND clustering_2 = ?",
                           0, 0, 0, 0),
                   row(7));

        execute("UPDATE %s SET value = ? WHERE partitionKey_1 IN (?, ?) AND partitionKey_2 = ? AND clustering_1 = ? AND clustering_2 = ?", 9, 0, 1, 1, 0, 1);
        flush(forceFlush);
        assertRows(execute("SELECT * FROM %s WHERE partitionKey_1 IN (?, ?) AND partitionKey_2 = ? AND clustering_1 = ? AND clustering_2 = ?",
                           0, 1, 1, 0, 1),
                   row(0, 1, 0, 1, 9),
                   row(1, 1, 0, 1, 9));

        execute("UPDATE %s SET value = ? WHERE partitionKey_1 IN (?, ?) AND partitionKey_2 IN (?, ?) AND clustering_1 = ? AND clustering_2 = ?", 10, 0, 1, 0, 1, 0, 1);
        flush(forceFlush);
        assertRows(execute("SELECT * FROM %s"),
                   row(0, 0, 0, 0, 7),
                   row(0, 0, 0, 1, 10),
                   row(0, 1, 0, 1, 10),
                   row(0, 1, 1, 1, 2),
                   row(1, 0, 0, 1, 10),
                   row(1, 1, 0, 1, 10));

        // missing primary key element
        assertInvalidMessage("Some partition key parts are missing: partitionkey_2",
                             "UPDATE %s SET value = ? WHERE partitionKey_1 = ? AND clustering_1 = ? AND clustering_2 = ?", 7, 1, 1);
    }

    @Test
    public void testCfmCompactStorageCQL()
    {
        String keyspace = "cql_test_keyspace_compact";
        String table = "test_table_compact";

        TableMetadata.Builder metadata =
        TableMetadata.builder(keyspace, table)
                     .flags(EnumSet.of(TableMetadata.Flag.DENSE))
                     .addPartitionKeyColumn("pk1", IntegerType.instance)
                     .addPartitionKeyColumn("pk2", AsciiType.instance)
                     .addClusteringColumn("ck1", ReversedType.getInstance(IntegerType.instance))
                     .addClusteringColumn("ck2", IntegerType.instance)
                     .addRegularColumn("reg", IntegerType.instance);

        SchemaLoader.createKeyspace(keyspace, KeyspaceParams.simple(1), metadata);

        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);

        String actual = SchemaCQLHelper.getTableMetadataAsCQL(cfs.metadata(), true, true, true);
        String expected = "CREATE TABLE IF NOT EXISTS cql_test_keyspace_compact.test_table_compact (\n" +
                          "    pk1 varint,\n" +
                          "    pk2 ascii,\n" +
                          "    ck1 varint,\n" +
                          "    ck2 varint,\n" +
                          "    reg varint,\n" +
                          "    PRIMARY KEY ((pk1, pk2), ck1, ck2)\n" +
                          ") WITH COMPACT STORAGE\n" +
                          "    AND ID = " + cfs.metadata.id + "\n" +
                          "    AND CLUSTERING ORDER BY (ck1 DESC, ck2 ASC)";
        assertTrue(String.format("Expected\n%s\nto contain\n%s", actual, expected),
                   actual.contains(expected));
    }

    @Test
    public void testCfmCounterCQL()
    {
        String keyspace = "cql_test_keyspace_counter";
        String table = "test_table_counter";

        TableMetadata.Builder metadata;
        metadata = TableMetadata.builder(keyspace, table)
                                .flags(EnumSet.of(TableMetadata.Flag.DENSE,
                                       TableMetadata.Flag.COUNTER))
                                .isCounter(true)
                                .addPartitionKeyColumn("pk1", IntegerType.instance)
                                .addPartitionKeyColumn("pk2", AsciiType.instance)
                                .addClusteringColumn("ck1", ReversedType.getInstance(IntegerType.instance))
                                .addClusteringColumn("ck2", IntegerType.instance)
                                .addRegularColumn("cnt", CounterColumnType.instance);

        SchemaLoader.createKeyspace(keyspace, KeyspaceParams.simple(1), metadata);

        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);

        String actual = SchemaCQLHelper.getTableMetadataAsCQL(cfs.metadata(), true, true, true);
        String expected = "CREATE TABLE IF NOT EXISTS cql_test_keyspace_counter.test_table_counter (\n" +
                          "    pk1 varint,\n" +
                          "    pk2 ascii,\n" +
                          "    ck1 varint,\n" +
                          "    ck2 varint,\n" +
                          "    cnt counter,\n" +
                          "    PRIMARY KEY ((pk1, pk2), ck1, ck2)\n" +
                          ") WITH COMPACT STORAGE\n" +
                          "    AND ID = " + cfs.metadata.id + "\n" +
                          "    AND CLUSTERING ORDER BY (ck1 DESC, ck2 ASC)";
        assertTrue(String.format("Expected\n%s\nto contain\n%s", actual, expected),
                   actual.contains(expected));
    }

    @Test
    public void testDenseTable() throws Throwable
    {
        String tableName = createTable("CREATE TABLE IF NOT EXISTS %s (" +
                                       "pk1 varint PRIMARY KEY," +
                                       "reg1 int)" +
                                       " WITH COMPACT STORAGE");

        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(tableName);

        String actual = SchemaCQLHelper.getTableMetadataAsCQL(cfs.metadata(), true, true, true);
        String expected = "CREATE TABLE IF NOT EXISTS " + keyspace() + "." + tableName + " (\n" +
                        "    pk1 varint,\n" +
                        "    reg1 int,\n" +
                        "    PRIMARY KEY (pk1)\n" +
                        ") WITH COMPACT STORAGE\n" +
                        "    AND ID = " + cfs.metadata.id + "\n";

        assertTrue(String.format("Expected\n%s\nto contain\n%s", actual, expected),
                   actual.contains(expected));
    }

    @Test
    public void testStaticCompactTable()
    {
        String tableName = createTable("CREATE TABLE IF NOT EXISTS %s (" +
                                       "pk1 varint PRIMARY KEY," +
                                       "reg1 int," +
                                       "reg2 int)" +
                                       " WITH COMPACT STORAGE");

        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(tableName);
        assertTrue(SchemaCQLHelper.getTableMetadataAsCQL(cfs.metadata(), true, true, true).contains(
        "CREATE TABLE IF NOT EXISTS " + keyspace() + "." + tableName + " (\n" +
        "    pk1 varint,\n" +
        "    reg1 int,\n" +
        "    reg2 int,\n" +
        "    PRIMARY KEY (pk1)\n" +
        ") WITH COMPACT STORAGE\n" +
        "    AND ID = " + cfs.metadata.id));
    }

    @Test
    public void testStaticCompactWithCounters()
    {
        String tableName = createTable("CREATE TABLE IF NOT EXISTS %s (" +
                                       "pk1 varint PRIMARY KEY," +
                                       "reg1 counter," +
                                       "reg2 counter)" +
                                       " WITH COMPACT STORAGE");


        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(tableName);

        String actual = SchemaCQLHelper.getTableMetadataAsCQL(cfs.metadata(), true, true, true);
        String expected = "CREATE TABLE IF NOT EXISTS " + keyspace() + "." + tableName + " (\n" +
                          "    pk1 varint,\n" +
                          "    reg1 counter,\n" +
                          "    reg2 counter,\n" +
                          "    PRIMARY KEY (pk1)\n" +
                          ") WITH COMPACT STORAGE\n" +
                          "    AND ID = " + cfs.metadata.id + "\n";
        assertTrue(String.format("Expected\n%s\nto contain\n%s", actual, expected),
                   actual.contains(expected));
    }

    @Test
    public void testDenseCompactTableWithoutRegulars() throws Throwable
    {
        String tableName = createTable("CREATE TABLE IF NOT EXISTS %s (" +
                                       "pk1 varint," +
                                       "ck1 int," +
                                       "PRIMARY KEY (pk1, ck1))" +
                                       " WITH COMPACT STORAGE");

        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(tableName);

        String actual = SchemaCQLHelper.getTableMetadataAsCQL(cfs.metadata(), true, true, true);
        String expected = "CREATE TABLE IF NOT EXISTS " + keyspace() + "." + tableName + " (\n" +
                          "    pk1 varint,\n" +
                          "    ck1 int,\n" +
                          "    PRIMARY KEY (pk1, ck1)\n" +
                          ") WITH COMPACT STORAGE\n" +
                          "    AND ID = " + cfs.metadata.id;
        assertTrue(String.format("Expected\n%s\nto contain\n%s", actual, expected),
                   actual.contains(expected));
    }

    @Test
    public void testCompactDynamic() throws Throwable
    {
        String tableName = createTable("CREATE TABLE IF NOT EXISTS %s (" +
                                       "pk1 varint," +
                                       "ck1 int," +
                                       "reg int," +
                                       "PRIMARY KEY (pk1, ck1))" +
                                       " WITH COMPACT STORAGE");

        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(tableName);

        String actual = SchemaCQLHelper.getTableMetadataAsCQL(cfs.metadata(), true, true, true);
        String expected = "CREATE TABLE IF NOT EXISTS " + keyspace() + "." + tableName + " (\n" +
                          "    pk1 varint,\n" +
                          "    ck1 int,\n" +
                          "    reg int,\n" +
                          "    PRIMARY KEY (pk1, ck1)\n" +
                          ") WITH COMPACT STORAGE\n" +
                          "    AND ID = " + cfs.metadata.id;

        assertTrue(String.format("Expected\n%s\nto contain\n%s", actual, expected),
                   actual.contains(expected));
    }

    /**
     * PartitionUpdateTest
     */

    @Test
    public void testOperationCountWithCompactTable()
    {
        createTable("CREATE TABLE %s (key text PRIMARY KEY, a int) WITH COMPACT STORAGE");
        TableMetadata cfm = currentTableMetadata();

        PartitionUpdate update = new RowUpdateBuilder(cfm, FBUtilities.timestampMicros(), "key0").add("a", 1)
                                                                                                 .buildUpdate();
        Assert.assertEquals(1, update.operationCount());

        update = new RowUpdateBuilder(cfm, FBUtilities.timestampMicros(), "key0").buildUpdate();
        Assert.assertEquals(0, update.operationCount());
    }

    /**
     * AlterTest
     */

    /**
     * Test for CASSANDRA-13917
     */
    @Test
    public void testAlterWithCompactStaticFormat() throws Throwable
    {
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b int, c int) WITH COMPACT STORAGE");

        assertInvalidMessage("Undefined column name column1 in table",
                             "ALTER TABLE %s RENAME column1 TO column2");
    }

    /**
     * Test for CASSANDRA-13917
     */
    @Test
    public void testAlterWithCompactNonStaticFormat() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, PRIMARY KEY (a, b)) WITH COMPACT STORAGE");
        assertInvalidMessage("Undefined column name column1 in table",
                             "ALTER TABLE %s RENAME column1 TO column2");

        createTable("CREATE TABLE %s (a int, b int, v int, PRIMARY KEY (a, b)) WITH COMPACT STORAGE");
        assertInvalidMessage("Undefined column name column1 in table",
                             "ALTER TABLE %s RENAME column1 TO column2");
    }

    /**
     * CreateTest
     */


    /**
     * Test for CASSANDRA-13917
     */
    @Test
    public void testCreateIndextWithCompactStaticFormat() throws Throwable
    {
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b int, c int) WITH COMPACT STORAGE");
        assertInvalidMessage("Undefined column name column1 in table",
                             "CREATE INDEX column1_index on %s (column1)");
        assertInvalidMessage("Undefined column name value in table",
                             "CREATE INDEX value_index on %s (value)");
    }

    /**
     * DeleteTest
     */

    /**
     * Test for CASSANDRA-13917
     */
    @Test
    public void testDeleteWithCompactStaticFormat() throws Throwable
    {
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b int, c int) WITH COMPACT STORAGE");
        testDeleteWithCompactFormat();

        // if column1 is present, hidden column is called column2
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b int, c int, column1 int) WITH COMPACT STORAGE");
        assertInvalidMessage("Undefined column name column2 in table",
                             "DELETE FROM %s WHERE a = 1 AND column2= 1");
        assertInvalidMessage("Undefined column name column2 in table",
                             "DELETE FROM %s WHERE a = 1 AND column2 = 1 AND value1 = 1");
        assertInvalidMessage("Undefined column name column2",
                             "DELETE column2 FROM %s WHERE a = 1");

        // if value is present, hidden column is called value1
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b int, c int, value int) WITH COMPACT STORAGE");
        assertInvalidMessage("Undefined column name value1 in table",
                             "DELETE FROM %s WHERE a = 1 AND value1 = 1");
        assertInvalidMessage("Undefined column name value1 in table",
                             "DELETE FROM %s WHERE a = 1 AND value1 = 1 AND column1 = 1");
        assertInvalidMessage("Undefined column name value1",
                             "DELETE value1 FROM %s WHERE a = 1");
    }

    /**
     * Test for CASSANDRA-13917
     */
    @Test
    public void testDeleteWithCompactNonStaticFormat() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, PRIMARY KEY (a, b)) WITH COMPACT STORAGE");
        execute("INSERT INTO %s (a, b) VALUES (1, 1)");
        execute("INSERT INTO %s (a, b) VALUES (2, 1)");
        assertRows(execute("SELECT a, b FROM %s"),
                   row(1, 1),
                   row(2, 1));
        testDeleteWithCompactFormat();

        createTable("CREATE TABLE %s (a int, b int, v int, PRIMARY KEY (a, b)) WITH COMPACT STORAGE");
        execute("INSERT INTO %s (a, b, v) VALUES (1, 1, 3)");
        execute("INSERT INTO %s (a, b, v) VALUES (2, 1, 4)");
        assertRows(execute("SELECT a, b, v FROM %s"),
                   row(1, 1, 3),
                   row(2, 1, 4));
        testDeleteWithCompactFormat();
    }

    private void testDeleteWithCompactFormat() throws Throwable
    {
        assertInvalidMessage("Undefined column name value in table",
                             "DELETE FROM %s WHERE a = 1 AND value = 1");
        assertInvalidMessage("Undefined column name column1 in table",
                             "DELETE FROM %s WHERE a = 1 AND column1= 1");
        assertInvalidMessage("Undefined column name value in table",
                             "DELETE FROM %s WHERE a = 1 AND value = 1 AND column1 = 1");
        assertInvalidMessage("Undefined column name value",
                             "DELETE value FROM %s WHERE a = 1");
        assertInvalidMessage("Undefined column name column1",
                             "DELETE column1 FROM %s WHERE a = 1");
    }

    /**
     * InsertTest
     */

    /**
     * Test for CASSANDRA-13917
     */
    @Test
    public void testInsertWithCompactStaticFormat() throws Throwable
    {
        testInsertWithCompactTable("CREATE TABLE %s (a int PRIMARY KEY, b int, c int) WITH COMPACT STORAGE");

        // if column1 is present, hidden column is called column2
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b int, c int, column1 int) WITH COMPACT STORAGE");
        execute("INSERT INTO %s (a, b, c, column1) VALUES (1, 1, 1, 1)");
        assertInvalidMessage("Undefined column name column2",
                             "INSERT INTO %s (a, b, c, column2) VALUES (1, 1, 1, 1)");

        // if value is present, hidden column is called value1
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b int, c int, value int) WITH COMPACT STORAGE");
        execute("INSERT INTO %s (a, b, c, value) VALUES (1, 1, 1, 1)");
        assertInvalidMessage("Undefined column name value1",
                             "INSERT INTO %s (a, b, c, value1) VALUES (1, 1, 1, 1)");
    }

    /**
     * Test for CASSANDRA-13917
     */
    @Test
    public void testInsertWithCompactNonStaticFormat() throws Throwable
    {
        testInsertWithCompactTable("CREATE TABLE %s (a int, b int, PRIMARY KEY (a, b)) WITH COMPACT STORAGE");
        testInsertWithCompactTable("CREATE TABLE %s (a int, b int, v int, PRIMARY KEY (a, b)) WITH COMPACT STORAGE");
    }

    private void testInsertWithCompactTable(String tableQuery) throws Throwable
    {
        createTable(tableQuery);

        // pass correct types to the hidden columns
        assertInvalidMessage("Undefined column name column1",
                             "INSERT INTO %s (a, b, column1) VALUES (?, ?, ?)",
                             1, 1, 1, ByteBufferUtil.bytes('a'));
        assertInvalidMessage("Undefined column name value",
                             "INSERT INTO %s (a, b, value) VALUES (?, ?, ?)",
                             1, 1, 1, ByteBufferUtil.bytes('a'));
        assertInvalidMessage("Undefined column name column1",
                             "INSERT INTO %s (a, b, column1, value) VALUES (?, ?, ?, ?)",
                             1, 1, 1, ByteBufferUtil.bytes('a'), ByteBufferUtil.bytes('b'));
        assertInvalidMessage("Undefined column name value",
                             "INSERT INTO %s (a, b, value, column1) VALUES (?, ?, ?, ?)",
                             1, 1, 1, ByteBufferUtil.bytes('a'), ByteBufferUtil.bytes('b'));

        // pass incorrect types to the hidden columns
        assertInvalidMessage("Undefined column name value",
                             "INSERT INTO %s (a, b, value) VALUES (?, ?, ?)",
                             1, 1, 1, 1);
        assertInvalidMessage("Undefined column name column1",
                             "INSERT INTO %s (a, b, column1) VALUES (?, ?, ?)",
                             1, 1, 1, 1);
        assertEmpty(execute("SELECT * FROM %s"));

        // pass null to the hidden columns
        assertInvalidMessage("Undefined column name value",
                             "INSERT INTO %s (a, b, value) VALUES (?, ?, ?)",
                             1, 1, null);
        assertInvalidMessage("Undefined column name column1",
                             "INSERT INTO %s (a, b, column1) VALUES (?, ?, ?)",
                             1, 1, null);
    }

    /**
     * SelectTest
     */

    /**
     * Test for CASSANDRA-13917
     */
    @Test
    public void testSelectWithCompactStaticFormat() throws Throwable
    {
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b int, c int) WITH COMPACT STORAGE");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 1, 1)");
        execute("INSERT INTO %s (a, b, c) VALUES (2, 1, 1)");
        assertRows(execute("SELECT a, b, c FROM %s"),
                   row(1, 1, 1),
                   row(2, 1, 1));
        testSelectWithCompactFormat();

        // if column column1 is present, hidden column is called column2
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b int, c int, column1 int) WITH COMPACT STORAGE");
        execute("INSERT INTO %s (a, b, c, column1) VALUES (1, 1, 1, 1)");
        execute("INSERT INTO %s (a, b, c, column1) VALUES (2, 1, 1, 2)");
        assertRows(execute("SELECT a, b, c, column1 FROM %s"),
                   row(1, 1, 1, 1),
                   row(2, 1, 1, 2));
        assertInvalidMessage("Undefined column name column2 in table",
                             "SELECT a, column2, value FROM %s");

        // if column value is present, hidden column is called value1
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b int, c int, value int) WITH COMPACT STORAGE");
        execute("INSERT INTO %s (a, b, c, value) VALUES (1, 1, 1, 1)");
        execute("INSERT INTO %s (a, b, c, value) VALUES (2, 1, 1, 2)");
        assertRows(execute("SELECT a, b, c, value FROM %s"),
                   row(1, 1, 1, 1),
                   row(2, 1, 1, 2));
        assertInvalidMessage("Undefined column name value1 in table",
                             "SELECT a, value1, value FROM %s");
    }

    /**
     * Test for CASSANDRA-13917
     */
    @Test
    public void testSelectWithCompactNonStaticFormat() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, PRIMARY KEY (a, b)) WITH COMPACT STORAGE");
        execute("INSERT INTO %s (a, b) VALUES (1, 1)");
        execute("INSERT INTO %s (a, b) VALUES (2, 1)");
        assertRows(execute("SELECT a, b FROM %s"),
                   row(1, 1),
                   row(2, 1));
        testSelectWithCompactFormat();

        createTable("CREATE TABLE %s (a int, b int, v int, PRIMARY KEY (a, b)) WITH COMPACT STORAGE");
        execute("INSERT INTO %s (a, b, v) VALUES (1, 1, 3)");
        execute("INSERT INTO %s (a, b, v) VALUES (2, 1, 4)");
        assertRows(execute("SELECT a, b, v FROM %s"),
                   row(1, 1, 3),
                   row(2, 1, 4));
        testSelectWithCompactFormat();
    }

    private void testSelectWithCompactFormat() throws Throwable
    {
        assertInvalidMessage("Undefined column name column1 in table",
                             "SELECT column1 FROM %s");
        assertInvalidMessage("Undefined column name value in table",
                             "SELECT value FROM %s");
        assertInvalidMessage("Undefined column name value in table",
                             "SELECT value, column1 FROM %s");
        assertInvalid("Undefined column name column1 in table ('column1 = NULL')",
                      "SELECT * FROM %s WHERE column1 = null ALLOW FILTERING");
        assertInvalid("Undefined column name value in table ('value = NULL')",
                      "SELECT * FROM %s WHERE value = null ALLOW FILTERING");
        assertInvalidMessage("Undefined column name column1 in table",
                             "SELECT WRITETIME(column1) FROM %s");
        assertInvalidMessage("Undefined column name value in table",
                             "SELECT WRITETIME(value) FROM %s");
    }

    /**
     * UpdateTest
     */

    /**
     * Test for CASSANDRA-13917
     */
    @Test
    public void testUpdateWithCompactStaticFormat() throws Throwable
    {
        testUpdateWithCompactFormat("CREATE TABLE %s (a int PRIMARY KEY, b int, c int) WITH COMPACT STORAGE");

        assertInvalidMessage("Undefined column name column1 in table",
                             "UPDATE %s SET b = 1 WHERE column1 = ?",
                             ByteBufferUtil.bytes('a'));
        assertInvalidMessage("Undefined column name value in table",
                             "UPDATE %s SET b = 1 WHERE value = ?",
                             ByteBufferUtil.bytes('a'));

        // if column1 is present, hidden column is called column2
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b int, c int, column1 int) WITH COMPACT STORAGE");
        execute("INSERT INTO %s (a, b, c, column1) VALUES (1, 1, 1, 1)");
        execute("UPDATE %s SET column1 = 6 WHERE a = 1");
        assertInvalidMessage("Undefined column name column2", "UPDATE %s SET column2 = 6 WHERE a = 0");
        assertInvalidMessage("Undefined column name value", "UPDATE %s SET value = 6 WHERE a = 0");

        // if value is present, hidden column is called value1
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b int, c int, value int) WITH COMPACT STORAGE");
        execute("INSERT INTO %s (a, b, c, value) VALUES (1, 1, 1, 1)");
        execute("UPDATE %s SET value = 6 WHERE a = 1");
        assertInvalidMessage("Undefined column name column1", "UPDATE %s SET column1 = 6 WHERE a = 1");
        assertInvalidMessage("Undefined column name value1", "UPDATE %s SET value1 = 6 WHERE a = 1");
    }

    /**
     * Test for CASSANDRA-13917
     */
    @Test
    public void testUpdateWithCompactNonStaticFormat() throws Throwable
    {
        testUpdateWithCompactFormat("CREATE TABLE %s (a int, b int, PRIMARY KEY (a, b)) WITH COMPACT STORAGE");
        testUpdateWithCompactFormat("CREATE TABLE %s (a int, b int, v int, PRIMARY KEY (a, b)) WITH COMPACT STORAGE");
    }

    private void testUpdateWithCompactFormat(String tableQuery) throws Throwable
    {
        createTable(tableQuery);
        // pass correct types to hidden columns
        assertInvalidMessage("Undefined column name column1",
                             "UPDATE %s SET column1 = ? WHERE a = 0",
                             ByteBufferUtil.bytes('a'));
        assertInvalidMessage("Undefined column name value",
                             "UPDATE %s SET value = ? WHERE a = 0",
                             ByteBufferUtil.bytes('a'));

        // pass incorrect types to hidden columns
        assertInvalidMessage("Undefined column name column1", "UPDATE %s SET column1 = 6 WHERE a = 0");
        assertInvalidMessage("Undefined column name value", "UPDATE %s SET value = 6 WHERE a = 0");
    }
}