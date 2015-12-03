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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.cache.KeyCacheKey;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.metrics.CacheMetrics;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.service.StorageService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNull;
import org.apache.cassandra.utils.Pair;


public class KeyCacheCqlTest extends CQLTester
{

    static final String commonColumnsDef =
    "part_key_a     int," +
    "part_key_b     text," +
    "clust_key_a    int," +
    "clust_key_b    text," +
    "clust_key_c    frozen<list<text>>," + // to make it really big
    "col_text       text," +
    "col_int        int," +
    "col_long       bigint," +
    "col_blob       blob,";
    static final String commonColumns =
    "part_key_a," +
    "part_key_b," +
    "clust_key_a," +
    "clust_key_b," +
    "clust_key_c," + // to make it really big
    "col_text," +
    "col_int," +
    "col_long";

    // 1200 chars
    static final String longString = "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789";

    @Test
    public void testSliceQueries() throws Throwable
    {
        createTable("CREATE TABLE %s (pk text, ck1 int, ck2 int, val text, vpk text, vck1 int, vck2 int, PRIMARY KEY (pk, ck1, ck2))");

        for (int pkInt = 0; pkInt < 20; pkInt++)
        {
            String pk = Integer.toString(pkInt);
            for (int ck1 = 0; ck1 < 10; ck1++)
            {
                for (int ck2 = 0; ck2 < 10; ck2++)
                {
                    execute("INSERT INTO %s (pk, ck1, ck2, val, vpk, vck1, vck2) VALUES (?, ?, ?, ?, ?, ?, ?)",
                            pk, ck1, ck2, makeStringValue(pk, ck1, ck2), pk, ck1, ck2);
                }
            }
        }

        StorageService.instance.forceKeyspaceFlush(KEYSPACE);

        for (int pkInt = 0; pkInt < 20; pkInt++)
        {
            String pk = Integer.toString(pkInt);
            assertClusterRows(execute("SELECT val, vpk, vck1, vck2 FROM %s WHERE pk=?", pk),
                              pk, 0, 10, 0, 10);

            for (int ck1 = 0; ck1 < 10; ck1++)
            {
                assertClusterRows(execute("SELECT val, vpk, vck1, vck2 FROM %s WHERE pk=? AND ck1=?", pk, ck1),
                                  pk, ck1, ck1+1, 0, 10);

                assertClusterRows(execute("SELECT val, vpk, vck1, vck2 FROM %s WHERE pk=? AND ck1<?", pk, ck1),
                                  pk, 0, ck1, 0, 10);
                assertClusterRows(execute("SELECT val, vpk, vck1, vck2 FROM %s WHERE pk=? AND ck1>?", pk, ck1),
                                  pk, ck1+1, 10, 0, 10);
                assertClusterRows(execute("SELECT val, vpk, vck1, vck2 FROM %s WHERE pk=? AND ck1<=?", pk, ck1),
                                  pk, 0, ck1+1, 0, 10);
                assertClusterRows(execute("SELECT val, vpk, vck1, vck2 FROM %s WHERE pk=? AND ck1>=?", pk, ck1),
                                  pk, ck1, 10, 0, 10);

                for (int ck2 = 0; ck2 < 10; ck2++)
                {
                    assertRows(execute("SELECT val, vpk, vck1, vck2 FROM %s WHERE pk=? AND ck1=? AND ck2=?", pk, ck1, ck2),
                               new Object[]{ makeStringValue(pk, ck1, ck2), pk, ck1, ck2 });

                    assertClusterRows(execute("SELECT val, vpk, vck1, vck2 FROM %s WHERE pk=? AND ck1=? AND ck2<?", pk, ck1, ck2),
                                      pk, ck1, ck1+1, 0, ck2);
                    assertClusterRows(execute("SELECT val, vpk, vck1, vck2 FROM %s WHERE pk=? AND ck1=? AND ck2>?", pk, ck1, ck2),
                                      pk, ck1, ck1+1, ck2+1, 10);
                    assertClusterRows(execute("SELECT val, vpk, vck1, vck2 FROM %s WHERE pk=? AND ck1=? AND ck2<=?", pk, ck1, ck2),
                                      pk, ck1, ck1+1, 0, ck2+1);
                    assertClusterRows(execute("SELECT val, vpk, vck1, vck2 FROM %s WHERE pk=? AND ck1=? AND ck2>=?", pk, ck1, ck2),
                                      pk, ck1, ck1+1, ck2, 10);
                }
            }
        }
    }

    private static void assertClusterRows(UntypedResultSet rows, String pk, int ck1from, int ck1to, int ck2from, int ck2to)
    {
        String info = "pk=" + pk + ", ck1from=" + ck1from + ", ck1to=" + ck1to + ", ck2from=" + ck2from + ", ck2to=" + ck2to;
        Iterator<UntypedResultSet.Row> iter = rows.iterator();
        int cnt = 0;
        int expect = (ck1to - ck1from) * (ck2to - ck2from);
        for (int ck1 = ck1from; ck1 < ck1to; ck1++)
        {
            for (int ck2 = ck2from; ck2 < ck2to; ck2++)
            {
                assertTrue("expected " + expect + " (already got " + cnt + ") rows, but more rows are available for " + info, iter.hasNext());
                UntypedResultSet.Row row = iter.next();
                assertEquals(makeStringValue(pk, ck1, ck2), row.getString("val"));
                assertEquals(pk, row.getString("vpk"));
                assertEquals(ck1, row.getInt("vck1"));
                assertEquals(ck2, row.getInt("vck2"));
            }
        }
        assertFalse("expected " + expect + " (already got " + cnt + ") rows, but more rows are available for " + info, iter.hasNext());
    }

    private static String makeStringValue(String pk, int ck1, int ck2)
    {
        return longString + ',' + pk + ',' + ck1 + ',' + ck2;
    }

    @Test
    public void test2iKeyCachePaths() throws Throwable
    {
        String table = createTable("CREATE TABLE %s ("
                                   + commonColumnsDef
                                   + "PRIMARY KEY ((part_key_a, part_key_b),clust_key_a,clust_key_b,clust_key_c))");
        createIndex("CREATE INDEX some_index ON %s (col_int)");
        insertData(table, "some_index", true);
        clearCache();

        CacheMetrics metrics = CacheService.instance.keyCache.getMetrics();

        for (int i = 0; i < 10; i++)
        {
            UntypedResultSet result = execute("SELECT part_key_a FROM %s WHERE col_int = ?", i);
            assertEquals(500, result.size());
        }

        long hits = metrics.hits.getCount();
        long requests = metrics.requests.getCount();
        assertEquals(0, hits);
        assertEquals(210, requests);

        for (int i = 0; i < 10; i++)
        {
            UntypedResultSet result = execute("SELECT part_key_a FROM %s WHERE col_int = ?", i);
            // 100 part-keys * 50 clust-keys
            // indexed on part-key % 10 = 10 index partitions
            // (50 clust-keys  *  100-part-keys  /  10 possible index-values) = 500
            assertEquals(500, result.size());
        }

        metrics = CacheService.instance.keyCache.getMetrics();
        hits = metrics.hits.getCount();
        requests = metrics.requests.getCount();
        assertEquals(200, hits);
        assertEquals(420, requests);

        CacheService.instance.keyCache.submitWrite(Integer.MAX_VALUE).get();

        int beforeSize = CacheService.instance.keyCache.size();

        CacheService.instance.keyCache.clear();

        Assert.assertEquals(0, CacheService.instance.keyCache.size());

        // then load saved
        CacheService.instance.keyCache.loadSaved();

        assertEquals(beforeSize, CacheService.instance.keyCache.size());

        for (int i = 0; i < 10; i++)
        {
            UntypedResultSet result = execute("SELECT part_key_a FROM %s WHERE col_int = ?", i);
            // 100 part-keys * 50 clust-keys
            // indexed on part-key % 10 = 10 index partitions
            // (50 clust-keys  *  100-part-keys  /  10 possible index-values) = 500
            assertEquals(500, result.size());
        }

        //Test Schema.getColumnFamilyStoreIncludingIndexes, several null check paths
        //are defensive and unreachable
        assertNull(Schema.instance.getColumnFamilyStoreIncludingIndexes(Pair.create("foo", "bar")));
        assertNull(Schema.instance.getColumnFamilyStoreIncludingIndexes(Pair.create(KEYSPACE, "bar")));

        dropTable("DROP TABLE %s");
        Schema.instance.updateVersion();

        //Test loading for a dropped 2i/table
        CacheService.instance.keyCache.clear();

        // then load saved
        CacheService.instance.keyCache.loadSaved();

        assertEquals(0, CacheService.instance.keyCache.size());
    }

    @Test
    public void test2iKeyCachePathsSaveKeysForDroppedTable() throws Throwable
    {
        String table = createTable("CREATE TABLE %s ("
                                   + commonColumnsDef
                                   + "PRIMARY KEY ((part_key_a, part_key_b),clust_key_a,clust_key_b,clust_key_c))");
        createIndex("CREATE INDEX some_index ON %s (col_int)");
        insertData(table, "some_index", true);
        clearCache();

        CacheMetrics metrics = CacheService.instance.keyCache.getMetrics();

        for (int i = 0; i < 10; i++)
        {
            UntypedResultSet result = execute("SELECT part_key_a FROM %s WHERE col_int = ?", i);
            assertEquals(500, result.size());
        }

        long hits = metrics.hits.getCount();
        long requests = metrics.requests.getCount();
        assertEquals(0, hits);
        assertEquals(210, requests);

        //

        for (int i = 0; i < 10; i++)
        {
            UntypedResultSet result = execute("SELECT part_key_a FROM %s WHERE col_int = ?", i);
            // 100 part-keys * 50 clust-keys
            // indexed on part-key % 10 = 10 index partitions
            // (50 clust-keys  *  100-part-keys  /  10 possible index-values) = 500
            assertEquals(500, result.size());
        }

        metrics = CacheService.instance.keyCache.getMetrics();
        hits = metrics.hits.getCount();
        requests = metrics.requests.getCount();
        assertEquals(200, hits);
        assertEquals(420, requests);

        dropTable("DROP TABLE %s");

        CacheService.instance.keyCache.submitWrite(Integer.MAX_VALUE).get();

        CacheService.instance.keyCache.clear();

        Assert.assertEquals(0, CacheService.instance.keyCache.size());

        // then load saved
        CacheService.instance.keyCache.loadSaved();

        Iterator<KeyCacheKey> iter = CacheService.instance.keyCache.keyIterator();
        while(iter.hasNext())
        {
            KeyCacheKey key = iter.next();
            Assert.assertFalse(key.ksAndCFName.left.equals("KEYSPACE"));
            Assert.assertFalse(key.ksAndCFName.right.startsWith(table));
        }
    }

    @Test
    public void testKeyCacheNonClustered() throws Throwable
    {
        String table = createTable("CREATE TABLE %s ("
                                   + commonColumnsDef
                                   + "PRIMARY KEY ((part_key_a, part_key_b)))");
        insertData(table, null, false);
        clearCache();

        for (int i = 0; i < 10; i++)
        {
            assertRows(execute("SELECT col_text FROM %s WHERE part_key_a = ? AND part_key_b = ?", i, Integer.toOctalString(i)),
                       new Object[]{ String.valueOf(i) + '-' + String.valueOf(0) });
        }

        CacheMetrics metrics = CacheService.instance.keyCache.getMetrics();
        long hits = metrics.hits.getCount();
        long requests = metrics.requests.getCount();
        assertEquals(0, hits);
        assertEquals(10, requests);

        for (int i = 0; i < 100; i++)
        {
            assertRows(execute("SELECT col_text FROM %s WHERE part_key_a = ? AND part_key_b = ?", i, Integer.toOctalString(i)),
                       new Object[]{ String.valueOf(i) + '-' + String.valueOf(0) });
        }

        hits = metrics.hits.getCount();
        requests = metrics.requests.getCount();
        assertEquals(10, hits);
        assertEquals(120, requests);
    }

    @Test
    public void testKeyCacheClustered() throws Throwable
    {
        String table = createTable("CREATE TABLE %s ("
                                   + commonColumnsDef
                                   + "PRIMARY KEY ((part_key_a, part_key_b),clust_key_a,clust_key_b,clust_key_c))");
        insertData(table, null, true);
        clearCache();

        // query on partition key

        // 10 queries, each 50 result rows
        for (int i = 0; i < 10; i++)
        {
            assertEquals(50, execute("SELECT col_text FROM %s WHERE part_key_a = ? AND part_key_b = ?", i, Integer.toOctalString(i)).size());
        }

        CacheMetrics metrics = CacheService.instance.keyCache.getMetrics();
        long hits = metrics.hits.getCount();
        long requests = metrics.requests.getCount();
        assertEquals(0, hits);
        assertEquals(10, requests);

        // 10 queries, each 50 result rows
        for (int i = 0; i < 10; i++)
        {
            assertEquals(50, execute("SELECT col_text FROM %s WHERE part_key_a = ? AND part_key_b = ?", i, Integer.toOctalString(i)).size());
        }

        metrics = CacheService.instance.keyCache.getMetrics();
        hits = metrics.hits.getCount();
        requests = metrics.requests.getCount();
        assertEquals(10, hits);
        assertEquals(10 + 10, requests);

        // 100 queries - must get a hit in key-cache
        for (int i = 0; i < 10; i++)
        {
            for (int c = 0; c < 10; c++)
            {
                assertRows(execute("SELECT col_text, col_long FROM %s WHERE part_key_a = ? AND part_key_b = ? and clust_key_a = ?", i, Integer.toOctalString(i), c),
                           new Object[]{ String.valueOf(i) + '-' + String.valueOf(c), (long) c });
            }
        }

        metrics = CacheService.instance.keyCache.getMetrics();
        hits = metrics.hits.getCount();
        requests = metrics.requests.getCount();
        assertEquals(10 + 100, hits);
        assertEquals(20 + 100, requests);

        // 5000 queries - first 10 partitions already in key cache
        for (int i = 0; i < 100; i++)
        {
            for (int c = 0; c < 50; c++)
            {
                assertRows(execute("SELECT col_text, col_long FROM %s WHERE part_key_a = ? AND part_key_b = ? and clust_key_a = ?", i, Integer.toOctalString(i), c),
                           new Object[]{ String.valueOf(i) + '-' + String.valueOf(c), (long) c });
            }
        }

        hits = metrics.hits.getCount();
        requests = metrics.requests.getCount();
        assertEquals(110 + 4910, hits);
        assertEquals(120 + 5500, requests);
    }

    // Inserts 100 partitions split over 10 sstables (flush after 10 partitions).
    // Clustered tables receive 50 CQL rows per partition.
    private void insertData(String table, String index, boolean withClustering) throws Throwable
    {
        prepareTable(table);
        if (index != null)
        {
            StorageService.instance.disableAutoCompaction(KEYSPACE, table + '.' + index);
            triggerBlockingFlush(Keyspace.open(KEYSPACE).getColumnFamilyStore(table).indexManager.getIndexByName(index));
        }

        for (int i = 0; i < 100; i++)
        {
            int partKeyA = i;
            String partKeyB = Integer.toOctalString(i);
            for (int c = 0; c < (withClustering ? 50 : 1); c++)
            {
                int clustKeyA = c;
                String clustKeyB = Integer.toOctalString(c);
                List<String> clustKeyC = makeList(clustKeyB);
                String colText = String.valueOf(i) + '-' + String.valueOf(c);
                int colInt = i % 10;
                long colLong = c;
                execute("INSERT INTO %s (" + commonColumns + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        partKeyA, partKeyB,
                        clustKeyA, clustKeyB, clustKeyC,
                        colText, colInt, colLong);
            }

            if (i % 10 == 9)
            {
                Keyspace.open(KEYSPACE).getColumnFamilyStore(table).forceFlush().get();
                if (index != null)
                    triggerBlockingFlush(Keyspace.open(KEYSPACE).getColumnFamilyStore(table).indexManager.getIndexByName(index));
            }
        }
    }

    private static void prepareTable(String table) throws IOException, InterruptedException, java.util.concurrent.ExecutionException
    {
        StorageService.instance.disableAutoCompaction(KEYSPACE, table);
        Keyspace.open(KEYSPACE).getColumnFamilyStore(table).forceFlush().get();
        Keyspace.open(KEYSPACE).getColumnFamilyStore(table).truncateBlocking();
    }

    private static List<String> makeList(String value)
    {
        List<String> list = new ArrayList<>(50);
        for (int i = 0; i < 50; i++)
        {
            list.add(value + i);
        }
        return list;
    }

    private static void clearCache()
    {
        CassandraMetricsRegistry.Metrics.getNames().forEach(CassandraMetricsRegistry.Metrics::remove);
        CacheService.instance.keyCache.clear();
        CacheMetrics metrics = CacheService.instance.keyCache.getMetrics();
        Assert.assertEquals(0, metrics.entries.getValue().intValue());
        Assert.assertEquals(0L, metrics.hits.getCount());
        Assert.assertEquals(0L, metrics.requests.getCount());
        Assert.assertEquals(0L, metrics.size.getValue().longValue());
    }

    private static void triggerBlockingFlush(Index index) throws Exception
    {
        assert index != null;
        Callable<?> flushTask = index.getBlockingFlushTask();
        if (flushTask != null)
            flushTask.call();
    }
}
