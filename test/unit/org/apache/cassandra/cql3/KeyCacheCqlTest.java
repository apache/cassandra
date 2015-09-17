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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.cache.KeyCacheKey;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.metrics.CacheMetrics;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.Pair;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

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
    "col_long       bigint,";
    static final String commonColumns =
    "part_key_a," +
    "part_key_b," +
    "clust_key_a," +
    "clust_key_b," +
    "clust_key_c," + // to make it really big
    "col_text," +
    "col_int," +
    "col_long";

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
        assertEquals(4900, hits);
        assertEquals(5250, requests);

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
        assertEquals(10000, hits);
        assertEquals(10500, requests);

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
        assertEquals(4900, hits);
        assertEquals(5250, requests);

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
        assertEquals(10000, hits);
        assertEquals(10500, requests);

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

    // Inserts 100 partitions split over 10 sstables (flush after 10 partitions).
    // Clustered tables receive 50 CQL rows per partition.
    private void insertData(String table, String index, boolean withClustering) throws Throwable
    {
        StorageService.instance.disableAutoCompaction(KEYSPACE, table);
        Keyspace.open(KEYSPACE).getColumnFamilyStore(table).forceFlush().get();
        Keyspace.open(KEYSPACE).getColumnFamilyStore(table).truncateBlocking();
        if (index != null)
        {
            StorageService.instance.disableAutoCompaction(KEYSPACE, table + '.' + index);
            Keyspace.open(KEYSPACE).getColumnFamilyStore(table).indexManager.getIndexesByNames(ImmutableSet.of(table + "." + index)).iterator().next().forceBlockingFlush();
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
                    Keyspace.open(KEYSPACE).getColumnFamilyStore(table).indexManager.getIndexesByNames(ImmutableSet.of(table + "." + index)).iterator().next().forceBlockingFlush();
            }
        }
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
        for (String name : ImmutableSet.copyOf(CassandraMetricsRegistry.Metrics.getMetrics().keySet()))
        {
            CassandraMetricsRegistry.Metrics.remove(name);
        }

        CacheService.instance.keyCache.clear();
        CacheMetrics metrics = CacheService.instance.keyCache.getMetrics();
        Assert.assertEquals(0, metrics.entries.getValue().intValue());
        Assert.assertEquals(0L, metrics.hits.getCount());
        Assert.assertEquals(0L, metrics.requests.getCount());
        Assert.assertEquals(0L, metrics.size.getValue().longValue());
    }
}
