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
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cache.KeyCacheKey;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.sstable.filter.BloomFilterMetrics;
import org.apache.cassandra.io.sstable.keycache.KeyCacheSupport;
import org.apache.cassandra.metrics.CacheMetrics;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.schema.CachingParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaKeyspace;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.service.StorageService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class KeyCacheCqlTest extends CQLTester
{
    private static boolean sstableImplCachesKeys;

    private static final String commonColumnsDef =
    "part_key_a     int," +
    "part_key_b     text," +
    "clust_key_a    int," +
    "clust_key_b    text," +
    "clust_key_c    frozen<list<text>>," + // to make it really big
    "col_text       text," +
    "col_int        int," +
    "col_long       bigint," +
    "col_blob       blob,";
    private static final String commonColumns =
    "part_key_a," +
    "part_key_b," +
    "clust_key_a," +
    "clust_key_b," +
    "clust_key_c," + // to make it really big
    "col_text," +
    "col_int," +
    "col_long";

    // 1200 chars
    private static final String longString =
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
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789";

    /**
     * Prevent system tables from populating the key cache to ensure that
     * the test can reliably check the size of the key cache size and its metrics.
     * Test tables will be created with caching enabled manually in the CQL statement,
     * see {@link KeyCacheCqlTest#createTable(String)}.
     *
     * Then call the base class initialization, which must be done after disabling the key cache.
     */
    @BeforeClass
    public static void setUpClass()
    {
        CachingParams.DEFAULT = CachingParams.CACHE_NOTHING;
        CQLTester.setUpClass();
        sstableImplCachesKeys = KeyCacheSupport.isSupportedBy(DatabaseDescriptor.getSelectedSSTableFormat());
    }

    /**
     * Create a table in KEYSPACE_PER_TEST_PER_TEST because it will get dropped synchronously by CQLTester after
     * each test, whereas the default keyspace gets dropped asynchronously and this may cause unexpected
     * flush operations during a test, which would change the expected result of metrics.
     *
     * Then add manual caching, since by default we have disabled cachinng for all other tables, to ensure
     * that we can assert on the key cache size and metrics.
     */
    @Override
    protected String createTable(String query)
    {
        return super.createTable(KEYSPACE_PER_TEST, query + " WITH caching = { 'keys' : 'ALL', 'rows_per_partition' : '0' }");
    }

    @Override
    protected UntypedResultSet execute(String query, Object... values)
    {
        return executeFormattedQuery(formatQuery(KEYSPACE_PER_TEST, query), values);
    }

    @Override
    protected String createIndex(String query)
    {
        return createIndex(KEYSPACE_PER_TEST, query);
    }

    @Override
    protected void dropTable(String query)
    {
        dropTable(KEYSPACE_PER_TEST, query);
    }

    @Test
    public void testSliceQueriesShallowIndexEntry() throws Throwable
    {
        DatabaseDescriptor.setColumnIndexCacheSize(0);
        testSliceQueries();
    }

    @Test
    public void testSliceQueriesIndexInfoOnHeap() throws Throwable
    {
        DatabaseDescriptor.setColumnIndexCacheSize(8);
        testSliceQueries();
    }

    private void testSliceQueries() throws Throwable
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

        StorageService.instance.forceKeyspaceFlush(KEYSPACE_PER_TEST, ColumnFamilyStore.FlushReason.UNIT_TESTS);

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
    public void test2iKeyCachePathsShallowIndexEntry() throws Throwable
    {
        DatabaseDescriptor.setColumnIndexCacheSize(0);
        test2iKeyCachePaths();
    }

    @Test
    public void test2iKeyCachePathsIndexInfoOnHeap() throws Throwable
    {
        DatabaseDescriptor.setColumnIndexCacheSize(8);
        test2iKeyCachePaths();
    }

    private void test2iKeyCachePaths() throws Throwable
    {
        String table = createTable("CREATE TABLE %s ("
                                   + commonColumnsDef
                                   + "PRIMARY KEY ((part_key_a, part_key_b),clust_key_a,clust_key_b,clust_key_c))");
        String indexName = createIndex("CREATE INDEX ON %s (col_int)");
        insertData(table, indexName, true);
        clearCache();

        CacheMetrics metrics = CacheService.instance.keyCache.getMetrics();

        long expectedRequests = 0;

        for (int i = 0; i < 10; i++)
        {
            UntypedResultSet result = execute("SELECT part_key_a FROM %s WHERE col_int = ?", i);
            assertEquals(500, result.size());
            // Index requests and table requests are both added to the same metric
            // We expect 10 requests on the index SSTables and 10 IN requests on the table SSTables + BF false positives
            expectedRequests += recentBloomFilterFalsePositives() + 20;
        }

        long hits = metrics.hits.getCount();
        long requests = metrics.requests.getCount();
        assertEquals(0, hits);
        assertEquals(sstableImplCachesKeys ? expectedRequests : 0, requests);

        for (int i = 0; i < 10; i++)
        {
            UntypedResultSet result = execute("SELECT part_key_a FROM %s WHERE col_int = ?", i);
            // 100 part-keys * 50 clust-keys
            // indexed on part-key % 10 = 10 index partitions
            // (50 clust-keys  *  100-part-keys  /  10 possible index-values) = 500
            assertEquals(500, result.size());
            // Index requests and table requests are both added to the same metric
            // We expect 10 requests on the index SSTables and 10 IN requests on the table SSTables + BF false positives
            expectedRequests += recentBloomFilterFalsePositives() + 20;
        }

        metrics = CacheService.instance.keyCache.getMetrics();
        hits = metrics.hits.getCount();
        requests = metrics.requests.getCount();
        assertEquals(sstableImplCachesKeys ? 200 : 0, hits);
        assertEquals(sstableImplCachesKeys ? expectedRequests : 0, requests);

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

        dropTable("DROP TABLE %s");
        assert Schema.instance.isSameVersion(SchemaKeyspace.calculateSchemaDigest());

        //Test loading for a dropped 2i/table
        CacheService.instance.keyCache.clear();

        // then load saved
        CacheService.instance.keyCache.loadSaved();

        assertEquals(0, CacheService.instance.keyCache.size());
    }

    @Test
    public void test2iKeyCachePathsSaveKeysForDroppedTableShallowIndexEntry() throws Throwable
    {
        DatabaseDescriptor.setColumnIndexCacheSize(0);
        test2iKeyCachePathsSaveKeysForDroppedTable();
    }

    @Test
    public void test2iKeyCachePathsSaveKeysForDroppedTableIndexInfoOnHeap() throws Throwable
    {
        DatabaseDescriptor.setColumnIndexCacheSize(8);
        test2iKeyCachePathsSaveKeysForDroppedTable();
    }

    private void test2iKeyCachePathsSaveKeysForDroppedTable() throws Throwable
    {
        String table = createTable("CREATE TABLE %s ("
                                   + commonColumnsDef
                                   + "PRIMARY KEY ((part_key_a, part_key_b),clust_key_a,clust_key_b,clust_key_c))");
        String indexName = createIndex("CREATE INDEX ON %s (col_int)");
        insertData(table, indexName, true);
        clearCache();

        CacheMetrics metrics = CacheService.instance.keyCache.getMetrics();

        long expectedNumberOfRequests = 0;

        for (int i = 0; i < 10; i++)
        {
            UntypedResultSet result = execute("SELECT part_key_a FROM %s WHERE col_int = ?", i);
            assertEquals(500, result.size());

            // Index requests and table requests are both added to the same metric
            // We expect 10 requests on the index SSTables and 10 IN requests on the table SSTables + BF false positives
            expectedNumberOfRequests += recentBloomFilterFalsePositives() + 20;
        }

        long hits = metrics.hits.getCount();
        long requests = metrics.requests.getCount();
        assertEquals(0, hits);
        assertEquals(sstableImplCachesKeys ? expectedNumberOfRequests : 0, requests);

        for (int i = 0; i < 10; i++)
        {
            UntypedResultSet result = execute("SELECT part_key_a FROM %s WHERE col_int = ?", i);
            // 100 part-keys * 50 clust-keys
            // indexed on part-key % 10 = 10 index partitions
            // (50 clust-keys  *  100-part-keys  /  10 possible index-values) = 500
            assertEquals(500, result.size());

            // Index requests and table requests are both added to the same metric
            // We expect 10 requests on the index SSTables and 10 IN requests on the table SSTables + BF false positives
            expectedNumberOfRequests += recentBloomFilterFalsePositives() + 20;
        }

        metrics = CacheService.instance.keyCache.getMetrics();
        hits = metrics.hits.getCount();
        requests = metrics.requests.getCount();
        assertEquals(sstableImplCachesKeys ? 200 : 0, hits);
        assertEquals(sstableImplCachesKeys ? expectedNumberOfRequests : 0, requests);

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
            TableMetadataRef tableMetadataRef = Schema.instance.getTableMetadataRef(key.tableId);
            Assert.assertFalse(tableMetadataRef.keyspace.equals("KEYSPACE_PER_TEST"));
            Assert.assertFalse(tableMetadataRef.name.startsWith(table));
        }
    }

    @Test
    public void testKeyCacheNonClusteredShallowIndexEntry() throws Throwable
    {
        DatabaseDescriptor.setColumnIndexCacheSize(0);
        testKeyCacheNonClustered();
    }

    @Test
    public void testKeyCacheNonClusteredIndexInfoOnHeap() throws Throwable
    {
        DatabaseDescriptor.setColumnIndexCacheSize(8);
        testKeyCacheNonClustered();
    }

    private void testKeyCacheNonClustered() throws Throwable
    {
        String table = createTable("CREATE TABLE %s ("
                                   + commonColumnsDef
                                   + "PRIMARY KEY ((part_key_a, part_key_b)))");
        insertData(table, null, false);
        clearCache();

        long expectedNumberOfRequests = 0;

        CacheMetrics metrics = CacheService.instance.keyCache.getMetrics();
        for (int i = 0; i < 10; i++)
        {
            assertRows(execute("SELECT col_text FROM %s WHERE part_key_a = ? AND part_key_b = ?", i, Integer.toOctalString(i)),
                       new Object[]{ String.valueOf(i) + '-' + String.valueOf(0) });

            // the data for the key is in 1 SSTable but we have to take into account bloom filter false positive
            expectedNumberOfRequests += recentBloomFilterFalsePositives() + 1;
        }

        long hits = metrics.hits.getCount();
        long requests = metrics.requests.getCount();
        assertEquals(0, hits);
        assertEquals(sstableImplCachesKeys ? 10 : 0, requests);

        for (int i = 0; i < 100; i++)
        {
            assertRows(execute("SELECT col_text FROM %s WHERE part_key_a = ? AND part_key_b = ?", i, Integer.toOctalString(i)),
                       new Object[]{ String.valueOf(i) + '-' + String.valueOf(0) });

            // the data for the key is in 1 SSTable but we have to take into account bloom filter false positive
            expectedNumberOfRequests += recentBloomFilterFalsePositives() + 1;
        }

        hits = metrics.hits.getCount();
        requests = metrics.requests.getCount();
        assertEquals(sstableImplCachesKeys ? 10 : 0, hits);
        assertEquals(sstableImplCachesKeys ? expectedNumberOfRequests : 0, requests);
    }

    @Test
    public void testKeyCacheClusteredShallowIndexEntry() throws Throwable
    {
        DatabaseDescriptor.setColumnIndexCacheSize(0);
        testKeyCacheClustered();
    }

    @Test
    public void testKeyCacheClusteredIndexInfoOnHeap() throws Throwable
    {
        DatabaseDescriptor.setColumnIndexCacheSize(8);
        testKeyCacheClustered();
    }

    private void testKeyCacheClustered() throws Throwable
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
        assertEquals(sstableImplCachesKeys ? 10 : 0, requests);

        // 10 queries, each 50 result rows
        for (int i = 0; i < 10; i++)
        {
            assertEquals(50, execute("SELECT col_text FROM %s WHERE part_key_a = ? AND part_key_b = ?", i, Integer.toOctalString(i)).size());
        }

        metrics = CacheService.instance.keyCache.getMetrics();
        hits = metrics.hits.getCount();
        requests = metrics.requests.getCount();
        assertEquals(sstableImplCachesKeys ? 10 : 0, hits);
        assertEquals(sstableImplCachesKeys ? 20 : 0, requests);

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
        assertEquals(sstableImplCachesKeys ? 10 + 100 : 0, hits);
        assertEquals(sstableImplCachesKeys ? 20 + 100 : 0, requests);

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
        assertEquals(sstableImplCachesKeys ? 110 + 4910 : 0, hits);
        assertEquals(sstableImplCachesKeys ? 120 + 5500 : 0, requests);
    }

    // Inserts 100 partitions split over 10 sstables (flush after 10 partitions).
    // Clustered tables receive 50 CQL rows per partition.
    private void insertData(String table, String index, boolean withClustering) throws Throwable
    {
        prepareTable(table);
        if (index != null)
        {
            StorageService.instance.disableAutoCompaction(KEYSPACE_PER_TEST, table + '.' + index);
            triggerBlockingFlush(Keyspace.open(KEYSPACE_PER_TEST).getColumnFamilyStore(table).indexManager.getIndexByName(index));
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
                Keyspace.open(KEYSPACE_PER_TEST).getColumnFamilyStore(table).forceFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS).get();
                if (index != null)
                    triggerBlockingFlush(Keyspace.open(KEYSPACE_PER_TEST).getColumnFamilyStore(table).indexManager.getIndexByName(index));
            }
        }
    }

    private static void prepareTable(String table) throws IOException, InterruptedException, java.util.concurrent.ExecutionException
    {
        StorageService.instance.disableAutoCompaction(KEYSPACE_PER_TEST, table);
        Keyspace.open(KEYSPACE_PER_TEST).getColumnFamilyStore(table).forceFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS).get();
        Keyspace.open(KEYSPACE_PER_TEST).getColumnFamilyStore(table).truncateBlocking();
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

    private long recentBloomFilterFalsePositives()
    {
        return getCurrentColumnFamilyStore(KEYSPACE_PER_TEST).metric.formatSpecificGauges.get(DatabaseDescriptor.getSelectedSSTableFormat())
                                                                                         .get(BloomFilterMetrics.instance.recentBloomFilterFalsePositives.name)
                                                                                         .getValue()
                                                                                         .longValue();
    }
}
