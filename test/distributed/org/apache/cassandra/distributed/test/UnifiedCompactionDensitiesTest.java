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

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.awaitility.Awaitility;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.ShardManager;
import org.apache.cassandra.db.compaction.UnifiedCompactionStrategy;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.Condition;
import org.apache.cassandra.utils.progress.ProgressEventType;

import static org.apache.cassandra.cql3.TombstonesWithIndexedSSTableTest.makeRandomString;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.utils.concurrent.Condition.newOneTimeCondition;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class UnifiedCompactionDensitiesTest extends TestBaseImpl
{
    @Test
    public void testTargetSSTableSize1Node1Dir() throws IOException
    {
        testTargetSSTableSize(1, 1);
    }

    @Test
    public void testTargetSSTableSize1Node2Dirs() throws IOException
    {
        testTargetSSTableSize(1, 2);
    }

    @Test
    public void testTargetSSTableSize2Nodes1Dir() throws IOException
    {
        testTargetSSTableSize(2, 1);
    }

    @Test
    public void testTargetSSTableSize2Nodes3Dirs() throws IOException
    {
        testTargetSSTableSize(2, 3);
    }

    private void testTargetSSTableSize(int nodeCount, int dataDirs) throws IOException
    {
        try (Cluster cluster = init(builder().withNodes(nodeCount)
                                             .withDataDirCount(dataDirs)
                                             .withConfig(cfg -> cfg.set("memtable_heap_space", "100MiB"))
                                             .start()))
        {
            cluster.schemaChange(withKeyspace("alter keyspace %s with replication = {'class': 'SimpleStrategy', 'replication_factor':1}"));
            cluster.schemaChange(withKeyspace("create table %s.tbl (id bigint primary key, value text) with compaction = {'class':'UnifiedCompactionStrategy', " +
                                                                                                                                    "'target_sstable_size' : '1MiB', " +
                                                                                                                                    "'min_sstable_size' : '0B', " +
                                                                                                                                    "'sstable_growth': '0'}"));
            long targetSize = 1L<<20;
            long targetMin = targetSize * 10 / 16;  // Size must be within sqrt(0.5), sqrt(2) of target, use 1.6 to account for estimations
            long targetMax = targetSize * 16 / 10;
            long toWrite = targetSize * nodeCount * dataDirs * 8; // 8 MiB per data directory, to be guaranteed to be over the 1MiB target size, and also different from the base shard count
            int payloadSize = 1024;
            cluster.forEach(x -> x.nodetool("disableautocompaction"));

            // The first flush will not have the flush size metric initialized, so first check distribution after compaction.
            int i = 0;
            for (; i < 2; ++i)
            {
                writeData(cluster, i * toWrite, toWrite, payloadSize);
                cluster.forEach(x -> x.flush(KEYSPACE));
            }

            cluster.forEach(x -> x.forceCompact(KEYSPACE, "tbl"));
            checkSSTableSizes(nodeCount, cluster, targetMin, targetMax);

            // Now check that the sstables created by flushes are of the right size.
            for (; i < 2; ++i)
            {
                writeData(cluster, i * toWrite, toWrite, payloadSize);
                cluster.forEach(x -> x.flush(KEYSPACE));
            }
            checkSSTableSizes(nodeCount, cluster, targetMin, targetMax);

            // Compact again, as this time there will be independent buckets whose splitting must also work correctly.
            cluster.forEach(x -> x.forceCompact(KEYSPACE, "tbl"));
            checkSSTableSizes(nodeCount, cluster, targetMin, targetMax);
        }
    }

    private static void shortenEstimatedPartitionCountCache()
    {
        try
        {
            Field period = TableMetrics.class.getDeclaredField("ESTIMATED_PARTITION_COUNT_CACHE_PERIOD_SECONDS");
            period.setAccessible(true);
            period.setLong(null, 1);
        }
        catch (ReflectiveOperationException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static void writeData(Cluster cluster, long offset, long toWrite, int payloadSize)
    {
        for (int i = 0; i < toWrite; i += payloadSize)
            cluster.coordinator(1).execute(withKeyspace("insert into %s.tbl (id, value) values (?, ?)"), ConsistencyLevel.ONE, i + offset, makeRandomString(payloadSize));
    }

    /**
     * The per-partition span floor is derived from the table's estimated partition count, so the strategy must
     * rebuild its shard manager when the cached estimate changes, and keep it while the estimate is stable.
     */
    @Test
    public void testShardManagerRebuiltWhenPartitionCountEstimateChanges() throws IOException
    {
        try (Cluster cluster = init(builder().withNodes(1).start()))
        {
            cluster.get(1).runOnInstance(UnifiedCompactionDensitiesTest::shortenEstimatedPartitionCountCache);
            cluster.schemaChange(withKeyspace("create table %s.tbl (id bigint primary key, value text) with compaction = " +
                                              "{'class':'UnifiedCompactionStrategy'}"));

            // Without sstables the estimate is the 65536 default and the shard manager is built with it.
            long[] initial = cluster.get(1).callOnInstance(UnifiedCompactionDensitiesTest::shardManagerIdentityAndEstimate);
            assertEquals(1 << 16, initial[1]);

            for (long id = 0; id < 2000; id++)
                cluster.coordinator(1).execute(withKeyspace("insert into %s.tbl (id, value) values (?, ?)"),
                                               ConsistencyLevel.ONE, id, makeRandomString(100));
            cluster.get(1).flush(KEYSPACE);

            // The gauge recomputes one second after it was last loaded, now from the flushed sstable.
            Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> cluster.get(1).callOnInstance(() -> {
                ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl");
                return ShardManager.estimatedPartitionCountInSSTables(cfs) != 1 << 16;
            }));

            long[] rebuilt = cluster.get(1).callOnInstance(UnifiedCompactionDensitiesTest::shardManagerIdentityAndEstimate);
            // The sstable key count is a cardinality estimate, so allow it some slack around the 2000 partitions.
            assertTrue("Estimated partition count " + rebuilt[1], rebuilt[1] > 1500 && rebuilt[1] < 2500);
            assertNotEquals("Shard manager was not rebuilt after the estimate changed", initial[0], rebuilt[0]);

            long[] unchanged = cluster.get(1).callOnInstance(UnifiedCompactionDensitiesTest::shardManagerIdentityAndEstimate);
            assertEquals(rebuilt[1], unchanged[1]);
            assertEquals("Shard manager was rebuilt although the estimate did not change", rebuilt[0], unchanged[0]);
        }
    }

    private static long[] shardManagerIdentityAndEstimate()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl");
        UnifiedCompactionStrategy strategy = (UnifiedCompactionStrategy) cfs.getCompactionStrategyManager().getStrategies().get(1).get(0);
        try
        {
            // getShardManager is package private and rebuilds the manager when the estimate has changed.
            Method getShardManager = UnifiedCompactionStrategy.class.getDeclaredMethod("getShardManager");
            getShardManager.setAccessible(true);
            return new long[]{ System.identityHashCode(getShardManager.invoke(strategy)),
                               ShardManager.estimatedPartitionCountInSSTables(cfs) };
        }
        catch (ReflectiveOperationException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * On a live node that owns part of the ring, a shard holding more than max_sstables_per_shard_factor times the
     * fanout sstables is compacted when no overlap bucket forms, with the factor set through CQL. With the factor at
     * 0 the same sstables are left alone.
     */
    @Test
    public void testOversizeShardCompactedWithPartialOwnership() throws IOException
    {
        // One data directory, as each directory has its own strategy and would get only some of the sstables.
        try (Cluster cluster = init(builder().withNodes(2).withDataDirCount(1).start()))
        {
            cluster.schemaChange(withKeyspace("alter keyspace %s with replication = {'class': 'SimpleStrategy', 'replication_factor':1}"));
            double[] owned = cluster.get(1).callOnInstance(UnifiedCompactionDensitiesTest::largestOwnedRange);

            assertEquals(6, liveSSTablesAfterCompaction(cluster, owned, "tbl_off", 0));
            assertThat(liveSSTablesAfterCompaction(cluster, owned, "tbl_on", 1), Matchers.lessThanOrEqualTo(4));
        }
    }

    // Creates the table, then writes six batches of keys, each inside its own sixth of node1's owned range and flushed
    // on its own, which gives node1 six disjoint sstables of similar density in one shard. With T4 they never form an
    // overlap bucket, and with a factor of 1 the shard limit is 4, so only the shard check can compact them. Returns
    // the live sstable count after background compaction has run.
    private static int liveSSTablesAfterCompaction(Cluster cluster, double[] owned, String table, int factor)
    {
        cluster.schemaChange(withKeyspace("create table %s." + table + " (id bigint primary key, value text) with compaction = " +
                                          "{'class':'UnifiedCompactionStrategy', 'scaling_parameters':'T4', " +
                                          "'max_sstables_per_shard_factor':'" + factor + "'}"));
        cluster.get(1).nodetool("disableautocompaction", KEYSPACE, table);
        for (List<Long> ids : idsInSlices(owned, 6, 200))
        {
            for (long id : ids)
                cluster.coordinator(1).execute(withKeyspace("insert into %s." + table + " (id, value) values (?, ?)"),
                                               ConsistencyLevel.ONE, id, makeRandomString(100));
            cluster.get(1).flush(KEYSPACE);
        }
        assertEquals(6, (int) cluster.get(1).callOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore(table).getLiveSSTables().size()));

        return cluster.get(1).callOnInstance(() -> {
            ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(table);
            // Let background compaction run several rounds.
            for (int i = 0; i < 5; i++)
                cfs.enableAutoCompaction(true);
            return cfs.getLiveSSTables().size();
        });
    }

    // The largest unwrapped piece of the ranges this node owns, as token values. The end of the ring is reported as
    // the maximum token so that the pair can be used for arithmetic.
    private static double[] largestOwnedRange()
    {
        Range<Token> largest = null;
        for (Range<Token> range : StorageService.instance.getLocalReplicas(KEYSPACE).ranges())
            for (Range<Token> unwrapped : range.unwrap())
                if (largest == null || largest.left.size(largest.right) < unwrapped.left.size(unwrapped.right))
                    largest = unwrapped;
        long left = (Long) largest.left.getTokenValue();
        long right = largest.right.isMinimum() ? Long.MAX_VALUE : (Long) largest.right.getTokenValue();
        return new double[]{ left, right };
    }

    // Ids whose tokens fall in each of the given number of equal slices of the range, perSlice ids per slice.
    private static List<List<Long>> idsInSlices(double[] range, int slices, int perSlice)
    {
        List<List<Long>> result = new ArrayList<>();
        for (int i = 0; i < slices; i++)
            result.add(new ArrayList<>());
        int filled = 0;
        for (long id = 0; filled < slices; id++)
        {
            double token = (Long) Murmur3Partitioner.instance.getToken(LongType.instance.decompose(id)).getTokenValue();
            if (token <= range[0] || token > range[1])
                continue;
            List<Long> slice = result.get(Math.min(slices - 1, (int) ((token - range[0]) / (range[1] - range[0]) * slices)));
            if (slice.size() == perSlice)
                continue;
            slice.add(id);
            if (slice.size() == perSlice)
                filled++;
        }
        return result;
    }

    @Test
    public void testRepairStreamedTinySSTablesFullSubrangeRepair() throws IOException
    {
        testRepairStreamedTinySSTables(false);
    }

    @Test
    public void testRepairStreamedTinySSTablesIncrementalRepair() throws IOException
    {
        testRepairStreamedTinySSTables(true);
    }

    /**
     * Repair streams sstables containing only the keys that differed between replicas. Such files
     * cover a sliver of the token range far below any shard's span but above the coverage floor in
     * {@link org.apache.cassandra.db.compaction.ShardManager}. Without a per-partition span floor
     * (CASSANDRA-21615) they get a density orders of magnitude above any real sstable's, land on
     * high, otherwise empty levels where no overlap set of size >= 2 forms, and are never selected
     * by background compaction, so every repair round grows the live sstable count.
     * <p>
     * Hold background compaction until all repair rounds finish, then verify that it replaces every
     * tiny sstable on the receiving node.
     */
    private void testRepairStreamedTinySSTables(boolean incremental) throws IOException
    {
        final int rounds = 6;
        try (Cluster cluster = init(builder().withNodes(2)
                                             .withConfig(cfg -> cfg.set("hinted_handoff_enabled", false)
                                                                   .with(NETWORK)
                                                                   .with(GOSSIP))
                                             .start()))
        {
            // TableMetrics caches the sstable partition count for five minutes. The first read happens at the first
            // flush, before any sstable exists, so the per-partition span floor would stay unavailable for longer than
            // this test runs. Shorten the period to one second before the table is created.
            cluster.forEach(x -> x.runOnInstance(UnifiedCompactionDensitiesTest::shortenEstimatedPartitionCountCache));

            cluster.schemaChange(withKeyspace("create table %s.tbl (id bigint primary key, value text) with compaction = " +
                                              "{'class':'UnifiedCompactionStrategy', 'scaling_parameters':'T4'}"));
            cluster.get(2).runOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl").disableAutoCompaction());

            // Base data on both replicas, so the table has realistic wide-coverage sstables.
            for (long id = 0; id < 5000; id++)
                cluster.coordinator(1).execute(withKeyspace("insert into %s.tbl (id, value) values (?, ?)"),
                                               ConsistencyLevel.ALL, id, makeRandomString(200));
            cluster.forEach(x -> x.flush(KEYSPACE));

            // Pairs of ids whose tokens are close enough that a two-key repair stream covers a tiny
            // sliver of the ring, and far enough from each other that no two slivers ever overlap.
            List<long[]> pairs = closeTokenPairs(rounds, 1L << 28, 1L << 36);

            for (int round = 0; round < rounds; round++)
            {
                // Wide flushes provide overlapping inputs for the eventual background compaction.
                for (long id = 10000 + round * 1000; id < 10500 + round * 1000; id++)
                    cluster.coordinator(1).execute(withKeyspace("insert into %s.tbl (id, value) values (?, ?)"),
                                                   ConsistencyLevel.ALL, id, makeRandomString(200));
                cluster.forEach(x -> x.flush(KEYSPACE));

                long[] pair = pairs.get(round); // {idA, idB, tokenA, tokenB}
                // Write the pair only to node1, so that node2 must receive it through repair streaming.
                for (int k = 0; k < 2; k++)
                    cluster.get(1).executeInternal(withKeyspace("insert into %s.tbl (id, value) values (?, ?)"),
                                                   pair[k], makeRandomString(100));
                cluster.get(1).flush(KEYSPACE);

                Set<String> beforeRepair = cluster.get(2).callOnInstance(UnifiedCompactionDensitiesTest::tinySSTableNames);
                repair(cluster, incremental, pair[2] - 1, pair[3]);

                // The repair must have actually streamed the missing rows to node2.
                for (int k = 0; k < 2; k++)
                    assertEquals(1, cluster.get(2).executeInternal(withKeyspace("select id from %s.tbl where id = ?"), pair[k]).length);

                Set<String> streamed = cluster.get(2).callOnInstance(UnifiedCompactionDensitiesTest::tinySSTableNames);
                streamed.removeAll(beforeRepair);
                assertTrue("Repair round " + round + " produced no new tiny sstable", !streamed.isEmpty());
            }

            cluster.get(2).runOnInstance(() -> {
                ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl");
                Set<String> originalTinySSTables = tinySSTableNames();
                assertTrue("No tiny sstables left to compact", !originalTinySSTables.isEmpty());
                Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
                    // Incremental repair may need pending-to-repaired promotion before compaction.
                    cfs.enableAutoCompaction(true);
                    Set<String> remaining = cfs.getLiveSSTables().stream()
                                               .map(sstable -> sstable.descriptor.toString())
                                               .filter(originalTinySSTables::contains)
                                               .collect(Collectors.toSet());
                    assertTrue("Background compaction left repair-streamed sstables: " + remaining, remaining.isEmpty());
                });
            });

            // Compacting the repair-streamed sstables must not lose or corrupt their data.
            for (long[] pair : pairs)
                for (int k = 0; k < 2; k++)
                    assertEquals("Repaired row lost after compaction, id " + pair[k], 1,
                                 cluster.get(2).executeInternal(withKeyspace("select id from %s.tbl where id = ?"), pair[k]).length);
            for (long id = 10000; id < 10500; id++)
                assertEquals(1, cluster.get(2).executeInternal(withKeyspace("select id from %s.tbl where id = ?"), id).length);
        }
    }

    private static Set<String> tinySSTableNames()
    {
        // Two-key repair streams are a few hundred bytes; ordinary flush shards are tens of KiB.
        return Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl").getLiveSSTables().stream()
                       .filter(sstable -> sstable.onDiskLength() < 8 * 1024)
                       .map(sstable -> sstable.descriptor.toString())
                       .collect(Collectors.toSet());
    }

    private static void repair(Cluster cluster, boolean incremental, long startToken, long endToken)
    {
        Map<String, String> options = incremental
                                      ? ImmutableMap.of("incremental", "true")
                                      : ImmutableMap.of("incremental", "false", "ranges", startToken + ":" + endToken);
        cluster.get(1).runOnInstance(ExecUtil.rethrow(() -> {
            Condition await = newOneTimeCondition();
            StorageService.instance.repair(KEYSPACE, options, ImmutableList.of((tag, event) -> {
                if (event.getType() == ProgressEventType.COMPLETE)
                    await.signalAll();
            })).right.get();
            assertTrue("Repair did not complete within timeout", await.await(1L, TimeUnit.MINUTES));
        }));
    }

    /**
     * Find pairs of bigint partition keys whose Murmur3 tokens are adjacent within [minGap, maxGap]
     * of each other, with all pairs mutually far apart. Deterministic for a fixed id range.
     */
    private static List<long[]> closeTokenPairs(int count, long minGap, long maxGap)
    {
        TreeMap<Long, Long> tokenToId = new TreeMap<>();
        for (long id = 1_000_000; id < 1_400_000; id++)
            tokenToId.put((Long) Murmur3Partitioner.instance.getToken(LongType.instance.decompose(id)).getTokenValue(), id);

        List<long[]> pairs = new ArrayList<>(count);
        Map.Entry<Long, Long> prev = null;
        long lastChosenToken = Long.MIN_VALUE + (1L << 45);
        for (Map.Entry<Long, Long> entry : tokenToId.entrySet())
        {
            if (pairs.size() == count)
                break;
            if (prev != null)
            {
                long gap = entry.getKey() - prev.getKey();
                if (gap >= minGap && gap <= maxGap && prev.getKey() - lastChosenToken > (1L << 44))
                {
                    pairs.add(new long[]{ prev.getValue(), entry.getValue(), prev.getKey(), entry.getKey() });
                    lastChosenToken = entry.getKey();
                }
            }
            prev = entry;
        }
        assertEquals("Not enough close token pairs found in the candidate id range", count, pairs.size());
        return pairs;
    }

    private void checkSSTableSizes(int nodeCount, Cluster cluster, long targetMin, long targetMax)
    {
        for (int i = 1; i <= nodeCount; ++i)
        {
            LongSummaryStatistics stats = cluster.get(i).callOnInstance(() -> {
                ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl");
                return cfs.getLiveSSTables().stream().mapToLong(SSTableReader::onDiskLength).summaryStatistics();
            });
            long sstableCount = stats.getCount();
            long minSize = stats.getMin();
            long maxSize = stats.getMax();

            LoggerFactory.getLogger(getClass()).info("Node {} sstables {} min/max size: {}/{} avg {} total {}",
                                                     i,
                                                     sstableCount,
                                                     FBUtilities.prettyPrintMemory(minSize),
                                                     FBUtilities.prettyPrintMemory(maxSize),
                                                     FBUtilities.prettyPrintBinary(stats.getAverage(), "", "B"),
                                                     FBUtilities.prettyPrintMemory(stats.getSum()));
            assertThat(sstableCount, Matchers.greaterThan(0L));
            assertThat(minSize, Matchers.greaterThan(targetMin));
            assertThat(maxSize, Matchers.lessThan(targetMax));
        }
    }
}
