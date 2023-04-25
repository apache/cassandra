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
package org.apache.cassandra.io.sstable.indexsummary;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.Util;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.compaction.CompactionInterruptedException;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.metrics.RestorableMeter;
import org.apache.cassandra.schema.CachingParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.SchemaTestUtil;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.ByteBufferUtil;

import static com.google.common.collect.ImmutableMap.of;
import static org.apache.cassandra.Util.assertOnDiskState;
import static org.apache.cassandra.io.sstable.Downsampling.BASE_SAMPLING_LEVEL;
import static org.apache.cassandra.io.sstable.indexsummary.IndexSummaryRedistribution.DOWNSAMPLE_THESHOLD;
import static org.apache.cassandra.io.sstable.indexsummary.IndexSummaryRedistribution.UPSAMPLE_THRESHOLD;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class IndexSummaryManagerTest<R extends SSTableReader & IndexSummarySupport<R>>
{
    private static final Logger logger = LoggerFactory.getLogger(IndexSummaryManagerTest.class);

    int originalMinIndexInterval;
    int originalMaxIndexInterval;
    long originalCapacity;

    private static final String KEYSPACE1 = "IndexSummaryManagerTest";
    // index interval of 8, no key caching
    private static final String CF_STANDARDLOWiINTERVAL = "StandardLowIndexInterval";
    private static final String CF_STANDARDRACE = "StandardRace";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        DatabaseDescriptor.daemonInitialization();
        Assume.assumeTrue("This test make sense only if the default SSTable format support index summary",
                          IndexSummarySupport.isSupportedBy(DatabaseDescriptor.getSelectedSSTableFormat()));

        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARDLOWiINTERVAL)
                                                .minIndexInterval(8)
                                                .maxIndexInterval(256)
                                                .caching(CachingParams.CACHE_NOTHING),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARDRACE)
                                                .minIndexInterval(8)
                                                .maxIndexInterval(256)
                                                .caching(CachingParams.CACHE_NOTHING));
    }

    @Before
    public void beforeTest()
    {
        String ksname = KEYSPACE1;
        String cfname = CF_STANDARDLOWiINTERVAL; // index interval of 8, no key caching
        Keyspace keyspace = Keyspace.open(ksname);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfname);
        originalMinIndexInterval = cfs.metadata().params.minIndexInterval;
        originalMaxIndexInterval = cfs.metadata().params.maxIndexInterval;
        originalCapacity = IndexSummaryManager.instance.getMemoryPoolCapacityInMB();
    }

    @After
    public void afterTest()
    {
        for (CompactionInfo.Holder holder : CompactionManager.instance.active.getCompactions())
        {
            holder.stop();
        }

        String ksname = KEYSPACE1;
        String cfname = CF_STANDARDLOWiINTERVAL; // index interval of 8, no key caching
        Keyspace keyspace = Keyspace.open(ksname);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfname);

        SchemaTestUtil.announceTableUpdate(cfs.metadata().unbuild().minIndexInterval(originalMinIndexInterval).build());
        SchemaTestUtil.announceTableUpdate(cfs.metadata().unbuild().maxIndexInterval(originalMaxIndexInterval).build());

        IndexSummaryManager.instance.setMemoryPoolCapacityInMB(originalCapacity);
    }

    private long totalOffHeapSize(List<R> sstables)
    {
        long total = 0;
        for (R sstable : sstables)
            total += sstable.getIndexSummary().getOffHeapSize();
        return total;
    }

    private List<R> resetSummaries(ColumnFamilyStore cfs, List<R> sstables, long originalOffHeapSize) throws IOException
    {
        for (R sstable : sstables)
            sstable.overrideReadMeter(new RestorableMeter(100.0, 100.0));

        try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.UNKNOWN))
        {
            sstables = redistributeSummaries(Collections.emptyList(), of(cfs.metadata.id, txn), originalOffHeapSize * sstables.size());
        }
        for (R sstable : sstables)
            assertEquals(BASE_SAMPLING_LEVEL, sstable.getIndexSummary().getSamplingLevel());

        return sstables;
    }

    private void validateData(ColumnFamilyStore cfs, int numPartition)
    {
        for (int i = 0; i < numPartition; i++)
        {
            Row row = Util.getOnlyRowUnfiltered(Util.cmd(cfs, String.format("%3d", i)).build());
            Cell<?> cell = row.getCell(cfs.metadata().getColumn(ByteBufferUtil.bytes("val")));
            assertNotNull(cell);
            assertEquals(100, cell.buffer().array().length);

        }
    }

    private final Comparator<SSTableReader> hotnessComparator = Comparator.comparingDouble(o -> o.getReadMeter().fifteenMinuteRate());

    private void createSSTables(String ksname, String cfname, int numSSTables, int numPartition)
    {
        Keyspace keyspace = Keyspace.open(ksname);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfname);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();

        ArrayList<Future> futures = new ArrayList<>(numSSTables);
        ByteBuffer value = ByteBuffer.wrap(new byte[100]);
        for (int sstable = 0; sstable < numSSTables; sstable++)
        {
            for (int p = 0; p < numPartition; p++)
            {

                String key = String.format("%3d", p);
                new RowUpdateBuilder(cfs.metadata(), 0, key)
                    .clustering("column")
                    .add("val", value)
                    .build()
                    .applyUnsafe();
            }
            futures.add(cfs.forceFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS));
        }
        for (Future future : futures)
        {
            try
            {
                future.get();
            }
            catch (InterruptedException | ExecutionException e)
            {
                throw new RuntimeException(e);
            }
        }
        assertEquals(numSSTables, ServerTestUtils.getLiveIndexSummarySupportingReaders(cfs).size());
        validateData(cfs, numPartition);
    }

    @Test
    public <R extends SSTableReader & IndexSummarySupport<R>> void testChangeMinIndexInterval() throws IOException
    {
        String ksname = KEYSPACE1;
        String cfname = CF_STANDARDLOWiINTERVAL; // index interval of 8, no key caching
        Keyspace keyspace = Keyspace.open(ksname);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfname);
        int numSSTables = 1;
        int numRows = 256;
        createSSTables(ksname, cfname, numSSTables, numRows);

        List<R> sstables = ServerTestUtils.getLiveIndexSummarySupportingReaders(cfs);
        for (R sstable : sstables)
            sstable.overrideReadMeter(new RestorableMeter(100.0, 100.0));

        for (R sstable : sstables)
            assertEquals(cfs.metadata().params.minIndexInterval, sstable.getIndexSummary().getEffectiveIndexInterval(), 0.001);

        // double the min_index_interval
        SchemaTestUtil.announceTableUpdate(cfs.metadata().unbuild().minIndexInterval(originalMinIndexInterval * 2).build());
        IndexSummaryManager.instance.redistributeSummaries();
        for (R sstable : ServerTestUtils.<R>getLiveIndexSummarySupportingReaders(cfs))
        {
            assertEquals(cfs.metadata().params.minIndexInterval, sstable.getIndexSummary().getEffectiveIndexInterval(), 0.001);
            assertEquals(numRows / cfs.metadata().params.minIndexInterval, sstable.getIndexSummary().size());
        }

        // return min_index_interval to its original value
        SchemaTestUtil.announceTableUpdate(cfs.metadata().unbuild().minIndexInterval(originalMinIndexInterval).build());
        IndexSummaryManager.instance.redistributeSummaries();
        for (R sstable : ServerTestUtils.<R>getLiveIndexSummarySupportingReaders(cfs))
        {
            assertEquals(cfs.metadata().params.minIndexInterval, sstable.getIndexSummary().getEffectiveIndexInterval(), 0.001);
            assertEquals(numRows / cfs.metadata().params.minIndexInterval, sstable.getIndexSummary().size());
        }

        // halve the min_index_interval, but constrain the available space to exactly what we have now; as a result,
        // the summary shouldn't change
        SchemaTestUtil.announceTableUpdate(cfs.metadata().unbuild().minIndexInterval(originalMinIndexInterval / 2).build());
        R sstable = ServerTestUtils.<R>getLiveIndexSummarySupportingReaders(cfs).iterator().next();
        long summarySpace = sstable.getIndexSummary().getOffHeapSize();
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstable, OperationType.UNKNOWN))
        {
            redistributeSummaries(Collections.emptyList(), of(cfs.metadata.id, txn), summarySpace);
        }

        sstable = ServerTestUtils.<R>getLiveIndexSummarySupportingReaders(cfs).iterator().next();
        assertEquals(originalMinIndexInterval, sstable.getIndexSummary().getEffectiveIndexInterval(), 0.001);
        assertEquals(numRows / originalMinIndexInterval, sstable.getIndexSummary().size());

        // keep the min_index_interval the same, but now give the summary enough space to grow by 50%
        double previousInterval = sstable.getIndexSummary().getEffectiveIndexInterval();
        int previousSize = sstable.getIndexSummary().size();
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstable, OperationType.UNKNOWN))
        {
            redistributeSummaries(Collections.emptyList(), of(cfs.metadata.id, txn), (long) Math.ceil(summarySpace * 1.5));
        }
        sstable = ServerTestUtils.<R>getLiveIndexSummarySupportingReaders(cfs).iterator().next();
        assertEquals(previousSize * 1.5, (double) sstable.getIndexSummary().size(), 1);
        assertEquals(previousInterval * (1.0 / 1.5), sstable.getIndexSummary().getEffectiveIndexInterval(), 0.001);

        // return min_index_interval to it's original value (double it), but only give the summary enough space
        // to have an effective index interval of twice the new min
        SchemaTestUtil.announceTableUpdate(cfs.metadata().unbuild().minIndexInterval(originalMinIndexInterval).build());
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstable, OperationType.UNKNOWN))
        {
            redistributeSummaries(Collections.emptyList(), of(cfs.metadata.id, txn), (long) Math.ceil(summarySpace / 2.0));
        }
        sstable = ServerTestUtils.<R>getLiveIndexSummarySupportingReaders(cfs).iterator().next();
        assertEquals(originalMinIndexInterval * 2, sstable.getIndexSummary().getEffectiveIndexInterval(), 0.001);
        assertEquals(numRows / (originalMinIndexInterval * 2), sstable.getIndexSummary().size());

        // raise the min_index_interval above our current effective interval, but set the max_index_interval lower
        // than what we actually have space for (meaning the index summary would ideally be smaller, but this would
        // result in an effective interval above the new max)
        SchemaTestUtil.announceTableUpdate(cfs.metadata().unbuild().minIndexInterval(originalMinIndexInterval * 4).build());
        SchemaTestUtil.announceTableUpdate(cfs.metadata().unbuild().maxIndexInterval(originalMinIndexInterval * 4).build());
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstable, OperationType.UNKNOWN))
        {
            redistributeSummaries(Collections.emptyList(), of(cfs.metadata.id, txn), 10);
        }
        sstable = ServerTestUtils.<R>getLiveIndexSummarySupportingReaders(cfs).iterator().next();
        assertEquals(cfs.metadata().params.minIndexInterval, sstable.getIndexSummary().getEffectiveIndexInterval(), 0.001);
    }

    @Test
    public void testChangeMaxIndexInterval() throws IOException
    {
        String ksname = KEYSPACE1;
        String cfname = CF_STANDARDLOWiINTERVAL; // index interval of 8, no key caching
        Keyspace keyspace = Keyspace.open(ksname);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfname);
        int numSSTables = 1;
        int numRows = 256;
        createSSTables(ksname, cfname, numSSTables, numRows);

        List<R> sstables = ServerTestUtils.getLiveIndexSummarySupportingReaders(cfs);
        for (R sstable : sstables)
            sstable.overrideReadMeter(new RestorableMeter(100.0, 100.0));

        try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.UNKNOWN))
        {
            redistributeSummaries(Collections.emptyList(), of(cfs.metadata.id, txn), 10);
        }
        sstables = ServerTestUtils.getLiveIndexSummarySupportingReaders(cfs);
        for (R sstable : sstables)
            assertEquals(cfs.metadata().params.maxIndexInterval, sstable.getIndexSummary().getEffectiveIndexInterval(), 0.01);

        // halve the max_index_interval
        SchemaTestUtil.announceTableUpdate(cfs.metadata().unbuild().maxIndexInterval(cfs.metadata().params.maxIndexInterval / 2).build());
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.UNKNOWN))
        {
            redistributeSummaries(Collections.emptyList(), of(cfs.metadata.id, txn), 1);
        }
        sstables = ServerTestUtils.getLiveIndexSummarySupportingReaders(cfs);
        for (R sstable : sstables)
        {
            assertEquals(cfs.metadata().params.maxIndexInterval, sstable.getIndexSummary().getEffectiveIndexInterval(), 0.01);
            assertEquals(numRows / cfs.metadata().params.maxIndexInterval, sstable.getIndexSummary().size());
        }

        // return max_index_interval to its original value
        SchemaTestUtil.announceTableUpdate(cfs.metadata().unbuild().maxIndexInterval(cfs.metadata().params.maxIndexInterval * 2).build());
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.UNKNOWN))
        {
            redistributeSummaries(Collections.emptyList(), of(cfs.metadata.id, txn), 1);
        }
        for (R sstable : ServerTestUtils.<R>getLiveIndexSummarySupportingReaders(cfs))
        {
            assertEquals(cfs.metadata().params.maxIndexInterval, sstable.getIndexSummary().getEffectiveIndexInterval(), 0.01);
            assertEquals(numRows / cfs.metadata().params.maxIndexInterval, sstable.getIndexSummary().size());
        }
    }

    @Test(timeout = 10000)
    public void testRedistributeSummaries() throws IOException
    {
        String ksname = KEYSPACE1;
        String cfname = CF_STANDARDLOWiINTERVAL; // index interval of 8, no key caching
        Keyspace keyspace = Keyspace.open(ksname);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfname);
        int numSSTables = 4;
        int numRows = 256;
        createSSTables(ksname, cfname, numSSTables, numRows);

        int minSamplingLevel = (BASE_SAMPLING_LEVEL * cfs.metadata().params.minIndexInterval) / cfs.metadata().params.maxIndexInterval;

        List<R> sstables = ServerTestUtils.getLiveIndexSummarySupportingReaders(cfs);
        for (R sstable : sstables)
            sstable.overrideReadMeter(new RestorableMeter(100.0, 100.0));

        long singleSummaryOffHeapSpace = sstables.get(0).getIndexSummary().getOffHeapSize();

        // there should be enough space to not downsample anything
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.UNKNOWN))
        {
            sstables = redistributeSummaries(Collections.emptyList(), of(cfs.metadata.id, txn), (singleSummaryOffHeapSpace * numSSTables));
        }
        for (R sstable : sstables)
            assertEquals(BASE_SAMPLING_LEVEL, sstable.getIndexSummary().getSamplingLevel());
        assertEquals(singleSummaryOffHeapSpace * numSSTables, totalOffHeapSize(sstables));
        validateData(cfs, numRows);

        // everything should get cut in half
        assert sstables.size() == 4;
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.UNKNOWN))
        {
            sstables = redistributeSummaries(Collections.emptyList(), of(cfs.metadata.id, txn), (singleSummaryOffHeapSpace * (numSSTables / 2)));
        }
        for (R sstable : sstables)
            assertEquals(BASE_SAMPLING_LEVEL / 2, sstable.getIndexSummary().getSamplingLevel());
        validateData(cfs, numRows);

        // everything should get cut to a quarter
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.UNKNOWN))
        {
            sstables = redistributeSummaries(Collections.emptyList(), of(cfs.metadata.id, txn), (singleSummaryOffHeapSpace * (numSSTables / 4)));
        }
        for (R sstable : sstables)
            assertEquals(BASE_SAMPLING_LEVEL / 4, sstable.getIndexSummary().getSamplingLevel());
        validateData(cfs, numRows);

        // upsample back up to half
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.UNKNOWN))
        {
            sstables = redistributeSummaries(Collections.emptyList(), of(cfs.metadata.id, txn), (singleSummaryOffHeapSpace * (numSSTables / 2) + 4));
        }
        assert sstables.size() == 4;
        for (R sstable : sstables)
            assertEquals(BASE_SAMPLING_LEVEL / 2, sstable.getIndexSummary().getSamplingLevel());
        validateData(cfs, numRows);

        // upsample back up to the original index summary
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.UNKNOWN))
        {
            sstables = redistributeSummaries(Collections.emptyList(), of(cfs.metadata.id, txn), (singleSummaryOffHeapSpace * numSSTables));
        }
        for (R sstable : sstables)
            assertEquals(BASE_SAMPLING_LEVEL, sstable.getIndexSummary().getSamplingLevel());
        validateData(cfs, numRows);

        // make two of the four sstables cold, only leave enough space for three full index summaries,
        // so the two cold sstables should get downsampled to be half of their original size
        sstables.get(0).overrideReadMeter(new RestorableMeter(50.0, 50.0));
        sstables.get(1).overrideReadMeter(new RestorableMeter(50.0, 50.0));
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.UNKNOWN))
        {
            sstables = redistributeSummaries(Collections.emptyList(), of(cfs.metadata.id, txn), (singleSummaryOffHeapSpace * 3));
        }
        Collections.sort(sstables, hotnessComparator);
        assertEquals(BASE_SAMPLING_LEVEL / 2, sstables.get(0).getIndexSummary().getSamplingLevel());
        assertEquals(BASE_SAMPLING_LEVEL / 2, sstables.get(1).getIndexSummary().getSamplingLevel());
        assertEquals(BASE_SAMPLING_LEVEL, sstables.get(2).getIndexSummary().getSamplingLevel());
        assertEquals(BASE_SAMPLING_LEVEL, sstables.get(3).getIndexSummary().getSamplingLevel());
        validateData(cfs, numRows);

        // small increases or decreases in the read rate don't result in downsampling or upsampling
        double lowerRate = 50.0 * (DOWNSAMPLE_THESHOLD + (DOWNSAMPLE_THESHOLD * 0.10));
        double higherRate = 50.0 * (UPSAMPLE_THRESHOLD - (UPSAMPLE_THRESHOLD * 0.10));
        sstables.get(0).overrideReadMeter(new RestorableMeter(lowerRate, lowerRate));
        sstables.get(1).overrideReadMeter(new RestorableMeter(higherRate, higherRate));
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.UNKNOWN))
        {
            sstables = redistributeSummaries(Collections.emptyList(), of(cfs.metadata.id, txn), (singleSummaryOffHeapSpace * 3));
        }
        Collections.sort(sstables, hotnessComparator);
        assertEquals(BASE_SAMPLING_LEVEL / 2, sstables.get(0).getIndexSummary().getSamplingLevel());
        assertEquals(BASE_SAMPLING_LEVEL / 2, sstables.get(1).getIndexSummary().getSamplingLevel());
        assertEquals(BASE_SAMPLING_LEVEL, sstables.get(2).getIndexSummary().getSamplingLevel());
        assertEquals(BASE_SAMPLING_LEVEL, sstables.get(3).getIndexSummary().getSamplingLevel());
        validateData(cfs, numRows);

        // reset, and then this time, leave enough space for one of the cold sstables to not get downsampled
        sstables = resetSummaries(cfs, sstables, singleSummaryOffHeapSpace);
        sstables.get(0).overrideReadMeter(new RestorableMeter(1.0, 1.0));
        sstables.get(1).overrideReadMeter(new RestorableMeter(2.0, 2.0));
        sstables.get(2).overrideReadMeter(new RestorableMeter(1000.0, 1000.0));
        sstables.get(3).overrideReadMeter(new RestorableMeter(1000.0, 1000.0));

        try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.UNKNOWN))
        {
            sstables = redistributeSummaries(Collections.emptyList(), of(cfs.metadata.id, txn), (singleSummaryOffHeapSpace * 3) + 50);
        }
        Collections.sort(sstables, hotnessComparator);

        if (sstables.get(0).getIndexSummary().getSamplingLevel() == minSamplingLevel)
            assertEquals(BASE_SAMPLING_LEVEL, sstables.get(1).getIndexSummary().getSamplingLevel());
        else
            assertEquals(BASE_SAMPLING_LEVEL, sstables.get(0).getIndexSummary().getSamplingLevel());

        assertEquals(BASE_SAMPLING_LEVEL, sstables.get(2).getIndexSummary().getSamplingLevel());
        assertEquals(BASE_SAMPLING_LEVEL, sstables.get(3).getIndexSummary().getSamplingLevel());
        validateData(cfs, numRows);


        // Cause a mix of upsampling and downsampling. We'll leave enough space for two full index summaries. The two
        // coldest sstables will get downsampled to 4/128 of their size, leaving us with 1 and 92/128th index
        // summaries worth of space.  The hottest sstable should get a full index summary, and the one in the middle
        // should get the remainder.
        sstables.get(0).overrideReadMeter(new RestorableMeter(0.0, 0.0));
        sstables.get(1).overrideReadMeter(new RestorableMeter(0.0, 0.0));
        sstables.get(2).overrideReadMeter(new RestorableMeter(92, 92));
        sstables.get(3).overrideReadMeter(new RestorableMeter(128.0, 128.0));
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.UNKNOWN))
        {
            sstables = redistributeSummaries(Collections.emptyList(), of(cfs.metadata.id, txn), (long) (singleSummaryOffHeapSpace + (singleSummaryOffHeapSpace * (92.0 / BASE_SAMPLING_LEVEL))));
        }
        Collections.sort(sstables, hotnessComparator);
        assertEquals(1, sstables.get(0).getIndexSummary().size());  // at the min sampling level
        assertEquals(1, sstables.get(0).getIndexSummary().size());  // at the min sampling level
        assertTrue(sstables.get(2).getIndexSummary().getSamplingLevel() > minSamplingLevel);
        assertTrue(sstables.get(2).getIndexSummary().getSamplingLevel() < BASE_SAMPLING_LEVEL);
        assertEquals(BASE_SAMPLING_LEVEL, sstables.get(3).getIndexSummary().getSamplingLevel());
        validateData(cfs, numRows);

        // Don't leave enough space for even the minimal index summaries
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.UNKNOWN))
        {
            sstables = redistributeSummaries(Collections.emptyList(), of(cfs.metadata.id, txn), 10);
        }
        for (R sstable : sstables)
            assertEquals(1, sstable.getIndexSummary().size());  // at the min sampling level
        validateData(cfs, numRows);
    }

    @Test
    public void testRebuildAtSamplingLevel() throws IOException
    {
        String ksname = KEYSPACE1;
        String cfname = CF_STANDARDLOWiINTERVAL;
        Keyspace keyspace = Keyspace.open(ksname);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfname);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();

        ByteBuffer value = ByteBuffer.wrap(new byte[100]);

        int numRows = 256;
        for (int row = 0; row < numRows; row++)
        {
            String key = String.format("%3d", row);
            new RowUpdateBuilder(cfs.metadata(), 0, key)
            .clustering("column")
            .add("val", value)
            .build()
            .applyUnsafe();
        }

        Util.flush(cfs);

        List<R> sstables = ServerTestUtils.getLiveIndexSummarySupportingReaders(cfs);
        assertEquals(1, sstables.size());
        R original = sstables.get(0);

        R sstable = original;
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstable, OperationType.UNKNOWN))
        {
            for (int samplingLevel = 1; samplingLevel < BASE_SAMPLING_LEVEL; samplingLevel++)
            {
                sstable = sstable.cloneWithNewSummarySamplingLevel(cfs, samplingLevel);
                assertEquals(samplingLevel, sstable.getIndexSummary().getSamplingLevel());
                int expectedSize = (numRows * samplingLevel) / (cfs.metadata().params.minIndexInterval * BASE_SAMPLING_LEVEL);
                assertEquals(expectedSize, sstable.getIndexSummary().size(), 1);
                txn.update(sstable, true);
                txn.checkpoint();
            }
            txn.finish();
        }
    }

    @Test
    public void testJMXFunctions() throws IOException
    {
        IndexSummaryManager manager = IndexSummaryManager.instance;

        // resize interval
        manager.setResizeIntervalInMinutes(-1);
        assertEquals(-1, DatabaseDescriptor.getIndexSummaryResizeIntervalInMinutes());
        assertNull(manager.getTimeToNextResize(TimeUnit.MINUTES));

        manager.setResizeIntervalInMinutes(10);
        assertEquals(10, manager.getResizeIntervalInMinutes());
        assertEquals(10, DatabaseDescriptor.getIndexSummaryResizeIntervalInMinutes());
        assertEquals(10, manager.getTimeToNextResize(TimeUnit.MINUTES), 1);
        manager.setResizeIntervalInMinutes(15);
        assertEquals(15, manager.getResizeIntervalInMinutes());
        assertEquals(15, DatabaseDescriptor.getIndexSummaryResizeIntervalInMinutes());
        assertEquals(15, manager.getTimeToNextResize(TimeUnit.MINUTES), 2);

        // memory pool capacity
        assertTrue(manager.getMemoryPoolCapacityInMB() >= 0);
        manager.setMemoryPoolCapacityInMB(10);
        assertEquals(10, manager.getMemoryPoolCapacityInMB());

        String ksname = KEYSPACE1;
        String cfname = CF_STANDARDLOWiINTERVAL; // index interval of 8, no key caching
        Keyspace keyspace = Keyspace.open(ksname);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfname);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();

        ByteBuffer value = ByteBuffer.wrap(new byte[100]);

        int numSSTables = 2;
        int numRows = 10;
        for (int sstable = 0; sstable < numSSTables; sstable++)
        {
            for (int row = 0; row < numRows; row++)
            {
                String key = String.format("%3d", row);
                new RowUpdateBuilder(cfs.metadata(), 0, key)
                .clustering("column")
                .add("val", value)
                .build()
                .applyUnsafe();
            }
            Util.flush(cfs);
        }

        assertTrue(manager.getAverageIndexInterval() >= cfs.metadata().params.minIndexInterval);
        Map<String, Integer> intervals = manager.getIndexIntervals();
        for (Map.Entry<String, Integer> entry : intervals.entrySet())
            if (entry.getKey().contains(CF_STANDARDLOWiINTERVAL))
                assertEquals(cfs.metadata().params.minIndexInterval, entry.getValue(), 0.001);

        manager.setMemoryPoolCapacityInMB(0);
        manager.redistributeSummaries();
        assertTrue(manager.getAverageIndexInterval() > cfs.metadata().params.minIndexInterval);
        intervals = manager.getIndexIntervals();
        for (Map.Entry<String, Integer> entry : intervals.entrySet())
        {
            if (entry.getKey().contains(CF_STANDARDLOWiINTERVAL))
                assertTrue(entry.getValue() >= cfs.metadata().params.minIndexInterval);
        }
    }

    @Test
    public void testCancelIndex() throws Exception
    {
        testCancelIndexHelper((cfs) -> CompactionManager.instance.stopCompaction("INDEX_SUMMARY"));
    }

    @Test
    public void testCancelIndexInterrupt() throws Exception
    {
        testCancelIndexHelper((cfs) -> CompactionManager.instance.interruptCompactionFor(Collections.singleton(cfs.metadata()), (sstable) -> true, false));
    }

    public void testCancelIndexHelper(Consumer<ColumnFamilyStore> cancelFunction) throws Exception
    {
        String ksname = KEYSPACE1;
        String cfname = CF_STANDARDLOWiINTERVAL; // index interval of 8, no key caching
        Keyspace keyspace = Keyspace.open(ksname);
        final ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfname);
        cfs.disableAutoCompaction();
        final int numSSTables = 8;
        int numRows = 256;
        createSSTables(ksname, cfname, numSSTables, numRows);

        List<R> allSSTables = ServerTestUtils.getLiveIndexSummarySupportingReaders(cfs);
        List<R> sstables = allSSTables.subList(0, 4);
        List<R> compacting = allSSTables.subList(4, 8);

        for (R sstable : sstables)
            sstable.overrideReadMeter(new RestorableMeter(100.0, 100.0));

        final long singleSummaryOffHeapSpace = sstables.get(0).getIndexSummary().getOffHeapSize();

        // everything should get cut in half
        final AtomicReference<CompactionInterruptedException> exception = new AtomicReference<>();
        // barrier to control when redistribution runs
        final CountDownLatch barrier = new CountDownLatch(1);
        CompactionInfo.Holder ongoingCompaction = new CompactionInfo.Holder()
        {
            public CompactionInfo getCompactionInfo()
            {
                return new CompactionInfo(cfs.metadata(), OperationType.UNKNOWN, 0, 0, nextTimeUUID(), compacting);
            }

            public boolean isGlobal()
            {
                return false;
            }
        };
        try (LifecycleTransaction ignored = cfs.getTracker().tryModify(compacting, OperationType.UNKNOWN))
        {
            CompactionManager.instance.active.beginCompaction(ongoingCompaction);

            Thread t = NamedThreadFactory.createAnonymousThread(new Runnable()
            {
                public void run()
                {
                    try
                    {
                        // Don't leave enough space for even the minimal index summaries
                        try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.UNKNOWN))
                        {
                            IndexSummaryManager.redistributeSummaries(new ObservableRedistribution(of(cfs.metadata.id, txn),
                                                                                                   0,
                                                                                                   singleSummaryOffHeapSpace,
                                                                                                   barrier));
                        }
                    }
                    catch (CompactionInterruptedException ex)
                    {
                        exception.set(ex);
                    }
                    catch (IOException ignored)
                    {
                    }
                }
            });

            t.start();
            while (CompactionManager.instance.getActiveCompactions() < 2 && t.isAlive())
                Thread.sleep(1);
            // to ensure that the stop condition check in IndexSummaryRedistribution::redistributeSummaries
            // is made *after* the halt request is made to the CompactionManager, don't allow the redistribution
            // to proceed until stopCompaction has been called.
            cancelFunction.accept(cfs);
            // allows the redistribution to proceed
            barrier.countDown();
            t.join();
        }
        finally
        {
            CompactionManager.instance.active.finishCompaction(ongoingCompaction);
        }

        assertNotNull("Expected compaction interrupted exception", exception.get());
        assertTrue("Expected no active compactions", CompactionManager.instance.active.getCompactions().isEmpty());

        Set<R> beforeRedistributionSSTables = new HashSet<>(allSSTables);
        Set<R> afterCancelSSTables = Sets.newHashSet(ServerTestUtils.getLiveIndexSummarySupportingReaders(cfs));
        Set<R> disjoint = Sets.symmetricDifference(beforeRedistributionSSTables, afterCancelSSTables);
        assertTrue(String.format("Mismatched files before and after cancelling redistribution: %s",
                                 Joiner.on(",").join(disjoint)),
                   disjoint.isEmpty());
        assertOnDiskState(cfs, 8);
        validateData(cfs, numRows);
    }

    @Test
    public void testPauseIndexSummaryManager() throws Exception
    {
        String ksname = KEYSPACE1;
        String cfname = CF_STANDARDLOWiINTERVAL; // index interval of 8, no key caching
        Keyspace keyspace = Keyspace.open(ksname);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfname);
        int numSSTables = 4;
        int numRows = 256;
        createSSTables(ksname, cfname, numSSTables, numRows);

        List<R> sstables = ServerTestUtils.getLiveIndexSummarySupportingReaders(cfs);
        for (R sstable : sstables)
            sstable.overrideReadMeter(new RestorableMeter(100.0, 100.0));

        long singleSummaryOffHeapSpace = sstables.get(0).getIndexSummary().getOffHeapSize();

        // everything should get cut in half
        assert sstables.size() == numSSTables;
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.UNKNOWN))
        {
            try (AutoCloseable toresume = CompactionManager.instance.pauseGlobalCompaction())
            {
                sstables = redistributeSummaries(Collections.emptyList(), of(cfs.metadata().id, txn), (singleSummaryOffHeapSpace * (numSSTables / 2)));
                fail("The redistribution should fail - we got paused before adding to active compactions, but after marking compacting");
            }
        }
        catch (CompactionInterruptedException e)
        {
            // expected
        }
        for (R sstable : sstables)
            assertEquals(BASE_SAMPLING_LEVEL, sstable.getIndexSummary().getSamplingLevel());
        validateData(cfs, numRows);
        assertOnDiskState(cfs, numSSTables);
    }

    private List<R> redistributeSummaries(List<R> compacting,
                                          Map<TableId, LifecycleTransaction> transactions,
                                          long memoryPoolBytes)
    throws IOException
    {
        long nonRedistributingOffHeapSize = compacting.stream().mapToLong(t -> t.getIndexSummary().getOffHeapSize()).sum();
        return IndexSummaryManager.redistributeSummaries(new IndexSummaryRedistribution(transactions,
                                                                                        nonRedistributingOffHeapSize,
                                                                                        memoryPoolBytes));
    }

    private static class ObservableRedistribution extends IndexSummaryRedistribution
    {
        CountDownLatch barrier;

        ObservableRedistribution(Map<TableId, LifecycleTransaction> transactions,
                                 long nonRedistributingOffHeapSize,
                                 long memoryPoolBytes,
                                 CountDownLatch barrier)
        {
            super(transactions, nonRedistributingOffHeapSize, memoryPoolBytes);
            this.barrier = barrier;
        }

        public <R extends SSTableReader & IndexSummarySupport<R>> List<R> redistributeSummaries() throws IOException
        {
            try
            {
                barrier.await();
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException("Interrupted waiting on test barrier");
            }
            return super.redistributeSummaries();
        }
    }
}
