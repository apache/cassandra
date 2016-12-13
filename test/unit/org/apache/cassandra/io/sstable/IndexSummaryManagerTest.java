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
package org.apache.cassandra.io.sstable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.compaction.CompactionInterruptedException;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.metrics.CompactionMetrics;
import org.apache.cassandra.metrics.RestorableMeter;
import org.apache.cassandra.schema.CachingParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.ByteBufferUtil;

import static com.google.common.collect.ImmutableMap.of;
import static java.util.Arrays.asList;
import static org.apache.cassandra.io.sstable.Downsampling.BASE_SAMPLING_LEVEL;
import static org.apache.cassandra.io.sstable.IndexSummaryRedistribution.DOWNSAMPLE_THESHOLD;
import static org.apache.cassandra.io.sstable.IndexSummaryRedistribution.UPSAMPLE_THRESHOLD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


@RunWith(OrderedJUnit4ClassRunner.class)
public class IndexSummaryManagerTest
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
        originalMinIndexInterval = cfs.metadata.params.minIndexInterval;
        originalMaxIndexInterval = cfs.metadata.params.maxIndexInterval;
        originalCapacity = IndexSummaryManager.instance.getMemoryPoolCapacityInMB();
    }

    @After
    public void afterTest()
    {
        for (CompactionInfo.Holder holder : CompactionMetrics.getCompactions())
        {
            holder.stop();
        }

        String ksname = KEYSPACE1;
        String cfname = CF_STANDARDLOWiINTERVAL; // index interval of 8, no key caching
        Keyspace keyspace = Keyspace.open(ksname);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfname);
        cfs.metadata.minIndexInterval(originalMinIndexInterval);
        cfs.metadata.maxIndexInterval(originalMaxIndexInterval);
        IndexSummaryManager.instance.setMemoryPoolCapacityInMB(originalCapacity);
    }

    private static long totalOffHeapSize(List<SSTableReader> sstables)
    {
        long total = 0;
        for (SSTableReader sstable : sstables)
            total += sstable.getIndexSummaryOffHeapSize();
        return total;
    }

    private static List<SSTableReader> resetSummaries(ColumnFamilyStore cfs, List<SSTableReader> sstables, long originalOffHeapSize) throws IOException
    {
        for (SSTableReader sstable : sstables)
            sstable.overrideReadMeter(new RestorableMeter(100.0, 100.0));

        try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.UNKNOWN))
        {
            sstables = redistributeSummaries(Collections.EMPTY_LIST, of(cfs.metadata.cfId, txn), originalOffHeapSize * sstables.size());
        }
        for (SSTableReader sstable : sstables)
            assertEquals(BASE_SAMPLING_LEVEL, sstable.getIndexSummarySamplingLevel());

        return sstables;
    }

    private void validateData(ColumnFamilyStore cfs, int numPartition)
    {
        for (int i = 0; i < numPartition; i++)
        {
            Row row = Util.getOnlyRowUnfiltered(Util.cmd(cfs, String.format("%3d", i)).build());
            Cell cell = row.getCell(cfs.metadata.getColumnDefinition(ByteBufferUtil.bytes("val")));
            assertNotNull(cell);
            assertEquals(100, cell.value().array().length);

        }
    }

    private Comparator<SSTableReader> hotnessComparator = new Comparator<SSTableReader>()
    {
        public int compare(SSTableReader o1, SSTableReader o2)
        {
            return Double.compare(o1.getReadMeter().fifteenMinuteRate(), o2.getReadMeter().fifteenMinuteRate());
        }
    };

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
                new RowUpdateBuilder(cfs.metadata, 0, key)
                    .clustering("column")
                    .add("val", value)
                    .build()
                    .applyUnsafe();
            }
            futures.add(cfs.forceFlush());
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
        assertEquals(numSSTables, cfs.getLiveSSTables().size());
        validateData(cfs, numPartition);
    }

    @Test
    public void testChangeMinIndexInterval() throws IOException
    {
        String ksname = KEYSPACE1;
        String cfname = CF_STANDARDLOWiINTERVAL; // index interval of 8, no key caching
        Keyspace keyspace = Keyspace.open(ksname);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfname);
        int numSSTables = 1;
        int numRows = 256;
        createSSTables(ksname, cfname, numSSTables, numRows);

        List<SSTableReader> sstables = new ArrayList<>(cfs.getLiveSSTables());
        for (SSTableReader sstable : sstables)
            sstable.overrideReadMeter(new RestorableMeter(100.0, 100.0));

        for (SSTableReader sstable : sstables)
            assertEquals(cfs.metadata.params.minIndexInterval, sstable.getEffectiveIndexInterval(), 0.001);

        // double the min_index_interval
        cfs.metadata.minIndexInterval(originalMinIndexInterval * 2);
        IndexSummaryManager.instance.redistributeSummaries();
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            assertEquals(cfs.metadata.params.minIndexInterval, sstable.getEffectiveIndexInterval(), 0.001);
            assertEquals(numRows / cfs.metadata.params.minIndexInterval, sstable.getIndexSummarySize());
        }

        // return min_index_interval to its original value
        cfs.metadata.minIndexInterval(originalMinIndexInterval);
        IndexSummaryManager.instance.redistributeSummaries();
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            assertEquals(cfs.metadata.params.minIndexInterval, sstable.getEffectiveIndexInterval(), 0.001);
            assertEquals(numRows / cfs.metadata.params.minIndexInterval, sstable.getIndexSummarySize());
        }

        // halve the min_index_interval, but constrain the available space to exactly what we have now; as a result,
        // the summary shouldn't change
        cfs.metadata.minIndexInterval(originalMinIndexInterval / 2);
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        long summarySpace = sstable.getIndexSummaryOffHeapSize();
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(asList(sstable), OperationType.UNKNOWN))
        {
            redistributeSummaries(Collections.EMPTY_LIST, of(cfs.metadata.cfId, txn), summarySpace);
        }

        sstable = cfs.getLiveSSTables().iterator().next();
        assertEquals(originalMinIndexInterval, sstable.getEffectiveIndexInterval(), 0.001);
        assertEquals(numRows / originalMinIndexInterval, sstable.getIndexSummarySize());

        // keep the min_index_interval the same, but now give the summary enough space to grow by 50%
        double previousInterval = sstable.getEffectiveIndexInterval();
        int previousSize = sstable.getIndexSummarySize();
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(asList(sstable), OperationType.UNKNOWN))
        {
            redistributeSummaries(Collections.EMPTY_LIST, of(cfs.metadata.cfId, txn), (long) Math.ceil(summarySpace * 1.5));
        }
        sstable = cfs.getLiveSSTables().iterator().next();
        assertEquals(previousSize * 1.5, (double) sstable.getIndexSummarySize(), 1);
        assertEquals(previousInterval * (1.0 / 1.5), sstable.getEffectiveIndexInterval(), 0.001);

        // return min_index_interval to it's original value (double it), but only give the summary enough space
        // to have an effective index interval of twice the new min
        cfs.metadata.minIndexInterval(originalMinIndexInterval);
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(asList(sstable), OperationType.UNKNOWN))
        {
            redistributeSummaries(Collections.EMPTY_LIST, of(cfs.metadata.cfId, txn), (long) Math.ceil(summarySpace / 2.0));
        }
        sstable = cfs.getLiveSSTables().iterator().next();
        assertEquals(originalMinIndexInterval * 2, sstable.getEffectiveIndexInterval(), 0.001);
        assertEquals(numRows / (originalMinIndexInterval * 2), sstable.getIndexSummarySize());

        // raise the min_index_interval above our current effective interval, but set the max_index_interval lower
        // than what we actually have space for (meaning the index summary would ideally be smaller, but this would
        // result in an effective interval above the new max)
        cfs.metadata.minIndexInterval(originalMinIndexInterval * 4);
        cfs.metadata.maxIndexInterval(originalMinIndexInterval * 4);
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(asList(sstable), OperationType.UNKNOWN))
        {
            redistributeSummaries(Collections.EMPTY_LIST, of(cfs.metadata.cfId, txn), 10);
        }
        sstable = cfs.getLiveSSTables().iterator().next();
        assertEquals(cfs.metadata.params.minIndexInterval, sstable.getEffectiveIndexInterval(), 0.001);
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

        List<SSTableReader> sstables = new ArrayList<>(cfs.getLiveSSTables());
        for (SSTableReader sstable : sstables)
            sstable.overrideReadMeter(new RestorableMeter(100.0, 100.0));

        try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.UNKNOWN))
        {
            redistributeSummaries(Collections.EMPTY_LIST, of(cfs.metadata.cfId, txn), 10);
        }
        sstables = new ArrayList<>(cfs.getLiveSSTables());
        for (SSTableReader sstable : sstables)
            assertEquals(cfs.metadata.params.maxIndexInterval, sstable.getEffectiveIndexInterval(), 0.01);

        // halve the max_index_interval
        cfs.metadata.maxIndexInterval(cfs.metadata.params.maxIndexInterval / 2);
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.UNKNOWN))
        {
            redistributeSummaries(Collections.EMPTY_LIST, of(cfs.metadata.cfId, txn), 1);
        }
        sstables = new ArrayList<>(cfs.getLiveSSTables());
        for (SSTableReader sstable : sstables)
        {
            assertEquals(cfs.metadata.params.maxIndexInterval, sstable.getEffectiveIndexInterval(), 0.01);
            assertEquals(numRows / cfs.metadata.params.maxIndexInterval, sstable.getIndexSummarySize());
        }

        // return max_index_interval to its original value
        cfs.metadata.maxIndexInterval(cfs.metadata.params.maxIndexInterval * 2);
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.UNKNOWN))
        {
            redistributeSummaries(Collections.EMPTY_LIST, of(cfs.metadata.cfId, txn), 1);
        }
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            assertEquals(cfs.metadata.params.maxIndexInterval, sstable.getEffectiveIndexInterval(), 0.01);
            assertEquals(numRows / cfs.metadata.params.maxIndexInterval, sstable.getIndexSummarySize());
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

        int minSamplingLevel = (BASE_SAMPLING_LEVEL * cfs.metadata.params.minIndexInterval) / cfs.metadata.params.maxIndexInterval;

        List<SSTableReader> sstables = new ArrayList<>(cfs.getLiveSSTables());
        for (SSTableReader sstable : sstables)
            sstable.overrideReadMeter(new RestorableMeter(100.0, 100.0));

        long singleSummaryOffHeapSpace = sstables.get(0).getIndexSummaryOffHeapSize();

        // there should be enough space to not downsample anything
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.UNKNOWN))
        {
            sstables = redistributeSummaries(Collections.EMPTY_LIST, of(cfs.metadata.cfId, txn), (singleSummaryOffHeapSpace * numSSTables));
        }
        for (SSTableReader sstable : sstables)
            assertEquals(BASE_SAMPLING_LEVEL, sstable.getIndexSummarySamplingLevel());
        assertEquals(singleSummaryOffHeapSpace * numSSTables, totalOffHeapSize(sstables));
        validateData(cfs, numRows);

        // everything should get cut in half
        assert sstables.size() == 4;
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.UNKNOWN))
        {
            sstables = redistributeSummaries(Collections.EMPTY_LIST, of(cfs.metadata.cfId, txn), (singleSummaryOffHeapSpace * (numSSTables / 2)));
        }
        for (SSTableReader sstable : sstables)
            assertEquals(BASE_SAMPLING_LEVEL / 2, sstable.getIndexSummarySamplingLevel());
        validateData(cfs, numRows);

        // everything should get cut to a quarter
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.UNKNOWN))
        {
            sstables = redistributeSummaries(Collections.EMPTY_LIST, of(cfs.metadata.cfId, txn), (singleSummaryOffHeapSpace * (numSSTables / 4)));
        }
        for (SSTableReader sstable : sstables)
            assertEquals(BASE_SAMPLING_LEVEL / 4, sstable.getIndexSummarySamplingLevel());
        validateData(cfs, numRows);

        // upsample back up to half
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.UNKNOWN))
        {
            sstables = redistributeSummaries(Collections.EMPTY_LIST, of(cfs.metadata.cfId, txn), (singleSummaryOffHeapSpace * (numSSTables / 2) + 4));
        }
        assert sstables.size() == 4;
        for (SSTableReader sstable : sstables)
            assertEquals(BASE_SAMPLING_LEVEL / 2, sstable.getIndexSummarySamplingLevel());
        validateData(cfs, numRows);

        // upsample back up to the original index summary
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.UNKNOWN))
        {
            sstables = redistributeSummaries(Collections.EMPTY_LIST, of(cfs.metadata.cfId, txn), (singleSummaryOffHeapSpace * numSSTables));
        }
        for (SSTableReader sstable : sstables)
            assertEquals(BASE_SAMPLING_LEVEL, sstable.getIndexSummarySamplingLevel());
        validateData(cfs, numRows);

        // make two of the four sstables cold, only leave enough space for three full index summaries,
        // so the two cold sstables should get downsampled to be half of their original size
        sstables.get(0).overrideReadMeter(new RestorableMeter(50.0, 50.0));
        sstables.get(1).overrideReadMeter(new RestorableMeter(50.0, 50.0));
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.UNKNOWN))
        {
            sstables = redistributeSummaries(Collections.EMPTY_LIST, of(cfs.metadata.cfId, txn), (singleSummaryOffHeapSpace * 3));
        }
        Collections.sort(sstables, hotnessComparator);
        assertEquals(BASE_SAMPLING_LEVEL / 2, sstables.get(0).getIndexSummarySamplingLevel());
        assertEquals(BASE_SAMPLING_LEVEL / 2, sstables.get(1).getIndexSummarySamplingLevel());
        assertEquals(BASE_SAMPLING_LEVEL, sstables.get(2).getIndexSummarySamplingLevel());
        assertEquals(BASE_SAMPLING_LEVEL, sstables.get(3).getIndexSummarySamplingLevel());
        validateData(cfs, numRows);

        // small increases or decreases in the read rate don't result in downsampling or upsampling
        double lowerRate = 50.0 * (DOWNSAMPLE_THESHOLD + (DOWNSAMPLE_THESHOLD * 0.10));
        double higherRate = 50.0 * (UPSAMPLE_THRESHOLD - (UPSAMPLE_THRESHOLD * 0.10));
        sstables.get(0).overrideReadMeter(new RestorableMeter(lowerRate, lowerRate));
        sstables.get(1).overrideReadMeter(new RestorableMeter(higherRate, higherRate));
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.UNKNOWN))
        {
            sstables = redistributeSummaries(Collections.EMPTY_LIST, of(cfs.metadata.cfId, txn), (singleSummaryOffHeapSpace * 3));
        }
        Collections.sort(sstables, hotnessComparator);
        assertEquals(BASE_SAMPLING_LEVEL / 2, sstables.get(0).getIndexSummarySamplingLevel());
        assertEquals(BASE_SAMPLING_LEVEL / 2, sstables.get(1).getIndexSummarySamplingLevel());
        assertEquals(BASE_SAMPLING_LEVEL, sstables.get(2).getIndexSummarySamplingLevel());
        assertEquals(BASE_SAMPLING_LEVEL, sstables.get(3).getIndexSummarySamplingLevel());
        validateData(cfs, numRows);

        // reset, and then this time, leave enough space for one of the cold sstables to not get downsampled
        sstables = resetSummaries(cfs, sstables, singleSummaryOffHeapSpace);
        sstables.get(0).overrideReadMeter(new RestorableMeter(1.0, 1.0));
        sstables.get(1).overrideReadMeter(new RestorableMeter(2.0, 2.0));
        sstables.get(2).overrideReadMeter(new RestorableMeter(1000.0, 1000.0));
        sstables.get(3).overrideReadMeter(new RestorableMeter(1000.0, 1000.0));

        try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.UNKNOWN))
        {
            sstables = redistributeSummaries(Collections.EMPTY_LIST, of(cfs.metadata.cfId, txn), (singleSummaryOffHeapSpace * 3) + 50);
        }
        Collections.sort(sstables, hotnessComparator);

        if (sstables.get(0).getIndexSummarySamplingLevel() == minSamplingLevel)
            assertEquals(BASE_SAMPLING_LEVEL, sstables.get(1).getIndexSummarySamplingLevel());
        else
            assertEquals(BASE_SAMPLING_LEVEL, sstables.get(0).getIndexSummarySamplingLevel());

        assertEquals(BASE_SAMPLING_LEVEL, sstables.get(2).getIndexSummarySamplingLevel());
        assertEquals(BASE_SAMPLING_LEVEL, sstables.get(3).getIndexSummarySamplingLevel());
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
            sstables = redistributeSummaries(Collections.EMPTY_LIST, of(cfs.metadata.cfId, txn), (long) (singleSummaryOffHeapSpace + (singleSummaryOffHeapSpace * (92.0 / BASE_SAMPLING_LEVEL))));
        }
        Collections.sort(sstables, hotnessComparator);
        assertEquals(1, sstables.get(0).getIndexSummarySize());  // at the min sampling level
        assertEquals(1, sstables.get(0).getIndexSummarySize());  // at the min sampling level
        assertTrue(sstables.get(2).getIndexSummarySamplingLevel() > minSamplingLevel);
        assertTrue(sstables.get(2).getIndexSummarySamplingLevel() < BASE_SAMPLING_LEVEL);
        assertEquals(BASE_SAMPLING_LEVEL, sstables.get(3).getIndexSummarySamplingLevel());
        validateData(cfs, numRows);

        // Don't leave enough space for even the minimal index summaries
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.UNKNOWN))
        {
            sstables = redistributeSummaries(Collections.EMPTY_LIST, of(cfs.metadata.cfId, txn), 10);
        }
        for (SSTableReader sstable : sstables)
            assertEquals(1, sstable.getIndexSummarySize());  // at the min sampling level
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
            new RowUpdateBuilder(cfs.metadata, 0, key)
            .clustering("column")
            .add("val", value)
            .build()
            .applyUnsafe();
        }

        cfs.forceBlockingFlush();

        List<SSTableReader> sstables = new ArrayList<>(cfs.getLiveSSTables());
        assertEquals(1, sstables.size());
        SSTableReader original = sstables.get(0);

        SSTableReader sstable = original;
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(asList(sstable), OperationType.UNKNOWN))
        {
            for (int samplingLevel = 1; samplingLevel < BASE_SAMPLING_LEVEL; samplingLevel++)
            {
                sstable = sstable.cloneWithNewSummarySamplingLevel(cfs, samplingLevel);
                assertEquals(samplingLevel, sstable.getIndexSummarySamplingLevel());
                int expectedSize = (numRows * samplingLevel) / (sstable.metadata.params.minIndexInterval * BASE_SAMPLING_LEVEL);
                assertEquals(expectedSize, sstable.getIndexSummarySize(), 1);
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
        assertNull(manager.getTimeToNextResize(TimeUnit.MINUTES));

        manager.setResizeIntervalInMinutes(10);
        assertEquals(10, manager.getResizeIntervalInMinutes());
        assertEquals(10, manager.getTimeToNextResize(TimeUnit.MINUTES), 1);
        manager.setResizeIntervalInMinutes(15);
        assertEquals(15, manager.getResizeIntervalInMinutes());
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
                new RowUpdateBuilder(cfs.metadata, 0, key)
                .clustering("column")
                .add("val", value)
                .build()
                .applyUnsafe();
            }
            cfs.forceBlockingFlush();
        }

        assertTrue(manager.getAverageIndexInterval() >= cfs.metadata.params.minIndexInterval);
        Map<String, Integer> intervals = manager.getIndexIntervals();
        for (Map.Entry<String, Integer> entry : intervals.entrySet())
            if (entry.getKey().contains(CF_STANDARDLOWiINTERVAL))
                assertEquals(cfs.metadata.params.minIndexInterval, entry.getValue(), 0.001);

        manager.setMemoryPoolCapacityInMB(0);
        manager.redistributeSummaries();
        assertTrue(manager.getAverageIndexInterval() > cfs.metadata.params.minIndexInterval);
        intervals = manager.getIndexIntervals();
        for (Map.Entry<String, Integer> entry : intervals.entrySet())
        {
            if (entry.getKey().contains(CF_STANDARDLOWiINTERVAL))
                assertTrue(entry.getValue() >= cfs.metadata.params.minIndexInterval);
        }
    }

    @Test
    public void testCancelIndex() throws Exception
    {
        String ksname = KEYSPACE1;
        String cfname = CF_STANDARDLOWiINTERVAL; // index interval of 8, no key caching
        Keyspace keyspace = Keyspace.open(ksname);
        final ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfname);
        final int numSSTables = 4;
        int numRows = 256;
        createSSTables(ksname, cfname, numSSTables, numRows);

        final List<SSTableReader> sstables = new ArrayList<>(cfs.getLiveSSTables());
        for (SSTableReader sstable : sstables)
            sstable.overrideReadMeter(new RestorableMeter(100.0, 100.0));

        final long singleSummaryOffHeapSpace = sstables.get(0).getIndexSummaryOffHeapSize();

        // everything should get cut in half
        final AtomicReference<CompactionInterruptedException> exception = new AtomicReference<>();
        // barrier to control when redistribution runs
        final CountDownLatch barrier = new CountDownLatch(1);

        Thread t = NamedThreadFactory.createThread(new Runnable()
        {
            public void run()
            {
                try
                {
                    // Don't leave enough space for even the minimal index summaries
                    try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.UNKNOWN))
                    {
                        IndexSummaryManager.redistributeSummaries(new ObservableRedistribution(Collections.EMPTY_LIST,
                                                                                               of(cfs.metadata.cfId, txn),
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
        while (CompactionManager.instance.getActiveCompactions() == 0 && t.isAlive())
            Thread.sleep(1);
        // to ensure that the stop condition check in IndexSummaryRedistribution::redistributeSummaries
        // is made *after* the halt request is made to the CompactionManager, don't allow the redistribution
        // to proceed until stopCompaction has been called.
        CompactionManager.instance.stopCompaction("INDEX_SUMMARY");
        // allows the redistribution to proceed
        barrier.countDown();
        t.join();

        assertNotNull("Expected compaction interrupted exception", exception.get());
        assertTrue("Expected no active compactions", CompactionMetrics.getCompactions().isEmpty());

        Set<SSTableReader> beforeRedistributionSSTables = new HashSet<>(sstables);
        Set<SSTableReader> afterCancelSSTables = new HashSet<>(cfs.getLiveSSTables());
        Set<SSTableReader> disjoint = Sets.symmetricDifference(beforeRedistributionSSTables, afterCancelSSTables);
        assertTrue(String.format("Mismatched files before and after cancelling redistribution: %s",
                                 Joiner.on(",").join(disjoint)),
                   disjoint.isEmpty());

        validateData(cfs, numRows);
    }

    private static List<SSTableReader> redistributeSummaries(List<SSTableReader> compacting,
                                                             Map<UUID, LifecycleTransaction> transactions,
                                                             long memoryPoolBytes)
    throws IOException
    {
        return IndexSummaryManager.redistributeSummaries(new IndexSummaryRedistribution(compacting,
                                                                                        transactions,
                                                                                        memoryPoolBytes));
    }

    private static class ObservableRedistribution extends IndexSummaryRedistribution
    {
        CountDownLatch barrier;

        ObservableRedistribution(List<SSTableReader> compacting,
                                 Map<UUID, LifecycleTransaction> transactions,
                                 long memoryPoolBytes,
                                 CountDownLatch barrier)
        {
            super(compacting, transactions, memoryPoolBytes);
            this.barrier = barrier;
        }

        public List<SSTableReader> redistributeSummaries() throws IOException
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
