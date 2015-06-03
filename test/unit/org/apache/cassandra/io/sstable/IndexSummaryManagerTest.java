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
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import junit.framework.Assert;
import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.metrics.RestorableMeter;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.apache.cassandra.io.sstable.Downsampling.BASE_SAMPLING_LEVEL;
import static org.apache.cassandra.io.sstable.IndexSummaryManager.DOWNSAMPLE_THESHOLD;
import static org.apache.cassandra.io.sstable.IndexSummaryManager.UPSAMPLE_THRESHOLD;
import static org.apache.cassandra.io.sstable.IndexSummaryManager.redistributeSummaries;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(OrderedJUnit4ClassRunner.class)
public class IndexSummaryManagerTest extends SchemaLoader
{
    private static final Logger logger = LoggerFactory.getLogger(IndexSummaryManagerTest.class);

    int originalMinIndexInterval;
    int originalMaxIndexInterval;
    long originalCapacity;

    @Before
    public void beforeTest()
    {
        String ksname = "Keyspace1";
        String cfname = "StandardLowIndexInterval"; // index interval of 8, no key caching
        Keyspace keyspace = Keyspace.open(ksname);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfname);
        originalMinIndexInterval = cfs.metadata.getMinIndexInterval();
        originalMaxIndexInterval = cfs.metadata.getMaxIndexInterval();
        originalCapacity = IndexSummaryManager.instance.getMemoryPoolCapacityInMB();
    }

    @After
    public void afterTest()
    {
        String ksname = "Keyspace1";
        String cfname = "StandardLowIndexInterval"; // index interval of 8, no key caching
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

    private static List<SSTableReader> resetSummaries(List<SSTableReader> sstables, long originalOffHeapSize) throws IOException
    {
        for (SSTableReader sstable : sstables)
            sstable.overrideReadMeter(new RestorableMeter(100.0, 100.0));

        sstables = redistributeSummaries(Collections.EMPTY_LIST, sstables, originalOffHeapSize * sstables.size());
        for (SSTableReader sstable : sstables)
            assertEquals(BASE_SAMPLING_LEVEL, sstable.getIndexSummarySamplingLevel());

        return sstables;
    }

    private void validateData(ColumnFamilyStore cfs, int numRows)
    {
        for (int i = 0; i < numRows; i++)
        {
            DecoratedKey key = Util.dk(String.format("%3d", i));
            QueryFilter filter = QueryFilter.getIdentityFilter(key, cfs.getColumnFamilyName(), System.currentTimeMillis());
            ColumnFamily row = cfs.getColumnFamily(filter);
            assertNotNull(row);
            Cell cell = row.getColumn(Util.cellname("column"));
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

    private void createSSTables(String ksname, String cfname, int numSSTables, int numRows)
    {
        Keyspace keyspace = Keyspace.open(ksname);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfname);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();

        ArrayList<Future> futures = new ArrayList<>(numSSTables);
        ByteBuffer value = ByteBuffer.wrap(new byte[100]);
        for (int sstable = 0; sstable < numSSTables; sstable++)
        {
            for (int row = 0; row < numRows; row++)
            {
                DecoratedKey key = Util.dk(String.format("%3d", row));
                Mutation rm = new Mutation(ksname, key.getKey());
                rm.add(cfname, Util.cellname("column"), value, 0);
                rm.applyUnsafe();
            }
            futures.add(cfs.forceFlush());
        }
        for (Future future : futures)
        {
            try
            {
                future.get();
            } catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
            catch (ExecutionException e)
            {
                throw new RuntimeException(e);
            }
        }
        assertEquals(numSSTables, cfs.getSSTables().size());
        validateData(cfs, numRows);
    }

    @Test
    public void testChangeMinIndexInterval() throws IOException
    {
        String ksname = "Keyspace1";
        String cfname = "StandardLowIndexInterval"; // index interval of 8, no key caching
        Keyspace keyspace = Keyspace.open(ksname);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfname);
        int numSSTables = 1;
        int numRows = 256;
        createSSTables(ksname, cfname, numSSTables, numRows);

        List<SSTableReader> sstables = new ArrayList<>(cfs.getSSTables());
        for (SSTableReader sstable : sstables)
            sstable.overrideReadMeter(new RestorableMeter(100.0, 100.0));

        for (SSTableReader sstable : sstables)
            assertEquals(cfs.metadata.getMinIndexInterval(), sstable.getEffectiveIndexInterval(), 0.001);

        // double the min_index_interval
        cfs.metadata.minIndexInterval(originalMinIndexInterval * 2);
        IndexSummaryManager.instance.redistributeSummaries();
        for (SSTableReader sstable : cfs.getSSTables())
        {
            assertEquals(cfs.metadata.getMinIndexInterval(), sstable.getEffectiveIndexInterval(), 0.001);
            assertEquals(numRows / cfs.metadata.getMinIndexInterval(), sstable.getIndexSummarySize());
        }

        // return min_index_interval to its original value
        cfs.metadata.minIndexInterval(originalMinIndexInterval);
        IndexSummaryManager.instance.redistributeSummaries();
        for (SSTableReader sstable : cfs.getSSTables())
        {
            assertEquals(cfs.metadata.getMinIndexInterval(), sstable.getEffectiveIndexInterval(), 0.001);
            assertEquals(numRows / cfs.metadata.getMinIndexInterval(), sstable.getIndexSummarySize());
        }

        // halve the min_index_interval, but constrain the available space to exactly what we have now; as a result,
        // the summary shouldn't change
        cfs.metadata.minIndexInterval(originalMinIndexInterval / 2);
        SSTableReader sstable = cfs.getSSTables().iterator().next();
        long summarySpace = sstable.getIndexSummaryOffHeapSize();
        IndexSummaryManager.redistributeSummaries(Collections.EMPTY_LIST, Arrays.asList(sstable), summarySpace);
        sstable = cfs.getSSTables().iterator().next();
        assertEquals(originalMinIndexInterval, sstable.getEffectiveIndexInterval(), 0.001);
        assertEquals(numRows / originalMinIndexInterval, sstable.getIndexSummarySize());

        // keep the min_index_interval the same, but now give the summary enough space to grow by 50%
        double previousInterval = sstable.getEffectiveIndexInterval();
        int previousSize = sstable.getIndexSummarySize();
        IndexSummaryManager.redistributeSummaries(Collections.EMPTY_LIST, Arrays.asList(sstable), (long) Math.ceil(summarySpace * 1.5));
        sstable = cfs.getSSTables().iterator().next();
        assertEquals(previousSize * 1.5, (double) sstable.getIndexSummarySize(), 1);
        assertEquals(previousInterval * (1.0 / 1.5), sstable.getEffectiveIndexInterval(), 0.001);

        // return min_index_interval to it's original value (double it), but only give the summary enough space
        // to have an effective index interval of twice the new min
        cfs.metadata.minIndexInterval(originalMinIndexInterval);
        IndexSummaryManager.redistributeSummaries(Collections.EMPTY_LIST, Arrays.asList(sstable), (long) Math.ceil(summarySpace / 2.0));
        sstable = cfs.getSSTables().iterator().next();
        assertEquals(originalMinIndexInterval * 2, sstable.getEffectiveIndexInterval(), 0.001);
        assertEquals(numRows / (originalMinIndexInterval * 2), sstable.getIndexSummarySize());

        // raise the min_index_interval above our current effective interval, but set the max_index_interval lower
        // than what we actually have space for (meaning the index summary would ideally be smaller, but this would
        // result in an effective interval above the new max)
        cfs.metadata.minIndexInterval(originalMinIndexInterval * 4);
        cfs.metadata.maxIndexInterval(originalMinIndexInterval * 4);
        IndexSummaryManager.redistributeSummaries(Collections.EMPTY_LIST, Arrays.asList(sstable), 10);
        sstable = cfs.getSSTables().iterator().next();
        assertEquals(cfs.metadata.getMinIndexInterval(), sstable.getEffectiveIndexInterval(), 0.001);
    }

    @Test
    public void testChangeMaxIndexInterval() throws IOException
    {
        String ksname = "Keyspace1";
        String cfname = "StandardLowIndexInterval"; // index interval of 8, no key caching
        Keyspace keyspace = Keyspace.open(ksname);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfname);
        int numSSTables = 1;
        int numRows = 256;
        createSSTables(ksname, cfname, numSSTables, numRows);

        List<SSTableReader> sstables = new ArrayList<>(cfs.getSSTables());
        for (SSTableReader sstable : sstables)
            sstable.overrideReadMeter(new RestorableMeter(100.0, 100.0));

        IndexSummaryManager.redistributeSummaries(Collections.EMPTY_LIST, sstables, 1);
        sstables = new ArrayList<>(cfs.getSSTables());
        for (SSTableReader sstable : sstables)
            assertEquals(cfs.metadata.getMaxIndexInterval(), sstable.getEffectiveIndexInterval(), 0.01);

        // halve the max_index_interval
        cfs.metadata.maxIndexInterval(cfs.metadata.getMaxIndexInterval() / 2);
        IndexSummaryManager.redistributeSummaries(Collections.EMPTY_LIST, sstables, 1);
        sstables = new ArrayList<>(cfs.getSSTables());
        for (SSTableReader sstable : sstables)
        {
            assertEquals(cfs.metadata.getMaxIndexInterval(), sstable.getEffectiveIndexInterval(), 0.01);
            assertEquals(numRows / cfs.metadata.getMaxIndexInterval(), sstable.getIndexSummarySize());
        }

        // return max_index_interval to its original value
        cfs.metadata.maxIndexInterval(cfs.metadata.getMaxIndexInterval() * 2);
        IndexSummaryManager.redistributeSummaries(Collections.EMPTY_LIST, sstables, 1);
        for (SSTableReader sstable : cfs.getSSTables())
        {
            assertEquals(cfs.metadata.getMaxIndexInterval(), sstable.getEffectiveIndexInterval(), 0.01);
            assertEquals(numRows / cfs.metadata.getMaxIndexInterval(), sstable.getIndexSummarySize());
        }
    }

    @Test(timeout = 10000)
    public void testRedistributeSummaries() throws IOException
    {
        String ksname = "Keyspace1";
        String cfname = "StandardLowIndexInterval"; // index interval of 8, no key caching
        Keyspace keyspace = Keyspace.open(ksname);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfname);
        int numSSTables = 4;
        int numRows = 256;
        createSSTables(ksname, cfname, numSSTables, numRows);

        int minSamplingLevel = (BASE_SAMPLING_LEVEL * cfs.metadata.getMinIndexInterval()) / cfs.metadata.getMaxIndexInterval();

        List<SSTableReader> sstables = new ArrayList<>(cfs.getSSTables());
        for (SSTableReader sstable : sstables)
            sstable.overrideReadMeter(new RestorableMeter(100.0, 100.0));

        long singleSummaryOffHeapSpace = sstables.get(0).getIndexSummaryOffHeapSize();

        // there should be enough space to not downsample anything
        sstables = redistributeSummaries(Collections.EMPTY_LIST, sstables, (singleSummaryOffHeapSpace * numSSTables));
        for (SSTableReader sstable : sstables)
            assertEquals(BASE_SAMPLING_LEVEL, sstable.getIndexSummarySamplingLevel());
        assertEquals(singleSummaryOffHeapSpace * numSSTables, totalOffHeapSize(sstables));
        validateData(cfs, numRows);

        // everything should get cut in half
        assert sstables.size() == 4;
        sstables = redistributeSummaries(Collections.EMPTY_LIST, sstables, (singleSummaryOffHeapSpace * (numSSTables / 2)));
        for (SSTableReader sstable : sstables)
            assertEquals(BASE_SAMPLING_LEVEL / 2, sstable.getIndexSummarySamplingLevel());
        validateData(cfs, numRows);

        // everything should get cut to a quarter
        sstables = redistributeSummaries(Collections.EMPTY_LIST, sstables, (singleSummaryOffHeapSpace * (numSSTables / 4)));
        for (SSTableReader sstable : sstables)
            assertEquals(BASE_SAMPLING_LEVEL / 4, sstable.getIndexSummarySamplingLevel());
        validateData(cfs, numRows);

        // upsample back up to half
        sstables = redistributeSummaries(Collections.EMPTY_LIST, sstables,(singleSummaryOffHeapSpace * (numSSTables / 2) + 4));
        assert sstables.size() == 4;
        for (SSTableReader sstable : sstables)
            assertEquals(BASE_SAMPLING_LEVEL / 2, sstable.getIndexSummarySamplingLevel());
        validateData(cfs, numRows);

        // upsample back up to the original index summary
        sstables = redistributeSummaries(Collections.EMPTY_LIST, sstables, (singleSummaryOffHeapSpace * numSSTables));
        for (SSTableReader sstable : sstables)
            assertEquals(BASE_SAMPLING_LEVEL, sstable.getIndexSummarySamplingLevel());
        validateData(cfs, numRows);

        // make two of the four sstables cold, only leave enough space for three full index summaries,
        // so the two cold sstables should get downsampled to be half of their original size
        sstables.get(0).overrideReadMeter(new RestorableMeter(50.0, 50.0));
        sstables.get(1).overrideReadMeter(new RestorableMeter(50.0, 50.0));
        sstables = redistributeSummaries(Collections.EMPTY_LIST, sstables, (singleSummaryOffHeapSpace * 3));
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
        sstables = redistributeSummaries(Collections.EMPTY_LIST, sstables, (singleSummaryOffHeapSpace * 3));
        Collections.sort(sstables, hotnessComparator);
        assertEquals(BASE_SAMPLING_LEVEL / 2, sstables.get(0).getIndexSummarySamplingLevel());
        assertEquals(BASE_SAMPLING_LEVEL / 2, sstables.get(1).getIndexSummarySamplingLevel());
        assertEquals(BASE_SAMPLING_LEVEL, sstables.get(2).getIndexSummarySamplingLevel());
        assertEquals(BASE_SAMPLING_LEVEL, sstables.get(3).getIndexSummarySamplingLevel());
        validateData(cfs, numRows);

        // reset, and then this time, leave enough space for one of the cold sstables to not get downsampled
        sstables = resetSummaries(sstables, singleSummaryOffHeapSpace);
        sstables.get(0).overrideReadMeter(new RestorableMeter(1.0, 1.0));
        sstables.get(1).overrideReadMeter(new RestorableMeter(2.0, 2.0));
        sstables.get(2).overrideReadMeter(new RestorableMeter(1000.0, 1000.0));
        sstables.get(3).overrideReadMeter(new RestorableMeter(1000.0, 1000.0));

        sstables = redistributeSummaries(Collections.EMPTY_LIST, sstables, (singleSummaryOffHeapSpace * 3) + 50);
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
        sstables = redistributeSummaries(Collections.EMPTY_LIST, sstables, (long) (singleSummaryOffHeapSpace + (singleSummaryOffHeapSpace * (92.0 / BASE_SAMPLING_LEVEL))));
        Collections.sort(sstables, hotnessComparator);
        assertEquals(1, sstables.get(0).getIndexSummarySize());  // at the min sampling level
        assertEquals(1, sstables.get(0).getIndexSummarySize());  // at the min sampling level
        assertTrue(sstables.get(2).getIndexSummarySamplingLevel() > minSamplingLevel);
        assertTrue(sstables.get(2).getIndexSummarySamplingLevel() < BASE_SAMPLING_LEVEL);
        assertEquals(BASE_SAMPLING_LEVEL, sstables.get(3).getIndexSummarySamplingLevel());
        validateData(cfs, numRows);

        // Don't leave enough space for even the minimal index summaries
        sstables = redistributeSummaries(Collections.EMPTY_LIST, sstables, 10);
        for (SSTableReader sstable : sstables)
            assertEquals(1, sstable.getIndexSummarySize());  // at the min sampling level
        validateData(cfs, numRows);
    }

    @Test
    public void testRebuildAtSamplingLevel() throws IOException
    {
        String ksname = "Keyspace1";
        String cfname = "StandardLowIndexInterval";
        Keyspace keyspace = Keyspace.open(ksname);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfname);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();

        ByteBuffer value = ByteBuffer.wrap(new byte[100]);

        int numRows = 256;
        for (int row = 0; row < numRows; row++)
        {
            DecoratedKey key = Util.dk(String.valueOf(row));
            Mutation rm = new Mutation(ksname, key.getKey());
            rm.add(cfname, Util.cellname("column"), value, 0);
            rm.apply();
        }
        cfs.forceBlockingFlush();

        List<SSTableReader> sstables = new ArrayList<>(cfs.getSSTables());
        assertEquals(1, sstables.size());
        SSTableReader original = sstables.get(0);

        SSTableReader sstable = original;
        for (int samplingLevel = 1; samplingLevel < BASE_SAMPLING_LEVEL; samplingLevel++)
        {
            SSTableReader prev = sstable;
            sstable = sstable.cloneWithNewSummarySamplingLevel(cfs, samplingLevel);
            assertEquals(samplingLevel, sstable.getIndexSummarySamplingLevel());
            int expectedSize = (numRows * samplingLevel) / (sstable.metadata.getMinIndexInterval() * BASE_SAMPLING_LEVEL);
            assertEquals(expectedSize, sstable.getIndexSummarySize(), 1);
            if (prev != original)
                prev.selfRef().release();
        }

        // don't leave replaced SSTRs around to break other tests
        cfs.getDataTracker().replaceWithNewInstances(Collections.singleton(original), Collections.singleton(sstable));
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

        String ksname = "Keyspace1";
        String cfname = "StandardLowIndexInterval"; // index interval of 8, no key caching
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
                DecoratedKey key = Util.dk(String.valueOf(row));
                Mutation rm = new Mutation(ksname, key.getKey());
                rm.add(cfname, Util.cellname("column"), value, 0);
                rm.apply();
            }
            cfs.forceBlockingFlush();
        }

        assertTrue(manager.getAverageIndexInterval() >= cfs.metadata.getMinIndexInterval());
        Map<String, Integer> intervals = manager.getIndexIntervals();
        for (Map.Entry<String, Integer> entry : intervals.entrySet())
            if (entry.getKey().contains("StandardLowIndexInterval"))
                assertEquals(cfs.metadata.getMinIndexInterval(), entry.getValue(), 0.001);

        manager.setMemoryPoolCapacityInMB(0);
        manager.redistributeSummaries();
        assertTrue(manager.getAverageIndexInterval() > cfs.metadata.getMinIndexInterval());
        intervals = manager.getIndexIntervals();
        for (Map.Entry<String, Integer> entry : intervals.entrySet())
        {
            if (entry.getKey().contains("StandardLowIndexInterval"))
                assertTrue(entry.getValue() >= cfs.metadata.getMinIndexInterval());
        }
    }
}
