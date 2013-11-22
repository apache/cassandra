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
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.metrics.RestorableMeter;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.io.sstable.Downsampling.BASE_SAMPLING_LEVEL;
import static org.apache.cassandra.io.sstable.Downsampling.MIN_SAMPLING_LEVEL;
import static org.apache.cassandra.io.sstable.IndexSummaryManager.DOWNSAMPLE_THESHOLD;
import static org.apache.cassandra.io.sstable.IndexSummaryManager.UPSAMPLE_THRESHOLD;
import static org.apache.cassandra.io.sstable.IndexSummaryManager.redistributeSummaries;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class IndexSummaryManagerTest extends SchemaLoader
{
    private static final Logger logger = LoggerFactory.getLogger(IndexSummaryManagerTest.class);


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
            sstable.readMeter = new RestorableMeter(100.0, 100.0);

        sstables = redistributeSummaries(Collections.EMPTY_LIST, sstables, originalOffHeapSize * sstables.size());
        for (SSTableReader sstable : sstables)
            assertEquals(BASE_SAMPLING_LEVEL, sstable.getIndexSummarySamplingLevel());

        return sstables;
    }

    private void validateData(ColumnFamilyStore cfs, int numRows)
    {
        for (int i = 0; i < numRows; i++)
        {
            DecoratedKey key = Util.dk(String.valueOf(i));
            QueryFilter filter = QueryFilter.getIdentityFilter(key, cfs.getColumnFamilyName(), System.currentTimeMillis());
            ColumnFamily row = cfs.getColumnFamily(filter);
            assertNotNull(row);
            Column column = row.getColumn(ByteBufferUtil.bytes("column"));
            assertNotNull(column);
            assertEquals(100, column.value().array().length);
        }
    }

    private Comparator<SSTableReader> hotnessComparator = new Comparator<SSTableReader>()
    {
        public int compare(SSTableReader o1, SSTableReader o2)
        {
            return Double.compare(o1.readMeter.fifteenMinuteRate(), o2.readMeter.fifteenMinuteRate());
        }
    };

    @Test
    public void testRedistributeSummaries() throws IOException
    {
        String ksname = "Keyspace1";
        String cfname = "StandardLowIndexInterval"; // index interval of 8, no key caching
        Keyspace keyspace = Keyspace.open(ksname);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfname);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();

        ByteBuffer value = ByteBuffer.wrap(new byte[100]);

        int numSSTables = 4;
        int numRows = 256;
        for (int sstable = 0; sstable < numSSTables; sstable++)
        {
            for (int row = 0; row < numRows; row++)
            {
                DecoratedKey key = Util.dk(String.valueOf(row));
                RowMutation rm = new RowMutation(ksname, key.key);
                rm.add(cfname, ByteBufferUtil.bytes("column"), value, 0);
                rm.apply();
            }
            cfs.forceBlockingFlush();
        }

        List<SSTableReader> sstables = new ArrayList<>(cfs.getSSTables());
        assertEquals(numSSTables, sstables.size());
        validateData(cfs, numRows);

        for (SSTableReader sstable : sstables)
            sstable.readMeter = new RestorableMeter(100.0, 100.0);

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
        sstables = redistributeSummaries(Collections.EMPTY_LIST, sstables,(singleSummaryOffHeapSpace * (numSSTables / 2)));
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
        sstables.get(0).readMeter = new RestorableMeter(50.0, 50.0);
        sstables.get(1).readMeter = new RestorableMeter(50.0, 50.0);
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
        sstables.get(0).readMeter = new RestorableMeter(lowerRate, lowerRate);
        sstables.get(1).readMeter = new RestorableMeter(higherRate, higherRate);
        sstables = redistributeSummaries(Collections.EMPTY_LIST, sstables, (singleSummaryOffHeapSpace * 3));
        Collections.sort(sstables, hotnessComparator);
        assertEquals(BASE_SAMPLING_LEVEL / 2, sstables.get(0).getIndexSummarySamplingLevel());
        assertEquals(BASE_SAMPLING_LEVEL / 2, sstables.get(1).getIndexSummarySamplingLevel());
        assertEquals(BASE_SAMPLING_LEVEL, sstables.get(2).getIndexSummarySamplingLevel());
        assertEquals(BASE_SAMPLING_LEVEL, sstables.get(3).getIndexSummarySamplingLevel());
        validateData(cfs, numRows);

        // reset, and then this time, leave enough space for one of the cold sstables to not get downsampled
        sstables = resetSummaries(sstables, singleSummaryOffHeapSpace);
        sstables.get(0).readMeter = new RestorableMeter(1.0, 1.0);
        sstables.get(1).readMeter = new RestorableMeter(2.0, 2.0);
        sstables.get(2).readMeter = new RestorableMeter(1000.0, 1000.0);
        sstables.get(3).readMeter = new RestorableMeter(1000.0, 1000.0);

        sstables = redistributeSummaries(Collections.EMPTY_LIST, sstables, (singleSummaryOffHeapSpace * 3) + 50);
        Collections.sort(sstables, hotnessComparator);

        if (sstables.get(0).getIndexSummarySamplingLevel() == MIN_SAMPLING_LEVEL)
            assertEquals(BASE_SAMPLING_LEVEL, sstables.get(1).getIndexSummarySamplingLevel());
        else
            assertEquals(BASE_SAMPLING_LEVEL, sstables.get(0).getIndexSummarySamplingLevel());

        assertEquals(BASE_SAMPLING_LEVEL, sstables.get(2).getIndexSummarySamplingLevel());
        assertEquals(BASE_SAMPLING_LEVEL, sstables.get(3).getIndexSummarySamplingLevel());
        validateData(cfs, numRows);


        // Cause a mix of upsampling and downsampling. We'll leave enough space for two full index summaries. The two
        // coldest sstables will get downsampled to 8/128 of their size, leaving us with 1 and 112/128th index
        // summaries worth of space.  The hottest sstable should get a full index summary, and the one in the middle
        // should get the remainder.
        sstables.get(0).readMeter = new RestorableMeter(0.0, 0.0);
        sstables.get(1).readMeter = new RestorableMeter(0.0, 0.0);
        sstables.get(2).readMeter = new RestorableMeter(100, 100);
        sstables.get(3).readMeter = new RestorableMeter(128.0, 128.0);
        sstables = redistributeSummaries(Collections.EMPTY_LIST, sstables, (long) (singleSummaryOffHeapSpace + (singleSummaryOffHeapSpace * (100.0 / BASE_SAMPLING_LEVEL))));
        Collections.sort(sstables, hotnessComparator);
        assertEquals(MIN_SAMPLING_LEVEL, sstables.get(0).getIndexSummarySamplingLevel());
        assertEquals(MIN_SAMPLING_LEVEL, sstables.get(1).getIndexSummarySamplingLevel());
        assertTrue(sstables.get(2).getIndexSummarySamplingLevel() > MIN_SAMPLING_LEVEL);
        assertTrue(sstables.get(2).getIndexSummarySamplingLevel() < BASE_SAMPLING_LEVEL);
        assertEquals(BASE_SAMPLING_LEVEL, sstables.get(3).getIndexSummarySamplingLevel());
        validateData(cfs, numRows);

        // Don't leave enough space for even the minimal index summaries
        sstables = redistributeSummaries(Collections.EMPTY_LIST, sstables, 100);
        for (SSTableReader sstable : sstables)
            assertEquals(MIN_SAMPLING_LEVEL, sstable.getIndexSummarySamplingLevel());
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
            RowMutation rm = new RowMutation(ksname, key.key);
            rm.add(cfname, ByteBufferUtil.bytes("column"), value, 0);
            rm.apply();
        }
        cfs.forceBlockingFlush();

        List<SSTableReader> sstables = new ArrayList<>(cfs.getSSTables());
        assertEquals(1, sstables.size());
        SSTableReader sstable = sstables.get(0);

        for (int samplingLevel = MIN_SAMPLING_LEVEL; samplingLevel < BASE_SAMPLING_LEVEL; samplingLevel++)
        {
            sstable = sstable.cloneWithNewSummarySamplingLevel(samplingLevel);
            assertEquals(samplingLevel, sstable.getIndexSummarySamplingLevel());
            int expectedSize = (numRows * samplingLevel) / (sstable.metadata.getIndexInterval() * BASE_SAMPLING_LEVEL);
            assertEquals(expectedSize, sstable.getIndexSummarySize(), 1);
        }
    }

    @Test
    public void testJMXFunctions() throws IOException
    {
        IndexSummaryManager manager = IndexSummaryManager.instance;

        // resize interval
        assertNotNull(manager.getResizeIntervalInMinutes());
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
                RowMutation rm = new RowMutation(ksname, key.key);
                rm.add(cfname, ByteBufferUtil.bytes("column"), value, 0);
                rm.apply();
            }
            cfs.forceBlockingFlush();
        }

        assertEquals(1.0, manager.getAverageSamplingRatio(), 0.001);
        Map<String, Double> samplingRatios = manager.getSamplingRatios();
        for (Map.Entry<String, Double> entry : samplingRatios.entrySet())
            assertEquals(1.0, entry.getValue(), 0.001);

        manager.setMemoryPoolCapacityInMB(0);
        manager.redistributeSummaries();
        assertTrue(manager.getAverageSamplingRatio() < 0.99);
        samplingRatios = manager.getSamplingRatios();
        for (Map.Entry<String, Double> entry : samplingRatios.entrySet())
        {
            if (entry.getKey().contains("StandardLowIndexInterval"))
                assertTrue(entry.getValue() < 0.9);
        }
    }
}