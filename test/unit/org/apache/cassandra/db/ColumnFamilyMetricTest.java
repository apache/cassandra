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
package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import java.util.Collection;


import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.metrics.ColumnFamilyMetrics;
import org.apache.cassandra.utils.ByteBufferUtil;

import com.google.common.base.Supplier;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.apache.cassandra.Util.cellname;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

public class ColumnFamilyMetricTest
{
    @BeforeClass
    public static void defineSchema() throws Exception
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace("Keyspace1",
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD("Keyspace1", "Standard2"));
    }

    @Test
    public void testSizeMetric()
    {
        Keyspace keyspace = Keyspace.open("Keyspace1");
        final ColumnFamilyStore store = keyspace.getColumnFamilyStore("Standard2");
        store.disableAutoCompaction();

        store.truncateBlocking();

        Supplier<Object> getLiveDiskSpaceUsed = new Supplier<Object>()
        {
            public Long get()
            {
                return store.metric.liveDiskSpaceUsed.getCount();
            }
        };

        Supplier<Object> getTotalDiskSpaceUsed = new Supplier<Object>()
        {
            public Long get()
            {
                return store.metric.totalDiskSpaceUsed.getCount();
            }
        };

        Util.spinAssertEquals(0L, getLiveDiskSpaceUsed, 30);
        Util.spinAssertEquals(0L, getTotalDiskSpaceUsed, 30);

        for (int j = 0; j < 10; j++)
        {
            ByteBuffer key = ByteBufferUtil.bytes(String.valueOf(j));
            applyStandard2Mutation(key, ByteBufferUtil.EMPTY_BYTE_BUFFER, j);
        }
        store.forceBlockingFlush();
        Collection<SSTableReader> sstables = store.getSSTables();
        long size = 0;
        for (SSTableReader reader : sstables)
        {
            size += reader.bytesOnDisk();
        }

        // size metrics should show the sum of all SSTable sizes
        assertEquals(size, store.metric.liveDiskSpaceUsed.getCount());
        assertEquals(size, store.metric.totalDiskSpaceUsed.getCount());

        store.truncateBlocking();

        // after truncate, size metrics should be down to 0
        Util.spinAssertEquals(0L, getLiveDiskSpaceUsed, 30);
        Util.spinAssertEquals(0L, getTotalDiskSpaceUsed, 30);

        store.enableAutoCompaction();
    }

    @Test
    public void testColUpdateTimeDeltaFiltering()
    {
        Keyspace keyspace = Keyspace.open("Keyspace1");
        ColumnFamilyStore store = keyspace.getColumnFamilyStore("Standard2");

        // This confirms another test/set up did not overflow the histogram
        store.metric.colUpdateTimeDeltaHistogram.cf.getSnapshot().get999thPercentile();

        ByteBuffer key = ByteBufferUtil.bytes(4242);
        applyStandard2Mutation(key, ByteBufferUtil.bytes("0"), 0);

        // The histogram should not have overflowed on the first write
        store.metric.colUpdateTimeDeltaHistogram.cf.getSnapshot().get999thPercentile();

        // smallest time delta that would overflow the histogram if unfiltered
        applyStandard2Mutation(key, ByteBufferUtil.bytes("1"), 18165375903307L);

        // CASSANDRA-11117 - update with large timestamp delta should not overflow the histogram
        store.metric.colUpdateTimeDeltaHistogram.cf.getSnapshot().get999thPercentile();
    }

    @Test
    public void testEstimatedColumnCountHistogramAndEstimatedRowSizeHistogram()
    {
        Keyspace keyspace = Keyspace.open("Keyspace1");
        ColumnFamilyStore store = keyspace.getColumnFamilyStore("Standard2");

        store.disableAutoCompaction();

        try
        {
            // Ensure that there is no SSTables
            store.truncateBlocking();

            assertArrayEquals(new long[0], store.metric.estimatedColumnCountHistogram.getValue());

            applyStandard2Mutation(bytes(0), bytes(0), 0);
            applyStandard2Mutation(bytes(1), bytes(1), 0);

            // Flushing first SSTable
            store.forceBlockingFlush();

            long[] estimatedColumnCountHistogram = store.metric.estimatedColumnCountHistogram.getValue();
            assertNumberOfNonZeroValue(estimatedColumnCountHistogram, 1);
            assertEquals(2, estimatedColumnCountHistogram[0]); //2 rows of one cell in 1 SSTable

            long[] estimatedRowSizeHistogram = store.metric.estimatedRowSizeHistogram.getValue();
            // Due to the timestamps we cannot guaranty the size of the row. So we can only check the number of histogram updates.
            assertEquals(sumValues(estimatedRowSizeHistogram), 2);

            applyStandard2Mutation(bytes(2), bytes(2), 0);

            // Flushing second SSTable
            store.forceBlockingFlush();

            estimatedColumnCountHistogram = store.metric.estimatedColumnCountHistogram.getValue();
            assertNumberOfNonZeroValue(estimatedColumnCountHistogram, 1);
            assertEquals(3, estimatedColumnCountHistogram[0]); //2 rows of one cell in the first SSTable and 1 row of one cell int the second sstable

            estimatedRowSizeHistogram = store.metric.estimatedRowSizeHistogram.getValue();
            assertEquals(sumValues(estimatedRowSizeHistogram), 3);
        }
        finally
        {
            store.enableAutoCompaction();
        }
    }

    @Test
    public void testAddHistogram()
    {
        long[] sums = new long[] {0, 0, 0};
        long[] smaller = new long[] {1, 2};

        long[] result = ColumnFamilyMetrics.addHistogram(sums, smaller);
        assertTrue(result == sums); // Check that we did not create a new array
        assertArrayEquals(new long[]{1, 2, 0}, result);

        long[] equal = new long[] {5, 6, 7};

        result = ColumnFamilyMetrics.addHistogram(sums, equal);
        assertTrue(result == sums); // Check that we did not create a new array
        assertArrayEquals(new long[]{6, 8, 7}, result);

        long[] empty = new long[0];

        result = ColumnFamilyMetrics.addHistogram(sums, empty);
        assertTrue(result == sums); // Check that we did not create a new array
        assertArrayEquals(new long[]{6, 8, 7}, result);

        long[] greater = new long[] {4, 3, 2, 1};
        result = ColumnFamilyMetrics.addHistogram(sums, greater);
        assertFalse(result == sums); // Check that we did not create a new array
        assertArrayEquals(new long[]{10, 11, 9, 1}, result);
    }

    private static void applyStandard2Mutation(ByteBuffer pk, ByteBuffer value, long timestamp)
    {
        Mutation m = new Mutation("Keyspace1", pk);
        m.add("Standard2", cellname("0"), value, timestamp);
        m.apply();
    }

    private static void assertNumberOfNonZeroValue(long[] array, int expectedCount)
    {
        int actualCount = countNonZeroValues(array);
        assertEquals("Unexpected number of non zero values. (expected: " + expectedCount + ", actual: " + actualCount + ")",
                     expectedCount, actualCount);
    }

    private static int countNonZeroValues(long[] array)
    {
        int count = 0;
        for (long value : array)
        {
            if (value != 0)
                count++;
        }
        return count;
    }

    private static long sumValues(long[] array)
    {
        long sum = 0;
        for (long value : array)
        {
            sum += value;
        }
        return sum;
    }
}
