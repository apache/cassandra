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
import java.util.Arrays;
import java.util.Collection;

import org.junit.BeforeClass;
import org.junit.Test;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistryListener;
import com.codahale.metrics.Timer;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.junit.Assert.*;

public class ColumnFamilyMetricTest
{
    @BeforeClass
    public static void defineSchema() throws Exception
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace("Keyspace1",
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD("Keyspace1", "Standard2"));

        // we need this to properly initialize various static fields in the whole system
        // since for the unit tests automatic schema flushing is disabled, the first flush may happen unexpectedly
        // late - after the whole system is already running, and some static fields may remain uninitialized
        // OTOH, late initialization of them may have creepy effects (for example NPEs in static initializers)
        // disclaimer: this is not a proper way to fix that
        StorageService.instance.forceKeyspaceFlush(SchemaConstants.SYSTEM_KEYSPACE_NAME, ColumnFamilyStore.FlushReason.UNIT_TESTS);
    }

    @Test
    public void testSizeMetric()
    {
        Keyspace keyspace = Keyspace.open("Keyspace1");
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore("Standard2");
        cfs.disableAutoCompaction();

        cfs.truncateBlocking();

        Util.spinAssertEquals(0L, cfs.metric.liveDiskSpaceUsed::getCount, 30);
        Util.spinAssertEquals(0L, cfs.metric.totalDiskSpaceUsed::getCount, 30);

        for (int j = 0; j < 10; j++)
        {
            applyMutation(cfs.metadata(), String.valueOf(j), ByteBufferUtil.EMPTY_BYTE_BUFFER, FBUtilities.timestampMicros());
        }
        Util.flush(cfs);
        Collection<SSTableReader> sstables = cfs.getLiveSSTables();
        long size = 0;
        for (SSTableReader reader : sstables)
        {
            size += reader.bytesOnDisk();
        }

        // size metrics should show the sum of all SSTable sizes
        assertEquals(size, cfs.metric.liveDiskSpaceUsed.getCount());
        assertEquals(size, cfs.metric.totalDiskSpaceUsed.getCount());

        cfs.truncateBlocking();

        // after truncate, size metrics should be down to 0
        Util.spinAssertEquals(0L, cfs.metric.liveDiskSpaceUsed::getCount, 30);
        Util.spinAssertEquals(0L, cfs.metric.totalDiskSpaceUsed::getCount, 30);

        cfs.enableAutoCompaction();
    }

    @Test
    public void testColUpdateTimeDeltaFiltering()
    {
        Keyspace keyspace = Keyspace.open("Keyspace1");
        ColumnFamilyStore store = keyspace.getColumnFamilyStore("Standard2");

        // This confirms another test/set up did not overflow the histogram
        store.metric.colUpdateTimeDeltaHistogram.cf.getSnapshot().get999thPercentile();

        applyMutation(store.metadata(), "4242", ByteBufferUtil.bytes("0"), 0);

        // The histogram should not have overflowed on the first write
        store.metric.colUpdateTimeDeltaHistogram.cf.getSnapshot().get999thPercentile();

        // smallest time delta that would overflow the histogram if unfiltered
        applyMutation(store.metadata(), "4242", ByteBufferUtil.bytes("1"), 18165375903307L);

        // CASSANDRA-11117 - update with large timestamp delta should not overflow the histogram
        store.metric.colUpdateTimeDeltaHistogram.cf.getSnapshot().get999thPercentile();
    }

    @Test
    public void testStartupRaceConditionOnMetricListeners()
    {
        // CASSANDRA-16228
        // Since the ColumnFamilyStore instance reference escapes during the construction
        // we have a race condition and listeners can see an instance that is in an unknown state.
        // This test just check that all callbacks can access the data without throwing any exception.
        TestBase listener = new TestBase();

        try {
            CassandraMetricsRegistry.Metrics.addListener(listener);

            SchemaLoader.createKeyspace("Keyspace2",
                                        KeyspaceParams.simple(1),
                                        SchemaLoader.standardCFMD("Keyspace2", "Standard2"));
        }
        finally {
            CassandraMetricsRegistry.Metrics.removeListener(listener);
        }
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

            applyMutation(store.metadata(), "0", bytes(0), FBUtilities.timestampMicros());
            applyMutation(store.metadata(), "1", bytes(1), FBUtilities.timestampMicros());

            // Flushing first SSTable
            Util.flush(store);

            long[] estimatedColumnCountHistogram = store.metric.estimatedColumnCountHistogram.getValue();
            assertNumberOfNonZeroValue(estimatedColumnCountHistogram, 1);
            assertEquals(2, estimatedColumnCountHistogram[0]); //2 rows of one cell in 1 SSTable

            long[] estimatedRowSizeHistogram = store.metric.estimatedPartitionSizeHistogram.getValue();
            // Due to the timestamps we cannot guaranty the size of the row. So we can only check the number of histogram updates.
            assertEquals(sumValues(estimatedRowSizeHistogram), 2);

            applyMutation(store.metadata(), "2", bytes(2), FBUtilities.timestampMicros());

            // Flushing second SSTable
            Util.flush(store);

            estimatedColumnCountHistogram = store.metric.estimatedColumnCountHistogram.getValue();
            assertNumberOfNonZeroValue(estimatedColumnCountHistogram, 1);
            assertEquals(3, estimatedColumnCountHistogram[0]); //2 rows of one cell in the first SSTable and 1 row of one cell int the second sstable

            estimatedRowSizeHistogram = store.metric.estimatedPartitionSizeHistogram.getValue();
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

        long[] result = TableMetrics.addHistogram(sums, smaller);
        assertTrue(result == sums); // Check that we did not create a new array
        assertArrayEquals(new long[]{1, 2, 0}, result);

        long[] equal = new long[] {5, 6, 7};

        result = TableMetrics.addHistogram(sums, equal);
        assertTrue(result == sums); // Check that we did not create a new array
        assertArrayEquals(new long[]{6, 8, 7}, result);

        long[] empty = new long[0];

        result = TableMetrics.addHistogram(sums, empty);
        assertTrue(result == sums); // Check that we did not create a new array
        assertArrayEquals(new long[]{6, 8, 7}, result);

        long[] greater = new long[] {4, 3, 2, 1};
        result = TableMetrics.addHistogram(sums, greater);
        assertFalse(result == sums); // Check that we created a new array
        assertArrayEquals(new long[]{10, 11, 9, 1}, result);
    }

    private static void applyMutation(TableMetadata metadata, Object pk, ByteBuffer value, long timestamp)
    {
        new RowUpdateBuilder(metadata, timestamp, pk).clustering("0")
                                                     .add("val", value)
                                                     .build()
                                                     .applyUnsafe();
    }

    private static void assertNumberOfNonZeroValue(long[] array, long expectedCount)
    {
        long actualCount = Arrays.stream(array).filter(v -> v != 0).count();
        if (expectedCount != actualCount)
            fail("Unexpected number of non zero values. (expected: " + expectedCount + ", actual: " + actualCount + " array: " + Arrays.toString(array)+ " )");
    }

    private static long sumValues(long[] array)
    {
        return Arrays.stream(array).sum();
    }

    private static class TestBase extends MetricRegistryListener.Base
    {
        @Override
        public void onGaugeAdded(String name, Gauge<?> gauge)
        {
            gauge.getValue();
        }

        @Override
        public void onGaugeRemoved(String name)
        {

        }

        @Override
        public void onCounterAdded(String name, Counter counter)
        {
            counter.getCount();
        }

        @Override
        public void onCounterRemoved(String name)
        {

        }

        @Override
        public void onHistogramAdded(String name, Histogram histogram)
        {
            histogram.getCount();
        }

        @Override
        public void onHistogramRemoved(String name)
        {

        }

        @Override
        public void onMeterAdded(String name, Meter meter)
        {
            meter.getCount();
        }

        @Override
        public void onMeterRemoved(String name)
        {

        }

        @Override
        public void onTimerAdded(String name, Timer timer)
        {
            timer.getCount();
        }

        @Override
        public void onTimerRemoved(String name)
        {

        }
    }
}
