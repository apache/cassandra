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

package org.apache.cassandra.io.sstable.filter;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReaderWithFilter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;

/**
 * All the readers of a table share the table's {@link BloomFilterTracker}, so tracker based gauges must count it once
 * regardless of the number of live sstables.
 */
public class BloomFilterMetricsTest extends CQLTester
{
    private static final int SSTABLES = 3;
    private static final int FALSE_POSITIVES = 5;

    @Test
    public void trackerBasedGaugesDoNotDependOnSSTableCount() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (int i = 0; i < SSTABLES; i++)
        {
            for (int j = 0; j < 10; j++)
                execute("INSERT INTO %s (k, v) VALUES (?, ?)", i * 10 + j, j);
            flush();
        }
        assertEquals(SSTABLES, cfs.getLiveSSTables().size());
        for (SSTableReader reader : cfs.getLiveSSTables())
            assertSame(cfs.getBloomFilterTracker(), ((SSTableReaderWithFilter) reader).getFilterTracker());

        for (int i = 0; i < FALSE_POSITIVES; i++)
            cfs.getBloomFilterTracker().addFalsePositive();
        cfs.getBloomFilterTracker().addTruePositive();
        cfs.getBloomFilterTracker().addLoadedBloomFilterHit();
        cfs.getBloomFilterTracker().addLoadedBloomFilterHit();

        BloomFilterMetrics<?> metrics = BloomFilterMetrics.instance;
        assertEquals(FALSE_POSITIVES, cfs.getBloomFilterFalsePositiveCount());

        // table level
        assertEquals(FALSE_POSITIVES, (long) metrics.bloomFilterFalsePositives.getTableGauge(cfs).getValue());
        assertEquals(2L, (long) metrics.loadedBloomFilterHits.getTableGauge(cfs).getValue());
        assertEquals((double) FALSE_POSITIVES / (FALSE_POSITIVES + 1),
                     metrics.bloomFilterFalseRatio.getTableGauge(cfs).getValue(), 0.0001);

        // keyspace and global levels must not be weighted by the number of sstables either
        Keyspace keyspace = cfs.keyspace;
        long keyspaceExpected = 0;
        for (ColumnFamilyStore store : keyspace.getColumnFamilyStores())
            keyspaceExpected += store.getBloomFilterFalsePositiveCount();
        assertNotEquals(0L, keyspaceExpected);
        assertEquals(keyspaceExpected, (long) metrics.bloomFilterFalsePositives.getKeyspaceGauge(keyspace).getValue());

        long globalExpected = 0;
        for (Keyspace ks : Keyspace.all())
            for (ColumnFamilyStore store : ks.getColumnFamilyStores())
                globalExpected += store.getBloomFilterFalsePositiveCount();
        assertEquals(globalExpected, (long) metrics.bloomFilterFalsePositives.getGlobalGauge().getValue());
    }

    /**
     * In-flight bloom filter memory (filters of sstables being written) belongs to the table, whether or not the table
     * has any live sstable yet, and must be counted for every table in the keyspace and global aggregates.
     */
    @Test
    public void offHeapMemoryGaugeIncludesInFlightMemoryOfEveryTable() throws Throwable
    {
        String table1 = createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        ColumnFamilyStore cfs1 = getCurrentColumnFamilyStore();
        String table2 = createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        ColumnFamilyStore cfs2 = getCurrentColumnFamilyStore();
        assertNotEquals(cfs1.metadata.id, cfs2.metadata.id);

        // table1 has live sstables, table2 has none (e.g. its first flush is in progress)
        cfs1.disableAutoCompaction();
        for (int i = 0; i < 2; i++)
        {
            execute("INSERT INTO " + keyspace() + "." + table1 + " (k, v) VALUES (?, ?)", i, i);
            cfs1.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        }
        assertEquals(2, cfs1.getLiveSSTables().size());
        assertEquals(0, cfs2.getLiveSSTables().size());

        long readers1 = 0;
        for (SSTableReader reader : cfs1.getLiveSSTables())
            readers1 += ((SSTableReaderWithFilter) reader).getFilterOffHeapSize();

        cfs1.metric.inFlightBloomFilterOffHeapMemoryUsed.set(1000);
        cfs2.metric.inFlightBloomFilterOffHeapMemoryUsed.set(20000);
        try
        {
            BloomFilterMetrics<?> metrics = BloomFilterMetrics.instance;
            assertEquals(readers1 + 1000, (long) metrics.bloomFilterOffHeapMemoryUsed.getTableGauge(cfs1).getValue());
            assertEquals(20000L, (long) metrics.bloomFilterOffHeapMemoryUsed.getTableGauge(cfs2).getValue());

            long keyspaceExpected = 0;
            for (ColumnFamilyStore store : cfs1.keyspace.getColumnFamilyStores())
            {
                keyspaceExpected += store.metric.inFlightBloomFilterOffHeapMemoryUsed.get();
                for (SSTableReader reader : store.getLiveSSTables())
                    keyspaceExpected += ((SSTableReaderWithFilter) reader).getFilterOffHeapSize();
            }
            assertEquals(keyspaceExpected, (long) metrics.bloomFilterOffHeapMemoryUsed.getKeyspaceGauge(cfs1.keyspace).getValue());

            long globalExpected = 0;
            for (Keyspace ks : Keyspace.all())
                for (ColumnFamilyStore store : ks.getColumnFamilyStores())
                {
                    globalExpected += store.metric.inFlightBloomFilterOffHeapMemoryUsed.get();
                    for (SSTableReader reader : store.getLiveSSTables())
                        if (reader instanceof SSTableReaderWithFilter)
                            globalExpected += ((SSTableReaderWithFilter) reader).getFilterOffHeapSize();
                }
            assertEquals(globalExpected, (long) metrics.bloomFilterOffHeapMemoryUsed.getGlobalGauge().getValue());
        }
        finally
        {
            cfs1.metric.inFlightBloomFilterOffHeapMemoryUsed.set(0);
            cfs2.metric.inFlightBloomFilterOffHeapMemoryUsed.set(0);
        }
    }
}
