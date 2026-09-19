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

package org.apache.cassandra.db.compaction.simple;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionPipelineCounts;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** A cell value larger than {@code max_value_size} must be refused by compaction on both paths. */
public class CompactionMaxValueSizeTest extends SimpleCompactionTest
{
    /** The value size written, larger than the lowered limit and under the default. */
    private static final int VALUE_SIZE = 2 << 20;

    /** The limit lowered to before compacting, below VALUE_SIZE. */
    private static final int LOWERED_LIMIT = 1 << 20;

    /** The timestamp both halves of the comparison scenario are written at. */
    private static final long TIE_TIMESTAMP = 5000L;

    @Test
    public void testOversizedCellValueIsRefusedWhenCopiedStraightThrough() throws Throwable
    {
        // Distinct partitions, so each value is streamed straight through to the writer.
        runRefusalScenario((table, cfs) -> {
            execute("INSERT INTO " + table + " (pk, ck, v) VALUES (?, ?, ?)", 0L, 0L, oversizedValue(1));
            cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);
            execute("INSERT INTO " + table + " (pk, ck, v) VALUES (?, ?, ?)", 1L, 0L, oversizedValue(2));
            cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);
        }, 2, false);
    }

    @Test
    public void testOversizedCellValueIsRefusedWhenBufferedForComparison() throws Throwable
    {
        // One clustering, two same-timestamp values in different sstables, so the merge compares
        // values and buffers one rather than streaming it through.
        runRefusalScenario((table, cfs) -> {
            execute("INSERT INTO " + table + " (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP " + TIE_TIMESTAMP,
                    0L, 0L, oversizedValue(1));
            cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);
            execute("INSERT INTO " + table + " (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP " + TIE_TIMESTAMP,
                    0L, 0L, oversizedValue(2));
            cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);
        }, 1, true);
    }

    /** Writes the two input sstables for one scenario. */
    private interface Writes
    {
        void write(String table, ColumnFamilyStore cfs) throws Throwable;
    }

    /**
     * Builds the table, runs {@code writes}, lowers {@code max_value_size} below the value size,
     * compacts, and asserts the refusal.
     *
     * @param expectedRows rows the inputs hold
     * @param sameTimestampTie whether the two inputs collide at one clustering on one timestamp
     */
    private void runRefusalScenario(Writes writes, int expectedRows, boolean sameTimestampTie) throws Throwable
    {
        String keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : " +
                                         "'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        // blob is variable-length, so its length is decoded and checked.
        String table = createTable(keyspace, "CREATE TABLE %s ( pk bigint, ck bigint, v blob, PRIMARY KEY(pk, ck))");
        execute("use " + keyspace + ";");
        Keyspace.system().forEach(k -> k.getColumnFamilyStores().forEach(ColumnFamilyStore::disableAutoCompaction));

        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();

        writes.write(table, cfs);

        Set<Descriptor> inputs = descriptors(cfs);
        assertEquals("the scenario needs two input sstables so that compaction merges rather than " +
                     "skipping", 2, inputs.size());
        assertEquals("the scenario must hold the rows it claims before compacting",
                     expectedRows, readValues(table));
        if (sameTimestampTie)
            assertSameTimestampTie(cfs, expectedRows);
        assertCursorPathWillRun(cfs);

        int originalLimit = DatabaseDescriptor.getMaxValueSize();
        assertTrue("the value must be writable under the limit in force at write time: value=" +
                   VALUE_SIZE + " limit=" + originalLimit, VALUE_SIZE <= originalLimit);

        Throwable thrown;
        DatabaseDescriptor.setMaxValueSize(LOWERED_LIMIT);
        try
        {
            int effectiveLimit = DatabaseDescriptor.getMaxValueSize();
            assertTrue("the lowered limit must be a real bound that this scenario's value exceeds, " +
                       "not a truncated one: requested=" + LOWERED_LIMIT + " effective=" +
                       effectiveLimit + " value=" + VALUE_SIZE,
                       effectiveLimit > 0 && effectiveLimit < VALUE_SIZE);
            CompactionPipelineCounts pipelines = CompactionPipelineCounts.mark();
            thrown = compactExpectingRefusal(cfs);
            CompactionPipelineCounts.assertPipelineRan(cursorCompactionEnabled &&
                                                       DatabaseDescriptor.getSelectedSSTableFormat().supportsCursorCompaction(),
                                                       pipelines);
        }
        finally
        {
            DatabaseDescriptor.setMaxValueSize(originalLimit);
        }

        CorruptSSTableException refusal = findCorruptSSTableException(thrown);
        assertNotNull("compaction must refuse an oversized value with a CorruptSSTableException, " +
                      "but the failure was: " + describe(thrown), refusal);
        // The refusal must be the max_value_size check, not some other corruption.
        assertTrue("the refusal must be the max_value_size check, but was: " + describe(thrown),
                   describe(thrown).contains("max_value_size"));

        // A refused value must leave no output behind.
        assertEquals("a refused compaction must not commit an output sstable", inputs, descriptors(cfs));

        // The inputs must still read at full length once the limit is restored.
        assertEquals("the input sstables must still be readable after the refusal",
                     expectedRows, readValues(table));
    }

    /**
     * Reads every row's blob and returns the row count, failing if any value does not decode to its
     * written length.
     */
    private int readValues(String table) throws Throwable
    {
        int rows = 0;
        for (UntypedResultSet.Row row : execute("SELECT pk, ck, v FROM " + table))
        {
            String at = " at pk=" + row.getLong("pk") + " ck=" + row.getLong("ck");
            assertTrue("expected every row to carry a value" + at, row.has("v"));
            assertEquals("a value did not decode to the length it was written with" + at,
                         VALUE_SIZE, row.getBytes("v").remaining());
            rows++;
        }
        return rows;
    }

    /** Asserts the two oversized values meet at one clustering on one timestamp, forcing a value comparison. */
    private static void assertSameTimestampTie(ColumnFamilyStore cfs, int expectedRows)
    {
        assertEquals("a same-timestamp tie resolves to the one row both writes target", 1, expectedRows);
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            StatsMetadata stats = sstable.getSSTableMetadata();
            assertEquals("every input of the tie scenario must be written wholly at the tie " +
                         "timestamp, or the merge resolves on timestamp instead of on value: " +
                         sstable.descriptor, TIE_TIMESTAMP, stats.minTimestamp);
            assertEquals("every input of the tie scenario must be written wholly at the tie " +
                         "timestamp, or the merge resolves on timestamp instead of on value: " +
                         sstable.descriptor, TIE_TIMESTAMP, stats.maxTimestamp);
        }
    }

    /** Runs the major compaction that must fail and returns what it threw. */
    private Throwable compactExpectingRefusal(ColumnFamilyStore cfs)
    {
        try
        {
            cfs.forceMajorCompaction();
        }
        catch (Throwable t)
        {
            return t;
        }
        fail("compaction accepted a cell value of " + VALUE_SIZE + " bytes under a max_value_size " +
             "of " + LOWERED_LIMIT);
        throw new AssertionError("unreachable");
    }

    private static CorruptSSTableException findCorruptSSTableException(Throwable t)
    {
        for (Throwable cause = t; cause != null; cause = cause.getCause())
        {
            if (cause instanceof CorruptSSTableException)
                return (CorruptSSTableException) cause;
            if (cause.getCause() == cause)
                break;
        }
        return null;
    }

    /** The whole cause chain's messages. */
    private static String describe(Throwable t)
    {
        StringBuilder sb = new StringBuilder();
        for (Throwable cause = t; cause != null; cause = cause.getCause())
        {
            sb.append(cause.getClass().getName()).append(": ").append(cause.getMessage()).append('\n');
            if (cause.getCause() == cause)
                break;
        }
        return sb.toString();
    }

    private static Set<Descriptor> descriptors(ColumnFamilyStore cfs)
    {
        Set<Descriptor> descriptors = new HashSet<>();
        for (SSTableReader sstable : cfs.getLiveSSTables())
            descriptors.add(sstable.descriptor);
        return descriptors;
    }

    private static ByteBuffer oversizedValue(int salt)
    {
        byte[] bytes = new byte[VALUE_SIZE];
        for (int i = 0; i < bytes.length; i++)
            bytes[i] = (byte) (i + salt);
        return ByteBuffer.wrap(bytes);
    }
}
