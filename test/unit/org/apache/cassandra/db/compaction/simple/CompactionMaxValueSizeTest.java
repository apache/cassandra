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
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * A cell value whose decoded length exceeds {@code max_value_size} must be refused by compaction,
 * not copied. {@link org.apache.cassandra.db.marshal.AbstractType#read} applies two checks to a
 * variable-length value's length — reject a negative one, reject one above the limit — and both
 * compaction paths have to apply both: the iterator path reaches them through
 * {@code Cell.Serializer.deserialize}, the cursor path mirrors them in
 * {@code SSTableCursorReader.copyCellContents}.
 * <p>
 * The differential harness cannot cover this. It compares the outputs of two runs that both
 * succeeded, and the correct behaviour here is that neither run produces an output at all. So the
 * assertion has to be absolute, which is what this suite's {diskAccessMode, cursor} parameter rows
 * give: the same refusal is required of both paths.
 * <p>
 * Two scenarios, because the cursor reaches its single check — in {@code copyCellContents} — from two
 * different merge shapes: a value streamed from reader to writer, and a value copied into the
 * compactor's temp buffer because a same-timestamp tie sent the merge into
 * {@code resolveRegular}'s value comparison. Both refuse; the second is the one the memtable can
 * silently take away.
 * <p>
 * The oversized length is produced by writing a value that is legal under the configured limit and
 * then lowering the limit before compacting, rather than by corrupting a length vint — which is the
 * shape real corruption takes, but is not something CQL can write. The check being exercised is the
 * same one either way: it tests the decoded length against
 * {@code DatabaseDescriptor.getMaxValueSize()} at read time.
 */
public class CompactionMaxValueSizeTest extends SimpleCompactionTest
{
    /** Twice LOWERED_LIMIT, and far under the 256MiB default the value is written under. */
    private static final int VALUE_SIZE = 2 << 20;

    /**
     * Below VALUE_SIZE, and a whole number of mebibytes: {@link DatabaseDescriptor#setMaxValueSize}
     * stores its argument as an {@code IntMebibytesBound}, so it divides by 1MiB and a sub-mebibyte
     * limit truncates to a limit of ZERO — under which every non-empty variable-length value is
     * refused and the scenario would no longer be about an oversized one. The limit is also global
     * while it is lowered, so it has to stay large enough that ordinary reads elsewhere are
     * unaffected. {@link #runRefusalScenario} asserts the effective limit after lowering rather than
     * trusting this constant.
     */
    private static final int LOWERED_LIMIT = 1 << 20;

    /** The explicit timestamp both halves of the comparison scenario are written at. */
    private static final long TIE_TIMESTAMP = 5000L;

    @Test
    public void testOversizedCellValueIsRefusedWhenCopiedStraightThrough() throws Throwable
    {
        // Distinct partitions: nothing ties, so each value is streamed from the reader straight to
        // the writer and copyCellContents is reached from SSTableCursorWriter.
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
        // One partition, one clustering, two same-timestamp values in different sstables. The merge
        // reaches the value comparison the shared decision defers to the compactor, which copies a
        // value into one of the compactor's temp buffers rather than streaming it to the
        // writer, so copyCellContents is reached from the compactor instead. Only reachable across
        // sstables: within one flush the memtable reconciles the pair and the comparison never
        // happens, which is what assertSameTimestampTie pins.
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
     * @param expectedRows rows the inputs hold, asserted again after the refusal to show the
     *                     sstables were not damaged
     * @param sameTimestampTie whether this scenario's two inputs must collide at one clustering on
     *                         one timestamp; asserted, so the scenario cannot stop reaching the
     *                         value comparison and silently duplicate the straight-through one
     */
    private void runRefusalScenario(Writes writes, int expectedRows, boolean sameTimestampTie) throws Throwable
    {
        String keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : " +
                                         "'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        // blob is variable-length, which is what puts a length vint on the wire ahead of the value;
        // a fixed-length type carries no vint and its length is never decoded, so neither path has
        // anything to check.
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
            // Bracketed here rather than inside compactExpectingRefusal, whose catch(Throwable)
            // would swallow the assertion. The counter still moves on a refused compaction: the
            // pipeline is selected in AbstractCompactionPipeline.create, and the value-size check
            // that refuses runs afterwards, inside the pipeline.
            CompactionPipelineCounts pipelines = CompactionPipelineCounts.mark();
            thrown = compactExpectingRefusal(cfs);
            CompactionPipelineCounts.assertPipelineRan(cursorCompactionEnabled && BigFormat.isSelected(), pipelines);
        }
        finally
        {
            DatabaseDescriptor.setMaxValueSize(originalLimit);
        }

        CorruptSSTableException refusal = findCorruptSSTableException(thrown);
        assertNotNull("compaction must refuse an oversized value with a CorruptSSTableException, " +
                      "but the failure was: " + describe(thrown), refusal);
        // Both paths format the same message, by construction: the cursor's check was written to
        // mirror AbstractType.read's wording. Asserting it separates this refusal from any other
        // corruption the compaction might have reported instead.
        assertTrue("the refusal must be the max_value_size check, but was: " + describe(thrown),
                   describe(thrown).contains("max_value_size"));

        // A refused value must leave no output behind: the inputs are still the live set, and no
        // partially written sstable was committed alongside them.
        assertEquals("a refused compaction must not commit an output sstable", inputs, descriptors(cfs));

        // ...and the refusal must be a clean one rather than damage: with the limit restored every
        // value decodes again at its full length. readValues projects v, so the values are really
        // read rather than skipped.
        assertEquals("the input sstables must still be readable after the refusal",
                     expectedRows, readValues(table));
    }

    /**
     * Reads every row's blob and returns the row count, failing if any value does not decode to its
     * written length. Projecting {@code v} matters: a query that does not select it leaves the column
     * fetched-but-not-queried, and {@code Cell.Serializer.deserialize} then skips the value instead of
     * decoding it, so the read would say nothing about the payload.
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

    /**
     * Every input carries exactly the tie timestamp, and the scenario resolves to the single row the
     * two of them collide on, so the two oversized values really do meet at one clustering on one
     * timestamp — which is what sends the merge into {@code resolveRegular}'s value comparison rather
     * than letting it decide on the timestamp and skip the loser's value, where no length check runs.
     * <p>
     * Guards against a scenario drifting off its clustering or its timestamp, not against a
     * deliberate rewrite: giving one of the two writes a TTL, for instance, moves the merge onto the
     * expiring-beats-live rule while leaving every assertion here satisfied.
     */
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

    /**
     * Runs the major compaction that must fail and returns what it threw.
     * <p>
     * Deliberately catches Throwable: the exception crosses a compaction executor thread and
     * {@code FBUtilities.waitOnFutures} may wrap it, so the type is asserted on the cause chain by
     * the caller rather than here.
     */
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

    /** The whole cause chain's messages, so an assertion failure names the real reason. */
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
