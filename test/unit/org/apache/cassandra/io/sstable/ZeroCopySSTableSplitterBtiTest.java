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

import java.util.Collections;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.TestDatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.AntiCompactionRunPlanner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReader.PartitionPositionBounds;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Every entry point of the zero-copy split has to refuse a non-BIG sstable, and refuse it in the shape its
 * caller expects: the two {@code split} overloads by throwing, and the two planners by answering "no".
 *
 * <p>The technique is to copy Data.db compression chunks verbatim and rewrite exactly one position field per
 * Index.db record. BTI has no Index.db at all -- its partition positions live inside the trie payloads of
 * Partitions.db and inside Rows.db, in variable-width encodings whose widths would change if the values were
 * rebased -- so there is nothing here that can be made to work for it, and the only correct behaviour is a
 * clean refusal.
 *
 * <p>Trunk ships BTI as a selectable format, so this refusal is a live path rather than a theoretical one:
 * {@code CompactionManager.doAntiCompaction} calls {@link AntiCompactionRunPlanner#plan} for every sstable it
 * is given and silently falls back to the rewriting anticompaction, and {@code CassandraOutgoingFile} calls
 * {@link ZeroCopySSTableSlice#plan} for every outgoing file. A throw from either would break a BTI cluster
 * that never asked for any of this.
 *
 * <p>The format is switched per test rather than per JVM: {@code TestDatabaseDescriptor} pauses compactions
 * around the change, so a table created after {@link #selectBtiFormat()} flushes BTI sstables. The original
 * format is restored in {@link #restoreSSTableFormat()} before {@code CQLTester} tears the schema down.
 */
public class ZeroCopySSTableSplitterBtiTest extends CQLTester
{
    private SSTableFormat<?, ?> savedFormat;

    @Before
    public void selectBtiFormat()
    {
        savedFormat = DatabaseDescriptor.getSelectedSSTableFormat();
        TestDatabaseDescriptor.setUnsafeSelectedSSTableFormat(BtiFormat.NAME);
    }

    @After
    public void restoreSSTableFormat()
    {
        if (savedFormat != null)
            TestDatabaseDescriptor.setUnsafeSelectedSSTableFormat(savedFormat);
    }

    @Test
    public void isSupportedIsFalseForBti() throws Throwable
    {
        SSTableReader parent = compressedBtiSSTable();

        // Compression is the *other* precondition, and it holds here: the format is the only thing wrong.
        assertTrue(parent.compression);
        assertFalse(ZeroCopySSTableSplitter.isSupported(parent));
    }

    @Test
    public void splitByCountThrowsNamingTheFormat() throws Throwable
    {
        SSTableReader parent = compressedBtiSSTable();

        assertThatThrownBy(() -> ZeroCopySSTableSplitter.split(parent, 2, null))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessageContaining(BtiFormat.NAME)
            .hasMessageContaining("BIG");
    }

    @Test
    public void splitByBoundariesThrowsNamingTheFormat() throws Throwable
    {
        SSTableReader parent = compressedBtiSSTable();

        assertThatThrownBy(() -> ZeroCopySSTableSplitter.split(parent, Collections.emptyList(), null))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessageContaining(BtiFormat.NAME)
            .hasMessageContaining("BIG");
    }

    /**
     * The planner is called from inside {@code doAntiCompaction}'s loop, which has no handler for it: it must
     * answer, not throw, and the answer must say the format is why so an operator can see it in the log.
     */
    @Test
    public void plannerReportsBtiIneligibleWithoutThrowingOrReadingAnIndex() throws Throwable
    {
        SSTableReader parent = compressedBtiSSTable();

        // A range that owns the whole sstable, i.e. the shape that would be eligible (2 runs) for BIG.
        Range<Token> everything = new Range<>(parent.getFirst().getToken(), parent.getLast().getToken());
        RangesAtEndpoint ranges = fullOnly(everything);

        AntiCompactionRunPlanner.Plan plan = AntiCompactionRunPlanner.plan(parent, ranges, nextTimeUUID());

        assertFalse(plan.toString(), plan.eligible);
        assertNotNull(plan.ineligibleReason);
        assertTrue(plan.ineligibleReason, plan.ineligibleReason.contains(BtiFormat.NAME));
        assertTrue(plan.ineligibleReason, plan.ineligibleReason.contains("BIG"));
        // refused before any walk, so nothing was counted
        assertEquals(0, plan.runCount);
        assertTrue(plan.boundaries.isEmpty());
        assertTrue(plan.perChild.isEmpty());
    }

    /**
     * Same contract on the streaming side: {@code CassandraOutgoingFile} asks for a plan for every file it is
     * about to send, and a refusal is what makes it fall back to the row-by-row stream.
     */
    @Test
    public void slicePlanRefusesBtiWithWrongFormat() throws Throwable
    {
        SSTableReader parent = compressedBtiSSTable();

        List<PartitionPositionBounds> wholeFile =
            Collections.singletonList(new PartitionPositionBounds(0, parent.uncompressedLength()));

        ZeroCopySSTableSlice.Plan plan = ZeroCopySSTableSlice.plan(parent, wholeFile, 1.0);

        assertFalse(plan.toString(), plan.isEligible());
        assertEquals(ZeroCopySSTableSlice.Reason.WRONG_FORMAT, plan.reason);
        assertTrue(plan.runs.isEmpty());
    }

    /**
     * One compressed BTI sstable. Compression matters: it is the splitter's other precondition, so leaving it
     * out would let these tests pass for the wrong reason.
     */
    private SSTableReader compressedBtiSSTable() throws Throwable
    {
        createTable("CREATE TABLE %s (pk text, ck int, val text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'class': 'LZ4Compressor', 'chunk_length_in_kb': '4'}");
        disableCompaction();
        for (int p = 0; p < 40; p++)
            execute("INSERT INTO %s (pk, ck, val) VALUES (?, ?, ?)", String.format("k%06d", p), 0, "value");
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        assertEquals(1, cfs.getLiveSSTables().size());
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();

        // Guards against the whole class silently degenerating into a second BIG-format test.
        assertTrue("expected a BTI sstable, got " + sstable.descriptor.getFormat().name(),
                   BtiFormat.is(sstable.descriptor.getFormat()));
        assertFalse(BigFormat.is(sstable.descriptor.getFormat()));
        return sstable;
    }

    private static RangesAtEndpoint fullOnly(Range<Token> range)
    {
        InetAddressAndPort local = FBUtilities.getBroadcastAddressAndPort();
        return RangesAtEndpoint.builder(local).add(Replica.fullReplica(local, range)).build();
    }
}
