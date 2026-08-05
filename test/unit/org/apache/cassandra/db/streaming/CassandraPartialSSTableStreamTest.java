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
package org.apache.cassandra.db.streaming;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import com.google.common.collect.Sets;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.TestDatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.lifecycle.StreamingLifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.IVerifier;
import org.apache.cassandra.io.sstable.KeyIterator;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableTxnSingleStreamWriter;
import org.apache.cassandra.io.sstable.ZeroCopySSTableSlice;
import org.apache.cassandra.io.sstable.ZeroCopySSTableSlice.Reason;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReader.PartitionPositionBounds;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.StreamingMetrics;
import org.apache.cassandra.net.AsyncStreamingOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.streaming.SessionInfo;
import org.apache.cassandra.streaming.StreamCoordinator;
import org.apache.cassandra.streaming.StreamEventHandler;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamResultFuture;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.StreamSummary;
import org.apache.cassandra.streaming.async.NettyStreamingConnectionFactory;
import org.apache.cassandra.streaming.messages.StreamMessageHeader;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.cassandra.utils.concurrent.Ref;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultFileRegion;
import io.netty.channel.embedded.EmbeddedChannel;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * A partial sstable stream, end to end over an embedded netty channel: {@link CassandraOutgoingFile} synthesises
 * the slice's components and sends them with a byte range of the parent's Data.db, and
 * {@link CassandraEntireSSTableStreamReader} -- unchanged, and unaware that any of this happened -- writes them
 * out as an sstable holding exactly the requested partitions.
 *
 * <p>What this covers that {@code ZeroCopySSTableSliceTest} does not: the component manifest the receiver is
 * driven by, the ranged {@code writeFileToChannel} that sends Data.db without sending the whole file, that the
 * received sstable has no Digest.crc32 and verifies without one, and that the sender falls back to the row-by-row
 * path -- correctly, and without failing the session -- for every refusal that is decided before the file count is
 * promised.
 *
 * <p>Format-agnostic: the tests run under whichever format the JVM selected, so a BTI run exercises the BTI slice
 * branches rather than skipping the class, and {@link #btiPartialStreamArrivesAsAnSSTableHoldingOnlyTheRange}
 * pins BTI end to end even on a BIG run. Nothing here may read Index.db directly for that reason -- see
 * {@link #keysInOrder}.
 */
public class CassandraPartialSSTableStreamTest extends CQLTester
{
    private boolean entireSSTables;
    private boolean partialEnabled;
    private double maxDeadSpace;
    private SSTableFormat<?, ?> selectedFormat;

    /**
     * Saved before anything can throw, so {@link #restoreConfig} cannot write a default back over the real
     * configuration when a test fails early. Nothing here may skip the class either, for the same reason.
     */
    @Before
    public void saveConfig()
    {
        entireSSTables = DatabaseDescriptor.streamEntireSSTables();
        partialEnabled = DatabaseDescriptor.getZeroCopyPartialStreamEnabled();
        maxDeadSpace = DatabaseDescriptor.getZeroCopyPartialStreamMaxDeadSpaceRatio();
        selectedFormat = DatabaseDescriptor.getSelectedSSTableFormat();
    }

    @After
    public void restoreConfig()
    {
        DatabaseDescriptor.getRawConfig().stream_entire_sstables = entireSSTables;
        DatabaseDescriptor.setZeroCopyPartialStreamEnabled(partialEnabled);
        DatabaseDescriptor.setZeroCopyPartialStreamMaxDeadSpaceRatio(maxDeadSpace);
        // The sstable format is global, so a test that selected one has to put it back or every test after it in
        // this JVM writes the wrong format.
        selectFormat(selectedFormat);
    }

    /** No-op unless the format really changes: switching it pauses and drains compactions cluster-wide. */
    private static void selectFormat(SSTableFormat<?, ?> format)
    {
        if (format != null && DatabaseDescriptor.getSelectedSSTableFormat() != format)
            TestDatabaseDescriptor.setUnsafeSelectedSSTableFormat(format);
    }

    private static void selectFormat(String name)
    {
        selectFormat(DatabaseDescriptor.getSSTableFormats().get(name));
    }

    @Test
    public void partialStreamArrivesAsAnSSTableHoldingOnlyTheRange() throws Throwable
    {
        DatabaseDescriptor.getRawConfig().stream_entire_sstables = true;
        DatabaseDescriptor.setZeroCopyPartialStreamEnabled(true);
        DatabaseDescriptor.setZeroCopyPartialStreamMaxDeadSpaceRatio(0.25);

        createCompressedTable(4);
        disableCompaction();
        insertPartitions(80, 4, 400);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader sstable = onlySSTable(cfs);
        List<DecoratedKey> keys = keysInOrder(sstable);
        assertEquals(80, keys.size());

        Range<Token> range = new Range<>(keys.get(19).getToken(), keys.get(59).getToken());
        List<DecoratedKey> expected = keys.subList(20, 60);
        List<PartitionPositionBounds> sections = sstable.getPositionsForRanges(Collections.singletonList(range));

        CassandraOutgoingFile outgoing = outgoingFile(sstable, range, sections);
        assertTrue("the sections do not cover the sstable, so this must be a slice", outgoing.isSliced());
        assertFalse(outgoing.computeShouldStreamEntireSSTables());

        // Everything the plan is assembled from, before an index has been read: the manifest is an estimate but
        // it names the components that will actually be sent (+1 for Data.db, which is ranges of the parent's and so
        // is not one of the synthesised ones), and Data.db's size in it is exact.
        assertEquals(sliceComponentCount(sstable, true), outgoing.getNumFiles());

        Set<String> temporariesSeen = new HashSet<>();
        Received received = streamAndReceive(cfs, sstable, outgoing,
                                             () -> sstable.descriptor.getTemporaryFiles()
                                                                     .forEach(f -> temporariesSeen.add(f.name())));
        try
        {
            assertTrue("the receiver was driven by the entire-sstable path", received.header.isEntireSSTable);
            assertEquals(expected.get(0), received.header.firstKey);
            assertFalse("the sender cannot digest what it sends by sendfile",
                        received.header.componentManifest.components().contains(Components.DIGEST));
            assertEquals(outgoing.slicePlan().physicalBytes, received.header.componentManifest.sizeOf(Components.DATA));

            // Every component the sender synthesised was on the wire under a tmpFileForStreaming name, so a crash
            // during the minutes a large slice spends on the socket leaves files scrubDataDirectories removes rather
            // than a set of index components that looks like a live sstable with no Data.db.
            for (Component component : received.header.componentManifest.components())
            {
                if (component.equals(Components.DATA))
                    continue;   // ranges of the parent's own file; never copied, never renamed
                String prefix = sstable.descriptor.fileFor(component).name() + '.';
                assertTrue(component + " was not streamed from a temporary: " + temporariesSeen,
                           temporariesSeen.stream().anyMatch(name -> name.startsWith(prefix)
                                                                    && name.endsWith(Descriptor.TMP_EXT)));
            }

            assertEquals(1, received.sstables.size());
            SSTableReader arrived = received.sstables.iterator().next();
            assertEquals(expected.get(0), arrived.getFirst());
            assertEquals(expected.get(expected.size() - 1), arrived.getLast());
            assertEquals(outgoing.slicePlan().physicalBytes, arrived.onDiskLength());

            assertContentMatches(sstable, arrived, expected);
            assertOnlyTheseKeysArePresent(arrived, keys, expected);
            assertReceivedHasNoDigestAndVerifies(cfs, arrived);
        }
        finally
        {
            received.close();
        }

        // ...and none of them outlives the stream, on the path where nothing failed either.
        assertEquals("streaming temporaries were left behind", Collections.emptyList(),
                     sstable.descriptor.getTemporaryFiles());
    }

    /**
     * The same round trip with BTI selected, which is a different slice entirely: Partitions.db and Rows.db are
     * rebased instead of Index.db and Summary.db, so the component list, the writer and the receiver's view of the
     * arrived sstable all differ. Selected explicitly rather than left to the JVM's configured format so that a
     * default (BIG) run covers it too -- the branches exist either way, and this class used to skip itself
     * entirely on a BTI run.
     */
    @Test
    public void btiPartialStreamArrivesAsAnSSTableHoldingOnlyTheRange() throws Throwable
    {
        selectFormat(BtiFormat.NAME);

        DatabaseDescriptor.getRawConfig().stream_entire_sstables = true;
        DatabaseDescriptor.setZeroCopyPartialStreamEnabled(true);
        DatabaseDescriptor.setZeroCopyPartialStreamMaxDeadSpaceRatio(0.25);

        createCompressedTable(4);
        disableCompaction();
        insertPartitions(80, 4, 400);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader sstable = onlySSTable(cfs);
        assertTrue("the format switch did not take effect, so this test is a duplicate of the BIG one",
                   BtiFormat.is(sstable.descriptor.getFormat()));

        List<DecoratedKey> keys = keysInOrder(sstable);
        assertEquals(80, keys.size());

        Range<Token> range = new Range<>(keys.get(19).getToken(), keys.get(59).getToken());
        List<DecoratedKey> expected = keys.subList(20, 60);
        List<PartitionPositionBounds> sections = sstable.getPositionsForRanges(Collections.singletonList(range));

        CassandraOutgoingFile outgoing = outgoingFile(sstable, range, sections);
        assertTrue("the sections do not cover the sstable, so this must be a slice", outgoing.isSliced());
        assertEquals(sliceComponentCount(sstable, true), outgoing.getNumFiles());

        Received received = streamAndReceive(cfs, sstable, outgoing);
        try
        {
            assertTrue(received.header.isEntireSSTable);
            assertEquals(expected.get(0), received.header.firstKey);
            assertFalse(received.header.componentManifest.components().contains(Components.DIGEST));
            assertEquals(outgoing.slicePlan().physicalBytes, received.header.componentManifest.sizeOf(Components.DATA));

            assertEquals(1, received.sstables.size());
            SSTableReader arrived = received.sstables.iterator().next();
            assertEquals(expected.get(0), arrived.getFirst());
            assertEquals(expected.get(expected.size() - 1), arrived.getLast());
            assertEquals(outgoing.slicePlan().physicalBytes, arrived.onDiskLength());

            assertContentMatches(sstable, arrived, expected);
            assertOnlyTheseKeysArePresent(arrived, keys, expected);
            assertReceivedHasNoDigestAndVerifies(cfs, arrived);
        }
        finally
        {
            received.close();
        }
    }

    /**
     * Several ranges, far enough apart to be separate runs: Data.db arrives as those ranges concatenated, with
     * everything between them never sent.
     */
    @Test
    public void multiRunPartialStreamArrivesAsOneSSTable() throws Throwable
    {
        DatabaseDescriptor.getRawConfig().stream_entire_sstables = true;
        DatabaseDescriptor.setZeroCopyPartialStreamEnabled(true);
        DatabaseDescriptor.setZeroCopyPartialStreamMaxDeadSpaceRatio(0.25);

        createCompressedTable(4);
        disableCompaction();
        insertPartitions(120, 4, 400);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader sstable = onlySSTable(cfs);
        List<DecoratedKey> keys = keysInOrder(sstable);

        List<Range<Token>> ranges = new ArrayList<>();
        ranges.add(new Range<>(keys.get(9).getToken(), keys.get(29).getToken()));
        ranges.add(new Range<>(keys.get(59).getToken(), keys.get(79).getToken()));
        ranges.add(new Range<>(keys.get(99).getToken(), keys.get(114).getToken()));
        List<Range<Token>> normalized = Range.normalize(ranges);

        List<DecoratedKey> expected = new ArrayList<>();
        expected.addAll(keys.subList(10, 30));
        expected.addAll(keys.subList(60, 80));
        expected.addAll(keys.subList(100, 115));

        List<PartitionPositionBounds> sections = sstable.getPositionsForRanges(normalized);
        CassandraOutgoingFile outgoing = new CassandraOutgoingFile(StreamOperation.BOOTSTRAP, sstable.ref(), sections,
                                                                  normalized, sstable.estimatedKeys());
        assertTrue(outgoing.isSliced());
        assertTrue("expected several runs, got " + outgoing.slicePlan(), outgoing.slicePlan().runs.size() > 1);

        Received received = streamAndReceive(cfs, sstable, outgoing);
        try
        {
            assertEquals(1, received.sstables.size());
            SSTableReader arrived = received.sstables.iterator().next();
            assertEquals(outgoing.slicePlan().physicalBytes, arrived.onDiskLength());
            assertEquals(outgoing.slicePlan().dataLength, arrived.uncompressedLength());
            assertContentMatches(sstable, arrived, expected);
            assertOnlyTheseKeysArePresent(arrived, keys, expected);
            assertReceivedHasNoDigestAndVerifies(cfs, arrived);
        }
        finally
        {
            received.close();
        }
    }

    /** The same, for an uncompressed sstable, whose grid is CRC.db's and whose CRC.db is sliced with it. */
    @Test
    public void uncompressedPartialStreamArrivesAsOneSSTable() throws Throwable
    {
        DatabaseDescriptor.getRawConfig().stream_entire_sstables = true;
        DatabaseDescriptor.setZeroCopyPartialStreamEnabled(true);
        DatabaseDescriptor.setZeroCopyPartialStreamMaxDeadSpaceRatio(1.0);

        createTable("CREATE TABLE %s (pk text, ck int, val text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'}");
        disableCompaction();
        insertPartitions(300, 4, 500);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader sstable = onlySSTable(cfs);
        assertFalse(sstable.compression);
        List<DecoratedKey> keys = keysInOrder(sstable);

        Range<Token> range = new Range<>(keys.get(49).getToken(), keys.get(249).getToken());
        List<DecoratedKey> expected = keys.subList(50, 250);
        List<PartitionPositionBounds> sections = sstable.getPositionsForRanges(Collections.singletonList(range));

        CassandraOutgoingFile outgoing = outgoingFile(sstable, range, sections);
        assertTrue(outgoing.isSliced());
        assertFalse(outgoing.slicePlan().compressed);

        Received received = streamAndReceive(cfs, sstable, outgoing);
        try
        {
            assertTrue(received.header.componentManifest.components().contains(Components.CRC));
            assertFalse(received.header.componentManifest.components().contains(Components.COMPRESSION_INFO));

            assertEquals(1, received.sstables.size());
            SSTableReader arrived = received.sstables.iterator().next();
            assertEquals(expected.get(0), arrived.getFirst());
            assertEquals(expected.get(expected.size() - 1), arrived.getLast());
            assertContentMatches(sstable, arrived, expected);
            assertOnlyTheseKeysArePresent(arrived, keys, expected);
            assertReceivedHasNoDigestAndVerifies(cfs, arrived);
        }
        finally
        {
            received.close();
        }
    }

    /**
     * With the dead space limit at zero, a range that does not begin on a compression chunk boundary is refused
     * and the ordinary path is used -- which is what the limit is for.
     *
     * <p>Also the only refusal the operator can take back, so it is the one that has to land in
     * {@code SlicedZeroCopyStreamsRefusedDeadSpace} rather than in {@code ...RefusedUnsliceable}: on a table where
     * slicing is structurally impossible the two counters are what tell a ratio set too low apart from a shape no
     * configuration would accept.
     */
    @Test
    public void fallsBackToRowByRowWhenDeadSpaceIsNotAllowed() throws Throwable
    {
        DatabaseDescriptor.getRawConfig().stream_entire_sstables = true;
        DatabaseDescriptor.setZeroCopyPartialStreamEnabled(true);
        DatabaseDescriptor.setZeroCopyPartialStreamMaxDeadSpaceRatio(0.0);

        createCompressedTable(4);
        disableCompaction();
        insertPartitions(80, 4, 400);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader sstable = onlySSTable(cfs);
        List<DecoratedKey> keys = keysInOrder(sstable);

        Range<Token> range = new Range<>(keys.get(19).getToken(), keys.get(59).getToken());
        List<PartitionPositionBounds> sections = sstable.getPositionsForRanges(Collections.singletonList(range));
        assertTrue("this range must start mid-chunk for the test to mean anything",
                   sections.get(0).lowerPosition % 4096 != 0);

        RefusalCounts before = RefusalCounts.snapshot();
        CassandraOutgoingFile outgoing = outgoingFile(sstable, range, sections);
        assertFalse(outgoing.isSliced());
        assertEquals(1, outgoing.getNumFiles());
        before.assertRefusedByDeadSpaceRatio();
    }

    /** Turning the feature off leaves the decision exactly where it was, and is not counted as a refusal. */
    @Test
    public void disabledMeansNoSlice() throws Throwable
    {
        DatabaseDescriptor.getRawConfig().stream_entire_sstables = true;
        DatabaseDescriptor.setZeroCopyPartialStreamEnabled(false);

        createCompressedTable(4);
        disableCompaction();
        insertPartitions(40, 4, 400);
        flush();

        SSTableReader sstable = onlySSTable(getCurrentColumnFamilyStore());
        List<DecoratedKey> keys = keysInOrder(sstable);
        Range<Token> range = new Range<>(keys.get(9).getToken(), keys.get(29).getToken());

        RefusalCounts before = RefusalCounts.snapshot();
        CassandraOutgoingFile outgoing = outgoingFile(sstable, range,
                                                     sstable.getPositionsForRanges(Collections.singletonList(range)));
        assertFalse(outgoing.isSliced());
        before.assertNothingCounted();
    }

    /**
     * A slice keeps the parent's sstable version, and only a version that can carry
     * {@code StatsMetadata#hasUnindexedRegions} may hold one: a reader of an older version would ignore the marker,
     * scan the interior dead regions linearly and hand back partitions the sstable does not claim. So an sstable
     * written before the marker existed is refused, no matter what its ranges look like, and the transfer goes
     * partition-by-partition instead -- which is a refusal no configuration takes back, hence
     * {@code ...RefusedUnsliceable}.
     *
     * <p>The old-version parent is a hardlinked copy of a current one under an older version's name. That is
     * legitimate for the one field this is about: {@code MetadataSerializer} hands each metadata component's own
     * bytes to its deserializer, so the current Statistics.db read as the older version simply stops before the
     * trailing marker byte -- which is exactly the "an older version cannot express it" case under test.
     */
    @Test
    public void refusesToSliceAnSSTableVersionThatCannotCarryTheMarker() throws Throwable
    {
        DatabaseDescriptor.getRawConfig().stream_entire_sstables = true;
        DatabaseDescriptor.setZeroCopyPartialStreamEnabled(true);
        DatabaseDescriptor.setZeroCopyPartialStreamMaxDeadSpaceRatio(1.0);

        createCompressedTable(4);
        disableCompaction();
        insertPartitions(80, 4, 400);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader current = onlySSTable(cfs);
        List<DecoratedKey> keys = keysInOrder(current);

        Range<Token> range = new Range<>(keys.get(19).getToken(), keys.get(59).getToken());
        List<DecoratedKey> expected = keys.subList(20, 60);
        List<PartitionPositionBounds> sections = current.getPositionsForRanges(Collections.singletonList(range));

        // Eligible at the current version, which is what proves the refusal below is the version and nothing else.
        assertTrue(ZeroCopySSTableSlice.plan(current, sections, 1.0).isEligible());

        Version legacyVersion = versionWithoutTheMarker(current.descriptor.getFormat());
        assertFalse(legacyVersion.toString(), legacyVersion.hasUnindexedRegionsMarker());

        SSTableReader legacy = openCopyAtVersion(cfs, current, legacyVersion);
        try
        {
            assertEquals(Reason.NO_UNINDEXED_REGIONS_MARKER,
                         ZeroCopySSTableSlice.plan(legacy, sections, 1.0).reason);

            RefusalCounts before = RefusalCounts.snapshot();
            CassandraOutgoingFile outgoing = outgoingFile(legacy, range, sections);
            assertFalse("a version that cannot hold the marker must not be sliced", outgoing.isSliced());
            assertEquals(1, outgoing.getNumFiles());
            before.assertRefusedAsUnsliceable();

            // And the refusal is a fallback, not a failure: the same range arrives partition-by-partition.
            Received received = streamAndReceiveRowByRow(cfs, legacy, outgoing);
            try
            {
                assertFalse("a fallback stream must not be announced as an entire sstable",
                            received.header.isEntireSSTable);
                assertNull(received.header.componentManifest);
                assertTrue("a compressed sstable falls back to the compressed row-by-row stream",
                           received.header.isCompressed());

                assertEquals(1, received.sstables.size());
                SSTableReader arrived = received.sstables.iterator().next();
                assertContentMatches(legacy, arrived, expected);
                assertOnlyTheseKeysArePresent(arrived, keys, expected);
            }
            finally
            {
                received.close();
                outgoing.finish();   // the ref the copy was handed, released the way a transfer task would
            }
        }
        finally
        {
            releaseCopy(legacy);
        }
    }

    /**
     * The storage-attached-index refusal, in the state that makes it necessary: the index exists and the sstable
     * carries none of its components. The component-set backstop in {@code plan()} cannot see that -- the
     * difference between what the sstable would stream and what the format declares is EMPTY -- so if the gate on
     * the table ever went away, this sstable would be sliced, and the receiver would either fail
     * {@code validateSSTableAttachedIndexes} and take down the session or publish an sstable whose rows answer no
     * index predicate for ever.
     *
     * <p>That state is a {@code CREATE INDEX} on a populated table, mid-build. It is reproduced deterministically
     * rather than raced: the index is created and built, and the parent under test is a hardlinked copy carrying
     * only its format's own components -- byte for byte what the flushed sstable was before the build touched it.
     * Racing the real build would make the assertion pass or fail depending on how fast it finished.
     */
    @Test
    public void refusesToSliceWhenTheTableHasStorageAttachedIndexes() throws Throwable
    {
        DatabaseDescriptor.getRawConfig().stream_entire_sstables = true;
        DatabaseDescriptor.setZeroCopyPartialStreamEnabled(true);
        DatabaseDescriptor.setZeroCopyPartialStreamMaxDeadSpaceRatio(1.0);

        createCompressedTable(4);
        disableCompaction();
        insertPartitions(80, 4, 400);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader flushed = onlySSTable(cfs);
        List<DecoratedKey> keys = keysInOrder(flushed);

        Range<Token> range = new Range<>(keys.get(19).getToken(), keys.get(59).getToken());
        List<DecoratedKey> expected = keys.subList(20, 60);
        List<PartitionPositionBounds> sections = flushed.getPositionsForRanges(Collections.singletonList(range));

        // Eligible before the index exists: everything that follows is the index and nothing else.
        assertTrue(ZeroCopySSTableSlice.plan(flushed, sections, 1.0).isEligible());
        assertFalse(cfs.indexManager.hasSSTableAttachedIndexes());

        createIndex("CREATE INDEX ON %s(val) USING 'sai'");
        cfs = getCurrentColumnFamilyStore();
        assertTrue("the fixture did not produce a storage-attached index, so this test proves nothing",
                   cfs.indexManager.hasSSTableAttachedIndexes());

        // The built sstable, which the backstop WOULD have refused on its own: the two gates are not the same test,
        // and the copy below is the case only the first of them can see.
        SSTableReader indexed = onlySSTable(cfs);
        assertFalse("the index build left no components behind, so the copy below proves nothing",
                    Sets.difference(indexed.getStreamingComponents(),
                                    indexed.descriptor.getFormat().allComponents()).isEmpty());

        SSTableReader unindexed = openCopyAtVersion(cfs, indexed, indexed.descriptor.version);
        try
        {
            // THE point of the test: the backstop has nothing to refuse this one on.
            assertTrue("this sstable carries index components, so it is not the case the gate exists for",
                       Sets.difference(unindexed.getStreamingComponents(),
                                       unindexed.descriptor.getFormat().allComponents()).isEmpty());

            assertEquals(Reason.SSTABLE_ATTACHED_INDEXES,
                         ZeroCopySSTableSlice.plan(unindexed, sections, 1.0).reason);

            RefusalCounts before = RefusalCounts.snapshot();
            CassandraOutgoingFile outgoing = outgoingFile(unindexed, range, sections);
            assertFalse("a table with storage-attached indexes must not be sliced", outgoing.isSliced());
            assertEquals(1, outgoing.getNumFiles());
            before.assertRefusedAsUnsliceable();

            // The fallback is what builds the index components on the receiver, through the ordinary flush
            // observers, so the transfer has to succeed rather than fail the session.
            Received received = streamAndReceiveRowByRow(cfs, unindexed, outgoing);
            try
            {
                assertFalse(received.header.isEntireSSTable);
                assertNull(received.header.componentManifest);

                assertEquals(1, received.sstables.size());
                SSTableReader arrived = received.sstables.iterator().next();
                assertContentMatches(unindexed, arrived, expected);
                assertOnlyTheseKeysArePresent(arrived, keys, expected);
            }
            finally
            {
                received.close();
                outgoing.finish();   // the ref the copy was handed, released the way a transfer task would
            }
        }
        finally
        {
            releaseCopy(unindexed);
        }
    }

    /**
     * A slice that plans but then fails while it is being synthesised must FAIL THE STREAM, not quietly fall back
     * to the row-by-row path.
     *
     * <p>By the time {@code writeSlice} runs, {@link CassandraOutgoingFile#getNumFiles()} has already promised the
     * peer this slice's component count: {@code StreamTransferTask.addTransferStream} summed it into the
     * {@code StreamSummary} the peer built its {@code StreamReceiveTask} from. A row-by-row stream makes the
     * receiver count 1 ({@link CassandraIncomingFile#getNumFiles()} keeps its initialiser unless the header says
     * entire-sstable), and {@code StreamReceiveTask.received} completes only on exact equality with that total. So
     * a fallback here would leave the peer's counter permanently short: no completion, no
     * {@code receiver.finished()}, and the sstables it had already written correctly never made live. Failing
     * loudly is recoverable; hanging is not.
     *
     * <p>Graceful degradation still exists, but it lives in {@code computeSlicePlan()}, which runs in the
     * constructor before the count is promised -- {@link #disabledMeansNoSlice} and
     * {@link #fallsBackToRowByRowWhenDeadSpaceIsNotAllowed} cover that side.
     *
     * <p>The failure is injected by taking the parent's Statistics.db away between planning, which only checks
     * that the file exists, and writing, which loads every metadata type out of it and refuses a partial load.
     * That is after {@code writeSlice} has committed to a slice and before it has written a byte.
     */
    @Test
    public void slicingFailurePropagatesRatherThanUnderDeliveringFiles() throws Throwable
    {
        DatabaseDescriptor.getRawConfig().stream_entire_sstables = true;
        DatabaseDescriptor.setZeroCopyPartialStreamEnabled(true);
        DatabaseDescriptor.setZeroCopyPartialStreamMaxDeadSpaceRatio(0.25);

        createCompressedTable(4);
        disableCompaction();
        insertPartitions(80, 4, 400);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader sstable = onlySSTable(cfs);
        List<DecoratedKey> keys = keysInOrder(sstable);

        Range<Token> range = new Range<>(keys.get(19).getToken(), keys.get(59).getToken());
        List<PartitionPositionBounds> sections = sstable.getPositionsForRanges(Collections.singletonList(range));

        CassandraOutgoingFile outgoing = outgoingFile(sstable, range, sections);
        assertTrue("the plan has to succeed or this test proves nothing", outgoing.isSliced());

        int version = MessagingService.current_version;
        StreamSession session = setupStreamingSessionForTest();

        int promised = outgoing.getNumFiles();
        long failuresBefore = StreamingMetrics.slicedZeroCopyStreamsFailed.getCount();
        File stats = sstable.descriptor.fileFor(Components.STATS);
        File stashed = new File(stats.parent(), stats.name() + ".stashed");
        Set<String> before = new HashSet<>();
        for (File f : sstable.descriptor.directory.tryList())
            before.add(f.name());
        stats.move(stashed);
        Throwable thrown = null;
        try
        {
            writeToWire(session, outgoing, version);
        }
        catch (Throwable t)
        {
            thrown = t;
        }
        finally
        {
            stashed.move(stats);
        }

        assertNotNull("a slice that cannot be synthesised must fail the stream, not fall back: getNumFiles() has" +
                      " already promised the peer " + promised + " files", thrown);
        assertEquals("the refusal is still counted", failuresBefore + 1,
                     StreamingMetrics.slicedZeroCopyStreamsFailed.getCount());
        // The promise is unchanged by the failure, so nothing downstream can quietly reinterpret it.
        assertEquals(promised, outgoing.getNumFiles());
        assertTrue("the slice is still the committed decision", outgoing.isSliced());

        // The synthesised components are not left behind in the data directory: a failing writeSlice is the one
        // place they can be orphaned, since the ComponentContext that would delete them is never constructed.
        Set<String> after = new HashSet<>();
        for (File f : sstable.descriptor.directory.tryList())
            after.add(f.name());
        after.removeAll(before);
        after.remove(stashed.name());
        assertTrue("a failed slice left files behind: " + after, after.isEmpty());
    }

    /**
     * The invariant that makes a stream completable: what the sender PROMISES in the stream plan
     * ({@link CassandraOutgoingFile#getNumFiles()}, summed by {@code StreamTransferTask.addTransferStream} into the
     * {@code StreamSummary}) has to equal what the receiver COUNTS for the same stream
     * ({@link CassandraIncomingFile#getNumFiles()}, accumulated by {@code StreamReceiveTask.received} and compared
     * to that total for exact equality).
     *
     * <p>Asserted here rather than assumed, because the two are computed by completely different code from
     * different inputs -- the sender from an estimated manifest built without reading an index, the receiver from
     * the measured manifest in the header it is actually sent -- and a mismatch does not fail anything at the
     * point it happens. It silently makes the peer's receive task uncompletable.
     */
    @Test
    public void promisedFileCountMatchesWhatTheReceiverCounts() throws Throwable
    {
        DatabaseDescriptor.getRawConfig().stream_entire_sstables = true;
        DatabaseDescriptor.setZeroCopyPartialStreamEnabled(true);
        DatabaseDescriptor.setZeroCopyPartialStreamMaxDeadSpaceRatio(0.25);

        createCompressedTable(4);
        disableCompaction();
        insertPartitions(80, 4, 400);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader sstable = onlySSTable(cfs);
        List<DecoratedKey> keys = keysInOrder(sstable);

        Range<Token> range = new Range<>(keys.get(19).getToken(), keys.get(59).getToken());
        List<PartitionPositionBounds> sections = sstable.getPositionsForRanges(Collections.singletonList(range));
        CassandraOutgoingFile outgoing = outgoingFile(sstable, range, sections);
        assertTrue("the plan has to succeed or this test proves nothing", outgoing.isSliced());

        int promised = outgoing.getNumFiles();

        int version = MessagingService.current_version;
        StreamSession session = setupStreamingSessionForTest();
        InetAddressAndPort peer = FBUtilities.getBroadcastAddressAndPort();
        ByteBuf serialized = writeToWire(session, outgoing, version);

        // Drive the receiver the way StreamReceiveTask does, with the total taken from the sender's promise rather
        // than hardcoded, so a divergence in either direction fails here.
        session.prepareReceiving(new StreamSummary(sstable.metadata().id, Collections.emptyList(), promised,
                                                  outgoing.getEstimatedSize()));
        StreamMessageHeader messageHeader = new StreamMessageHeader(sstable.metadata().id, peer, session.planId(),
                                                                   false, 0, 0, 0, null);
        CassandraIncomingFile incoming = new CassandraIncomingFile(cfs, session, messageHeader);
        incoming.read(new DataInputBuffer(serialized.nioBuffer(), false), version);

        StreamingLifecycleTransaction txn = new StreamingLifecycleTransaction();
        SSTableTxnSingleStreamWriter writer = (SSTableTxnSingleStreamWriter) incoming.getSSTable();
        Received received = new Received(null, writer.transferOwnershipTo(txn), txn);
        try
        {
            assertTrue("a slice is announced as an entire sstable", incoming.isEntireSSTable());
            assertEquals("the sender promised " + promised + " files but the receiver counted " +
                         incoming.getNumFiles() + "; StreamReceiveTask.received would never reach its total",
                         promised, incoming.getNumFiles());
        }
        finally
        {
            received.close();
        }
    }

    // ----------------------------------------------------------------------------------------------------
    // Wire harness
    // ----------------------------------------------------------------------------------------------------

    private static final class Received implements AutoCloseable
    {
        final CassandraStreamHeader header;
        final Collection<SSTableReader> sstables;
        private final StreamingLifecycleTransaction txn;

        Received(CassandraStreamHeader header, Collection<SSTableReader> sstables, StreamingLifecycleTransaction txn)
        {
            this.header = header;
            this.sstables = sstables;
            this.txn = txn;
        }

        /**
         * The received sstables belong to the streaming transaction that took ownership of them, so aborting it
         * is what releases and removes them; there is no self-ref of our own to drop.
         */
        @Override
        public void close()
        {
            txn.abort();
        }
    }

    /**
     * Write the stream to a buffer through an embedded channel, then read it back the way an incoming stream is
     * read: deserialise the header, then hand the rest to the reader the header selects.
     */
    private Received streamAndReceive(ColumnFamilyStore cfs, SSTableReader sstable, CassandraOutgoingFile outgoing)
    throws Throwable
    {
        return streamAndReceive(cfs, sstable, outgoing, () -> {});
    }

    /**
     * @param whileWriting run on every outbound message, i.e. at points where the sender's own files still exist.
     *                     They are deleted before the stream returns, so this is the only place to look at them.
     */
    private Received streamAndReceive(ColumnFamilyStore cfs, SSTableReader sstable, CassandraOutgoingFile outgoing,
                                      Runnable whileWriting)
    throws Throwable
    {
        int version = MessagingService.current_version;
        StreamSession session = setupStreamingSessionForTest();
        InetAddressAndPort peer = FBUtilities.getBroadcastAddressAndPort();

        ByteBuf serialized = writeToWire(session, outgoing, version, whileWriting);

        DataInputBuffer in = new DataInputBuffer(serialized.nioBuffer(), false);
        CassandraStreamHeader header = CassandraStreamHeader.serializer.deserialize(in, version);

        session.prepareReceiving(new StreamSummary(sstable.metadata().id, Collections.emptyList(), 1, header.size()));
        StreamMessageHeader messageHeader = new StreamMessageHeader(sstable.metadata().id, peer, session.planId(),
                                                                   false, 0, 0, 0, null);
        CassandraEntireSSTableStreamReader reader = new CassandraEntireSSTableStreamReader(messageHeader, header, session);
        SSTableTxnSingleStreamWriter writer = (SSTableTxnSingleStreamWriter) reader.read(in);
        StreamingLifecycleTransaction txn = new StreamingLifecycleTransaction();
        return new Received(header, writer.transferOwnershipTo(txn), txn);
    }

    /**
     * The same for a stream the sender refused to slice: driven through {@link CassandraIncomingFile}, which is what
     * dispatches on {@code isEntireSSTable} in production, so a header that disagreed with the bytes behind it would
     * fail here rather than be papered over by picking the reader ourselves. The header is deserialised a second time
     * from the same buffer only so that the test can assert on it.
     */
    private Received streamAndReceiveRowByRow(ColumnFamilyStore cfs, SSTableReader sstable,
                                              CassandraOutgoingFile outgoing)
    throws Throwable
    {
        int version = MessagingService.current_version;
        StreamSession session = setupStreamingSessionForTest();
        InetAddressAndPort peer = FBUtilities.getBroadcastAddressAndPort();

        ByteBuf serialized = writeToWire(session, outgoing, version);
        CassandraStreamHeader header =
            CassandraStreamHeader.serializer.deserialize(new DataInputBuffer(serialized.nioBuffer(), false), version);

        session.prepareReceiving(new StreamSummary(sstable.metadata().id, Collections.emptyList(),
                                                  outgoing.getNumFiles(), header.size()));
        StreamMessageHeader messageHeader = new StreamMessageHeader(sstable.metadata().id, peer, session.planId(),
                                                                   false, 0, 0, 0, null);
        CassandraIncomingFile incoming = new CassandraIncomingFile(cfs, session, messageHeader);
        incoming.read(new DataInputBuffer(serialized.nioBuffer(), false), version);
        assertFalse("the receiver was driven by the entire-sstable path", incoming.isEntireSSTable());
        assertEquals("a row-by-row stream is one file at both ends", outgoing.getNumFiles(), incoming.getNumFiles());

        StreamingLifecycleTransaction txn = new StreamingLifecycleTransaction();
        SSTableTxnSingleStreamWriter writer = (SSTableTxnSingleStreamWriter) incoming.getSSTable();
        return new Received(header, writer.transferOwnershipTo(txn), txn);
    }

    /** Everything {@code outgoing} writes, in order, as one buffer. */
    private ByteBuf writeToWire(StreamSession session, CassandraOutgoingFile outgoing, int version) throws IOException
    {
        return writeToWire(session, outgoing, version, () -> {});
    }

    private ByteBuf writeToWire(StreamSession session, CassandraOutgoingFile outgoing, int version,
                                Runnable whileWriting) throws IOException
    {
        ByteBuf serialized = Unpooled.buffer(1 << 20);
        EmbeddedChannel channel = createMockNettyChannel(serialized, whileWriting);
        try (AsyncStreamingOutputPlus out = new AsyncStreamingOutputPlus(channel))
        {
            outgoing.write(session, out, version);
            out.flush();
        }
        return serialized;
    }

    private EmbeddedChannel createMockNettyChannel(ByteBuf serialized, Runnable whileWriting)
    {
        WritableByteChannel wbc = new WritableByteChannel()
        {
            private boolean isOpen = true;

            public int write(ByteBuffer src)
            {
                int size = src.remaining();
                serialized.writeBytes(src);
                return size;
            }

            public boolean isOpen()
            {
                return isOpen;
            }

            public void close()
            {
                isOpen = false;
            }
        };

        return new EmbeddedChannel(new ChannelOutboundHandlerAdapter()
        {
            @Override
            public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception
            {
                whileWriting.run();
                // The header and any SSL-path component arrive as buffers, the zero-copy Data.db as a file
                // region; both have to land in the same buffer in the order they were written.
                if (msg instanceof DefaultFileRegion)
                    ((DefaultFileRegion) msg).transferTo(wbc, 0);
                else if (msg instanceof ByteBuf)
                    serialized.writeBytes((ByteBuf) msg);
                else
                    throw new AssertionError("unexpected outbound message " + msg.getClass());
                super.write(ctx, msg, promise);
            }
        });
    }

    private StreamSession setupStreamingSessionForTest()
    {
        StreamCoordinator streamCoordinator = new StreamCoordinator(StreamOperation.BOOTSTRAP, 1,
                                                                   new NettyStreamingConnectionFactory(),
                                                                   false, false, null, PreviewKind.NONE);
        StreamResultFuture future = StreamResultFuture.createInitiator(nextTimeUUID(), StreamOperation.BOOTSTRAP,
                                                                      Collections.<StreamEventHandler>emptyList(),
                                                                      streamCoordinator);

        InetAddressAndPort peer = FBUtilities.getBroadcastAddressAndPort();
        streamCoordinator.addSessionInfo(new SessionInfo(peer, 0, peer, Collections.emptyList(),
                                                        Collections.emptyList(), StreamSession.State.INITIALIZED,
                                                        null));

        StreamSession session = streamCoordinator.getOrCreateOutboundSession(peer);
        session.init(future);
        return session;
    }

    private static CassandraOutgoingFile outgoingFile(SSTableReader sstable, Range<Token> range,
                                                     List<PartitionPositionBounds> sections)
    {
        Ref<SSTableReader> ref = sstable.ref();
        return new CassandraOutgoingFile(StreamOperation.BOOTSTRAP, ref, sections,
                                         Collections.singletonList(range), sstable.estimatedKeys());
    }

    // ----------------------------------------------------------------------------------------------------
    // Assertions and scaffolding
    // ----------------------------------------------------------------------------------------------------

    private static void assertContentMatches(SSTableReader parent, SSTableReader arrived, List<DecoratedKey> expected)
    {
        Set<DecoratedKey> wanted = new HashSet<>(expected);
        int compared = 0;
        try (ISSTableScanner parentScanner = parent.getScanner();
             ISSTableScanner scanner = arrived.getScanner())
        {
            while (scanner.hasNext())
            {
                assertTrue("more partitions arrived than were asked for", compared < expected.size());
                try (UnfilteredRowIterator actual = scanner.next())
                {
                    assertEquals("partition " + compared, expected.get(compared), actual.partitionKey());

                    UnfilteredRowIterator sent = null;
                    try
                    {
                        while (parentScanner.hasNext())
                        {
                            UnfilteredRowIterator candidate = parentScanner.next();
                            if (wanted.contains(candidate.partitionKey()))
                            {
                                sent = candidate;
                                break;
                            }
                            candidate.close();
                        }
                        assertNotNull("the sender ran out of partitions at " + compared, sent);
                        assertEquals(sent.partitionKey(), actual.partitionKey());
                        int i = 0;
                        while (sent.hasNext())
                        {
                            assertTrue("row " + i + " of " + actual.partitionKey() + " did not arrive", actual.hasNext());
                            assertEquals(sent.next(), actual.next());
                            i++;
                        }
                        assertFalse("extra rows arrived for " + actual.partitionKey(), actual.hasNext());
                        assertTrue(i > 0);
                    }
                    finally
                    {
                        if (sent != null)
                            sent.close();
                    }
                }
                compared++;
            }
        }
        assertEquals("partitions are missing", expected.size(), compared);
    }

    /**
     * A received slice has NO Digest.crc32, and that absence is the contract rather than an omission: the sender
     * cannot digest bytes that leave by {@code sendfile} without entering its process, and the receiver must not
     * digest them either -- a checksum computed from the bytes that ARRIVED cannot tell them apart from the bytes
     * that were SENT, which is the one thing a digest is for, so writing one here would report success from
     * {@code nodetool verify} for a transfer that had been corrupted in flight.
     *
     * <p>So the component is absent, and {@code nodetool verify} answers its absence by saying so and doing the
     * extended verification instead -- which has to pass, since that is now the only integrity check a received
     * sstable gets.
     */
    private static void assertReceivedHasNoDigestAndVerifies(ColumnFamilyStore cfs, SSTableReader arrived)
    throws IOException
    {
        assertFalse("the receiver invented a digest of the bytes it received, which cannot detect a transfer that" +
                    " corrupted them", arrived.descriptor.fileFor(Components.DIGEST).exists());
        assertFalse(arrived.getComponents().contains(Components.DIGEST));
        assertNull("a digest validator exists, so verify() would take the whole-file CRC path",
                   arrived.maybeGetDigestValidator());

        // Neither quick nor extended: exactly what `nodetool verify` does by default, which is where the fallback
        // has to happen. Offline because the arrived sstables are held by a streaming transaction, not by the
        // tracker's live set.
        List<String> output = new ArrayList<>();
        OutputHandler handler = new OutputHandler.LogOutput()
        {
            @Override
            public void output(String msg)
            {
                output.add(msg);
                super.output(msg);
            }
        };
        try (IVerifier verifier = arrived.getVerifier(cfs, handler, true,
                                                     IVerifier.options().extendedVerification(false).build()))
        {
            verifier.verify();
        }
        assertTrue("the verifier did not report the missing digest: " + output,
                   output.stream().anyMatch(m -> m.contains("Data digest missing")));
        assertTrue("the verifier did not fall through to the extended walk: " + output,
                   output.stream().anyMatch(m -> m.contains("Extended Verify requested")));
    }

    private static void assertOnlyTheseKeysArePresent(SSTableReader arrived, List<DecoratedKey> all,
                                                     List<DecoratedKey> expected)
    {
        Set<DecoratedKey> wanted = new HashSet<>(expected);
        for (DecoratedKey key : all)
        {
            // getPosition returns a negative value rather than a null index entry when the key is absent
            long position = arrived.getPosition(key, SSTableReader.Operator.EQ);
            if (wanted.contains(key))
                assertTrue("the received sstable cannot find " + key, position >= 0);
            else
                assertTrue("the received sstable exposes " + key + ", which was not sent", position < 0);
        }
    }

    /**
     * The sstable's keys in its own order. Read through {@link SSTableReader#keyIterator()} rather than out of
     * Index.db, so that the BTI tests here are not a different fixture from the BIG ones.
     */
    private static List<DecoratedKey> keysInOrder(SSTableReader sstable) throws IOException
    {
        List<DecoratedKey> keys = new ArrayList<>();
        try (KeyIterator iterator = sstable.keyIterator())
        {
            while (iterator.hasNext())
                keys.add(iterator.next());
        }
        return keys;
    }

    /** Components a slice of {@code sstable} would send: the synthesised ones for its format, plus Data.db. */
    private static int sliceComponentCount(SSTableReader sstable, boolean compressed)
    {
        return ZeroCopySSTableSlice.componentsFor(sstable.descriptor.getFormat(), compressed).size() + 1;
    }

    /**
     * The newest version of {@code format} that cannot carry {@code StatsMetadata#hasUnindexedRegions}, i.e. the one
     * immediately before the bump that added it. Asserted against {@code hasUnindexedRegionsMarker()} by the caller,
     * so a further version bump cannot silently turn its test into a no-op.
     */
    private static Version versionWithoutTheMarker(SSTableFormat<?, ?> format)
    {
        if (BigFormat.is(format))
            return format.getVersion("pa");
        if (BtiFormat.is(format))
            return format.getVersion("ea");
        throw new AssertionError("no known pre-marker version for format " + format.name());
    }

    /**
     * A second reader over the same bytes: {@code parent}'s components hardlinked under a fresh descriptor at
     * {@code version}, opened offline so that nothing registers it with the tracker.
     * <p>
     * Only the format's own components are linked, which is what makes this stand in for an sstable that predates a
     * {@code CREATE INDEX} -- and TOC.txt is left out with them, since the parent's would name components the copy
     * does not have. The component set is passed to {@code open} explicitly for the same reason.
     */
    private static SSTableReader openCopyAtVersion(ColumnFamilyStore cfs, SSTableReader parent, Version version)
    {
        Set<Component> components = new HashSet<>();
        for (Component component : parent.getComponents())
        {
            if (component.equals(Components.TOC) || !parent.descriptor.getFormat().allComponents().contains(component))
                continue;
            if (parent.descriptor.fileFor(component).exists())
                components.add(component);
        }

        Descriptor copy = cfs.newSSTableDescriptor(parent.descriptor.directory, version);
        SSTable.hardlink(parent.descriptor, copy, components);
        return SSTableReader.openNoValidation(copy, components, cfs);
    }

    /** Drops the copy's self ref and the hardlinks it was opened from; the parent's own files are untouched. */
    private static void releaseCopy(SSTableReader copy)
    {
        Descriptor descriptor = copy.descriptor;
        Set<Component> components = copy.getComponents();
        copy.selfRef().release();
        for (Component component : components)
            descriptor.fileFor(component).deleteIfExists();
    }

    /**
     * The refusal counters before an outgoing file was built. {@code SlicedZeroCopyStreamsRefused} is documented as
     * the sum of the two reasons, and it is only useful if that holds, so every assertion here checks all three.
     */
    private static final class RefusalCounts
    {
        final long refused;
        final long deadSpace;
        final long unsliceable;

        private RefusalCounts()
        {
            refused = StreamingMetrics.slicedZeroCopyStreamsRefused.getCount();
            deadSpace = StreamingMetrics.slicedZeroCopyStreamsRefusedDeadSpace.getCount();
            unsliceable = StreamingMetrics.slicedZeroCopyStreamsRefusedUnsliceable.getCount();
        }

        static RefusalCounts snapshot()
        {
            return new RefusalCounts();
        }

        void assertRefusedByDeadSpaceRatio()
        {
            assertEquals("SlicedZeroCopyStreamsRefused", refused + 1,
                         StreamingMetrics.slicedZeroCopyStreamsRefused.getCount());
            assertEquals("SlicedZeroCopyStreamsRefusedDeadSpace", deadSpace + 1,
                         StreamingMetrics.slicedZeroCopyStreamsRefusedDeadSpace.getCount());
            assertEquals("the ratio's refusal must not be counted as one no configuration would take back",
                         unsliceable, StreamingMetrics.slicedZeroCopyStreamsRefusedUnsliceable.getCount());
        }

        void assertRefusedAsUnsliceable()
        {
            assertEquals("SlicedZeroCopyStreamsRefused", refused + 1,
                         StreamingMetrics.slicedZeroCopyStreamsRefused.getCount());
            assertEquals("SlicedZeroCopyStreamsRefusedUnsliceable", unsliceable + 1,
                         StreamingMetrics.slicedZeroCopyStreamsRefusedUnsliceable.getCount());
            assertEquals("raising the dead space ratio would not make this sliceable, so it must not be counted there",
                         deadSpace, StreamingMetrics.slicedZeroCopyStreamsRefusedDeadSpace.getCount());
        }

        void assertNothingCounted()
        {
            assertEquals("SlicedZeroCopyStreamsRefused", refused,
                         StreamingMetrics.slicedZeroCopyStreamsRefused.getCount());
            assertEquals("SlicedZeroCopyStreamsRefusedDeadSpace", deadSpace,
                         StreamingMetrics.slicedZeroCopyStreamsRefusedDeadSpace.getCount());
            assertEquals("SlicedZeroCopyStreamsRefusedUnsliceable", unsliceable,
                         StreamingMetrics.slicedZeroCopyStreamsRefusedUnsliceable.getCount());
        }
    }

    private static SSTableReader onlySSTable(ColumnFamilyStore cfs)
    {
        Set<SSTableReader> live = cfs.getLiveSSTables();
        assertEquals("expected exactly one sstable", 1, live.size());
        return live.iterator().next();
    }

    private String createCompressedTable(int chunkLengthInKb) throws Throwable
    {
        return createTable("CREATE TABLE %s (pk text, ck int, val text, PRIMARY KEY (pk, ck)) " +
                           "WITH compression = {'class': 'LZ4Compressor', 'chunk_length_in_kb': '" +
                           chunkLengthInKb + "'}");
    }

    private void insertPartitions(int partitions, int rowsPerPartition, int valueBytes) throws Throwable
    {
        for (int p = 0; p < partitions; p++)
            for (int c = 0; c < rowsPerPartition; c++)
                execute("INSERT INTO %s (pk, ck, val) VALUES (?, ?, ?)", String.format("k%06d", p), c,
                        randomText(valueBytes));
    }

    private static String randomText(int length)
    {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        char[] chars = new char[length];
        for (int i = 0; i < length; i++)
            chars[i] = (char) ('!' + random.nextInt(94));
        return new String(chars);
    }

}
