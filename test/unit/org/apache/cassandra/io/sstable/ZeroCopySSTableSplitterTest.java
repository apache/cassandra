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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.zip.CRC32;

import com.google.common.util.concurrent.RateLimiter;

import org.junit.Assume;
import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.compaction.CompactionInterruptedException;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.streaming.CassandraOutgoingFile;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.ZeroCopySSTableSplitter.Child;
import org.apache.cassandra.io.sstable.ZeroCopySSTableSplitter.Result;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.TOCComponent;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.format.big.BigFormat.Components;
import org.apache.cassandra.io.sstable.format.big.BigTableReader;
import org.apache.cassandra.io.sstable.format.big.RowIndexEntry;
import org.apache.cassandra.io.sstable.indexsummary.IndexSummary;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileInputStreamPlus;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.utils.BloomFilterSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.IFilter;
import org.apache.cassandra.utils.OutputHandler;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * End-to-end correctness of {@link ZeroCopySSTableSplitter}: the children must be readable, and their
 * concatenation must be indistinguishable from the parent.
 *
 * <p>The load bearing assertions are:
 * <ul>
 *   <li>{@link #assertConcatenatedContentEquals} -- every partition, row, cell, timestamp and deletion of the
 *       children concatenated in token order equals the parent, exactly;</li>
 *   <li>{@link #assertPointReads} -- every parent key is found in exactly one child and reads back identically;</li>
 *   <li>{@link #assertStructure} -- the chunk arithmetic of FACT 9 recomputed independently from the parent's
 *       Index.db and CompressionInfo.db, including "no trailing slack" and "offsets[0] == 0";</li>
 *   <li>{@link #assertComponents} -- Filter/Summary/Digest/TOC are the ones on disk and are self-consistent.</li>
 * </ul>
 *
 * <p>Every test here is BIG-only and starts with {@code Assume.assumeTrue(BigFormat.isSelected())}. That is not
 * merely because {@link ZeroCopySSTableSplitter#isSupported} refuses BTI: {@link #readIndex} parses Index.db by
 * hand and {@link #assertComponents} parses Summary.db, neither of which BTI has. The BTI side of the contract
 * -- refusal rather than a wrong answer -- is covered elsewhere.
 */
public class ZeroCopySSTableSplitterTest extends CQLTester
{
    private static final SSTableReadsListener NOOP = SSTableReadsListener.NOOP_LISTENER;

    // ----------------------------------------------------------------------------------------------------
    // Tests
    // ----------------------------------------------------------------------------------------------------

    /**
     * The core test. 80 narrow partitions (no promoted index), 4 children, then everything: content
     * equivalence, point reads, structure, components, dead prefixes, and a reopen purely from disk.
     */
    @Test
    public void splitFourWaysIsEquivalentToTheParent() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());

        createCompressedTable(4);
        disableCompaction();
        insertPartitions(80, 5, 480);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader parent = onlySSTable(cfs);
        assertTrue(parent.compression);
        assertTrue(ZeroCopySSTableSplitter.isSupported(parent));
        assertEquals(4096, parent.getCompressionMetadata().chunkLength());
        // more than one chunk, otherwise the whole exercise is trivial
        assertTrue(parent.uncompressedLength() > 20L * 4096);

        Result result = ZeroCopySSTableSplitter.split(parent, 4, null);
        try
        {
            assertEquals(4, result.children.size());
            assertStructure(cfs, parent, result);
            assertComponents(cfs, result);
            assertConcatenatedContentEquals(parent, readers(result));
            assertPointReads(parent, result);

            // The dead prefix must genuinely exist for at least one child, and that child must still read.
            Child dead = firstChildWithDeadPrefix(result);
            assertNotNull("no child started off a chunk boundary; the dead-prefix path was not exercised", dead);
            assertTrue(dead.deadPrefixBytes > 0);
            // getPosition returns a data position, negative when the key is absent.
            long firstPosition = dead.reader.getPosition(dead.first, SSTableReader.Operator.EQ, false);
            assertTrue("the child cannot find its own first key", firstPosition >= 0);
            assertEquals(dead.deadPrefixBytes, firstPosition);
            assertTrue(firstPosition < dead.reader.getCompressionMetadata().chunkLength());
            try (UnfilteredRowIterator expected = parent.rowIterator(dead.first, Slices.ALL, allColumns(cfs), false, NOOP);
                 UnfilteredRowIterator actual = dead.reader.rowIterator(dead.first, Slices.ALL, allColumns(cfs), false, NOOP))
            {
                assertSamePartition(expected, actual);
            }
        }
        finally
        {
            release(result);
        }

        // Reopen purely from the on-disk files: nothing may depend on in-memory state.
        List<SSTableReader> reopened = new ArrayList<>();
        try
        {
            for (Child child : result.children)
                reopened.add(SSTableReader.open(cfs, child.descriptor, child.components, cfs.metadata));

            for (int i = 0; i < reopened.size(); i++)
            {
                assertEquals(result.children.get(i).first, reopened.get(i).getFirst());
                assertEquals(result.children.get(i).last, reopened.get(i).getLast());
                assertEquals(result.children.get(i).dataLength, reopened.get(i).uncompressedLength());
            }
            assertConcatenatedContentEquals(parent, reopened);
        }
        finally
        {
            for (SSTableReader reader : reopened)
                reader.selfRef().release();
        }
    }

    /**
     * REGRESSION: the parent here is built by a COMPACTION, not by a flush, and is reopened from disk.
     *
     * <p>Those two properties together are what every other test in this class lacks, and they are the normal
     * state of an anticompaction target. A compaction-produced sstable carries one more chunk offset than its
     * {@code dataLength} needs: {@code SSTableRewriter.doPrepare} syncs the data file twice --
     * {@code switchWriter(null)} -> {@code openFinalEarly()} -> {@code dataFile.sync()}, then
     * {@code prepareToCommit()} -> {@code syncInternal()} -- and {@code CompressedSequentialWriter.flushData}
     * appends a chunk unconditionally, even on an empty buffer. So the physical file ends a few bytes past the
     * last chunk holding data and {@code chunkCount == ceil(dataLength / chunkLength) + 1}. A flush calls
     * {@code flushData} exactly once and has neither property.
     *
     * <p>This test pins preemptive open on by hand rather than inheriting it. {@code sstable_preemptive_open_interval}
     * is what makes {@code SSTableRewriter.switchWriter(null)} call {@code openFinalEarly()} and so sync the data
     * file the first of the two times; with it unset ({@code getSSTablePreemptiveOpenIntervalInMiB() == -1},
     * {@code calculateOpenInterval} yields {@code Long.MAX_VALUE}) there is one sync, no trailing chunk, and this
     * test silently stops testing anything. Trunk's shipped {@code conf/cassandra.yaml} and
     * {@code test/conf/cassandra.yaml} both leave it at 50MiB today, so the set/restore below is currently a
     * no-op -- it is kept deliberately, because the value of the guard is that the regression cannot be
     * un-covered by a future change to either yaml. (In the 4.1 fork this call was load bearing: the test yaml
     * left it unset there.) The "guard the guard" assertions below fail loudly if it ever stops working.
     *
     * <p>The reopen matters just as much: {@code CompressionMetadata.Writer.open} trims the offsets table to
     * {@code ceil(dataLength / chunkLength)} and resets {@code compressedLength} to {@code offsets[thatCount]},
     * so the reader a compaction hands back hides the trailing chunk completely. Only a reader built by
     * {@code CompressionMetadata.create} -- startup, {@code nodetool refresh}, streaming receive, i.e. anything
     * that has been through a restart -- sees the physical file length.
     *
     * <p>The bug: the splitter took the end of a child's last chunk to be {@code compressedFileLength} whenever
     * {@code lastChunk + 1} reached {@code ceil(dataLength / chunkLength)}, so the LAST child copied the trailing
     * chunk's bytes as slack. A reader derives a chunk's length from the following offset, so the child's final
     * chunk then claimed to be longer than it was, and every read of it failed its inline CRC32 -- or, once the
     * inflated length crossed {@code maxCompressedLength}, took the raw-chunk branch and returned compressed
     * bytes as row data. Digest.crc32 could not catch it, being computed over whatever bytes were written, and
     * the parent had already been obsoleted by then.
     */
    @Test
    public void splitOfCompactionProducedParentDoesNotAbsorbTheTrailingChunk() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());

        int previousInterval = DatabaseDescriptor.getSSTablePreemptiveOpenIntervalInMiB();
        SSTableReader parent = null;
        try
        {
            // What conf/cassandra.yaml ships and what Config defaults to; pinned so that a future change to
            // either yaml cannot silently stop this test from producing a trailing chunk.
            DatabaseDescriptor.setSSTablePreemptiveOpenIntervalInMiB(50);

            createCompressedTable(4);
            disableCompaction();
            insertPartitions(60, 5, 480);
            flush();
            insertPartitions(60, 5, 480);
            flush();

            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            assertEquals("need two sstables to have something to compact", 2, cfs.getLiveSSTables().size());
            cfs.forceMajorCompaction();
            SSTableReader compacted = onlySSTable(cfs);

            parent = SSTableReader.open(cfs, compacted.descriptor, compacted.getComponents(), cfs.metadata);

            long[] offsets = readChunkOffsets(parent.descriptor);
            CompressionMetadata meta = parent.getCompressionMetadata();
            int chunkLength = meta.chunkLength();
            int dataChunks = (int) ((meta.dataLength + chunkLength - 1) / chunkLength);
            long physical = parent.descriptor.fileFor(Components.DATA).length();

            // Guard the guard. If compaction ever stops emitting the trailing chunk, or the reopen stops
            // exposing it, this test silently stops testing anything -- so fail loudly instead.
            assertEquals("a compaction-produced sstable is expected to carry exactly one trailing " +
                         "zero-uncompressed-length chunk; without it this test cannot exercise the regression",
                         dataChunks + 1, offsets.length);
            assertEquals("the parent must be the on-disk view, whose length includes the trailing chunk",
                         physical, meta.compressedFileLength);
            assertTrue("the trailing chunk must put the physical end past the last data chunk",
                       physical > offsets[dataChunks]);
            assertTrue("more than one chunk, otherwise the whole exercise is trivial", dataChunks > 20);

            Result result = ZeroCopySSTableSplitter.split(parent, 3, null);
            try
            {
                assertEquals(3, result.children.size());

                // The last child is the only one that could have swallowed the trailing chunk.
                Child last = result.children.get(result.children.size() - 1);
                assertEquals("the last child must end at the last DATA chunk", dataChunks - 1, last.lastChunk);
                assertEquals("the last child must stop at the end of the last data chunk",
                             offsets[dataChunks] - offsets[(int) last.firstChunk], last.physicalBytes);
                assertEquals("and that must be its exact on-disk length, head pad aside",
                             last.onDiskLength(), last.descriptor.fileFor(Components.DATA).length());
                assertTrue("the trailing slack must not have been copied",
                           last.physicalBytes < physical - offsets[(int) last.firstChunk]);

                // The failure mode was confined to the final chunk, so read it: a wrong derived length shows up
                // as a CorruptSSTableException here and nowhere else.
                try (RandomAccessReader in = last.reader.openDataReader())
                {
                    in.seek(last.reader.uncompressedLength() - 1);
                    in.readByte();
                }

                assertStructure(cfs, parent, result);
                assertComponents(cfs, result);
                assertConcatenatedContentEquals(parent, readers(result));
                assertPointReads(parent, result);
            }
            finally
            {
                release(result);
            }
        }
        finally
        {
            if (parent != null)
                parent.selfRef().release();
            DatabaseDescriptor.setSSTablePreemptiveOpenIntervalInMiB(previousInterval);
        }
    }

    /**
     * A stop request aborts the copy and leaves nothing behind, and the {@link ZeroCopySSTableSplitter.Progress}
     * holder carries what the callers of {@link CompactionInfo.Holder#stop()} need in order to find it.
     *
     * <p>This is the wiring that makes {@code nodetool stop ANTICOMPACTION}, {@code nodetool stop --id},
     * TRUNCATE, DROP and {@code runWithCompactionsDisabled} work. Every one of them walks
     * {@code CompactionManager.active.getCompactions()} and decides whether to stop a holder from its
     * {@link CompactionInfo}: {@code stopCompaction} matches on {@code getTaskType()},
     * {@code stopCompactionById} on {@code getTaskId()}, and {@code interruptCompactionFor} on
     * {@code getTableMetadata()} plus the sstables in {@code shouldStop}. Before this existed the split
     * registered nothing, so all of them silently found no work to stop -- and truncate reported success while
     * the copy carried on.
     */
    @Test
    public void stopRequestAbortsTheSplitAndLeavesNoFilesBehind() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());

        createCompressedTable(4);
        disableCompaction();
        insertPartitions(80, 5, 480);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader parent = onlySSTable(cfs);
        int sstablesBefore = countDataFiles(parent.descriptor);

        ZeroCopySSTableSplitter.Progress progress =
            ZeroCopySSTableSplitter.progressFor(parent, RateLimiter.create(Double.MAX_VALUE));

        // What nodetool stop / truncate / drop look at to decide this operation is theirs to cancel.
        CompactionInfo info = progress.getCompactionInfo();
        assertEquals(OperationType.ANTICOMPACTION, info.getTaskType());
        assertEquals(cfs.metadata(), info.getTableMetadata());
        assertNotNull("a null task id would make nodetool stop --id unable to address this", info.getTaskId());
        assertEquals("the parent must be in the info, or interruptCompactionFor cannot match it",
                     Collections.singleton(parent), info.getSSTables());
        assertEquals(CompactionInfo.Unit.BYTES, info.getUnit());
        assertTrue("total must be positive or compactionstats shows no progress", info.getTotal() > 0);
        assertFalse(progress.isStopRequested());

        progress.stop();
        assertTrue(progress.isStopRequested());

        try
        {
            ZeroCopySSTableSplitter.split(parent, 4, null, progress);
            fail("a stopped split must raise CompactionInterruptedException rather than finish");
        }
        catch (CompactionInterruptedException expected)
        {
            // exactly what the rewrite path raises when its CompactionIterator is interrupted
        }

        assertEquals("an aborted split must not leave child sstables on disk",
                     sstablesBefore, countDataFiles(parent.descriptor));
        assertEquals("the parent must be untouched", parent, onlySSTable(cfs));
    }

    private static int countDataFiles(Descriptor descriptor)
    {
        File[] files = descriptor.directory.tryList((dir, name) -> name.endsWith("-Data.db"));
        return files == null ? 0 : files.length;
    }

    /**
     * A split child with a dead prefix is still eligible for entire-SSTable zero-copy streaming.
     *
     * <p>Entire-SSTable streaming copies every component file verbatim, so it is legal whenever the
     * requested ranges cover all of the child's live data. {@link CassandraOutgoingFile#contained} used to
     * compare the requested byte span against the physical data length, which a dead prefix makes
     * unreachable ({@code transferLength == uncompressedLength() - deadPrefixBytes}), needlessly refusing
     * the fast path until the child was recompacted. The check now measures against the live span, so the
     * child is eligible as-is. A genuinely partial range must still fall back to the rewrite path.
     */
    @Test
    public void childWithDeadPrefixIsEligibleForEntireSSTableStreaming() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());

        createCompressedTable(4);
        disableCompaction();
        insertPartitions(80, 5, 480);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader parent = onlySSTable(cfs);

        Result result = ZeroCopySSTableSplitter.split(parent, 4, null);
        try
        {
            Child dead = firstChildWithDeadPrefix(result);
            assertNotNull("no child started off a chunk boundary; the dead-prefix path was not exercised", dead);
            assertTrue(dead.deadPrefixBytes > 0);

            SSTableReader child = dead.reader;

            // The first live partition sits at deadPrefixBytes, so getPositionsForRanges() over the whole
            // token range yields [deadPrefixBytes, uncompressedLength) -- a span short of the physical length.
            long firstPosition = child.getPosition(child.getFirst().getToken().minKeyBound(),
                                                   SSTableReader.Operator.GT);
            assertEquals(dead.deadPrefixBytes, firstPosition);

            List<Range<Token>> fullRange = Range.normalize(Collections.singletonList(
                new Range<>(cfs.getPartitioner().getMinimumToken(), child.getLast().getToken())));
            List<SSTableReader.PartitionPositionBounds> sections = child.getPositionsForRanges(fullRange);
            long transferLength = sections.stream().mapToLong(p -> p.upperPosition - p.lowerPosition).sum();
            assertEquals(child.uncompressedLength() - firstPosition, transferLength);
            assertTrue("the dead prefix must make the byte span fall short of the physical length",
                       transferLength < child.uncompressedLength());

            CassandraOutgoingFile cof = new CassandraOutgoingFile(StreamOperation.BOOTSTRAP, child.ref(),
                                                                  sections, fullRange, child.estimatedKeys());
            try
            {
                // The whole live span is requested, so despite the dead prefix the child is eligible.
                assertTrue("a dead prefix must not disqualify a fully-covered child", cof.contained(sections, child));

                // A range covering only part of the child must still fall back to the rewrite path.
                List<Rec> childIndex = readIndex(child.descriptor);
                assertTrue("need at least two partitions for a partial range", childIndex.size() >= 2);
                Token midToken = child.decorateKey(childIndex.get(childIndex.size() / 2).key).getToken();
                List<Range<Token>> partialRange = Range.normalize(Collections.singletonList(
                    new Range<>(cfs.getPartitioner().getMinimumToken(), midToken)));
                List<SSTableReader.PartitionPositionBounds> partialSections = child.getPositionsForRanges(partialRange);
                assertFalse("a partial range must not be treated as containing the whole sstable",
                            cof.contained(partialSections, child));
            }
            finally
            {
                cof.finish();
            }
        }
        finally
        {
            release(result);
        }
    }

    /**
     * Wide partitions: every partition carries a promoted index blob and spans several compression chunks.
     * The blob is copied verbatim, so slice reads (which navigate it) must return identical results, and the
     * column-index cache is forced to zero so the blob is re-read from the CHILD's Index.db on every lookup
     * (ShallowIndexedEntry) rather than being served from an on-heap copy.
     */
    @Test
    public void widePartitionsPreserveThePromotedIndex() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());

        int previousCacheSize = DatabaseDescriptor.getColumnIndexCacheSizeInKiB();
        DatabaseDescriptor.setColumnIndexCacheSize(0);
        try
        {
            createCompressedTable(4);
            disableCompaction();
            int partitions = 12;
            int rowsPerPartition = 40;
            insertPartitions(partitions, rowsPerPartition, 1000);
            flush();

            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            SSTableReader parent = onlySSTable(cfs);
            List<Rec> parentIndex = readIndex(parent.descriptor);
            assertEquals(partitions, parentIndex.size());

            int chunkLength = parent.getCompressionMetadata().chunkLength();
            for (int r = 0; r < parentIndex.size(); r++)
            {
                assertTrue("partition " + r + " has no promoted index", parentIndex.get(r).promoted != null);
                long end = r + 1 < parentIndex.size() ? parentIndex.get(r + 1).position : parent.uncompressedLength();
                assertTrue("partition " + r + " does not span multiple chunks",
                           end - parentIndex.get(r).position > chunkLength);
            }

            Result result = ZeroCopySSTableSplitter.split(parent, 3, null);
            try
            {
                assertEquals(3, result.children.size());
                assertStructure(cfs, parent, result);
                assertComponents(cfs, result);
                assertConcatenatedContentEquals(parent, readers(result));
                assertPointReads(parent, result);

                for (Child child : result.children)
                {
                    // SSTableReader.getPosition() only hands back the data position, so go through the BIG
                    // reader's own entry lookup -- isIndexed() is the whole point of this assertion.
                    RowIndexEntry entry = ((BigTableReader) child.reader).getRowIndexEntry(child.first,
                                                                                           SSTableReader.Operator.EQ,
                                                                                           false,
                                                                                           NOOP);
                    assertNotNull(entry);
                    assertTrue("child lost the promoted index for " + child.first, entry.isIndexed());
                }

                assertSliceReadsMatch(cfs, parent, result, rowsPerPartition);
            }
            finally
            {
                release(result);
            }
        }
        finally
        {
            DatabaseDescriptor.setColumnIndexCacheSize(previousCacheSize);
        }
    }

    /** One child: the Data.db copy must be byte identical to the parent's, and there is no dead prefix. */
    @Test
    public void singleChildCopiesTheParentByteForByte() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());

        createCompressedTable(4);
        disableCompaction();
        insertPartitions(25, 4, 400);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader parent = onlySSTable(cfs);

        Result result = ZeroCopySSTableSplitter.split(parent, 1, null);
        try
        {
            assertEquals(1, result.children.size());
            Child only = result.children.get(0);
            assertEquals(0, only.firstChunk);
            assertEquals(0, only.shift);
            assertEquals(0, only.deadPrefixBytes);
            assertEquals(0, result.totalDeadPrefixBytes);
            assertEquals(0, result.duplicatedChunkBytes);
            assertEquals(parent.uncompressedLength(), only.dataLength);
            assertEquals(parent.descriptor.fileFor(Components.DATA).length(), only.physicalBytes);

            assertStructure(cfs, parent, result);
            assertComponents(cfs, result);
            assertConcatenatedContentEquals(parent, readers(result));
            assertPointReads(parent, result);

            assertArrayEquals("a one-way split must reproduce Data.db exactly",
                              Files.readAllBytes(parent.descriptor.fileFor(Components.DATA).toPath()),
                              Files.readAllBytes(only.descriptor.fileFor(Components.DATA).toPath()));
            assertEquals(readDigest(parent.descriptor), readDigest(only.descriptor));
        }
        finally
        {
            release(result);
        }
    }

    /**
     * A single-partition sstable is still splittable one way, and cannot be split further. Also covers the
     * "very first and very last partition are the same partition" boundary.
     */
    @Test
    public void singlePartitionSSTable() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());

        createCompressedTable(4);
        disableCompaction();
        insertPartitions(1, 12, 900);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader parent = onlySSTable(cfs);
        assertEquals(1, readIndex(parent.descriptor).size());
        assertTrue(parent.uncompressedLength() > parent.getCompressionMetadata().chunkLength());

        try
        {
            ZeroCopySSTableSplitter.split(parent, 2, null);
            fail("a single-partition sstable cannot be split two ways");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage().contains("cannot split"));
        }

        Result result = ZeroCopySSTableSplitter.split(parent, 1, null);
        try
        {
            assertEquals(1, result.children.size());
            assertEquals(1, result.children.get(0).partitionCount);
            assertEquals(parent.getFirst(), result.children.get(0).first);
            assertEquals(parent.getLast(), result.children.get(0).last);
            assertStructure(cfs, parent, result);
            assertComponents(cfs, result);
            assertConcatenatedContentEquals(parent, readers(result));
        }
        finally
        {
            release(result);
        }
    }

    /**
     * One child per partition, all of them inside a single compression chunk. Every child then copies the
     * same physical chunk and only its own Index.db entry keeps it apart; the concatenation must still be
     * exactly the parent.
     */
    @Test
    public void oneChildPerPartitionInsideASingleChunk() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());

        createCompressedTable(4);
        disableCompaction();
        insertPartitions(3, 1, 100);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader parent = onlySSTable(cfs);
        assertTrue("expected the whole sstable to fit in one chunk",
                   parent.uncompressedLength() <= parent.getCompressionMetadata().chunkLength());

        try
        {
            ZeroCopySSTableSplitter.split(parent, 4, null);
            fail("expected a refusal for more children than partitions");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage().contains("cannot split"));
        }

        try
        {
            ZeroCopySSTableSplitter.split(parent, 0, null);
            fail("expected a refusal for numChildren < 1");
        }
        catch (IllegalArgumentException e)
        {
            // expected
        }

        Result result = ZeroCopySSTableSplitter.split(parent, 3, null);
        try
        {
            assertEquals(3, result.children.size());
            for (Child child : result.children)
            {
                assertEquals(1, child.partitionCount);
                assertEquals(0, child.firstChunk);
                assertEquals(0, child.lastChunk);
                assertEquals(0, child.shift);
            }
            // children 1 and 2 start inside chunk 0, so they must carry a dead prefix
            assertEquals(0, result.children.get(0).deadPrefixBytes);
            assertTrue(result.children.get(1).deadPrefixBytes > 0);
            assertTrue(result.children.get(2).deadPrefixBytes > 0);

            assertStructure(cfs, parent, result);
            assertComponents(cfs, result);
            assertConcatenatedContentEquals(parent, readers(result));
            assertPointReads(parent, result);
        }
        finally
        {
            release(result);
        }
    }

    /** The explicit-boundary form, including the "boundary range contains no partition" case. */
    @Test
    public void explicitBoundaries() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());

        createCompressedTable(4);
        disableCompaction();
        insertPartitions(60, 4, 400);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader parent = onlySSTable(cfs);
        List<Rec> parentIndex = readIndex(parent.descriptor);
        assertEquals(60, parentIndex.size());

        DecoratedKey first = parent.decorateKey(parentIndex.get(17).key);
        DecoratedKey second = parent.decorateKey(parentIndex.get(41).key);

        Result result = ZeroCopySSTableSplitter.split(parent, Arrays.asList(first, second), null);
        try
        {
            assertEquals(3, result.children.size());
            assertEquals(17, result.children.get(0).partitionCount);
            assertEquals(24, result.children.get(1).partitionCount);
            assertEquals(19, result.children.get(2).partitionCount);
            assertEquals(first, result.children.get(1).first);
            assertEquals(second, result.children.get(2).first);

            assertStructure(cfs, parent, result);
            assertComponents(cfs, result);
            assertConcatenatedContentEquals(parent, readers(result));
            assertPointReads(parent, result);
        }
        finally
        {
            release(result);
        }

        // A boundary equal to the very first key leaves an empty leading run: no child is emitted for it.
        Result degenerate = ZeroCopySSTableSplitter.split(parent, Collections.singletonList(parent.getFirst()), null);
        try
        {
            assertEquals(1, degenerate.children.size());
            assertEquals(60, degenerate.children.get(0).partitionCount);
            assertConcatenatedContentEquals(parent, readers(degenerate));
        }
        finally
        {
            release(degenerate);
        }

        try
        {
            ZeroCopySSTableSplitter.split(parent, Arrays.asList(second, first), null);
            fail("expected non-increasing boundaries to be rejected");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage().contains("strictly increasing"));
        }
    }

    /**
     * A split boundary that lands exactly on a compression chunk boundary: the second child then has no dead
     * prefix and the two children share no chunk at all.
     * <p>
     * Every partition here has an identical serialised size S (fixed width key, fixed width value, fixed
     * timestamp), so partition r starts at r*S and some r = L / gcd(S, L) &lt;= L is necessarily a multiple
     * of the 1 KiB chunk length -- which is why 1200 partitions are written.
     */
    @Test
    public void splitBoundaryOnAChunkBoundary() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());

        createCompressedTable(1);
        disableCompaction();
        int partitions = 1200;
        String value = fixedText(24);
        for (int p = 0; p < partitions; p++)
            execute("INSERT INTO %s (pk, ck, val) VALUES (?, ?, ?) USING TIMESTAMP 1000", key(p), 0, value);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader parent = onlySSTable(cfs);
        int chunkLength = parent.getCompressionMetadata().chunkLength();
        assertEquals(1024, chunkLength);

        List<Rec> parentIndex = readIndex(parent.descriptor);
        assertEquals(partitions, parentIndex.size());
        long size = parentIndex.get(1).position - parentIndex.get(0).position;
        for (int r = 1; r < parentIndex.size(); r++)
            assertEquals("partitions were expected to be identically sized",
                         size, parentIndex.get(r).position - parentIndex.get(r - 1).position);

        int aligned = -1;
        for (int r = 1; r < parentIndex.size(); r++)
        {
            if (parentIndex.get(r).position % chunkLength == 0)
            {
                aligned = r;
                break;
            }
        }
        assertTrue("no partition start landed on a chunk boundary (partition size " + size + ')', aligned > 0);

        DecoratedKey boundary = parent.decorateKey(parentIndex.get(aligned).key);
        Result result = ZeroCopySSTableSplitter.split(parent, Collections.singletonList(boundary), null);
        try
        {
            assertEquals(2, result.children.size());
            Child head = result.children.get(0);
            Child tail = result.children.get(1);

            assertEquals(aligned, head.partitionCount);
            assertEquals(partitions - aligned, tail.partitionCount);
            assertEquals(0, tail.deadPrefixBytes);
            assertEquals(parentIndex.get(aligned).position, tail.shift);
            assertEquals("an aligned boundary must not duplicate a chunk", head.lastChunk + 1, tail.firstChunk);
            assertEquals(0, result.duplicatedChunkBytes);
            // the head's Data.db ends exactly where the tail's begins: no byte is in both children
            assertEquals(parent.descriptor.fileFor(Components.DATA).length(),
                         head.physicalBytes + tail.physicalBytes);

            assertStructure(cfs, parent, result);
            assertComponents(cfs, result);
            assertConcatenatedContentEquals(parent, readers(result));
        }
        finally
        {
            release(result);
        }
    }

    /**
     * FACT 7: Scrubber and Verifier both walk Data.db linearly from position 0 and were patched to start at
     * the first index position instead. So both must now ACCEPT a child that carries a dead prefix.
     */
    @Test
    public void verifierAndScrubberAcceptAChildWithADeadPrefix() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());

        createCompressedTable(4);
        disableCompaction();
        insertPartitions(40, 4, 500);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader parent = onlySSTable(cfs);

        Result result = ZeroCopySSTableSplitter.split(parent, 3, null);
        SSTableReader consumedByTxn = null;
        try
        {
            Child dead = firstChildWithDeadPrefix(result);
            assertNotNull("no child started off a chunk boundary", dead);

            // Extended verification reads every partition off Data.db linearly and validates Digest.crc32,
            // the index, the summary and the bloom filter. It throws CorruptSSTableException on failure.
            for (Child child : result.children)
            {
                try (IVerifier verifier = verifier(cfs, child.reader,
                                                   IVerifier.options().extendedVerification(true)))
                {
                    verifier.verify();
                }
            }

            // Scrubber rewrites the child from a linear Data.db walk; every partition must come back good.
            // LifecycleTransaction.offline() hands the reader to a dummy Tracker that owns and releases it
            // (LifecycleTransaction.java:143-149), so this child must not be released again below.
            consumedByTxn = dead.reader;
            IScrubber.ScrubResult scrubResult;
            try (LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.SCRUB, dead.reader);
                 IScrubber scrubber = dead.descriptor.getFormat().getScrubber(cfs,
                                                                              txn,
                                                                              new OutputHandler.LogOutput(),
                                                                              IScrubber.options().checkData().build()))
            {
                scrubResult = scrubber.scrubWithResult();
            }
            assertEquals(dead.partitionCount, scrubResult.goodPartitions);
            assertEquals(0, scrubResult.badPartitions);
            assertEquals(0, scrubResult.emptyPartitions);
        }
        finally
        {
            releaseExcept(result, consumedByTxn);
            LifecycleTransaction.waitForDeletions();
        }
    }

    /**
     * The ALIGNED layout, which is what extent sharing costs: a child's Data.db starts with up to 64 KiB of the
     * parent's previous compression chunk, so its {@code offsets[0]} is that pad instead of 0 and every physical
     * offset in it is shifted.
     *
     * <p>This forces the layout on rather than requiring a filesystem that can share extents -- no developer
     * laptop and no CI box can, and this must not be a test that only ever runs on xfs. The layout is a
     * property of {@code copyPlan}, not of the mechanism: a padded range that gets copied instead of cloned
     * produces a byte-identical child, so copying it here exercises exactly the file a reflink would have
     * produced. What is NOT covered by forcing it is the ioctl itself, which either shares the range or reports
     * that it cannot.
     *
     * <p>Everything is asserted through the ordinary readers, because the point is that nothing downstream
     * notices. The one consumer that did notice, and had to be fixed, is {@code MmappedRegions}: it placed
     * segments at a cumulative sum of chunk lengths seeded at physical 0, so a padded file's last chunk ran off
     * the end of the last mapped region. That only happens when Data.db is memory mapped, and
     * {@code test/conf/cassandra.yaml} says {@code disk_access_mode: mmap_index_only} -- which
     * {@code DatabaseDescriptor.applyConfig} rewrites to {@code standard} for the data file, mapping only the
     * index. Under that mode the whole class of bugs is invisible here. So rather than assert the global mode
     * (which would simply fail), this test switches the data path to {@code mmap} for its own duration and
     * restores it afterwards, and does all of its work -- table creation, flush, split, reads -- while it is
     * switched: {@code IOOptions} is snapshotted per reader at open time, so a reader opened before the switch
     * would not be mapped. That is why a child of more than one chunk is not enough either: it has to be read
     * to the last byte, which the {@code openDataReader} seeks below do.
     *
     * <p>The direct, mode-independent coverage of the same fix lives in
     * {@code MmappedRegionsTest.testMapForCompressionMetadataWithFrontPad}.
     */
    @Test
    public void alignedChildrenAreReadableEverywhere() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());

        Config.DiskAccessMode previousMode = DatabaseDescriptor.getDiskAccessMode();
        DatabaseDescriptor.setDiskAccessMode(Config.DiskAccessMode.mmap);
        try
        {
            alignedChildrenAreReadableEverywhereUnderMmap();
        }
        finally
        {
            DatabaseDescriptor.setDiskAccessMode(previousMode);
        }
    }

    private void alignedChildrenAreReadableEverywhereUnderMmap() throws Throwable
    {
        createCompressedTable(4);
        disableCompaction();
        // Big enough that the parent spans several 64 KiB alignment units, so the pads are real residues of
        // O(i) rather than just "everything before this chunk".
        insertPartitions(400, 5, 480);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader parent = onlySSTable(cfs);
        assertEquals("this test needs the mmap read path to be the one under test",
                     Config.DiskAccessMode.mmap, parent.ioOptions.defaultDiskAccessMode);
        assertTrue("the parent must span several alignment units for the residues to mean anything",
                   parent.descriptor.fileFor(Components.DATA).length() > 4 * 64 * 1024);

        Result result;
        ZeroCopySSTableSplitter.forceAlignedLayoutForTesting = true;
        try
        {
            result = ZeroCopySSTableSplitter.split(parent, 4, null);
        }
        finally
        {
            ZeroCopySSTableSplitter.forceAlignedLayoutForTesting = false;
        }

        try
        {
            assertEquals(4, result.children.size());

            // Guard the guard: without a padded child this test asserts nothing new. Only the first child can
            // legitimately have no pad, its first chunk being at physical 0.
            assertEquals("the first child starts at physical 0 and cannot be padded",
                         0, result.children.get(0).headPadBytes);
            int padded = 0;
            for (Child child : result.children)
            {
                if (child.headPadBytes > 0)
                    padded++;
                assertTrue("head pad must be under one alignment unit", child.headPadBytes < 64 * 1024);
                assertEquals("offsets[0] must be the head pad",
                             child.headPadBytes, child.reader.getCompressionMetadata().chunkFor(0).offset);
                assertEquals("the pad is on disk and nowhere else",
                             child.onDiskLength(), child.descriptor.fileFor(Components.DATA).length());
                // The uncompressed dead prefix is a DIFFERENT thing and must not have moved: the head pad is
                // physical, the dead prefix is where the first partition sits in uncompressed space.
                long firstPosition = child.reader.getPosition(child.first, SSTableReader.Operator.EQ, false);
                assertTrue("the child cannot find its own first key", firstPosition >= 0);
                assertEquals(child.deadPrefixBytes, firstPosition);
            }
            assertTrue("no child was padded; the aligned layout was not exercised", padded > 0);
            assertEquals(sumHeadPad(result), result.totalHeadPadBytes);
            assertTrue("the pad is accounted for in the result", result.totalHeadPadBytes > 0);

            // Reading, in every way there is to read.
            assertStructure(cfs, parent, result);
            assertComponents(cfs, result);
            assertConcatenatedContentEquals(parent, readers(result));
            assertPointReads(parent, result);

            // The last byte of every child, which is the read the MmappedRegions bug broke and nothing else did.
            for (Child child : result.children)
            {
                try (RandomAccessReader in = child.reader.openDataReader())
                {
                    in.seek(child.reader.uncompressedLength() - 1);
                    in.readByte();
                }
            }

            // Digest.crc32 covers the pad, because Verifier CRCs the whole physical file with no reference to
            // CompressionInfo.db. Extended verification also walks Data.db linearly and rebuilds the index.
            for (Child child : result.children)
            {
                assertEquals(String.valueOf(crc32Of(child.descriptor.fileFor(Components.DATA))),
                             readDigest(child.descriptor));
                try (IVerifier verifier = verifier(cfs, child.reader,
                                                   IVerifier.options().extendedVerification(true)))
                {
                    verifier.verify();
                }
            }
        }
        finally
        {
            release(result);
        }

        // And from a cold open, where CompressionMetadata is built from the file length rather than handed over.
        List<SSTableReader> reopened = new ArrayList<>();
        try
        {
            for (Child child : result.children)
                reopened.add(SSTableReader.open(cfs, child.descriptor, child.components, cfs.metadata));
            assertConcatenatedContentEquals(parent, reopened);
            for (int i = 0; i < reopened.size(); i++)
                assertEquals(result.children.get(i).onDiskLength(),
                             reopened.get(i).getCompressionMetadata().compressedFileLength);
        }
        finally
        {
            for (SSTableReader reader : reopened)
                reader.selfRef().release();
        }
    }

    /**
     * Digest.crc32 is optional. It is the only component whose cost is proportional to the DATA rather than to
     * the index -- one full sequential read of every child -- so with the extents shared it is the entire
     * remaining cost of a split, and {@code zero_copy_split_digest_enabled: false} takes a split down to its
     * Index.db pass.
     *
     * <p>What this pins is that skipping it is a supported state and not a broken one:
     * <ul>
     *   <li>the file does not exist, TOC does not claim it, and the component set does not contain it -- the
     *       three have to agree or {@code Descriptor.discoverComponents} and the transaction's file bookkeeping
     *       disagree about what belongs to the sstable;</li>
     *   <li>the children still open, read and scan identically, from memory and from a cold open;</li>
     *   <li>{@code Verifier} still passes, and passes by the documented route: a missing digest makes it say so
     *       and upgrade to a full extended verification rather than fail. That upgrade is the whole cost of this
     *       option, so it is asserted directly rather than inferred from "verify did not throw".</li>
     * </ul>
     */
    @Test
    public void digestIsOptional() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());

        createCompressedTable(4);
        disableCompaction();
        insertPartitions(60, 4, 480);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader parent = onlySSTable(cfs);

        Result result;
        boolean previousDigestEnabled = DatabaseDescriptor.getZeroCopySplitDigestEnabled();
        DatabaseDescriptor.setZeroCopySplitDigestEnabled(false);
        try
        {
            result = ZeroCopySSTableSplitter.split(parent, 3, null);
        }
        finally
        {
            DatabaseDescriptor.setZeroCopySplitDigestEnabled(previousDigestEnabled);
        }

        try
        {
            assertEquals(3, result.children.size());
            for (Child child : result.children)
            {
                String context = "child " + child.descriptor;
                assertFalse(context + ": Digest.crc32 must not have been written",
                            child.descriptor.fileFor(Components.DIGEST).exists());
                assertFalse(context + ": DIGEST must not be a component", child.components.contains(Components.DIGEST));
                assertFalse(context + ": TOC must not list DIGEST",
                            TOCComponent.loadTOC(child.descriptor, false).contains(Components.DIGEST));
                assertFalse(context + ": nothing on disk may claim DIGEST",
                            child.descriptor.discoverComponents().contains(Components.DIGEST));
            }

            // Everything else is unchanged, including the components that ARE written.
            assertStructure(cfs, parent, result);
            assertComponents(cfs, result);
            assertConcatenatedContentEquals(parent, readers(result));
            assertPointReads(parent, result);

            // The documented Verifier fallback: not quick, not extended, no digest -> says so, then does the
            // full walk and succeeds.
            for (Child child : result.children)
            {
                List<String> output = new ArrayList<>();
                OutputHandler handler = new OutputHandler.LogOutput()
                {
                    @Override
                    public void output(String msg)
                    {
                        output.add(msg);
                    }
                };
                IVerifier.Options options = IVerifier.options().extendedVerification(false).build();
                try (IVerifier verifier = child.reader.getVerifier(cfs, handler, true, options))
                {
                    verifier.verify();
                }
                assertTrue("Verifier did not report the missing digest: " + output,
                           output.stream().anyMatch(m -> m.contains("Data digest missing")));
                assertTrue("Verifier did not fall through to the extended walk: " + output,
                           output.stream().anyMatch(m -> m.contains("Extended Verify requested")));
            }

            // ...and the quick path, which never looks at the digest at all.
            for (Child child : result.children)
            {
                try (IVerifier verifier = verifier(cfs, child.reader, IVerifier.options().quick(true)))
                {
                    verifier.verify();
                }
            }
        }
        finally
        {
            release(result);
        }

        // A cold open must not miss the component either: componentsFor() rediscovers from TOC.
        List<SSTableReader> reopened = new ArrayList<>();
        try
        {
            for (Child child : result.children)
                reopened.add(SSTableReader.open(cfs, child.descriptor, child.components, cfs.metadata));
            assertConcatenatedContentEquals(parent, reopened);
        }
        finally
        {
            for (SSTableReader reader : reopened)
                reader.selfRef().release();
        }
    }

    /** An uncompressed parent is refused up front rather than producing a child with a misaligned CRC.db. */
    @Test
    public void uncompressedParentIsRefused() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());

        createTable("CREATE TABLE %s (pk text, ck int, val text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'}");
        disableCompaction();
        insertPartitions(10, 2, 300);
        flush();

        SSTableReader parent = onlySSTable(getCurrentColumnFamilyStore());
        assertFalse(parent.compression);
        assertFalse(ZeroCopySSTableSplitter.isSupported(parent));

        try
        {
            ZeroCopySSTableSplitter.split(parent, 2, null);
            fail("expected an uncompressed parent to be refused");
        }
        catch (UnsupportedOperationException e)
        {
            assertTrue(e.getMessage(),
                       e.getMessage().startsWith(ZeroCopySSTableSplitter.UNCOMPRESSED_UNSUPPORTED_MESSAGE));
        }
    }

    // ----------------------------------------------------------------------------------------------------
    // Content equivalence
    // ----------------------------------------------------------------------------------------------------

    /**
     * The single most important assertion in this file: the children, scanned in order and concatenated,
     * produce exactly the parent's partition stream -- same keys in the same order, same partition level
     * deletions, same rows/range tombstones, same cells, same timestamps.
     */
    private static void assertConcatenatedContentEquals(SSTableReader parent, List<SSTableReader> children)
    {
        int compared = 0;
        try (ISSTableScanner parentScanner = parent.getScanner())
        {
            for (SSTableReader child : children)
            {
                try (ISSTableScanner childScanner = child.getScanner())
                {
                    while (childScanner.hasNext())
                    {
                        assertTrue("children yielded more partitions than the parent has (at " + compared + ')',
                                   parentScanner.hasNext());
                        try (UnfilteredRowIterator expected = parentScanner.next();
                             UnfilteredRowIterator actual = childScanner.next())
                        {
                            assertSamePartition(expected, actual);
                        }
                        compared++;
                    }
                }
            }
            assertFalse("the parent has partitions that no child covers (after " + compared + ')',
                        parentScanner.hasNext());
        }
        assertTrue("nothing was compared", compared > 0);
    }

    private static void assertSamePartition(UnfilteredRowIterator expected, UnfilteredRowIterator actual)
    {
        String context = "partition " + expected.partitionKey();
        assertEquals(context, expected.partitionKey(), actual.partitionKey());
        assertEquals(context + ": partition level deletion",
                     expected.partitionLevelDeletion(), actual.partitionLevelDeletion());
        assertEquals(context + ": static row", expected.staticRow(), actual.staticRow());
        assertEquals(context + ": columns", expected.columns(), actual.columns());
        assertEquals(context + ": reverse order", expected.isReverseOrder(), actual.isReverseOrder());

        int i = 0;
        while (expected.hasNext())
        {
            assertTrue(context + ": child ran out of rows after " + i, actual.hasNext());
            assertEquals(context + ": unfiltered " + i, expected.next(), actual.next());
            i++;
        }
        assertFalse(context + ": child has extra rows after " + i, actual.hasNext());
        assertTrue(context + ": expected at least one row", i > 0);
    }

    /** Every parent key is owned by exactly one child, reads back identically there, and is absent elsewhere. */
    private void assertPointReads(SSTableReader parent, Result result) throws IOException
    {
        ColumnFilter columns = ColumnFilter.all(parent.metadata());
        for (Rec rec : readIndex(parent.descriptor))
        {
            DecoratedKey key = parent.decorateKey(rec.key);
            int owners = 0;
            for (Child child : result.children)
            {
                boolean inRange = key.compareTo(child.first) >= 0 && key.compareTo(child.last) <= 0;
                // getPosition returns the data position, or a negative value when the key is not present.
                long position = child.reader.getPosition(key, SSTableReader.Operator.EQ, false);
                if (!inRange)
                {
                    assertTrue("child " + child.descriptor + " must not contain " + key, position < 0);
                    continue;
                }
                owners++;
                // getPosition(EQ) consults the bloom filter first, so an absent key here would also be a filter
                // false negative, i.e. silent data loss.
                assertTrue("child " + child.descriptor + " lost " + key, position >= 0);
                try (UnfilteredRowIterator expected = parent.rowIterator(key, Slices.ALL, columns, false, NOOP);
                     UnfilteredRowIterator actual = child.reader.rowIterator(key, Slices.ALL, columns, false, NOOP))
                {
                    assertSamePartition(expected, actual);
                }
            }
            assertEquals("exactly one child must own " + key, 1, owners);
        }
    }

    /** Clustering-level slice reads inside wide partitions: this is what navigates the copied promoted index. */
    private void assertSliceReadsMatch(ColumnFamilyStore cfs, SSTableReader parent, Result result, int rowsPerPartition)
    throws IOException
    {
        TableMetadata metadata = cfs.metadata();
        ClusteringComparator comparator = metadata.comparator;
        Slices slices = Slices.with(comparator, Slice.make(comparator.make(rowsPerPartition / 3),
                                                           comparator.make(2 * rowsPerPartition / 3)));
        ColumnFilter columns = ColumnFilter.all(metadata);

        int checked = 0;
        for (Child child : result.children)
        {
            for (Rec rec : readIndex(child.descriptor))
            {
                DecoratedKey key = metadata.partitioner.decorateKey(rec.key);
                for (boolean reversed : new boolean[]{ false, true })
                {
                    try (UnfilteredRowIterator expected = parent.rowIterator(key, slices, columns, reversed, NOOP);
                         UnfilteredRowIterator actual = child.reader.rowIterator(key, slices, columns, reversed, NOOP))
                    {
                        assertSamePartition(expected, actual);
                    }
                }
                checked++;
            }
        }
        assertTrue(checked > 0);
    }

    // ----------------------------------------------------------------------------------------------------
    // Structural assertions -- FACT 9 recomputed independently of the implementation
    // ----------------------------------------------------------------------------------------------------

    private void assertStructure(ColumnFamilyStore cfs, SSTableReader parent, Result result) throws IOException
    {
        List<Rec> parentIndex = readIndex(parent.descriptor);
        int n = parentIndex.size();

        CompressionMetadata meta = parent.getCompressionMetadata();
        int chunkLength = meta.chunkLength();
        long parentUncompressed = parent.uncompressedLength();
        assertEquals(meta.dataLength, parentUncompressed);
        long parentPhysical = parent.descriptor.fileFor(Components.DATA).length();
        long[] parentOffsets = readChunkOffsets(parent.descriptor);
        int parentDataChunks = (int) ((parentUncompressed + chunkLength - 1) / chunkLength);
        // A flushed parent's offsets table stops at the last data chunk and its metadata length is the physical
        // length; a compaction-produced one has a trailing chunk beyond both. Either is legal input.
        assertTrue("offsets table must address every data chunk", parentOffsets.length >= parentDataChunks);

        StatsMetadata parentStats = parent.getSSTableMetadata();

        long physicalSum = 0;
        long deadSum = 0;
        long duplicatedSum = 0;
        long partitionSum = 0;
        int cursor = 0;
        long previousLastChunk = -1;

        for (Child child : result.children)
        {
            String context = "child " + child.descriptor;
            int from = cursor;
            assertTrue(context + " starts past the end of the parent", from < n);
            int to = from + (int) child.partitionCount;
            assertTrue(context + " runs past the end of the parent", to <= n);

            assertEquals(context + ": first key", parentIndex.get(from).key, child.first.getKey());
            assertEquals(context + ": last key", parentIndex.get(to - 1).key, child.last.getKey());
            assertEquals(context + ": reader.first", child.first, child.reader.getFirst());
            assertEquals(context + ": reader.last", child.last, child.reader.getLast());
            assertTrue(context + ": first > last", child.first.compareTo(child.last) <= 0);

            long lo = parentIndex.get(from).position;
            long hi = to < n ? parentIndex.get(to).position : parentUncompressed;
            long firstChunk = lo / chunkLength;
            long lastChunk = (hi - 1) / chunkLength;
            long dataLength = hi - firstChunk * chunkLength;
            long physicalBytes = chunkEndOnDisk(parentOffsets, lastChunk, parentPhysical)
                                 - parentOffsets[(int) firstChunk];

            assertEquals(context + ": firstChunk", firstChunk, child.firstChunk);
            assertEquals(context + ": lastChunk", lastChunk, child.lastChunk);
            assertEquals(context + ": shift", firstChunk * chunkLength, child.shift);
            assertEquals(context + ": deadPrefixBytes", lo % chunkLength, child.deadPrefixBytes);
            assertEquals(context + ": dataLength", dataLength, child.dataLength);
            assertEquals(context + ": physicalBytes", physicalBytes, child.physicalBytes);
            // (C - 1) * L < Dp <= C * L
            long chunkCount = lastChunk - firstChunk + 1;
            assertTrue(context + ": (C-1)*L < Dp", (chunkCount - 1) * chunkLength < dataLength);
            assertTrue(context + ": Dp <= C*L", dataLength <= chunkCount * chunkLength);

            // FACT 6: not one byte of trailing slack on disk. The head pad is the one thing that may sit in
            // front of the run -- zero unless the child was aligned so its extents could be shared with the
            // parent -- so every physical length here is measured from the pad, not from 0.
            long pad = child.headPadBytes;
            assertTrue(context + ": head pad must be under one alignment unit", pad < 64 * 1024);
            assertTrue(context + ": head pad must be O(i) mod alignment, or nothing",
                       pad == 0 || pad == parentOffsets[(int) firstChunk] % (64 * 1024));
            assertEquals(context + ": on-disk Data.db length", pad + physicalBytes, child.onDiskLength());
            assertEquals(context + ": physical Data.db length",
                         pad + physicalBytes, child.descriptor.fileFor(Components.DATA).length());
            assertEquals(context + ": uncompressedLength", dataLength, child.reader.uncompressedLength());

            CompressionMetadata childMeta = child.reader.getCompressionMetadata();
            assertEquals(context + ": offsets[0]", pad, childMeta.chunkFor(0).offset);
            assertEquals(context + ": chunkLength", chunkLength, childMeta.chunkLength());
            assertEquals(context + ": maxCompressedLength", meta.maxCompressedLength(), childMeta.maxCompressedLength());
            assertEquals(context + ": CompressionInfo dataLength", dataLength, childMeta.dataLength);
            assertEquals(context + ": compressedFileLength", pad + physicalBytes, childMeta.compressedFileLength);
            // the last chunk plus its 4 byte inline CRC32 must end exactly at the physical end of the file
            CompressionMetadata.Chunk tail = childMeta.chunkFor((chunkCount - 1) * chunkLength);
            assertEquals(context + ": last chunk overruns the file",
                         pad + physicalBytes, tail.offset + tail.length + 4);

            // Index.db: same keys, same promoted blobs, positions rebased by exactly shift.
            List<Rec> childIndex = readIndex(child.descriptor);
            assertEquals(context + ": partition count", child.partitionCount, childIndex.size());
            assertEquals(context + ": first index position", lo % chunkLength, childIndex.get(0).position);
            assertTrue(context + ": first index position must be inside the first chunk",
                       childIndex.get(0).position < chunkLength);
            for (int r = 0; r < childIndex.size(); r++)
            {
                Rec expected = parentIndex.get(from + r);
                Rec actual = childIndex.get(r);
                assertEquals(context + ": key " + r, expected.key, actual.key);
                assertEquals(context + ": position " + r,
                             expected.position - firstChunk * chunkLength, actual.position);
                assertArrayEquals(context + ": promoted index blob " + r + " must be copied verbatim",
                                  expected.promoted, actual.promoted);
            }

            long firstEntryPosition = child.reader.getPosition(child.first, SSTableReader.Operator.EQ, false);
            assertTrue(context + ": cannot find its own first key", firstEntryPosition >= 0);
            assertEquals(context + ": first entry position", lo % chunkLength, firstEntryPosition);

            // Statistics.db: the header and the min/max encoding bases MUST be inherited verbatim or every
            // relocated row silently decodes wrong; the two derived fields must be recomputed.
            StatsMetadata childStats = child.reader.getSSTableMetadata();
            assertEquals(context + ": header columns", parent.header.columns(), child.reader.header.columns());
            assertEquals(context + ": header stats", parent.header.stats(), child.reader.header.stats());
            assertEquals(context + ": minTimestamp", parentStats.minTimestamp, childStats.minTimestamp);
            assertEquals(context + ": maxTimestamp", parentStats.maxTimestamp, childStats.maxTimestamp);
            assertEquals(context + ": minLocalDeletionTime",
                         parentStats.minLocalDeletionTime, childStats.minLocalDeletionTime);
            assertEquals(context + ": maxLocalDeletionTime",
                         parentStats.maxLocalDeletionTime, childStats.maxLocalDeletionTime);
            assertEquals(context + ": minTTL", parentStats.minTTL, childStats.minTTL);
            assertEquals(context + ": maxTTL", parentStats.maxTTL, childStats.maxTTL);
            assertEquals(context + ": sstableLevel", parentStats.sstableLevel, childStats.sstableLevel);
            assertEquals(context + ": repairedAt", parentStats.repairedAt, childStats.repairedAt);
            assertEquals(context + ": originatingHostId", parentStats.originatingHostId, childStats.originatingHostId);
            assertEquals(context + ": compressionRatio",
                         (double) (pad + physicalBytes) / dataLength, childStats.compressionRatio, 1e-9);
            assertEquals(context + ": estimatedPartitionSize count",
                         child.partitionCount, childStats.estimatedPartitionSize.count());

            physicalSum += physicalBytes;
            deadSum += lo % chunkLength;
            partitionSum += child.partitionCount;
            if (previousLastChunk == firstChunk)
                duplicatedSum += chunkEndOnDisk(parentOffsets, firstChunk, parentPhysical)
                                 - parentOffsets[(int) firstChunk];
            previousLastChunk = lastChunk;
            cursor = to;
        }

        assertEquals("children must cover every parent partition exactly once", n, cursor);
        assertEquals("partition counts must sum to the parent's", n, partitionSum);
        assertEquals(physicalSum, result.totalPhysicalBytesCopied);
        assertEquals(deadSum, result.totalDeadPrefixBytes);
        assertEquals(duplicatedSum, result.duplicatedChunkBytes);
        assertEquals(parent.getFirst(), result.children.get(0).first);
        assertEquals(parent.getLast(), result.children.get(result.children.size() - 1).last);
    }

    /**
     * End of chunk {@code k}, inclusive of its 4-byte inline CRC32, derived from the offsets table exactly as it
     * exists in CompressionInfo.db.
     *
     * <p>The physical file length is the end of chunk {@code k} only when there is no entry after {@code k}.
     * This used to key off {@code ceil(dataLength / chunkLength)} instead -- the same formula production used --
     * so it agreed with the code it was supposed to be checking, and both were wrong for a compaction-produced
     * parent, which carries one extra chunk offset past the end of its data. See
     * {@link #splitOfCompactionProducedParentDoesNotAbsorbTheTrailingChunk}.
     */
    private static long chunkEndOnDisk(long[] offsets, long k, long compressedFileLength)
    {
        assertTrue("chunk " + k + " is not in the offsets table", k >= 0 && k < offsets.length);
        return k + 1 < offsets.length ? offsets[(int) (k + 1)] : compressedFileLength;
    }

    /**
     * The chunk offsets as stored, parsed here rather than through {@link CompressionMetadata}.
     * <p>
     * Trunk writes a serialised {@code CompressionDictionary} after the offsets table; it is deliberately not
     * read, since nothing here needs it and stopping at the offsets keeps this parser independent of CEP-49.
     */
    private static long[] readChunkOffsets(Descriptor descriptor) throws IOException
    {
        try (FileInputStreamPlus in = descriptor.fileFor(Components.COMPRESSION_INFO).newInputStream())
        {
            in.readUTF();                       // compressor class name
            int optionCount = in.readInt();
            for (int i = 0; i < optionCount; i++)
            {
                in.readUTF();
                in.readUTF();
            }
            in.readInt();                       // chunkLength
            if (descriptor.version.hasMaxCompressedLength())
                in.readInt();                   // maxCompressedLength
            in.readLong();                      // dataLength
            long[] offsets = new long[in.readInt()];
            for (int i = 0; i < offsets.length; i++)
                offsets[i] = in.readLong();
            return offsets;
        }
    }

    // ----------------------------------------------------------------------------------------------------
    // Component sanity
    // ----------------------------------------------------------------------------------------------------

    private void assertComponents(ColumnFamilyStore cfs, Result result) throws IOException
    {
        TableMetadata metadata = cfs.metadata();
        for (Child child : result.children)
        {
            String context = "child " + child.descriptor;

            // TOC.txt lists exactly the components that exist on disk, and nothing else exists on disk.
            assertEquals(context + ": TOC", child.components, TOCComponent.loadTOC(child.descriptor, false));
            assertEquals(context + ": files on disk",
                         child.components, child.descriptor.discoverComponents());
            assertTrue(context + ": no Filter.db", child.components.contains(Components.FILTER));
            assertTrue(context + ": no CompressionInfo.db", child.components.contains(Components.COMPRESSION_INFO));
            assertFalse(context + ": a compressed sstable must not have a CRC.db",
                        child.components.contains(Components.CRC));
            for (Component component : child.components)
                assertTrue(context + ": missing " + component, child.descriptor.fileFor(component).exists());

            // Digest.crc32, when it was written at all, is the decimal CRC32 of every physical byte of Data.db.
            // It is optional (zero_copy_split_digest_enabled), and the two states must be exactly two states:
            // the component is claimed and the file is right, or it is claimed nowhere and exists nowhere. A
            // file on disk that TOC does not list, or the reverse, is what the checks above would catch.
            if (child.components.contains(Components.DIGEST))
            {
                assertEquals(context + ": digest",
                             Long.toString(crc32Of(child.descriptor.fileFor(Components.DATA))),
                             readDigest(child.descriptor));
            }
            else
            {
                assertFalse(context + ": Digest.crc32 must not exist when it is not a component",
                            child.descriptor.fileFor(Components.DIGEST).exists());
            }

            // Statistics.db: all four metadata components must deserialise standalone. This is the component
            // whose loss is unrecoverable -- it carries the SerializationHeader every relocated row is decoded
            // against, plus the repair state -- and it is written here through a SequentialWriter (so that it is
            // fsynced) rather than through MetadataSerializer.rewriteSSTableMetadata, so assert the bytes are
            // still exactly what the deserialiser expects.
            Map<MetadataType, MetadataComponent> childMetadata =
                child.descriptor.getMetadataSerializer()
                                .deserialize(child.descriptor, EnumSet.allOf(MetadataType.class));
            for (MetadataType type : MetadataType.values())
                assertNotNull(context + ": Statistics.db is missing " + type, childMetadata.get(type));

            // ...and it is written in place, so no tmp file may survive the split.
            assertFalse(context + ": leftover Statistics.db tmp file",
                        child.descriptor.tmpFileFor(Components.STATS).exists());

            List<Rec> childIndex = readIndex(child.descriptor);

            // Bloom filter: a false negative is data loss, so every owned key must be present.
            try (FileInputStreamPlus in = child.descriptor.fileFor(Components.FILTER).newInputStream();
                 IFilter filter = BloomFilterSerializer.forVersion(child.descriptor.version.hasOldBfFormat())
                                                       .deserialize(in))
            {
                for (Rec rec : childIndex)
                    assertTrue(context + ": bloom filter false negative",
                               filter.isPresent(metadata.partitioner.decorateKey(rec.key)));
            }

            // Summary.db deserialises standalone with the schema's index interval (otherwise the read path
            // silently deletes it and rebuilds), and carries the child's own first/last keys.
            try (FileInputStreamPlus in = child.descriptor.fileFor(Components.SUMMARY).newInputStream())
            {
                IndexSummary summary = IndexSummary.serializer.deserialize(in,
                                                                           metadata.partitioner,
                                                                           metadata.params.minIndexInterval,
                                                                           metadata.params.maxIndexInterval);
                try
                {
                    assertTrue(context + ": empty summary", summary.size() > 0);
                    assertEquals(context + ": summary minIndexInterval",
                                 metadata.params.minIndexInterval, summary.getMinIndexInterval());
                }
                finally
                {
                    summary.close();
                }
                assertEquals(context + ": summary first key",
                             child.first, metadata.partitioner.decorateKey(ByteBufferUtil.readWithLength(in)));
                assertEquals(context + ": summary last key",
                             child.last, metadata.partitioner.decorateKey(ByteBufferUtil.readWithLength(in)));
            }
        }
    }

    // ----------------------------------------------------------------------------------------------------
    // Plumbing
    // ----------------------------------------------------------------------------------------------------

    /** One parent/child Index.db record, parsed independently of {@code ZeroCopySSTableSplitter}. */
    private static final class Rec
    {
        final ByteBuffer key;
        final long position;
        final byte[] promoted;   // null when promotedSize == 0

        Rec(ByteBuffer key, long position, byte[] promoted)
        {
            this.key = key;
            this.position = position;
            this.promoted = promoted;
        }
    }

    private static List<Rec> readIndex(Descriptor descriptor) throws IOException
    {
        List<Rec> records = new ArrayList<>();
        try (RandomAccessReader in = RandomAccessReader.open(descriptor.fileFor(Components.PRIMARY_INDEX)))
        {
            long length = in.length();
            while (in.getFilePointer() != length)
            {
                ByteBuffer key = ByteBufferUtil.readWithShortLength(in);
                long position = RowIndexEntry.Serializer.readPosition(in);
                int promotedSize = in.readUnsignedVInt32();
                byte[] promoted = null;
                if (promotedSize > 0)
                {
                    promoted = new byte[promotedSize];
                    in.readFully(promoted);
                }
                records.add(new Rec(key, position, promoted));
            }
        }
        return records;
    }

    /**
     * The trunk idiom for building a verifier, cf. {@code VerifyTest.getVerifier}. {@code isOffline} is true
     * because a split child is not in the tracker: it keeps the verifier off the compaction rate limiter and
     * out of the local-ranges ownership check, which is what the fork's {@code new Verifier(cfs, r, true, ...)}
     * did.
     */
    private static IVerifier verifier(ColumnFamilyStore cfs, SSTableReader sstable, IVerifier.Options.Builder options)
    {
        return sstable.getVerifier(cfs, new OutputHandler.LogOutput(), true, options.build());
    }

    private static long crc32Of(File file) throws IOException
    {
        CRC32 crc = new CRC32();
        byte[] buffer = new byte[8192];
        try (FileInputStreamPlus in = file.newInputStream())
        {
            int n;
            while ((n = in.read(buffer)) > 0)
                crc.update(buffer, 0, n);
        }
        return crc.getValue();
    }

    private static String readDigest(Descriptor descriptor) throws IOException
    {
        byte[] bytes = Files.readAllBytes(descriptor.fileFor(Components.DIGEST).toPath());
        return new String(bytes, StandardCharsets.UTF_8).trim();
    }

    private static SSTableReader onlySSTable(ColumnFamilyStore cfs)
    {
        Set<SSTableReader> live = cfs.getLiveSSTables();
        assertEquals("expected exactly one sstable", 1, live.size());
        return live.iterator().next();
    }

    private static List<SSTableReader> readers(Result result)
    {
        List<SSTableReader> readers = new ArrayList<>(result.children.size());
        for (Child child : result.children)
            readers.add(child.reader);
        return readers;
    }

    private static long sumHeadPad(Result result)
    {
        long sum = 0;
        for (Child child : result.children)
            sum += child.headPadBytes;
        return sum;
    }

    private static Child firstChildWithDeadPrefix(Result result)
    {
        for (Child child : result.children)
        {
            if (child.deadPrefixBytes > 0)
                return child;
        }
        return null;
    }

    private static void release(Result result)
    {
        releaseExcept(result, null);
    }

    /**
     * A child handed to {@link LifecycleTransaction#offline} is owned by that transaction's dummy Tracker,
     * which releases it on close (LifecycleTransaction.java:143-149). Releasing it again here would throw
     * "Attempted to release a reference that has already been released" and mask the real assertions.
     */
    private static void releaseExcept(Result result, SSTableReader consumed)
    {
        for (Child child : result.children)
            if (child.reader != consumed)
                child.reader.selfRef().release();
    }

    private static ColumnFilter allColumns(ColumnFamilyStore cfs)
    {
        return ColumnFilter.all(cfs.metadata());
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
                execute("INSERT INTO %s (pk, ck, val) VALUES (?, ?, ?)", key(p), c, randomText(valueBytes));
    }

    private static String key(int p)
    {
        return String.format("k%06d", p);
    }

    /** Near-incompressible payload, so the sstable really does span many compression chunks. */
    private static String randomText(int length)
    {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        char[] chars = new char[length];
        for (int i = 0; i < length; i++)
            chars[i] = (char) ('!' + random.nextInt(94));
        return new String(chars);
    }

    private static String fixedText(int length)
    {
        char[] chars = new char[length];
        Arrays.fill(chars, 'v');
        return new String(chars);
    }
}
