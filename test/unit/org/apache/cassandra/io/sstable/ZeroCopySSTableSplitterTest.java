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

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Assume;
import org.junit.Test;
import org.mockito.ArgumentMatchers;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.CompactionPipelineCounts;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.streaming.CassandraOutgoingFile;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.ZeroCopySSTableSplitter.Child;
import org.apache.cassandra.io.sstable.ZeroCopySSTableSplitter.Result;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.format.big.BigTableReader;
import org.apache.cassandra.io.sstable.format.big.RowIndexEntry;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.OutgoingStream;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.io.sstable.SSTableReadsListener.NOOP_LISTENER;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/** Focused end-to-end coverage for the BIG-only first phase splitter. */
public class ZeroCopySSTableSplitterTest extends CQLTester
{
    @After
    public void resetHooks()
    {
        ZeroCopySSTableSplitter.forceAlignedLayoutForTesting = false;
        ZeroCopySSTableSplitter.failBeforeChildForTesting = null;
        ZeroCopySSTableSplitter.failAfterChildOpenForTesting = null;
        ZeroCopySSTableSplitter.availableFileDescriptorsForTesting = null;
    }

    @Test
    public void streamingByteShareSelectionMatchesReference()
    {
        Random random = new Random(0x5EED);
        for (int trial = 0; trial < 500; trial++)
        {
            int partitions = 1 + random.nextInt(100);
            long[] positions = new long[partitions];
            long position = random.nextInt(4096);
            for (int i = 0; i < partitions; i++)
            {
                positions[i] = position;
                position += 1 + random.nextInt(64 * 1024);
            }
            long uncompressedLength = position + random.nextInt(64 * 1024);

            for (int children = 1; children <= Math.min(partitions, 12); children++)
            {
                int[] expected = ZeroCopySSTableSplitter.chooseByByteShare(positions,
                                                                           uncompressedLength,
                                                                           children);
                ZeroCopySSTableSplitter.RunSelector selector =
                    new ZeroCopySSTableSplitter.RunSelector(uncompressedLength, children, partitions);
                for (int i = 0; i < partitions; i++)
                    selector.offer(i, positions[i]);

                ZeroCopySSTableSplitter.Runs actual = selector.finish(0);
                assertArrayEquals(expected, actual.runStarts);
                for (int i = 0; i < children; i++)
                    assertEquals(positions[expected[i]], actual.runPositions[i]);
            }
        }
    }

    @Test
    public void shiftedParentIndexIsRejectedBeforeChildCreation() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());

        createCompressedTable();
        disableCompaction();
        insertPartitions(60, 4, 480, new Random(9));
        flush();

        SSTableReader parent = onlySSTable(getCurrentColumnFamilyStore());
        File indexFile = parent.descriptor.fileFor(BigFormat.Components.PRIMARY_INDEX);
        byte[] originalIndex = Files.readAllBytes(indexFile.toPath());
        long secondRecord;
        long secondPosition;
        try (RandomAccessReader index = RandomAccessReader.open(indexFile))
        {
            ByteBufferUtil.readWithShortLength(index);
            RowIndexEntry.Serializer.readPosition(index);
            int promotedSize = index.readUnsignedVInt32();
            if (promotedSize > 0)
                index.skipBytesFully(promotedSize);
            secondRecord = index.getFilePointer();

            ByteBufferUtil.readWithShortLength(index);
            secondPosition = RowIndexEntry.Serializer.readPosition(index);
        }
        assertTrue(secondPosition > 0);
        assertTrue(secondPosition < parent.getCompressionMetadata().chunkLength());

        Set<String> before = fileNames(parent.descriptor.directory);
        AtomicBoolean childCreationStarted = new AtomicBoolean();
        ZeroCopySSTableSplitter.failBeforeChildForTesting = ignored -> {
            childCreationStarted.set(true);
            throw new AssertionError("parent authentication ran after child creation began");
        };
        try
        {
            Files.write(indexFile.toPath(),
                        Arrays.copyOfRange(originalIndex, Math.toIntExact(secondRecord), originalIndex.length),
                        StandardOpenOption.TRUNCATE_EXISTING,
                        StandardOpenOption.WRITE);

            try
            {
                ZeroCopySSTableSplitter.splitForTesting(parent, 3);
                fail("a shifted parent Index.db must be rejected");
            }
            catch (CorruptSSTableException expected)
            {
                assertNotNull(expected.getCause());
                assertTrue(expected.getCause().getMessage(),
                           expected.getCause().getMessage().contains("Statistics.db"));
            }
        }
        finally
        {
            ZeroCopySSTableSplitter.failBeforeChildForTesting = null;
            Files.write(indexFile.toPath(),
                        originalIndex,
                        StandardOpenOption.TRUNCATE_EXISTING,
                        StandardOpenOption.WRITE);
        }

        assertFalse(childCreationStarted.get());
        assertEquals(before, fileNames(parent.descriptor.directory));
        assertEquals(60, scan(parent));
    }

    @Test
    public void insufficientFileDescriptorBudgetIsRejectedBeforeChildCreation() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());

        createCompressedTable();
        disableCompaction();
        insertPartitions(60, 4, 480, new Random(10));
        flush();

        SSTableReader parent = onlySSTable(getCurrentColumnFamilyStore());
        Set<String> before = fileNames(parent.descriptor.directory);
        AtomicBoolean childCreationStarted = new AtomicBoolean();
        ZeroCopySSTableSplitter.availableFileDescriptorsForTesting = () -> 15L;
        ZeroCopySSTableSplitter.failBeforeChildForTesting = ignored -> {
            childCreationStarted.set(true);
            throw new AssertionError("file-descriptor preflight ran after child creation began");
        };
        try
        {
            try
            {
                ZeroCopySSTableSplitter.splitForTesting(parent, 4);
                fail("a split requiring 16 file descriptors must be refused when only 15 are available");
            }
            catch (IllegalStateException expected)
            {
                assertTrue(expected.getMessage(), expected.getMessage().contains("4 children"));
                assertTrue(expected.getMessage(), expected.getMessage().contains("16 additional"));
                assertTrue(expected.getMessage(), expected.getMessage().contains("15 are available"));
                assertTrue(expected.getMessage(), expected.getMessage().contains("--size"));
            }
        }
        finally
        {
            ZeroCopySSTableSplitter.availableFileDescriptorsForTesting = null;
            ZeroCopySSTableSplitter.failBeforeChildForTesting = null;
        }

        assertFalse(childCreationStarted.get());
        assertEquals(before, fileNames(parent.descriptor.directory));
        assertEquals(60, scan(parent));
    }

    @Test
    public void interiorParentIndexPositionMismatchIsRejectedBeforeChildCreation() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());

        createCompressedTable();
        disableCompaction();
        insertPartitions(60, 4, 480, new Random(11));
        flush();

        SSTableReader parent = onlySSTable(getCurrentColumnFamilyStore());
        File indexFile = parent.descriptor.fileFor(BigFormat.Components.PRIMARY_INDEX);
        byte[] originalIndex = Files.readAllBytes(indexFile.toPath());
        long interiorPositionOffset;
        long interiorPositionEnd;
        long interiorPosition;
        try (RandomAccessReader index = RandomAccessReader.open(indexFile))
        {
            ByteBufferUtil.readWithShortLength(index);
            RowIndexEntry.Serializer.readPosition(index);
            int promotedSize = index.readUnsignedVInt32();
            if (promotedSize > 0)
                index.skipBytesFully(promotedSize);

            ByteBufferUtil.readWithShortLength(index);
            interiorPositionOffset = index.getFilePointer();
            interiorPosition = RowIndexEntry.Serializer.readPosition(index);
            interiorPositionEnd = index.getFilePointer();
        }
        assertTrue(interiorPosition > 0);
        assertTrue(interiorPositionEnd > interiorPositionOffset);

        byte[] corruptedIndex = Arrays.copyOf(originalIndex, originalIndex.length);
        // Change only a payload bit in the final vint byte, preserving the record's encoded width and every endpoint.
        int corruptAt = Math.toIntExact(interiorPositionEnd - 1);
        corruptedIndex[corruptAt] ^= 1;

        Set<String> before = fileNames(parent.descriptor.directory);
        AtomicBoolean childCreationStarted = new AtomicBoolean();
        ZeroCopySSTableSplitter.failBeforeChildForTesting = ignored -> {
            childCreationStarted.set(true);
            throw new AssertionError("parent authentication ran after child creation began");
        };
        try
        {
            Files.write(indexFile.toPath(),
                        corruptedIndex,
                        StandardOpenOption.TRUNCATE_EXISTING,
                        StandardOpenOption.WRITE);

            try
            {
                ZeroCopySSTableSplitter.splitForTesting(parent, 3);
                fail("an interior Index.db position that points at the wrong Data.db bytes must be rejected");
            }
            catch (CorruptSSTableException expected)
            {
                assertTrue(expected.getMessage(), expected.getMessage().contains("Index.db"));
            }
        }
        finally
        {
            ZeroCopySSTableSplitter.failBeforeChildForTesting = null;
            Files.write(indexFile.toPath(),
                        originalIndex,
                        StandardOpenOption.TRUNCATE_EXISTING,
                        StandardOpenOption.WRITE);
        }

        assertFalse(childCreationStarted.get());
        assertEquals(before, fileNames(parent.descriptor.directory));
        assertEquals(60, scan(parent));
    }

    @Test
    public void alignedChildrenAreMarkedAndReadable() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());

        createCompressedTable();
        disableCompaction();
        insertPartitions(240, 5, 480, new Random(1));
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader parent = onlySSTable(cfs);
        assertEquals("pa", parent.descriptor.version.version);
        assertEquals(0, parent.getSSTableMetadata().firstPartitionPosition);
        assertTrue(((BigTableReader) parent).getKeyCache().isEnabled());

        ZeroCopySSTableSplitter.forceAlignedLayoutForTesting = true;
        Result result = ZeroCopySSTableSplitter.splitForTesting(parent, 4);
        ZeroCopySSTableSplitter.forceAlignedLayoutForTesting = false;
        try
        {
            assertEquals(4, result.children.size());
            assertEquals(0, result.children.get(0).headPadBytes);
            assertContentEquals(parent, result);

            int padded = 0;
            int partitions = 0;
            for (Child child : result.children)
            {
                assertEquals("pb", child.descriptor.version.version);
                assertEquals(child.deadPrefixBytes, child.reader.getSSTableMetadata().firstPartitionPosition);
                assertFalse("split children must be opened through the offline reader path",
                            ((BigTableReader) child.reader).getKeyCache().isEnabled());

                SSTableReader.PartitionPositionBounds fullRange = child.reader.getPositionsForFullRange();
                assertEquals(child.deadPrefixBytes, fullRange.lowerPosition);
                assertEquals(child.reader.uncompressedLength(), fullRange.upperPosition);
                assertEquals(child.headPadBytes, child.reader.getCompressionMetadata().chunkFor(0).offset);
                assertTrue(child.headPadBytes < 64 * 1024);
                if (child.headPadBytes > 0)
                    padded++;

                assertEquals(child.partitionCount, scan(child.reader));
                partitions += child.partitionCount;

                try (org.apache.cassandra.io.util.RandomAccessReader data = child.reader.openDataReader())
                {
                    data.seek(child.reader.uncompressedLength() - 1);
                    data.readByte();
                }

                SSTableReader reopened = SSTableReader.open(cfs, child.descriptor, child.components, cfs.metadata);
                try
                {
                    assertEquals(child.partitionCount, scan(reopened));
                    assertEquals(child.reader.getSSTableMetadata().firstPartitionPosition,
                                 reopened.getSSTableMetadata().firstPartitionPosition);
                }
                finally
                {
                    reopened.selfRef().release();
                }
            }
            assertTrue("forced alignment produced no padded child", padded > 0);
            assertEquals(240, partitions);
        }
        finally
        {
            release(result);
        }
    }

    @Test
    public void childrenPreserveCompletePartitionContent() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());

        createTable("CREATE TABLE %s (pk text, ck int, static_val text static, val text, " +
                    "PRIMARY KEY (pk, ck)) WITH compression = " +
                    "{'class': 'LZ4Compressor', 'chunk_length_in_kb': '4'}");
        disableCompaction();
        Random random = new Random(12);
        for (int partition = 0; partition < 100; partition++)
        {
            String key = String.format("k%06d", partition);
            execute("UPDATE %s USING TIMESTAMP ? SET static_val = ? WHERE pk = ?",
                    1_000_000L + partition, "static-" + partition, key);
            for (int clustering = 0; clustering < 8; clustering++)
            {
                execute("INSERT INTO %s (pk, ck, val) VALUES (?, ?, ?) USING TIMESTAMP ? AND TTL ?",
                        key,
                        clustering,
                        randomText(600, random),
                        2_000_000L + partition * 10L + clustering,
                        3600);
            }
            execute("DELETE val FROM %s USING TIMESTAMP ? WHERE pk = ? AND ck = ?",
                    3_000_000L + partition, key, 1);
            execute("DELETE FROM %s USING TIMESTAMP ? WHERE pk = ? AND ck >= ? AND ck < ?",
                    4_000_000L + partition, key, 3, 5);
        }
        flush();

        SSTableReader parent = onlySSTable(getCurrentColumnFamilyStore());
        Result result = ZeroCopySSTableSplitter.splitForTesting(parent, 4);
        try
        {
            assertContentEquals(parent, result);
        }
        finally
        {
            release(result);
        }
    }

    @Test
    public void promotedRowIndexesSurviveTheRebase() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());

        int previousCacheSize = DatabaseDescriptor.getColumnIndexCacheSizeInKiB();
        DatabaseDescriptor.setColumnIndexCacheSize(0);
        try
        {
            createCompressedTable();
            disableCompaction();
            insertPartitions(12, 40, 1000, new Random(2));
            flush();

            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            SSTableReader parent = onlySSTable(cfs);
            RowIndexEntry parentEntry = ((BigTableReader) parent).getRowIndexEntry(parent.getFirst(),
                                                                                   SSTableReader.Operator.EQ,
                                                                                   false,
                                                                                   NOOP_LISTENER);
            assertNotNull(parentEntry);
            assertTrue("fixture did not create a promoted row index", parentEntry.isIndexed());

            Result result = ZeroCopySSTableSplitter.splitForTesting(parent, 3);
            try
            {
                int partitions = 0;
                for (Child child : result.children)
                {
                    RowIndexEntry childEntry = ((BigTableReader) child.reader).getRowIndexEntry(child.first,
                                                                                                SSTableReader.Operator.EQ,
                                                                                                false,
                                                                                                NOOP_LISTENER);
                    assertNotNull(childEntry);
                    assertTrue("child lost the promoted row index for " + child.first, childEntry.isIndexed());
                    Slices slices = Slices.with(cfs.getComparator(),
                                                Slice.make(Clustering.make(ByteBufferUtil.bytes(10)),
                                                           Clustering.make(ByteBufferUtil.bytes(29))));
                    try (UnfilteredRowIterator expected = ((BigTableReader) parent).rowIterator(child.first,
                                                                                                slices,
                                                                                                ColumnFilter.all(cfs.metadata()),
                                                                                                false,
                                                                                                NOOP_LISTENER);
                         UnfilteredRowIterator actual = ((BigTableReader) child.reader).rowIterator(child.first,
                                                                                                   slices,
                                                                                                   ColumnFilter.all(cfs.metadata()),
                                                                                                   false,
                                                                                                   NOOP_LISTENER))
                    {
                        assertTrue("rebased promoted index returned different rows for " + child.first,
                                   Util.sameContent(expected, actual));
                    }
                    partitions += scan(child.reader);
                }
                assertEquals(12, partitions);
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

    @Test
    public void deadPrefixChildStreamsAsAnEntireSSTable() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());

        createCompressedTable();
        disableCompaction();
        insertPartitions(120, 5, 480, new Random(6));
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader parent = onlySSTable(cfs);
        Result result = ZeroCopySSTableSplitter.splitForTesting(parent, 3);
        boolean previousStreamEntireSSTables = DatabaseDescriptor.setStreamEntireSSTables(true);
        try
        {
            Child source = result.children.stream()
                                          .filter(child -> child.deadPrefixBytes > 0)
                                          .findFirst()
                                          .orElseThrow(() -> new AssertionError("no child has a retained prefix"));
            Token minimum = cfs.getPartitioner().getMinimumToken();
            List<Range<Token>> ranges = Range.normalize(Collections.singletonList(new Range<>(minimum, minimum)));
            List<SSTableReader.PartitionPositionBounds> sections = source.reader.getPositionsForRanges(ranges);
            CassandraOutgoingFile outgoing = new CassandraOutgoingFile(StreamOperation.OTHER,
                                                                        source.reader.ref(),
                                                                        sections,
                                                                        ranges,
                                                                        source.reader.estimatedKeysForRanges(ranges),
                                                                        new CassandraVersion("7.0"));
            boolean ownedByPlan = false;
            try
            {
                assertTrue(outgoing.computeShouldStreamEntireSSTables());

                cfs.clearUnsafe();
                MessagingService.instance().waitUntilListeningUnchecked();
                StorageService.instance.initServer();
                List<OutgoingStream> streams = Collections.singletonList(outgoing);
                StreamPlan plan = new StreamPlan(StreamOperation.OTHER)
                                  .transferStreams(FBUtilities.getBroadcastAddressAndPort(), streams);
                ownedByPlan = true;
                plan.execute().get();
            }
            finally
            {
                if (!ownedByPlan)
                    outgoing.finish();
            }

            SSTableReader received = onlySSTable(cfs);
            assertEquals("pb", received.descriptor.version.version);
            assertTrue(received.hasSplitPrefix());
            assertEquals(source.partitionCount, scan(received));
        }
        finally
        {
            DatabaseDescriptor.setStreamEntireSSTables(previousStreamEntireSSTables);
            release(result);
        }
    }

    @Test
    public void sizeBasedSplitUsesCompressedChunkSpansAsAMaximum() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());

        createCompressedTable();
        disableCompaction();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 96; i++)
            keys.add(String.format("k%06d", i));
        keys.sort(Comparator.comparing(key -> cfs.getPartitioner().decorateKey(ByteBufferUtil.bytes(key))));

        Random random = new Random(3);
        for (int i = 0; i < keys.size(); i++)
        {
            // Compression ratio changes sharply in on-disk token order; equal uncompressed shares cannot enforce a
            // compressed maximum for this fixture.
            String value = i < 72 ? fixedText(24 * 1024) : randomText(24 * 1024, random);
            execute("INSERT INTO %s (pk, ck, val) VALUES (?, ?, ?)", keys.get(i), 0, value);
        }
        flush();

        SSTableReader parent = onlySSTable(cfs);
        long maximum = 96 * 1024;
        assertTrue(parent.descriptor.fileFor(Components.DATA).length() > maximum);

        try (LifecycleTransaction transaction = LifecycleTransaction.offline(OperationType.UNKNOWN, parent))
        {
            Result result;
            List<Long> diskReservations = new ArrayList<>();
            ZeroCopySSTableSplitter.forceAlignedLayoutForTesting = true;
            ZeroCopySSTableSplitter.failBeforeChildForTesting = built -> {
                long reserved = CompactionManager.instance.active.estimatedRemainingWriteToDiskBytes()
                                                          .values()
                                                          .stream()
                                                          .mapToLong(Long::longValue)
                                                          .sum();
                diskReservations.add(reserved);
            };
            try
            {
                result = ZeroCopySSTableSplitter.splitBySize(parent, maximum, transaction);
            }
            finally
            {
                ZeroCopySSTableSplitter.forceAlignedLayoutForTesting = false;
                ZeroCopySSTableSplitter.failBeforeChildForTesting = null;
            }
            try
            {
                assertTrue(result.children.size() > 2);
                int partitions = 0;
                long[] childBytes = new long[result.children.size()];
                int childIndex = 0;
                for (Child child : result.children)
                {
                    long length = child.descriptor.fileFor(Components.DATA).length();
                    assertTrue(child.descriptor + " exceeds the compressed maximum: " + length, length <= maximum);
                    partitions += scan(child.reader);
                    childBytes[childIndex++] = child.reader.bytesOnDisk();
                }
                assertEquals(keys.size(), partitions);
                assertEquals(result.children.size(), diskReservations.size());

                long remainingBytes = 0;
                for (int i = childBytes.length - 1; i >= 0; i--)
                {
                    remainingBytes += childBytes[i];
                    assertTrue("reservation before child " + i + " was " + diskReservations.get(i) +
                               " bytes, below the " + remainingBytes + " bytes still written",
                               diskReservations.get(i) >= remainingBytes);
                }
            }
            finally
            {
                release(result);
            }
        }
    }

    @Test
    public void lifecycleTracksBeforeFilesAndPublishedChildrenCompact() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());

        createCompressedTable();
        disableCompaction();
        insertPartitions(60, 6, 480, new Random(4));
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader parent = onlySSTable(cfs);
        LifecycleTransaction real = cfs.getTracker().tryModify(parent, OperationType.COMPACTION);
        assertNotNull(real);
        LifecycleTransaction transaction = spy(real);
        AtomicInteger tracked = new AtomicInteger();
        doAnswer(invocation -> {
            SSTable pending = invocation.getArgument(0);
            for (Component component : pending.getComponents())
            {
                assertFalse("component existed before trackNew: " + component,
                            pending.descriptor.fileFor(component).exists());
            }
            tracked.incrementAndGet();
            return invocation.callRealMethod();
        }).when(transaction).trackNew(ArgumentMatchers.any(SSTable.class));

        boolean committed = false;
        try
        {
            Result result = ZeroCopySSTableSplitter.splitForTesting(parent, 3, transaction);
            assertEquals(result.children.size(), tracked.get());
            verify(transaction, times(result.children.size())).trackNew(ArgumentMatchers.any(SSTable.class));

            for (Child child : result.children)
                transaction.update(child.reader, false);
            transaction.obsoleteOriginals();
            transaction.prepareToCommit();
            transaction.commit();
            committed = true;
        }
        finally
        {
            if (!committed)
                transaction.abort();
        }

        boolean cursorCompactionEnabled = DatabaseDescriptor.cursorCompactionEnabled();
        DatabaseDescriptor.setCursorCompactionEnabled(true);
        try
        {
            CompactionPipelineCounts pipelines = CompactionPipelineCounts.mark();
            cfs.forceMajorCompaction();
            CompactionPipelineCounts.assertPipelineRan(true, pipelines);

            assertEquals(60, scanLive(cfs));
            for (SSTableReader compacted : cfs.getLiveSSTables())
            {
                assertEquals("pa", compacted.descriptor.version.version);
                assertFalse("a rewrite must reclaim the retained prefix", compacted.hasSplitPrefix());
                assertEquals(0, compacted.getPositionsForFullRange().lowerPosition);
            }
        }
        finally
        {
            DatabaseDescriptor.setCursorCompactionEnabled(cursorCompactionEnabled);
        }
    }

    @Test
    public void corruptAuthenticatedCursorStartMarksSSTableSuspect() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());

        createCompressedTable();
        disableCompaction();
        insertPartitions(60, 6, 480, new Random(7));
        flush();

        SSTableReader parent = onlySSTable(getCurrentColumnFamilyStore());
        Result result = ZeroCopySSTableSplitter.splitForTesting(parent, 3);
        File corruptData = FileUtils.createTempFile("split-cursor-corruption", ".db");
        try
        {
            Child child = result.children.stream()
                                         .filter(candidate -> candidate.deadPrefixBytes > 0)
                                         .findFirst()
                                         .orElseThrow(() -> new AssertionError("no child has a retained prefix"));

            // The cursor only needs the byte immediately preceding its authenticated first partition for this check.
            // A sparse zero-filled file makes that byte a non-END_OF_PARTITION flag without corrupting compression.
            try (FileChannel channel = corruptData.newWriteChannel(File.WriteMode.OVERWRITE))
            {
                channel.position(child.reader.uncompressedLength() - 1);
                channel.write(ByteBuffer.wrap(new byte[1]));
            }

            SSTableReader reader = spy(child.reader);
            doAnswer(ignored -> RandomAccessReader.open(corruptData))
            .when(reader).openDataReaderForScan(ArgumentMatchers.isNull());
            try
            {
                new SSTableCursorReader(reader);
                fail("an invalid boundary at an authenticated cursor start must be reported as sstable corruption");
            }
            catch (CorruptSSTableException expected)
            {
            }
            assertTrue(reader.isMarkedSuspect());
        }
        finally
        {
            corruptData.deleteIfExists();
            release(result);
        }
    }

    @Test
    public void emptyMovedStartCursorStaysAtPhysicalEnd() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());

        createCompressedTable();
        disableCompaction();
        insertPartitions(12, 2, 128, new Random(8));
        flush();

        // cloneWithNewStart validates first <= last, while the null full-range state is reached transiently as a
        // rewriter consumes the reader. Model that public reader contract directly.
        SSTableReader movedStart = spy(onlySSTable(getCurrentColumnFamilyStore()));
        doAnswer(ignored -> null).when(movedStart).getPositionsForFullRange();
        try (SSTableCursorReader cursor = new SSTableCursorReader(movedStart))
        {
            assertEquals(SSTableCursorReader.State.DONE, cursor.state());
            assertEquals(movedStart.uncompressedLength(), cursor.position());
            try
            {
                cursor.seekPartition(0);
                fail("an empty MOVED_START cursor must not seek into the hidden physical file");
            }
            catch (IllegalArgumentException expected)
            {
            }
        }
    }

    @Test
    public void failureAfterTrackingCurrentChildDeletesAndUntracksEverything() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());

        createCompressedTable();
        disableCompaction();
        insertPartitions(60, 4, 480, new Random(5));
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader parent = onlySSTable(cfs);
        Set<String> before = fileNames(parent.descriptor.directory);

        LifecycleTransaction real = cfs.getTracker().tryModify(parent, OperationType.COMPACTION);
        assertNotNull(real);
        LifecycleTransaction transaction = spy(real);
        AtomicBoolean committed = new AtomicBoolean();
        try
        {
            ZeroCopySSTableSplitter.failBeforeChildForTesting = built -> {
                if (built == 1)
                    throw new IllegalStateException("injected after trackNew");
            };

            try
            {
                ZeroCopySSTableSplitter.splitForTesting(parent, 4, transaction);
                fail("injected failure was not raised");
            }
            catch (IllegalStateException expected)
            {
                assertTrue(expected.getMessage(), expected.getMessage().contains("injected after trackNew"));
            }
            finally
            {
                ZeroCopySSTableSplitter.failBeforeChildForTesting = null;
            }

            verify(transaction, times(2)).trackNew(ArgumentMatchers.any(SSTable.class));
            verify(transaction, times(2)).untrackNew(ArgumentMatchers.any(SSTable.class));

            // A caller can safely reuse the transaction for its rewrite fallback; committing it must not preserve a
            // stale ADD record for either the complete first child or the incomplete current child.
            transaction.prepareToCommit();
            transaction.commit();
            committed.set(true);
        }
        finally
        {
            if (!committed.get())
                transaction.abort();
        }

        LifecycleTransaction.waitForDeletions();
        assertEquals(before, fileNames(parent.descriptor.directory));
        assertEquals(parent, onlySSTable(cfs));
        assertEquals(60, scan(parent));
    }

    @Test
    public void failureAfterChildOpenReleasesReader() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());

        createCompressedTable();
        disableCompaction();
        insertPartitions(60, 4, 480, new Random(11));
        flush();

        SSTableReader parent = onlySSTable(getCurrentColumnFamilyStore());
        AtomicReference<SSTableReader> opened = new AtomicReference<>();
        ZeroCopySSTableSplitter.failAfterChildOpenForTesting = reader -> {
            opened.set(reader);
            throw new IllegalStateException("injected after child open");
        };
        try
        {
            ZeroCopySSTableSplitter.splitForTesting(parent, 3);
            fail("injected failure was not raised");
        }
        catch (IllegalStateException expected)
        {
            assertTrue(expected.getMessage(), expected.getMessage().contains("injected after child open"));
        }
        finally
        {
            ZeroCopySSTableSplitter.failAfterChildOpenForTesting = null;
        }

        assertNotNull(opened.get());
        assertEquals(0, opened.get().selfRef().globalCount());
    }

    private String createCompressedTable() throws Throwable
    {
        return createTable("CREATE TABLE %s (pk text, ck int, val text, PRIMARY KEY (pk, ck)) " +
                           "WITH compression = {'class': 'LZ4Compressor', 'chunk_length_in_kb': '4'}");
    }

    private void insertPartitions(int partitions, int rowsPerPartition, int valueBytes, Random random) throws Throwable
    {
        for (int partition = 0; partition < partitions; partition++)
        {
            String value = randomText(valueBytes, random);
            for (int clustering = 0; clustering < rowsPerPartition; clustering++)
            {
                execute("INSERT INTO %s (pk, ck, val) VALUES (?, ?, ?)",
                        String.format("k%06d", partition), clustering, value);
            }
        }
    }

    private static String randomText(int length, Random random)
    {
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

    private static SSTableReader onlySSTable(ColumnFamilyStore cfs)
    {
        assertEquals(1, cfs.getLiveSSTables().size());
        return cfs.getLiveSSTables().iterator().next();
    }

    private static int scanLive(ColumnFamilyStore cfs)
    {
        int partitions = 0;
        for (SSTableReader reader : cfs.getLiveSSTables())
            partitions += scan(reader);
        return partitions;
    }

    private static int scan(SSTableReader reader)
    {
        int partitions = 0;
        try (ISSTableScanner scanner = reader.getScanner())
        {
            while (scanner.hasNext())
            {
                try (UnfilteredRowIterator ignored = scanner.next())
                {
                    partitions++;
                }
            }
        }
        return partitions;
    }

    private static void assertContentEquals(SSTableReader parent, Result result)
    {
        try (ISSTableScanner parentScanner = parent.getScanner())
        {
            for (Child child : result.children)
            {
                try (ISSTableScanner childScanner = child.reader.getScanner())
                {
                    while (childScanner.hasNext())
                    {
                        assertTrue("children contain a partition absent from the parent", parentScanner.hasNext());
                        try (UnfilteredRowIterator expected = parentScanner.next();
                             UnfilteredRowIterator actual = childScanner.next())
                        {
                            assertTrue("partition content differs for " + expected.partitionKey(),
                                       Util.sameContent(expected, actual));
                        }
                    }
                }
            }
            assertFalse("children omitted parent partitions", parentScanner.hasNext());
        }
    }

    private static Set<String> fileNames(File directory)
    {
        return Arrays.stream(directory.tryList()).map(File::name).collect(Collectors.toSet());
    }

    private static void release(Result result)
    {
        for (Child child : result.children)
            child.reader.selfRef().release();
    }
}
