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
package org.apache.cassandra.io.sstable.format.bti;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Lists;
import com.google.common.collect.Multiset;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.Util;
import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.io.tries.TrieNode;
import org.apache.cassandra.io.tries.Walker;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.PageAware;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.cassandra.io.util.WrappingRebufferer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class PartitionIndexTest
{
    private final static Logger logger = LoggerFactory.getLogger(PartitionIndexTest.class);

    private final static long SEED = System.nanoTime();
    private final static Random random = new Random(SEED);

    static final ByteComparable.Version VERSION = Walker.BYTE_COMPARABLE_VERSION;

    static
    {
        DatabaseDescriptor.daemonInitialization();
    }

    static final IPartitioner partitioner = Util.testPartitioner();
    //Lower the size of the indexes when running without the chunk cache, otherwise the test times out on Jenkins
    static final int COUNT = ChunkCache.instance != null ? 245256 : 24525;

    @Parameterized.Parameters()
    public static Collection<Object[]> generateData()
    {
        return Arrays.asList(new Object[]{ Config.DiskAccessMode.standard },
                             new Object[]{ Config.DiskAccessMode.mmap });
    }

    @Parameterized.Parameter(value = 0)
    public static Config.DiskAccessMode accessMode = Config.DiskAccessMode.standard;

    @BeforeClass
    public static void beforeClass()
    {
        logger.info("Using random seed: {}", SEED);
    }

    /**
     * Tests last-nodes-sizing failure uncovered during code review.
     */
    @Test
    public void testSizingBug() throws IOException
    {
        for (int i = 1; i < COUNT; i *= 10)
        {
            testGetEq(generateRandomIndex(i));
            testGetEq(generateSequentialIndex(i));
        }
    }

    @Test
    public void testGetEq() throws IOException
    {
        testGetEq(generateRandomIndex(COUNT));
        testGetEq(generateSequentialIndex(COUNT));
    }

    @Test
    public void testBrokenFile() throws IOException
    {
        // put some garbage in the file
        final Pair<List<DecoratedKey>, PartitionIndex> data = generateRandomIndex(COUNT);
        File f = new File(data.right.getFileHandle().path());
        try (FileChannel ch = FileChannel.open(f.toPath(), StandardOpenOption.WRITE))
        {
            ch.write(generateRandomKey().getKey(), f.length() * 2 / 3);
        }

        assertThatThrownBy(() -> testGetEq(data)).isInstanceOfAny(AssertionError.class, IndexOutOfBoundsException.class, IllegalArgumentException.class);
    }

    @Test
    public void testLongKeys() throws IOException
    {
        testGetEq(generateLongKeysIndex(COUNT / 10));
    }

    void testGetEq(Pair<List<DecoratedKey>, PartitionIndex> data)
    {
        List<DecoratedKey> keys = data.left;
        try (PartitionIndex summary = data.right;
             PartitionIndex.Reader reader = summary.openReader())
        {
            for (int i = 0; i < data.left.size(); i++)
            {
                assertEquals(i, reader.exactCandidate(keys.get(i)));
                DecoratedKey key = generateRandomKey();
                assertEquals(eq(keys, key), eq(keys, key, reader.exactCandidate(key)));
            }
        }
    }

    @Test
    public void testGetGt() throws IOException
    {
        testGetGt(generateRandomIndex(COUNT));
        testGetGt(generateSequentialIndex(COUNT));
    }

    private void testGetGt(Pair<List<DecoratedKey>, PartitionIndex> data) throws IOException
    {
        List<DecoratedKey> keys = data.left;
        try (PartitionIndex summary = data.right;
             PartitionIndex.Reader reader = summary.openReader())
        {
            for (int i = 0; i < data.left.size(); i++)
            {
                assertEquals(i < data.left.size() - 1 ? i + 1 : -1, gt(keys, keys.get(i), reader));
                DecoratedKey key = generateRandomKey();
                assertEquals(gt(keys, key), gt(keys, key, reader));
            }
        }
    }

    @Test
    public void testGetGe() throws IOException
    {
        testGetGe(generateRandomIndex(COUNT));
        testGetGe(generateSequentialIndex(COUNT));
    }

    public void testGetGe(Pair<List<DecoratedKey>, PartitionIndex> data) throws IOException
    {
        List<DecoratedKey> keys = data.left;
        try (PartitionIndex summary = data.right;
             PartitionIndex.Reader reader = summary.openReader())
        {
            for (int i = 0; i < data.left.size(); i++)
            {
                assertEquals(i, ge(keys, keys.get(i), reader));
                DecoratedKey key = generateRandomKey();
                assertEquals(ge(keys, key), ge(keys, key, reader));
            }
        }
    }


    @Test
    public void testGetLt() throws IOException
    {
        testGetLt(generateRandomIndex(COUNT));
        testGetLt(generateSequentialIndex(COUNT));
    }

    public void testGetLt(Pair<List<DecoratedKey>, PartitionIndex> data) throws IOException
    {
        List<DecoratedKey> keys = data.left;
        try (PartitionIndex summary = data.right;
             PartitionIndex.Reader reader = summary.openReader())
        {
            for (int i = 0; i < data.left.size(); i++)
            {
                assertEquals(i - 1, lt(keys, keys.get(i), reader));
                DecoratedKey key = generateRandomKey();
                assertEquals(lt(keys, key), lt(keys, key, reader));
            }
        }
    }

    private long gt(List<DecoratedKey> keys, DecoratedKey key, PartitionIndex.Reader summary) throws IOException
    {
        return Optional.ofNullable(summary.ceiling(key, (pos, assumeNoMatch, sk) -> (assumeNoMatch || keys.get((int) pos).compareTo(sk) > 0) ? pos : null)).orElse(-1L);
    }

    private long ge(List<DecoratedKey> keys, DecoratedKey key, PartitionIndex.Reader summary) throws IOException
    {
        return Optional.ofNullable(summary.ceiling(key, (pos, assumeNoMatch, sk) -> (assumeNoMatch || keys.get((int) pos).compareTo(sk) >= 0) ? pos : null)).orElse(-1L);
    }


    private long lt(List<DecoratedKey> keys, DecoratedKey key, PartitionIndex.Reader summary) throws IOException
    {
        return Optional.ofNullable(summary.floor(key, (pos, assumeNoMatch, sk) -> (assumeNoMatch || keys.get((int) pos).compareTo(sk) < 0) ? pos : null)).orElse(-1L);
    }

    private long eq(List<DecoratedKey> keys, DecoratedKey key, long exactCandidate)
    {
        int idx = (int) exactCandidate;
        if (exactCandidate == PartitionIndex.NOT_FOUND)
            return -1;
        return (keys.get(idx).equals(key)) ? idx : -1;
    }

    private long gt(List<DecoratedKey> keys, DecoratedKey key)
    {
        int index = Collections.binarySearch(keys, key);
        if (index < 0)
            index = -1 - index;
        else
            ++index;
        return index < keys.size() ? index : -1;
    }

    private long lt(List<DecoratedKey> keys, DecoratedKey key)
    {
        int index = Collections.binarySearch(keys, key);

        if (index < 0)
            index = -index - 2;

        return index >= 0 ? index : -1;
    }

    private long ge(List<DecoratedKey> keys, DecoratedKey key)
    {
        int index = Collections.binarySearch(keys, key);
        if (index < 0)
            index = -1 - index;
        return index < keys.size() ? index : -1;
    }

    private long eq(List<DecoratedKey> keys, DecoratedKey key)
    {
        int index = Collections.binarySearch(keys, key);
        return index >= 0 ? index : -1;
    }

    @Test
    public void testAddEmptyKey() throws Exception
    {
        IPartitioner p = new RandomPartitioner();
        File file = FileUtils.createTempFile("ColumnTrieReaderTest", "");

        FileHandle.Builder fhBuilder = makeHandle(file);
        try (SequentialWriter writer = makeWriter(file);
             PartitionIndexBuilder builder = new PartitionIndexBuilder(writer, fhBuilder)
        )
        {
            DecoratedKey key = p.decorateKey(ByteBufferUtil.EMPTY_BYTE_BUFFER);
            builder.addEntry(key, 42);
            builder.complete();
            try (PartitionIndex summary = loadPartitionIndex(fhBuilder, writer);
                 PartitionIndex.Reader reader = summary.openReader())
            {
                assertEquals(1, summary.size());
                assertEquals(42, reader.getLastIndexPosition());
                assertEquals(42, reader.exactCandidate(key));
            }
        }
    }

    @Test
    public void testIteration() throws IOException
    {
        Pair<List<DecoratedKey>, PartitionIndex> random = generateRandomIndex(COUNT);
        checkIteration(random.left.size(), random.right);
        random.right.close();
    }

    public void checkIteration(int keysSize, PartitionIndex index)
    {
        try (PartitionIndex enforceIndexClosing = index;
             PartitionIndex.IndexPosIterator iter = index.allKeysIterator())
        {
            int i = 0;
            while (true)
            {
                long pos = iter.nextIndexPos();
                if (pos == PartitionIndex.NOT_FOUND)
                    break;
                assertEquals(i, pos);
                ++i;
            }
            assertEquals(keysSize, i);
        }
    }

    @Test
    public void testConstrainedIteration() throws IOException
    {
        Pair<List<DecoratedKey>, PartitionIndex> random = generateRandomIndex(COUNT);
        try (PartitionIndex summary = random.right)
        {
            List<DecoratedKey> keys = random.left;
            Random rand = new Random();

            for (int i = 0; i < 1000; ++i)
            {
                boolean exactLeft = rand.nextBoolean();
                boolean exactRight = rand.nextBoolean();
                DecoratedKey left = exactLeft ? keys.get(rand.nextInt(keys.size())) : generateRandomKey();
                DecoratedKey right = exactRight ? keys.get(rand.nextInt(keys.size())) : generateRandomKey();
                if (right.compareTo(left) < 0)
                {
                    DecoratedKey t = left;
                    left = right;
                    right = t;
                    boolean b = exactLeft;
                    exactLeft = exactRight;
                    exactRight = b;
                }

                try (PartitionIndex.IndexPosIterator iter = new PartitionIndex.IndexPosIterator(summary, left, right))
                {
                    long p = iter.nextIndexPos();
                    if (p == PartitionIndex.NOT_FOUND)
                    {
                        int idx = (int) ge(keys, left); // first greater key
                        if (idx == -1)
                            continue;
                        assertTrue(left + " <= " + keys.get(idx) + " <= " + right + " but " + idx + " wasn't iterated.", right.compareTo(keys.get(idx)) < 0);
                        continue;
                    }

                    int idx = (int) p;
                    if (p > 0)
                        assertTrue(left.compareTo(keys.get(idx - 1)) > 0);
                    if (p < keys.size() - 1)
                        assertTrue(left.compareTo(keys.get(idx + 1)) < 0);
                    if (exactLeft)      // must be precise on exact, otherwise could be in any relation
                        assertSame(left, keys.get(idx));
                    while (true)
                    {
                        ++idx;
                        long pos = iter.nextIndexPos();
                        if (pos == PartitionIndex.NOT_FOUND)
                            break;
                        assertEquals(idx, pos);
                    }
                    --idx; // seek at last returned
                    if (idx < keys.size() - 1)
                        assertTrue(right.compareTo(keys.get(idx + 1)) < 0);
                    if (idx > 0)
                        assertTrue(right.compareTo(keys.get(idx - 1)) > 0);
                    if (exactRight)      // must be precise on exact, otherwise could be in any relation
                        assertSame(right, keys.get(idx));
                }
                catch (AssertionError e)
                {
                    StringBuilder buf = new StringBuilder();
                    buf.append(String.format("Left %s%s Right %s%s%n", left.byteComparableAsString(VERSION), exactLeft ? "#" : "", right.byteComparableAsString(VERSION), exactRight ? "#" : ""));
                    try (PartitionIndex.IndexPosIterator iter2 = new PartitionIndex.IndexPosIterator(summary, left, right))
                    {
                        long pos;
                        while ((pos = iter2.nextIndexPos()) != PartitionIndex.NOT_FOUND)
                            buf.append(keys.get((int) pos).byteComparableAsString(VERSION)).append("\n");
                        buf.append(String.format("Left %s%s Right %s%s%n", left.byteComparableAsString(VERSION), exactLeft ? "#" : "", right.byteComparableAsString(VERSION), exactRight ? "#" : ""));
                    }
                    logger.error(buf.toString(), e);
                    throw e;
                }
            }
        }
    }

    @Test
    public void testPartialIndex() throws IOException
    {
        for (int reps = 0; reps < 10; ++reps)
        {
            File file = FileUtils.createTempFile("ColumnTrieReaderTest", "");
            List<DecoratedKey> list = Lists.newArrayList();
            int parts = 15;
            FileHandle.Builder fhBuilder = makeHandle(file);
            try (SequentialWriter writer = makeWriter(file);
                 PartitionIndexBuilder builder = new PartitionIndexBuilder(writer, fhBuilder)
            )
            {
                writer.setPostFlushListener(builder::markPartitionIndexSynced);
                for (int i = 0; i < COUNT; i++)
                {
                    DecoratedKey key = generateRandomLengthKey();
                    list.add(key);
                }
                Collections.sort(list);
                AtomicInteger callCount = new AtomicInteger();

                int i = 0;
                for (int part = 1; part <= parts; ++part)
                {
                    for (; i < COUNT * part / parts; i++)
                        builder.addEntry(list.get(i), i);

                    final long addedSize = i;
                    builder.buildPartial(index ->
                                         {
                                             int indexSize = Collections.binarySearch(list, index.lastKey()) + 1;
                                             assert indexSize >= addedSize - 1;
                                             checkIteration(indexSize, index);
                                             callCount.incrementAndGet();
                                         }, 0, i * 1024L);
                    builder.markDataSynced(i * 1024L);
                    // verifier will be called when the sequentialWriter finishes a chunk
                }

                for (; i < COUNT; ++i)
                    builder.addEntry(list.get(i), i);
                builder.complete();
                try (PartitionIndex index = loadPartitionIndex(fhBuilder, writer))
                {
                    checkIteration(list.size(), index);
                }
                if (COUNT / parts > 16000)
                {
                    assertTrue(String.format("Expected %d or %d calls, got %d", parts, parts - 1, callCount.get()),
                               callCount.get() == parts - 1 || callCount.get() == parts);
                }
            }
        }
    }

    @Test
    public void testDeepRecursion()
    {
        CompletableFuture<Void> future = new CompletableFuture<>();
        // Check that long repeated strings don't cause stack overflow
        // Test both normal completion and partial construction.
        Thread t = new Thread(null, () ->
        {
            try
            {
                File file = FileUtils.createTempFile("ColumnTrieReaderTest", "");
                SequentialWriter writer = new SequentialWriter(file, SequentialWriterOption.newBuilder().finishOnClose(true).build());
                List<DecoratedKey> list = Lists.newArrayList();
                String longString = "";
                for (int i = 0; i < PageAware.PAGE_SIZE + 99; ++i)
                {
                    longString += i;
                }
                IPartitioner partitioner = ByteOrderedPartitioner.instance;
                list.add(partitioner.decorateKey(ByteBufferUtil.bytes(longString + "A")));
                list.add(partitioner.decorateKey(ByteBufferUtil.bytes(longString + "B")));
                list.add(partitioner.decorateKey(ByteBufferUtil.bytes(longString + "C")));
                list.add(partitioner.decorateKey(ByteBufferUtil.bytes(longString + "D")));
                list.add(partitioner.decorateKey(ByteBufferUtil.bytes(longString + "E")));

                FileHandle.Builder fhBuilder = new FileHandle.Builder(file)
                                               .bufferSize(PageAware.PAGE_SIZE)
                                               .withChunkCache(ChunkCache.instance);
                try (PartitionIndexBuilder builder = new PartitionIndexBuilder(writer, fhBuilder))
                {
                    int i = 0;
                    for (i = 0; i < 3; ++i)
                        builder.addEntry(list.get(i), i);

                    writer.setPostFlushListener(builder::markPartitionIndexSynced);
                    AtomicInteger callCount = new AtomicInteger();

                    final int addedSize = i;
                    builder.buildPartial(index ->
                                         {
                                             int indexSize = Collections.binarySearch(list, index.lastKey()) + 1;
                                             assert indexSize >= addedSize - 1;
                                             checkIteration(indexSize, index);
                                             index.close();
                                             callCount.incrementAndGet();
                                         }, 0, i * 1024L);

                    for (; i < list.size(); ++i)
                        builder.addEntry(list.get(i), i);
                    builder.complete();

                    try (PartitionIndex index = PartitionIndex.load(fhBuilder, partitioner, true))
                    {
                        checkIteration(list.size(), index);
                    }
                }
                future.complete(null);
            }
            catch (Throwable err)
            {
                future.completeExceptionally(err);
            }
        }, "testThread", 32 * 1024);

        t.start();
        future.join();
    }

    class JumpingFile extends SequentialWriter
    {
        long[] cutoffs;
        long[] offsets;

        JumpingFile(File file, SequentialWriterOption option, long... cutoffsAndOffsets)
        {
            super(file, option);
            assert (cutoffsAndOffsets.length & 1) == 0;
            cutoffs = new long[cutoffsAndOffsets.length / 2];
            offsets = new long[cutoffs.length];
            for (int i = 0; i < cutoffs.length; ++i)
            {
                cutoffs[i] = cutoffsAndOffsets[i * 2];
                offsets[i] = cutoffsAndOffsets[i * 2 + 1];
            }
        }

        @Override
        public long position()
        {
            return jumped(super.position(), cutoffs, offsets);
        }
    }

    class JumpingRebufferer extends WrappingRebufferer
    {
        long[] cutoffs;
        long[] offsets;

        JumpingRebufferer(Rebufferer source, long... cutoffsAndOffsets)
        {
            super(source);
            assert (cutoffsAndOffsets.length & 1) == 0;
            cutoffs = new long[cutoffsAndOffsets.length / 2];
            offsets = new long[cutoffs.length];
            for (int i = 0; i < cutoffs.length; ++i)
            {
                cutoffs[i] = cutoffsAndOffsets[i * 2];
                offsets[i] = cutoffsAndOffsets[i * 2 + 1];
            }
        }

        @Override
        public BufferHolder rebuffer(long position)
        {
            long pos;

            int idx = Arrays.binarySearch(offsets, position);
            if (idx < 0)
                idx = -2 - idx;
            pos = position;
            if (idx >= 0)
                pos = pos - offsets[idx] + cutoffs[idx];

            super.rebuffer(pos);

            if (idx < cutoffs.length - 1 && buffer.limit() + offset > cutoffs[idx + 1])
                buffer.limit((int) (cutoffs[idx + 1] - offset));
            if (idx >= 0)
                offset = offset - cutoffs[idx] + offsets[idx];

            return this;
        }

        @Override
        public long fileLength()
        {
            return jumped(wrapped.fileLength(), cutoffs, offsets);
        }

        @Override
        public String toString()
        {
            return Arrays.toString(cutoffs) + Arrays.toString(offsets);
        }
    }

    public class PartitionIndexJumping extends PartitionIndex
    {
        final long[] cutoffsAndOffsets;

        public PartitionIndexJumping(FileHandle fh, long trieRoot, long keyCount, DecoratedKey first, DecoratedKey last,
                                     long... cutoffsAndOffsets)
        {
            super(fh, trieRoot, keyCount, first, last);
            this.cutoffsAndOffsets = cutoffsAndOffsets;
        }

        @Override
        protected Rebufferer instantiateRebufferer()
        {
            return new JumpingRebufferer(super.instantiateRebufferer(), cutoffsAndOffsets);
        }
    }

    long jumped(long pos, long[] cutoffs, long[] offsets)
    {
        int idx = Arrays.binarySearch(cutoffs, pos);
        if (idx < 0)
            idx = -2 - idx;
        if (idx < 0)
            return pos;
        return pos - cutoffs[idx] + offsets[idx];
    }

    @Test
    public void testPointerGrowth() throws IOException
    {
        for (int reps = 0; reps < 10; ++reps)
        {
            File file = FileUtils.createTempFile("ColumnTrieReaderTest", "");
            long[] cutoffsAndOffsets = new long[]{
            2 * 4096, 1L << 16,
            4 * 4096, 1L << 24,
            6 * 4096, 1L << 31,
            8 * 4096, 1L << 32,
            10 * 4096, 1L << 33,
            12 * 4096, 1L << 34,
            14 * 4096, 1L << 40,
            16 * 4096, 1L << 42
            };

            List<DecoratedKey> list = Lists.newArrayList();
            FileHandle.Builder fhBuilder = makeHandle(file);
            try (SequentialWriter writer = makeJumpingWriter(file, cutoffsAndOffsets);
                 PartitionIndexBuilder builder = new PartitionIndexBuilder(writer, fhBuilder)
            )
            {
                writer.setPostFlushListener(builder::markPartitionIndexSynced);
                for (int i = 0; i < COUNT; i++)
                {
                    DecoratedKey key = generateRandomKey();
                    list.add(key);
                }
                Collections.sort(list);

                for (int i = 0; i < COUNT; ++i)
                    builder.addEntry(list.get(i), i);
                long root = builder.complete();

                try (FileHandle fh = fhBuilder.complete();
                     PartitionIndex index = new PartitionIndexJumping(fh, root, COUNT, null, null, cutoffsAndOffsets);
                     Analyzer analyzer = new Analyzer(index))
                {
                    checkIteration(list.size(), index);

                    analyzer.run();
                    if (analyzer.countPerType.elementSet().size() < 7)
                    {
                        Assert.fail("Expecting at least 7 different node types, got " + analyzer.countPerType.elementSet().size() + "\n" + analyzer.countPerType);
                    }
                }
            }
        }
    }

    @Test
    public void testDumpTrieToFile() throws IOException
    {
        File file = FileUtils.createTempFile("testDumpTrieToFile", "index");

        ArrayList<DecoratedKey> list = Lists.newArrayList();
        FileHandle.Builder fhBuilder = makeHandle(file);
        try (SequentialWriter writer = new SequentialWriter(file, SequentialWriterOption.DEFAULT);
             PartitionIndexBuilder builder = new PartitionIndexBuilder(writer, fhBuilder)
        )
        {
            writer.setPostFlushListener(builder::markPartitionIndexSynced);
            for (int i = 0; i < 1000; i++)
            {
                DecoratedKey key = generateRandomKey();
                list.add(key);
            }
            Collections.sort(list);

            for (int i = 0; i < 1000; ++i)
                builder.addEntry(list.get(i), i);
            long root = builder.complete();

            try (FileHandle fh = fhBuilder.complete();
                 PartitionIndex index = new PartitionIndex(fh, root, 1000, null, null))
            {
                File dump = FileUtils.createTempFile("testDumpTrieToFile", "dumpedTrie");
                index.dumpTrie(dump.toString());
                String dumpContent = String.join("\n", Files.readAllLines(dump.toPath()));
                logger.info("Dumped trie: \n{}", dumpContent);
                assertFalse(dumpContent.isEmpty());
            }
        }
    }

    public static class Analyzer extends PartitionIndex.Reader
    {
        Multiset<TrieNode> countPerType = HashMultiset.create();

        public Analyzer(PartitionIndex index)
        {
            super(index);
        }

        public void run()
        {
            run(root);
        }

        void run(long node)
        {
            go(node);

            countPerType.add(nodeType);

            int tr = transitionRange();
            for (int i = 0; i < tr; ++i)
            {
                long child = transition(i);
                if (child == NONE)
                    continue;
                run(child);
                go(node);
            }
        }
    }


    private Pair<List<DecoratedKey>, PartitionIndex> generateRandomIndex(int size) throws IOException
    {
        return generateIndex(size, this::generateRandomKey);
    }

    Pair<List<DecoratedKey>, PartitionIndex> generateLongKeysIndex(int size) throws IOException
    {
        return generateIndex(size, this::generateLongKey);
    }

    private Pair<List<DecoratedKey>, PartitionIndex> generateSequentialIndex(int size) throws IOException
    {
        return generateIndex(size, new Supplier<DecoratedKey>()
        {
            long i = 0;

            public DecoratedKey get()
            {
                return sequentialKey(i++);
            }
        });
    }

    Pair<List<DecoratedKey>, PartitionIndex> generateIndex(int size, Supplier<DecoratedKey> keyGenerator) throws IOException
    {
        File file = FileUtils.createTempFile("ColumnTrieReaderTest", "");
        List<DecoratedKey> list = Lists.newArrayList();
        FileHandle.Builder fhBuilder = makeHandle(file);
        try (SequentialWriter writer = makeWriter(file);
             PartitionIndexBuilder builder = new PartitionIndexBuilder(writer, fhBuilder)
        )
        {
            for (int i = 0; i < size; i++)
            {
                DecoratedKey key = keyGenerator.get();
                list.add(key);
            }
            Collections.sort(list);

            for (int i = 0; i < size; i++)
                builder.addEntry(list.get(i), i);
            builder.complete();

            PartitionIndex summary = loadPartitionIndex(fhBuilder, writer);

            return Pair.create(list, summary);
        }
    }

    DecoratedKey generateRandomKey()
    {
        UUID uuid = new UUID(random.nextLong(), random.nextLong());
        return partitioner.decorateKey(ByteBufferUtil.bytes(uuid));
    }

    DecoratedKey generateRandomLengthKey()
    {
        Random rand = ThreadLocalRandom.current();
        int length = nextPowerRandom(rand, 100, 10, 2);     // favor long strings
        StringBuilder s = new StringBuilder();
        for (int i = 0; i < length; ++i)
            s.append(alphabet.charAt(nextPowerRandom(rand, 0, alphabet.length(), 2))); // favor clashes at a

        return partitioner.decorateKey(ByteBufferUtil.bytes(s.toString()));
    }

    DecoratedKey generateLongKey()
    {
        Random rand = ThreadLocalRandom.current();
        int length = nextPowerRandom(rand, 10000, 2000, 2);     // favor long strings
        StringBuilder s = new StringBuilder();
        for (int i = 0; i < length; ++i)
            s.append(alphabet.charAt(nextPowerRandom(rand, 0, alphabet.length(), 2))); // favor clashes at a

        return partitioner.decorateKey(ByteBufferUtil.bytes(s.toString()));
    }

    int nextPowerRandom(Random rand, int x0, int x1, double power)
    {
        double r = Math.pow(rand.nextDouble(), power);
        return x0 + (int) ((x1 - x0) * r);
    }

    private static final String alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

    DecoratedKey sequentialKey(long i)
    {
        String s = "";
        for (int j = 50; j >= 0; j--)
        {
            int p = (int) Math.pow(10, j);
            int idx = (int) ((j + i) / p);
            s += alphabet.charAt(idx % alphabet.length());
        }
        return partitioner.decorateKey(ByteBufferUtil.bytes(s));
    }


    protected SequentialWriter makeWriter(File file)
    {
        return new SequentialWriter(file, SequentialWriterOption.newBuilder().finishOnClose(false).build());
    }

    public SequentialWriter makeJumpingWriter(File file, long[] cutoffsAndOffsets)
    {
        return new JumpingFile(file, SequentialWriterOption.newBuilder().finishOnClose(true).build(), cutoffsAndOffsets);
    }

    protected FileHandle.Builder makeHandle(File file)
    {
        return new FileHandle.Builder(file)
               .bufferSize(PageAware.PAGE_SIZE)
               .mmapped(accessMode == Config.DiskAccessMode.mmap)
               .withChunkCache(ChunkCache.instance);
    }

    protected PartitionIndex loadPartitionIndex(FileHandle.Builder fhBuilder, SequentialWriter writer) throws IOException
    {
        return PartitionIndex.load(fhBuilder, partitioner, false);
    }
}
