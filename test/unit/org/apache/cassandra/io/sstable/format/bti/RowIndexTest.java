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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.format.bti.RowIndexReader.IndexInfo;
import org.apache.cassandra.io.tries.Walker;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class RowIndexTest
{
    private final static Logger logger = LoggerFactory.getLogger(RowIndexTest.class);
    private final Version version = new BtiFormat(null).getLatestVersion();

    static final ByteComparable.Version VERSION = Walker.BYTE_COMPARABLE_VERSION;

    static final Random RANDOM;

    static
    {
        long seed = System.currentTimeMillis();
        logger.info("seed = " + seed);
        RANDOM = new Random(seed);

        DatabaseDescriptor.daemonInitialization();
    }

    static final ClusteringComparator comparator = new ClusteringComparator(UUIDType.instance);
    static final long END_MARKER = 1L << 40;
    static final int COUNT = 8192;

    @Parameterized.Parameters()
    public static Collection<Object[]> generateData()
    {
        return Arrays.asList(new Object[]{ Config.DiskAccessMode.standard },
                             new Object[]{ Config.DiskAccessMode.mmap });
    }

    @Parameterized.Parameter(value = 0)
    public static Config.DiskAccessMode accessMode = Config.DiskAccessMode.standard;

    @Test
    public void testSingletons() throws IOException
    {
        Pair<List<ClusteringPrefix<?>>, RowIndexReader> random = generateRandomIndexSingletons(COUNT);
        RowIndexReader summary = random.right;
        List<ClusteringPrefix<?>> keys = random.left;
        for (int i = 0; i < COUNT; i++)
        {
            assertEquals(i, summary.separatorFloor(comparator.asByteComparable(keys.get(i))).offset);
        }
        summary.close();
    }

    @Test
    public void testSpans() throws IOException
    {
        Pair<List<ClusteringPrefix<?>>, RowIndexReader> random = generateRandomIndexQuads(COUNT);
        RowIndexReader summary = random.right;
        List<ClusteringPrefix<?>> keys = random.left;
        int missCount = 0;
        IndexInfo ii;
        for (int i = 0; i < COUNT; i++)
        {
            // These need to all be within the span
            assertEquals(i, (ii = summary.separatorFloor(comparator.asByteComparable(keys.get(4 * i + 1)))).offset);
            assertEquals(i, summary.separatorFloor(comparator.asByteComparable(keys.get(4 * i + 2))).offset);
            assertEquals(i, summary.separatorFloor(comparator.asByteComparable(keys.get(4 * i + 3))).offset);

            // check other data
            assertEquals(i + 2, ii.openDeletion.markedForDeleteAt());
            assertEquals(i + 3, ii.openDeletion.localDeletionTime());

            // before entry. hopefully here, but could end up in prev if matches prevMax too well
            ii = summary.separatorFloor(comparator.asByteComparable(keys.get(4 * i)));
            if (ii.offset != i)
            {
                ++missCount;
                assertEquals(i - 1, ii.offset);
            }
        }
        ii = summary.separatorFloor(comparator.asByteComparable(keys.get(4 * COUNT)));
        if (ii.offset != END_MARKER)
        {
            ++missCount;
            assertEquals(COUNT - 1, ii.offset);
        }
        ii = summary.separatorFloor(comparator.asByteComparable(ClusteringBound.BOTTOM));
        assertEquals(0, ii.offset);

        ii = summary.separatorFloor(comparator.asByteComparable(ClusteringBound.TOP));
        assertEquals(END_MARKER, ii.offset);

        summary.close();
        if (missCount > COUNT / 5)
            logger.error("Unexpectedly high miss count: {}/{}", missCount, COUNT);
    }

    File file;
    DataOutputStreamPlus dos;
    RowIndexWriter writer;
    FileHandle fh;
    long root;

    @After
    public void cleanUp()
    {
        FileUtils.closeQuietly(dos);
        FileUtils.closeQuietly(writer);
        FileUtils.closeQuietly(fh);
    }

    public RowIndexTest() throws IOException
    {
        this(FileUtils.createTempFile("ColumnTrieReaderTest", ""));
    }

    RowIndexTest(File file) throws IOException
    {
        this(file, new SequentialWriter(file, SequentialWriterOption.newBuilder().finishOnClose(true).build()));
    }

    RowIndexTest(File file, DataOutputStreamPlus dos) throws IOException
    {
        this.file = file;
        this.dos = dos;

        // write some junk
        dos.writeUTF("JUNK");
        dos.writeUTF("JUNK");

        writer = new RowIndexWriter(comparator, dos, version);
    }

    public void complete() throws IOException
    {
        root = writer.complete(END_MARKER);
        dos.writeUTF("JUNK");
        dos.writeUTF("JUNK");
        dos.close();
        dos = null;
    }

    public RowIndexReader completeAndRead() throws IOException
    {
        complete();

        FileHandle.Builder builder = new FileHandle.Builder(file).mmapped(accessMode == Config.DiskAccessMode.mmap);
        fh = builder.complete();
        try (RandomAccessReader rdr = fh.createReader())
        {
            assertEquals("JUNK", rdr.readUTF());
            assertEquals("JUNK", rdr.readUTF());
        }
        return new RowIndexReader(fh, root, version);
    }

    @Test
    public void testAddEmptyKey() throws Exception
    {
        ClusteringPrefix<?> key = Clustering.EMPTY;
        writer.add(key, key, new IndexInfo(42, DeletionTime.LIVE));
        try (RowIndexReader summary = completeAndRead())
        {
            IndexInfo i = summary.min();
            assertEquals(42, i.offset);

            i = summary.separatorFloor(comparator.asByteComparable(ClusteringBound.BOTTOM));
            assertEquals(42, i.offset);

            i = summary.separatorFloor(comparator.asByteComparable(ClusteringBound.TOP));
            assertEquals(END_MARKER, i.offset);

            i = summary.separatorFloor(comparator.asByteComparable(key));
            assertEquals(42, i.offset);
        }
    }

    @Test
    public void testAddDuplicateEmptyThrow() throws Exception
    {
        ClusteringPrefix<?> key = Clustering.EMPTY;
        Throwable t = null;
        writer.add(key, key, new IndexInfo(42, DeletionTime.LIVE));
        try
        {
            writer.add(key, key, new IndexInfo(43, DeletionTime.LIVE));
            try (RowIndexReader summary = completeAndRead())
            {
                // failing path
            }
        }
        catch (AssertionError e)
        {
            // correct path
            t = e;
            logger.info("Got " + e.getMessage());
        }
        Assert.assertNotNull("Should throw an assertion error.", t);
    }

    @Test
    public void testAddDuplicateThrow() throws Exception
    {
        ClusteringPrefix<?> key = generateRandomKey();
        Throwable t = null;
        writer.add(key, key, new IndexInfo(42, DeletionTime.LIVE));
        try
        {
            writer.add(key, key, new IndexInfo(43, DeletionTime.LIVE));
            try (RowIndexReader summary = completeAndRead())
            {
                // failing path
            }
        }
        catch (AssertionError e)
        {
            // correct path
            t = e;
            logger.info("Got " + e.getMessage());
        }
        Assert.assertNotNull("Should throw an assertion error.", t);
    }

    @Test
    public void testAddOutOfOrderThrow() throws Exception
    {
        ClusteringPrefix<?> key1 = generateRandomKey();
        ClusteringPrefix<?> key2 = generateRandomKey();
        while (comparator.compare(key1, key2) <= 0) // make key2 smaller than 1
            key2 = generateRandomKey();

        Throwable t = null;
        writer.add(key1, key1, new IndexInfo(42, DeletionTime.LIVE));
        try
        {
            writer.add(key2, key2, new IndexInfo(43, DeletionTime.LIVE));
            try (RowIndexReader summary = completeAndRead())
            {
                // failing path
            }
        }
        catch (AssertionError e)
        {
            // correct path
            t = e;
            logger.info("Got " + e.getMessage());
        }
        Assert.assertNotNull("Should throw an assertion error.", t);
    }

    @Test
    public void testConstrainedIteration() throws IOException
    {
        // This is not too relevant: due to the way we construct separators we can't be good enough on the left side.
        Pair<List<ClusteringPrefix<?>>, RowIndexReader> random = generateRandomIndexSingletons(COUNT);
        List<ClusteringPrefix<?>> keys = random.left;

        for (int i = 0; i < 500; ++i)
        {
            boolean exactLeft = RANDOM.nextBoolean();
            boolean exactRight = RANDOM.nextBoolean();
            ClusteringPrefix<?> left = exactLeft ? keys.get(RANDOM.nextInt(keys.size())) : generateRandomKey();
            ClusteringPrefix<?> right = exactRight ? keys.get(RANDOM.nextInt(keys.size())) : generateRandomKey();
            if (comparator.compare(right, left) < 0)
            {
                ClusteringPrefix<?> t = left;
                left = right;
                right = t;
                boolean b = exactLeft;
                exactLeft = exactRight;
                exactRight = b;
            }

            try (RowIndexReverseIterator iter = new RowIndexReverseIterator(fh, root, comparator.asByteComparable(left), comparator.asByteComparable(right), random.right.version))
            {
                IndexInfo indexInfo = iter.nextIndexInfo();
                if (indexInfo == null)
                {
                    int idx = Collections.binarySearch(keys, right, comparator);
                    if (idx < 0)
                        idx = -2 - idx; // less than or equal
                    if (idx <= 0)
                        continue;
                    assertTrue(comparator.asByteComparable(left) + " <= "
                               + comparator.asByteComparable(keys.get(idx)) + " <= "
                               + comparator.asByteComparable(right) + " but " + idx + " wasn't iterated.",
                               comparator.compare(left, keys.get(idx - 1)) > 0);
                    continue;
                }

                int idx = (int) indexInfo.offset;
                if (indexInfo.offset == END_MARKER)
                    idx = keys.size();
                if (idx > 0)
                    assertTrue(comparator.compare(right, keys.get(idx - 1)) > 0);
                if (idx < keys.size() - 1)
                    assertTrue(comparator.compare(right, keys.get(idx + 1)) < 0);
                if (exactRight)      // must be precise on exact, otherwise could be in any relation
                    assertEquals(right, keys.get(idx));
                while (true)
                {
                    --idx;
                    IndexInfo ii = iter.nextIndexInfo();
                    if (ii == null)
                        break;
                    assertEquals(idx, (int) ii.offset);
                }
                ++idx; // seek at last returned
                if (idx < keys.size() - 1)
                    assertTrue(comparator.compare(left, keys.get(idx + 1)) < 0);
                // Because of the way we build the index (using non-prefix separator) we are usually going to miss the last item.
                if (idx >= 2)
                    assertTrue(comparator.compare(left, keys.get(idx - 2)) > 0);
            }
            catch (AssertionError e)
            {
                logger.error(e.getMessage(), e);
                ClusteringPrefix<?> ll = left;
                ClusteringPrefix<?> rr = right;
                logger.info(keys.stream()
                                .filter(x -> comparator.compare(ll, x) <= 0 && comparator.compare(x, rr) <= 0)
                                .map(clustering -> comparator.asByteComparable(clustering))
                                .map(bc -> bc.byteComparableAsString(VERSION))
                                .collect(Collectors.joining(", ")));
                logger.info("Left {}{} Right {}{}", comparator.asByteComparable(left), exactLeft ? "#" : "", comparator.asByteComparable(right), exactRight ? "#" : "");
                try (RowIndexReverseIterator iter2 = new RowIndexReverseIterator(fh, root, comparator.asByteComparable(left), comparator.asByteComparable(right), version))
                {
                    IndexInfo ii;
                    while ((ii = iter2.nextIndexInfo()) != null)
                    {
                        logger.info(comparator.asByteComparable(keys.get((int) ii.offset)).toString());
                    }
                    logger.info("Left {}{} Right {}{}", comparator.asByteComparable(left), exactLeft ? "#" : "", comparator.asByteComparable(right), exactRight ? "#" : "");
                }
                throw e;
            }
        }
    }

    @Test
    public void testReverseIteration() throws IOException
    {
        Pair<List<ClusteringPrefix<?>>, RowIndexReader> random = generateRandomIndexSingletons(COUNT);
        List<ClusteringPrefix<?>> keys = random.left;

        for (int i = 0; i < 1000; ++i)
        {
            boolean exactRight = RANDOM.nextBoolean();
            ClusteringPrefix<?> right = exactRight ? keys.get(RANDOM.nextInt(keys.size())) : generateRandomKey();

            int idx = 0;
            try (RowIndexReverseIterator iter = new RowIndexReverseIterator(fh, root, ByteComparable.EMPTY, comparator.asByteComparable(right), random.right.version))
            {
                IndexInfo indexInfo = iter.nextIndexInfo();
                if (indexInfo == null)
                {
                    idx = Collections.binarySearch(keys, right, comparator);
                    if (idx < 0)
                        idx = -2 - idx; // less than or equal
                    assertTrue(comparator.asByteComparable(keys.get(idx)) + " <= "
                               + comparator.asByteComparable(right) + " but " + idx + " wasn't iterated.",
                               idx < 0);
                    continue;
                }

                idx = (int) indexInfo.offset;
                if (indexInfo.offset == END_MARKER)
                    idx = keys.size();
                if (idx > 0)
                    assertTrue(comparator.compare(right, keys.get(idx - 1)) > 0);
                if (idx < keys.size() - 1)
                    assertTrue(comparator.compare(right, keys.get(idx + 1)) < 0);
                if (exactRight)      // must be precise on exact, otherwise could be in any relation
                    assertEquals(right, keys.get(idx));
                while (true)
                {
                    --idx;
                    IndexInfo ii = iter.nextIndexInfo();
                    if (ii == null)
                        break;
                    assertEquals(idx, (int) ii.offset);
                }
                assertEquals(-1, idx);
            }
            catch (AssertionError e)
            {
                logger.error(e.getMessage(), e);
                ClusteringPrefix<?> rr = right;
                logger.info(keys.stream()
                                .filter(x -> comparator.compare(x, rr) <= 0)
                                .map(comparator::asByteComparable)
                                .map(bc -> bc.byteComparableAsString(VERSION))
                                .collect(Collectors.joining(", ")));
                logger.info("Right {}{}", comparator.asByteComparable(right), exactRight ? "#" : "");
                try (RowIndexReverseIterator iter2 = new RowIndexReverseIterator(fh, root, ByteComparable.EMPTY, comparator.asByteComparable(right), version))
                {
                    IndexInfo ii;
                    while ((ii = iter2.nextIndexInfo()) != null)
                    {
                        logger.info(comparator.asByteComparable(keys.get((int) ii.offset)).toString());
                    }
                }
                logger.info("Right {}{}", comparator.asByteComparable(right), exactRight ? "#" : "");
                throw e;
            }
        }
    }

    private Pair<List<ClusteringPrefix<?>>, RowIndexReader> generateRandomIndexSingletons(int size) throws IOException
    {
        List<ClusteringPrefix<?>> list = generateList(size);
        for (int i = 0; i < size; i++)
        {
            assert i == 0 || comparator.compare(list.get(i - 1), list.get(i)) < 0;
            assert i == 0 || ByteComparable.compare(comparator.asByteComparable(list.get(i - 1)), comparator.asByteComparable(list.get(i)), VERSION) < 0 :
            String.format("%s bs %s versus %s bs %s", list.get(i - 1).clustering().clusteringString(comparator.subtypes()), comparator.asByteComparable(list.get(i - 1)), list.get(i).clustering().clusteringString(comparator.subtypes()), comparator.asByteComparable(list.get(i)));
            writer.add(list.get(i), list.get(i), new IndexInfo(i, DeletionTime.LIVE));
        }

        RowIndexReader summary = completeAndRead();
        return Pair.create(list, summary);
    }

    List<ClusteringPrefix<?>> generateList(int size)
    {
        List<ClusteringPrefix<?>> list = Lists.newArrayList();

        Set<ClusteringPrefix<?>> set = Sets.newHashSet();
        for (int i = 0; i < size; i++)
        {
            ClusteringPrefix<?> key = generateRandomKey(); // keys must be unique
            while (!set.add(key))
                key = generateRandomKey();
            list.add(key);
        }
        list.sort(comparator);
        return list;
    }

    private Pair<List<ClusteringPrefix<?>>, RowIndexReader> generateRandomIndexQuads(int size) throws IOException
    {
        List<ClusteringPrefix<?>> list = generateList(4 * size + 1);
        for (int i = 0; i < size; i++)
            writer.add(list.get(i * 4 + 1), list.get(i * 4 + 3), new IndexInfo(i, DeletionTime.build(i + 2, i + 3)));

        RowIndexReader summary = completeAndRead();
        return Pair.create(list, summary);
    }

    ClusteringPrefix<?> generateRandomKey()
    {
        UUID uuid = randomSeededUUID();
        ClusteringPrefix<?> key = comparator.make(uuid);
        return key;
    }

    private static UUID randomSeededUUID()
    {
        byte[] randomBytes = new byte[16];
        RANDOM.nextBytes(randomBytes);
        return UUID.nameUUIDFromBytes(randomBytes);
    }
}
