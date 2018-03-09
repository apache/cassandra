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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.Lists;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Assume;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.io.sstable.IndexSummaryBuilder.downsample;
import static org.apache.cassandra.io.sstable.IndexSummaryBuilder.entriesAtSamplingLevel;
import static org.apache.cassandra.io.sstable.Downsampling.BASE_SAMPLING_LEVEL;
import static org.junit.Assert.*;

public class IndexSummaryTest
{
    private final static Random random = new Random();

    @BeforeClass
    public static void initDD()
    {
        DatabaseDescriptor.daemonInitialization();

        final long seed = System.nanoTime();
        System.out.println("Using seed: " + seed);
        random.setSeed(seed);
    }

    IPartitioner partitioner = Util.testPartitioner();

    @BeforeClass
    public static void setup()
    {
        final long seed = System.nanoTime();
        System.out.println("Using seed: " + seed);
        random.setSeed(seed);
    }

    @Test
    public void testIndexSummaryKeySizes() throws IOException
    {
        // On Circle CI we normally don't have enough off-heap memory for this test so ignore it
        Assume.assumeTrue(System.getenv("CIRCLECI") == null);

        testIndexSummaryProperties(32, 100);
        testIndexSummaryProperties(64, 100);
        testIndexSummaryProperties(100, 100);
        testIndexSummaryProperties(1000, 100);
        testIndexSummaryProperties(10000, 100);
    }

    private void testIndexSummaryProperties(int keySize, int numKeys) throws IOException
    {
        final int minIndexInterval = 1;
        final List<DecoratedKey> keys = new ArrayList<>(numKeys);

        try (IndexSummaryBuilder builder = new IndexSummaryBuilder(numKeys, minIndexInterval, BASE_SAMPLING_LEVEL))
        {
            for (int i = 0; i < numKeys; i++)
            {
                byte[] randomBytes = new byte[keySize];
                random.nextBytes(randomBytes);
                DecoratedKey key = partitioner.decorateKey(ByteBuffer.wrap(randomBytes));
                keys.add(key);
                builder.maybeAddEntry(key, i);
            }

            try(IndexSummary indexSummary = builder.build(partitioner))
            {
                assertEquals(numKeys, keys.size());
                assertEquals(minIndexInterval, indexSummary.getMinIndexInterval());
                assertEquals(numKeys, indexSummary.getMaxNumberOfEntries());
                assertEquals(numKeys + 1, indexSummary.getEstimatedKeyCount());

                for (int i = 0; i < numKeys; i++)
                    assertEquals(keys.get(i).getKey(), ByteBuffer.wrap(indexSummary.getKey(i)));
            }
        }
    }

    /**
     * Test an index summary whose total size is bigger than 2GB,
     * the index summary builder should log an error but it should still
     * create an index summary, albeit one that does not cover the entire sstable.
     */
    @Test
    public void testLargeIndexSummary() throws IOException
    {
        // On Circle CI we normally don't have enough off-heap memory for this test so ignore it
        Assume.assumeTrue(System.getenv("CIRCLECI") == null);

        final int numKeys = 1000000;
        final int keySize = 3000;
        final int minIndexInterval = 1;

        try (IndexSummaryBuilder builder = new IndexSummaryBuilder(numKeys, minIndexInterval, BASE_SAMPLING_LEVEL))
        {
            for (int i = 0; i < numKeys; i++)
            {
                byte[] randomBytes = new byte[keySize];
                random.nextBytes(randomBytes);
                DecoratedKey key = partitioner.decorateKey(ByteBuffer.wrap(randomBytes));
                builder.maybeAddEntry(key, i);
            }

            try (IndexSummary indexSummary = builder.build(partitioner))
            {
                assertNotNull(indexSummary);
                assertEquals(numKeys, indexSummary.getMaxNumberOfEntries());
                assertEquals(numKeys + 1, indexSummary.getEstimatedKeyCount());
            }
        }
    }

    /**
     * Test an index summary whose total size is bigger than 2GB,
     * having updated IndexSummaryBuilder.defaultExpectedKeySize to match the size,
     * the index summary should be downsampled automatically.
     */
    @Test
    public void testLargeIndexSummaryWithExpectedSizeMatching() throws IOException
    {
        // On Circle CI we normally don't have enough off-heap memory for this test so ignore it
        Assume.assumeTrue(System.getenv("CIRCLECI") == null);

        final int numKeys = 1000000;
        final int keySize = 3000;
        final int minIndexInterval = 1;

        long oldExpectedKeySize = IndexSummaryBuilder.defaultExpectedKeySize;
        IndexSummaryBuilder.defaultExpectedKeySize = 3000;

        try (IndexSummaryBuilder builder = new IndexSummaryBuilder(numKeys, minIndexInterval, BASE_SAMPLING_LEVEL))
        {
            for (int i = 0; i < numKeys; i++)
            {
                byte[] randomBytes = new byte[keySize];
                random.nextBytes(randomBytes);
                DecoratedKey key = partitioner.decorateKey(ByteBuffer.wrap(randomBytes));
                builder.maybeAddEntry(key, i);
            }

            try (IndexSummary indexSummary = builder.build(partitioner))
            {
                assertNotNull(indexSummary);
                assertEquals(minIndexInterval * 2, indexSummary.getMinIndexInterval());
                assertEquals(numKeys / 2, indexSummary.getMaxNumberOfEntries());
                assertEquals(numKeys + 2, indexSummary.getEstimatedKeyCount());
            }
        }
        finally
        {
            IndexSummaryBuilder.defaultExpectedKeySize = oldExpectedKeySize;
        }
    }

    @Test
    public void testGetKey()
    {
        Pair<List<DecoratedKey>, IndexSummary> random = generateRandomIndex(100, 1);
        for (int i = 0; i < 100; i++)
            assertEquals(random.left.get(i).getKey(), ByteBuffer.wrap(random.right.getKey(i)));
        random.right.close();
    }

    @Test
    public void testBinarySearch()
    {
        Pair<List<DecoratedKey>, IndexSummary> random = generateRandomIndex(100, 1);
        for (int i = 0; i < 100; i++)
            assertEquals(i, random.right.binarySearch(random.left.get(i)));
        random.right.close();
    }

    @Test
    public void testGetPosition()
    {
        Pair<List<DecoratedKey>, IndexSummary> random = generateRandomIndex(100, 2);
        for (int i = 0; i < 50; i++)
            assertEquals(i*2, random.right.getPosition(i));
        random.right.close();
    }

    @Test
    public void testSerialization() throws IOException
    {
        Pair<List<DecoratedKey>, IndexSummary> random = generateRandomIndex(100, 1);
        DataOutputBuffer dos = new DataOutputBuffer();
        IndexSummary.serializer.serialize(random.right, dos);
        // write junk
        dos.writeUTF("JUNK");
        dos.writeUTF("JUNK");
        FileUtils.closeQuietly(dos);
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(dos.toByteArray()));
        IndexSummary is = IndexSummary.serializer.deserialize(dis, partitioner, 1, 1);
        for (int i = 0; i < 100; i++)
            assertEquals(i, is.binarySearch(random.left.get(i)));
        // read the junk
        assertEquals(dis.readUTF(), "JUNK");
        assertEquals(dis.readUTF(), "JUNK");
        is.close();
        FileUtils.closeQuietly(dis);
        random.right.close();
    }

    @Test
    public void testAddEmptyKey() throws Exception
    {
        IPartitioner p = new RandomPartitioner();
        try (IndexSummaryBuilder builder = new IndexSummaryBuilder(1, 1, BASE_SAMPLING_LEVEL))
        {
            builder.maybeAddEntry(p.decorateKey(ByteBufferUtil.EMPTY_BYTE_BUFFER), 0);
            IndexSummary summary = builder.build(p);
            assertEquals(1, summary.size());
            assertEquals(0, summary.getPosition(0));
            assertArrayEquals(new byte[0], summary.getKey(0));

            DataOutputBuffer dos = new DataOutputBuffer();
            IndexSummary.serializer.serialize(summary, dos);
            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(dos.toByteArray()));
            IndexSummary loaded = IndexSummary.serializer.deserialize(dis, p, 1, 1);

            assertEquals(1, loaded.size());
            assertEquals(summary.getPosition(0), loaded.getPosition(0));
            assertArrayEquals(summary.getKey(0), summary.getKey(0));
            summary.close();
            loaded.close();
        }
    }

    private Pair<List<DecoratedKey>, IndexSummary> generateRandomIndex(int size, int interval)
    {
        List<DecoratedKey> list = Lists.newArrayList();
        try (IndexSummaryBuilder builder = new IndexSummaryBuilder(list.size(), interval, BASE_SAMPLING_LEVEL))
        {
            for (int i = 0; i < size; i++)
            {
                UUID uuid = UUID.randomUUID();
                DecoratedKey key = partitioner.decorateKey(ByteBufferUtil.bytes(uuid));
                list.add(key);
            }
            Collections.sort(list);
            for (int i = 0; i < size; i++)
                builder.maybeAddEntry(list.get(i), i);
            IndexSummary summary = builder.build(partitioner);
            return Pair.create(list, summary);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testDownsamplePatterns()
    {
        assertEquals(Arrays.asList(0), Downsampling.getSamplingPattern(0));
        assertEquals(Arrays.asList(0), Downsampling.getSamplingPattern(1));

        assertEquals(Arrays.asList(1, 0), Downsampling.getSamplingPattern(2));
        assertEquals(Arrays.asList(3, 1, 2, 0), Downsampling.getSamplingPattern(4));
        assertEquals(Arrays.asList(7, 3, 5, 1, 6, 2, 4, 0), Downsampling.getSamplingPattern(8));
        assertEquals(Arrays.asList(15, 7, 11, 3, 13, 5, 9, 1, 14, 6, 10, 2, 12, 4, 8, 0), Downsampling.getSamplingPattern(16));
    }

    private static boolean shouldSkip(int index, List<Integer> startPoints)
    {
        for (int start : startPoints)
        {
            if ((index - start) % BASE_SAMPLING_LEVEL == 0)
                return true;
        }
        return false;
    }

    @Test
    public void testDownsample()
    {
        final int NUM_KEYS = 4096;
        final int INDEX_INTERVAL = 128;
        final int ORIGINAL_NUM_ENTRIES = NUM_KEYS / INDEX_INTERVAL;


        Pair<List<DecoratedKey>, IndexSummary> random = generateRandomIndex(NUM_KEYS, INDEX_INTERVAL);
        List<DecoratedKey> keys = random.left;
        IndexSummary original = random.right;

        // sanity check on the original index summary
        for (int i = 0; i < ORIGINAL_NUM_ENTRIES; i++)
            assertEquals(keys.get(i * INDEX_INTERVAL).getKey(), ByteBuffer.wrap(original.getKey(i)));

        List<Integer> samplePattern = Downsampling.getSamplingPattern(BASE_SAMPLING_LEVEL);

        // downsample by one level, then two levels, then three levels...
        int downsamplingRound = 1;
        for (int samplingLevel = BASE_SAMPLING_LEVEL - 1; samplingLevel >= 1; samplingLevel--)
        {
            try (IndexSummary downsampled = downsample(original, samplingLevel, 128, partitioner);)
            {
                assertEquals(entriesAtSamplingLevel(samplingLevel, original.getMaxNumberOfEntries()), downsampled.size());

                int sampledCount = 0;
                List<Integer> skipStartPoints = samplePattern.subList(0, downsamplingRound);
                for (int i = 0; i < ORIGINAL_NUM_ENTRIES; i++)
                {
                    if (!shouldSkip(i, skipStartPoints))
                    {
                        assertEquals(keys.get(i * INDEX_INTERVAL).getKey(), ByteBuffer.wrap(downsampled.getKey(sampledCount)));
                        sampledCount++;
                    }
                }

                testPosition(original, downsampled, keys);
                downsamplingRound++;
            }
        }

        // downsample one level each time
        IndexSummary previous = original;
        downsamplingRound = 1;
        for (int downsampleLevel = BASE_SAMPLING_LEVEL - 1; downsampleLevel >= 1; downsampleLevel--)
        {
            IndexSummary downsampled = downsample(previous, downsampleLevel, 128, partitioner);
            if (previous != original)
                previous.close();
            assertEquals(entriesAtSamplingLevel(downsampleLevel, original.getMaxNumberOfEntries()), downsampled.size());

            int sampledCount = 0;
            List<Integer> skipStartPoints = samplePattern.subList(0, downsamplingRound);
            for (int i = 0; i < ORIGINAL_NUM_ENTRIES; i++)
            {
                if (!shouldSkip(i, skipStartPoints))
                {
                    assertEquals(keys.get(i * INDEX_INTERVAL).getKey(), ByteBuffer.wrap(downsampled.getKey(sampledCount)));
                    sampledCount++;
                }
            }

            testPosition(original, downsampled, keys);
            previous = downsampled;
            downsamplingRound++;
        }
        previous.close();
        original.close();
    }

    private void testPosition(IndexSummary original, IndexSummary downsampled, List<DecoratedKey> keys)
    {
        for (DecoratedKey key : keys)
        {
            long orig = SSTableReader.getIndexScanPositionFromBinarySearchResult(original.binarySearch(key), original);
            int binarySearch = downsampled.binarySearch(key);
            int index = SSTableReader.getIndexSummaryIndexFromBinarySearchResult(binarySearch);
            int scanFrom = (int) SSTableReader.getIndexScanPositionFromBinarySearchResult(index, downsampled);
            assert scanFrom <= orig;
            int effectiveInterval = downsampled.getEffectiveIndexIntervalAfterIndex(index);
            DecoratedKey k = null;
            for (int i = 0 ; k != key && i < effectiveInterval && scanFrom < keys.size() ; i++, scanFrom ++)
                k = keys.get(scanFrom);
            assert k == key;
        }
    }

    @Test
    public void testOriginalIndexLookup()
    {
        for (int i = BASE_SAMPLING_LEVEL; i >= 1; i--)
            assertEquals(i, Downsampling.getOriginalIndexes(i).size());

        ArrayList<Integer> full = new ArrayList<>();
        for (int i = 0; i < BASE_SAMPLING_LEVEL; i++)
            full.add(i);

        assertEquals(full, Downsampling.getOriginalIndexes(BASE_SAMPLING_LEVEL));
        // the entry at index 127 is the first to go
        assertEquals(full.subList(0, full.size() - 1), Downsampling.getOriginalIndexes(BASE_SAMPLING_LEVEL - 1));

        // spot check a few values (these depend on BASE_SAMPLING_LEVEL being 128)
        assertEquals(128, BASE_SAMPLING_LEVEL);
        assertEquals(Arrays.asList(0, 32, 64, 96), Downsampling.getOriginalIndexes(4));
        assertEquals(Arrays.asList(0, 64), Downsampling.getOriginalIndexes(2));
        assertEquals(Arrays.asList(0), Downsampling.getOriginalIndexes(1));
    }

    @Test
    public void testGetNumberOfSkippedEntriesAfterIndex()
    {
        int indexInterval = 128;
        for (int i = 0; i < BASE_SAMPLING_LEVEL; i++)
            assertEquals(indexInterval, Downsampling.getEffectiveIndexIntervalAfterIndex(i, BASE_SAMPLING_LEVEL, indexInterval));

        // with one round of downsampling, only the last summary entry has been removed, so only the last index will have
        // double the gap until the next sample
        for (int i = 0; i < BASE_SAMPLING_LEVEL - 2; i++)
            assertEquals(indexInterval, Downsampling.getEffectiveIndexIntervalAfterIndex(i, BASE_SAMPLING_LEVEL - 1, indexInterval));
        assertEquals(indexInterval * 2, Downsampling.getEffectiveIndexIntervalAfterIndex(BASE_SAMPLING_LEVEL - 2, BASE_SAMPLING_LEVEL - 1, indexInterval));

        // at samplingLevel=2, the retained summary points are [0, 64] (assumes BASE_SAMPLING_LEVEL is 128)
        assertEquals(128, BASE_SAMPLING_LEVEL);
        assertEquals(64 * indexInterval, Downsampling.getEffectiveIndexIntervalAfterIndex(0, 2, indexInterval));
        assertEquals(64 * indexInterval, Downsampling.getEffectiveIndexIntervalAfterIndex(1, 2, indexInterval));
    }
}
