/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.utils;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.NumberFormat;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Locale;
import java.util.Random;
import java.util.Set;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.util.BufferedDataOutputStreamPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.IFilter.FilterKey;
import org.apache.cassandra.utils.KeyGenerator.RandomStringGenerator;
import org.apache.cassandra.utils.obs.IBitSet;
import org.apache.cassandra.utils.obs.MemoryLimiter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class BloomFilterTest
{
    public IFilter bfInvHashes;
    public MemoryLimiter memoryLimiter;

    public static IFilter testSerialize(IFilter f, boolean oldBfFormat) throws IOException
    {
        f.add(FilterTestHelper.bytes("a"));
        DataOutputBuffer out = new DataOutputBuffer();
        if (oldBfFormat)
        {
            SerializationsTest.serializeOldBfFormat((BloomFilter) f, out);
        }
        else
        {
            BloomFilter.serializer.serialize((BloomFilter) f, out);
        }

        ByteArrayInputStream in = new ByteArrayInputStream(out.getData(), 0, out.getLength());
        IFilter f2 = BloomFilter.serializer.deserialize(new DataInputStream(in), oldBfFormat);

        assert f2.isPresent(FilterTestHelper.bytes("a"));
        assert !f2.isPresent(FilterTestHelper.bytes("b"));
        return f2;
    }

    static void compare(IBitSet bs, IBitSet newbs)
    {
        assertEquals(bs.capacity(), newbs.capacity());
        for (long i = 0; i < bs.capacity(); i++)
            assertEquals(bs.get(i), newbs.get(i));
    }

    @Before
    public void setup()
    {
        // Set a high limit so that normal tests won't reach it, but we don't want Long.MAX_VALUE because
        // we want to test what happens when we reach it
        System.setProperty(BloomFilter.MAX_MEMORY_MB_PROP, Long.toString(128 << 10));
        memoryLimiter = new MemoryLimiter(128L << 30, "Allocating %s for bloom filter would reach max of %s (current %s)");
        bfInvHashes = FilterFactory.getFilter(10000L, FilterTestHelper.MAX_FAILURE_RATE);
    }

    @After
    public void destroy()
    {
        bfInvHashes.close();
        assertEquals(0, memoryLimiter.memoryAllocated());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testBloomLimits1()
    {
        int maxBuckets = BloomCalculations.probs.length - 1;
        int maxK = BloomCalculations.probs[maxBuckets].length - 1;

        // possible
        BloomCalculations.computeBloomSpec(maxBuckets, BloomCalculations.probs[maxBuckets][maxK]);

        // impossible, throws
        BloomCalculations.computeBloomSpec(maxBuckets, BloomCalculations.probs[maxBuckets][maxK] / 2);
    }

    @Test
    public void testOne()
    {
        bfInvHashes.add(FilterTestHelper.bytes("a"));
        assert bfInvHashes.isPresent(FilterTestHelper.bytes("a"));
        assert !bfInvHashes.isPresent(FilterTestHelper.bytes("b"));
    }

    @Test
    public void testFalsePositivesInt()
    {
        FilterTestHelper.testFalsePositives(bfInvHashes, FilterTestHelper.intKeys(), FilterTestHelper.randomKeys2());
    }

    @Test
    public void testFalsePositivesRandom()
    {
        FilterTestHelper.testFalsePositives(bfInvHashes, FilterTestHelper.randomKeys(), FilterTestHelper.randomKeys2());
    }

    @Test
    public void testWords()
    {
        if (KeyGenerator.WordGenerator.WORDS == 0)
        {
            return;
        }
        IFilter bf2 = FilterFactory.getFilter(KeyGenerator.WordGenerator.WORDS / 2, FilterTestHelper.MAX_FAILURE_RATE);
        int skipEven = KeyGenerator.WordGenerator.WORDS % 2 == 0 ? 0 : 2;
        FilterTestHelper.testFalsePositives(bf2,
                                            new KeyGenerator.WordGenerator(skipEven, 2),
                                            new KeyGenerator.WordGenerator(1, 2));
        bf2.close();
    }

    @Test
    public void testSerialize() throws IOException
    {
        BloomFilterTest.testSerialize(bfInvHashes, true).close();
        BloomFilterTest.testSerialize(bfInvHashes, false).close();
    }

    @Test
    @Ignore
    public void testManyRandom()
    {
        testManyRandom(FilterTestHelper.randomKeys());
    }

    private static void testManyRandom(Iterator<ByteBuffer> keys)
    {
        int MAX_HASH_COUNT = 128;
        Set<Long> hashes = new HashSet<>();
        long collisions = 0;
        while (keys.hasNext())
        {
            hashes.clear();
            FilterKey buf = FilterTestHelper.wrap(keys.next());
            BloomFilter bf = (BloomFilter) FilterFactory.getFilter(10, 1);
            for (long hashIndex : bf.getHashBuckets(buf, MAX_HASH_COUNT, 1024 * 1024))
            {
                hashes.add(hashIndex);
            }
            collisions += (MAX_HASH_COUNT - hashes.size());
            bf.close();
        }
        assertTrue("collisions=" + collisions, collisions <= 100);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testOffHeapException()
    {
        long numKeys = ((long) Integer.MAX_VALUE) * 64L + 1L; // approx 128 Billion
        FilterFactory.getFilter(numKeys, 0.01d).close();
    }

    @Test
    public void compareCachedKey()
    {
        try (BloomFilter bf1 = (BloomFilter) FilterFactory.getFilter(FilterTestHelper.ELEMENTS / 2, FilterTestHelper.MAX_FAILURE_RATE);
             BloomFilter bf2 = (BloomFilter) FilterFactory.getFilter(FilterTestHelper.ELEMENTS / 2, FilterTestHelper.MAX_FAILURE_RATE);
             BloomFilter bf3 = (BloomFilter) FilterFactory.getFilter(FilterTestHelper.ELEMENTS / 2, FilterTestHelper.MAX_FAILURE_RATE))
        {
            RandomStringGenerator gen1 = new KeyGenerator.RandomStringGenerator(new Random().nextInt(), FilterTestHelper.ELEMENTS);

            // make sure all bitsets are empty.
            compare(bf1.bitset, bf2.bitset);
            compare(bf1.bitset, bf3.bitset);

            while (gen1.hasNext())
            {
                ByteBuffer key = gen1.next();
                FilterKey cached = FilterTestHelper.wrapCached(key);
                bf1.add(FilterTestHelper.wrap(key));
                bf2.add(cached);
                bf3.add(cached);
            }

            compare(bf1.bitset, bf2.bitset);
            compare(bf1.bitset, bf3.bitset);
        }
    }

    @Test
    public void testHugeBFSerialization() throws Exception
    {
        ByteBuffer test = ByteBuffer.wrap(new byte[]{ 0, 1 });

        File file = FileUtils.createDeletableTempFile("bloomFilterTest-", ".dat");
        BloomFilter filter = (BloomFilter) FilterFactory.getFilter(((long) Integer.MAX_VALUE / 8) + 1, 0.01d);
        filter.add(FilterTestHelper.wrap(test));
        DataOutputStreamPlus out = new BufferedDataOutputStreamPlus(new FileOutputStream(file));
        BloomFilter.serializer.serialize(filter, out);
        out.close();
        filter.close();

        DataInputStream in = new DataInputStream(new FileInputStream(file));
        IFilter filter2 = BloomFilter.serializer.deserialize(in, false);
        assertTrue(filter2.isPresent(FilterTestHelper.wrap(test)));
        FileUtils.closeQuietly(in);
        filter2.close();
    }

    @Test
    public void testMurmur3FilterHash()
    {
        IPartitioner partitioner = new Murmur3Partitioner();
        Iterator<ByteBuffer> gen = new KeyGenerator.RandomStringGenerator(new Random().nextInt(), FilterTestHelper.ELEMENTS);
        long[] expected = new long[2];
        long[] actual = new long[2];
        while (gen.hasNext())
        {
            expected[0] = 1;
            expected[1] = 2;
            actual[0] = 3;
            actual[1] = 4;
            ByteBuffer key = gen.next();
            FilterKey expectedKey = FilterTestHelper.wrap(key);
            FilterKey actualKey = partitioner.decorateKey(key);
            actualKey.filterHash(actual);
            expectedKey.filterHash(expected);
            Assert.assertArrayEquals(expected, actual);
        }
    }

    @Test
    public void testMaxMemoryExceeded()
    {
        long allocSize = 2L * (1 << 20);
        double fpChance = 0.01;
        long size;

        try (IFilter filter = FilterFactory.getFilter(allocSize, fpChance, memoryLimiter))
        {
            size = filter.offHeapSize();
        }
        assertNotEquals(0, size);

        memoryLimiter = new MemoryLimiter(3 * size / 2, "Allocating %s for bloom filter would reach max of %s (current %s)");

        try (IFilter filter = FilterFactory.getFilter(allocSize, fpChance, memoryLimiter))
        {
            assertNotNull(filter);
            assertTrue(filter instanceof BloomFilter);

            long memBefore = memoryLimiter.memoryAllocated();

            try (IFilter blankFilter = FilterFactory.getFilter(allocSize, fpChance, memoryLimiter))
            {
                assertNotNull(blankFilter);
                assertTrue(blankFilter instanceof AlwaysPresentFilter);

                assertEquals(memBefore, memoryLimiter.memoryAllocated());
            }
        }
    }

    @Test
    public void testMaxMemoryExceededOnDeserialize() throws IOException
    {
        long allocSize = 2L * (1 << 20);
        double fpChance = 0.01;
        long size;

        DataOutputBuffer out = new DataOutputBuffer();
        try (IFilter filter = FilterFactory.getFilter(allocSize, fpChance, memoryLimiter))
        {
            size = filter.offHeapSize();
            BloomFilter.serializer.serialize((BloomFilter) filter, out);
        }
        assertNotEquals(0, size);

        memoryLimiter = new MemoryLimiter(3 * size / 2, "Allocating %s for bloom filter would reach max of %s (current %s)");

        try (IFilter filter = FilterFactory.getFilter(allocSize, fpChance, memoryLimiter))
        {
            assertNotNull(filter);
            assertTrue(filter instanceof BloomFilter);

            long memBefore = memoryLimiter.memoryAllocated();

            ByteArrayInputStream in = new ByteArrayInputStream(out.getData(), 0, out.getLength());
            try (IFilter blankFilter = new BloomFilterSerializer(memoryLimiter).deserialize(new DataInputStream(in), false))
            {
                assertNotNull(blankFilter);
                assertTrue(blankFilter instanceof AlwaysPresentFilter);
                assertEquals(memBefore, memoryLimiter.memoryAllocated());
            }
        }
    }

    @Test
    @Ignore // this is a test that can be used to print out the sizes of BFs
    public void testBloomFilterSize()
    {
        int[] nks = new int[]{
        100_000, 500_000,
        1_000_000, 5_000_000,
        10_000_000, 50_000_000,
        100_000_000, 500_000_000 };

        //double[] fps = new double[] { 0.01, 0.05, 0.1, 0.2, 0.25 };
        double[] fps = new double[]{ 0.01, 0.1 };

        for (int nk : nks)
        {
            for (double fp : fps)
            {
                IFilter filter = FilterFactory.getFilter(nk, fp);
                System.out.println(String.format("%s keys %s FP chance => %s",
                                                 NumberFormat.getNumberInstance(Locale.US).format(nk),
                                                 NumberFormat.getNumberInstance(Locale.US).format(fp),
                                                 FBUtilities.prettyPrintMemory(filter.serializedSize())));
            }
        }
    }
}