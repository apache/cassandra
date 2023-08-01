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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileInputStreamPlus;
import org.apache.cassandra.io.util.FileOutputStreamPlus;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.IFilter.FilterKey;
import org.apache.cassandra.utils.KeyGenerator.RandomStringGenerator;
import org.apache.cassandra.utils.obs.IBitSet;

import static org.junit.Assert.assertEquals;

public class BloomFilterTest
{
    public IFilter bfInvHashes;



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
            BloomFilterSerializer.forVersion(false).serialize((BloomFilter) f, out);
        }

        ByteArrayInputStream in = new ByteArrayInputStream(out.getData(), 0, out.getLength());
        IFilter f2 = BloomFilterSerializer.forVersion(oldBfFormat).deserialize(Util.DataInputStreamPlusImpl.wrap(in));

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
        bfInvHashes = FilterFactory.getFilter(10000L, FilterTestHelper.MAX_FAILURE_RATE);
    }

    @After
    public void destroy()
    {
        bfInvHashes.close();
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
        Assert.assertTrue("collisions=" + collisions, collisions <= 100);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testOffHeapException()
    {
        long numKeys = ((long)Integer.MAX_VALUE) * 64L + 1L; // approx 128 Billion
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
    @Ignore
    public void testHugeBFSerialization() throws IOException
    {
        ByteBuffer test = ByteBuffer.wrap(new byte[] {0, 1});

        File file = FileUtils.createDeletableTempFile("bloomFilterTest-", ".dat");
        BloomFilter filter = (BloomFilter) FilterFactory.getFilter(((long) Integer.MAX_VALUE / 8) + 1, 0.01d);
        filter.add(FilterTestHelper.wrap(test));
        FileOutputStreamPlus out = file.newOutputStream(File.WriteMode.OVERWRITE);
        BloomFilterSerializer serializer = BloomFilterSerializer.forVersion(false);
        serializer.serialize(filter, out);
        out.close();
        filter.close();

        FileInputStreamPlus in = file.newInputStream();
        BloomFilter filter2 = BloomFilterSerializer.forVersion(false).deserialize(in);
        Assert.assertTrue(filter2.isPresent(FilterTestHelper.wrap(test)));
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
}
