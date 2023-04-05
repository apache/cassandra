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

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.cassandra.utils.FilterFactory.getFilter;
import static org.apache.cassandra.utils.FilterTestHelper.testFalsePositives;

public class LongBloomFilterTest
{
    private static final Logger logger = LoggerFactory.getLogger(LongBloomFilterTest.class);

    /**
     * NB: needs to run with -mx1G
     */
    @Test
    public void testBigInt()
    {
        int size = 10 * 1000 * 1000;
        IFilter bf = getFilter(size, FilterTestHelper.spec.bucketsPerElement);
        double fp = testFalsePositives(bf,
                                       new KeyGenerator.IntGenerator(size),
                                       new KeyGenerator.IntGenerator(size, size * 2));
        logger.info("Bloom filter false positive: {}", fp);
    }

    @Test
    public void testBigRandom()
    {
        int size = 10 * 1000 * 1000;
        IFilter bf = getFilter(size, FilterTestHelper.spec.bucketsPerElement);
        double fp = testFalsePositives(bf,
                                       new KeyGenerator.RandomStringGenerator(new Random().nextInt(), size),
                                       new KeyGenerator.RandomStringGenerator(new Random().nextInt(), size));
        logger.info("Bloom filter false positive: {}", fp);
    }

    /**
     * NB: needs to run with -mx1G
     */
    @Test
    public void testConstrained()
    {
        int size = 10 * 1000 * 1000;
        try (IFilter bf = getFilter(size, 0.01))
        {
            double fp = testFalsePositives(bf,
                                           new KeyGenerator.IntGenerator(size),
                                           new KeyGenerator.IntGenerator(size, size * 2));
            logger.info("Bloom filter false positive: {}", fp);
        }
    }

    private static void testConstrained(double targetFp, int elements, int staticBitCount, long ... staticBits)
    {
        for (long bits : staticBits)
        {
            try (IFilter bf = getFilter(elements, targetFp))
            {
                SequentialHashGenerator gen = new SequentialHashGenerator(staticBitCount, bits);
                long[] hash = new long[2];
                for (int i = 0 ; i < elements ; i++)
                {
                    gen.nextHash(hash);
                    bf.add(filterKey(hash[0], hash[1]));
                }
                int falsePositiveCount = 0;
                for (int i = 0 ; i < elements ; i++)
                {
                    gen.nextHash(hash);
                    if (bf.isPresent(filterKey(hash[0], hash[1])))
                        falsePositiveCount++;
                }
                double fp = falsePositiveCount / (double) elements;
                double ratio = fp/targetFp;
                System.out.printf("%.2f, ", ratio);
            }
        }
        System.out.printf("%d elements, %d static bits, %.2f target\n", elements, staticBitCount, targetFp);
    }

    private static IFilter.FilterKey filterKey(final long hash1, final long hash2)
    {
        return new IFilter.FilterKey()
        {
            public void filterHash(long[] dest)
            {
                dest[0] = hash1;
                dest[1] = hash2;
            }
        };
    }

    @Test
    public void testBffp()
    {
        System.out.println("Bloom filter false posiitive");
        long[] staticBits = staticBits(4, 0);
        testConstrained(0.01d, 10 << 20, 0, staticBits);
        testConstrained(0.01d, 1 << 20, 6, staticBits);
        testConstrained(0.01d, 10 << 20, 6, staticBits);
        testConstrained(0.01d, 1 << 19, 10, staticBits);
        testConstrained(0.01d, 1 << 20, 10, staticBits);
        testConstrained(0.01d, 10 << 20, 10, staticBits);
        testConstrained(0.1d, 10 << 20, 0, staticBits);
        testConstrained(0.1d, 10 << 20, 8, staticBits);
        testConstrained(0.1d, 10 << 20, 10, staticBits);
    }

    static long[] staticBits(int random, long ... fixed)
    {
        long[] result = new long[random + fixed.length];
        System.arraycopy(fixed, 0, result, 0, fixed.length);
        for (int i = 0 ; i < random ; i++)
            result[fixed.length + i] = ThreadLocalRandom.current().nextLong();
        return result;
    }

    private static class SequentialHashGenerator
    {
        final long mask;
        final long staticBits;
        int next;
        private SequentialHashGenerator(int staticBitCount, long staticBits) {
            this.mask = -1 >>> staticBitCount;
            this.staticBits = staticBits & ~mask;
        }
        void nextHash(long[] fill)
        {
            MurmurHash.hash3_x64_128(ByteBufferUtil.bytes(next), 0, 4, 0, fill);
            fill[0] &= mask;
            fill[0] |= staticBits;
            next++;
        }
    }

    @Test
    public void timeit()
    {
        int size = 300 * FilterTestHelper.ELEMENTS;
        IFilter bf = getFilter(size, FilterTestHelper.spec.bucketsPerElement);
        double sumfp = 0;
        for (int i = 0; i < 10; i++)
        {
            testFalsePositives(bf,
                               new KeyGenerator.RandomStringGenerator(new Random().nextInt(), size),
                               new KeyGenerator.RandomStringGenerator(new Random().nextInt(), size));

            bf.clear();
        }
        logger.info("Bloom filter mean false positive: {}", sumfp / 10);
    }
}
