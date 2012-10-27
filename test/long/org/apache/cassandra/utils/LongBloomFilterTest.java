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

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LongBloomFilterTest
{
    private static final Logger logger = LoggerFactory.getLogger(LongBloomFilterTest.class);

    /**
     * NB: needs to run with -mx1G
     */
    public void testBigInt(FilterFactory.Type type)
    {
        int size = 10 * 1000 * 1000;
        IFilter bf = FilterFactory.getFilter(size, FilterTestHelper.spec.bucketsPerElement, type, false);
        double fp = FilterTestHelper.testFalsePositives(bf, new KeyGenerator.IntGenerator(size),
                                                            new KeyGenerator.IntGenerator(size, size * 2));
        logger.info("Bloom filter false positive: {}", fp);
    }

    public void testBigRandom(FilterFactory.Type type)
    {
        int size = 10 * 1000 * 1000;
        IFilter bf = FilterFactory.getFilter(size, FilterTestHelper.spec.bucketsPerElement, type, false);
        double fp = FilterTestHelper.testFalsePositives(bf, new KeyGenerator.RandomStringGenerator(new Random().nextInt(), size),
                                                            new KeyGenerator.RandomStringGenerator(new Random().nextInt(), size));
        logger.info("Bloom filter false positive: {}", fp);
    }

    public void timeit(FilterFactory.Type type)
    {
        int size = 300 * FilterTestHelper.ELEMENTS;
        IFilter bf = FilterFactory.getFilter(size, FilterTestHelper.spec.bucketsPerElement, type, false);
        double sumfp = 0;
        for (int i = 0; i < 10; i++)
        {
            FilterTestHelper.testFalsePositives(bf, new KeyGenerator.RandomStringGenerator(new Random().nextInt(), size),
                                                    new KeyGenerator.RandomStringGenerator(new Random().nextInt(), size));

            bf.clear();
        }
        logger.info("Bloom filter mean false positive: {}", sumfp/10);
    }

    @Test
    public void testBigIntMurm2()
    {
        testBigInt(FilterFactory.Type.MURMUR2);
    }

    @Test
    public void testBigRandomMurm2()
    {
        testBigRandom(FilterFactory.Type.MURMUR2);
    }

    @Test
    public void timeitMurm2()
    {
        timeit(FilterFactory.Type.MURMUR2);
    }

    @Test
    public void testBigIntMurm3()
    {
        testBigInt(FilterFactory.Type.MURMUR3);
    }

    @Test
    public void testBigRandomMurm3()
    {
        testBigRandom(FilterFactory.Type.MURMUR3);
    }

    @Test
    public void timeitMurm3()
    {
        timeit(FilterFactory.Type.MURMUR3);
    }
}
