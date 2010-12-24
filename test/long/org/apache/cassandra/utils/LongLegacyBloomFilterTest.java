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

import java.io.IOException;
import java.util.Random;

import org.junit.Test;

public class LongLegacyBloomFilterTest
{
    public LegacyBloomFilter bf;

    /**
     * NB: needs to run with -mx1G
     */
    @Test
    public void testBigInt()
    {
        int size = 10 * 1000 * 1000;
        bf = LegacyBloomFilter.getFilter(size, FilterTestHelper.spec.bucketsPerElement);
        FilterTestHelper.testFalsePositives(bf,
                                            new KeyGenerator.IntGenerator(size),
                                            new KeyGenerator.IntGenerator(size, size * 2));
    }

    @Test
    public void testBigRandom()
    {
        int size = 10 * 1000 * 1000;
        bf = LegacyBloomFilter.getFilter(size, FilterTestHelper.spec.bucketsPerElement);
        FilterTestHelper.testFalsePositives(bf,
                                            new KeyGenerator.RandomStringGenerator(new Random().nextInt(), size),
                                            new KeyGenerator.RandomStringGenerator(new Random().nextInt(), size));
    }

    @Test
    public void timeit()
    {
        int size = 300 * FilterTestHelper.ELEMENTS;
        bf = LegacyBloomFilter.getFilter(size, FilterTestHelper.spec.bucketsPerElement);
        for (int i = 0; i < 10; i++)
        {
            FilterTestHelper.testFalsePositives(bf,
                                                new KeyGenerator.RandomStringGenerator(new Random().nextInt(), size),
                                                new KeyGenerator.RandomStringGenerator(new Random().nextInt(), size));
            bf.clear();
        }
    }
}
