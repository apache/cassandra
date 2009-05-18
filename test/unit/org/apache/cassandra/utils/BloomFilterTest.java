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

import org.junit.Before;
import org.junit.Test;

public class BloomFilterTest
{
    public BloomFilter bf;
    public BloomCalculations.BloomSpecification spec = BloomCalculations.computeBucketsAndK(0.0001);
    static final int ELEMENTS = 10000;

    public BloomFilterTest()
    {
        bf = new BloomFilter(ELEMENTS, spec.bucketsPerElement);
        assert bf != null;
    }

    @Before
    public void clear()
    {
        bf.clear();
    }

    @Test
    public void testOne()
    {
        bf.add("a");
        assert bf.isPresent("a");
        assert !bf.isPresent("b");
    }

    @Test
    public void testFalsePositivesInt()
    {
        FilterTest.testFalsePositives(bf, FilterTest.intKeys(), FilterTest.randomKeys2());
    }

    @Test
    public void testFalsePositivesRandom()
    {
        FilterTest.testFalsePositives(bf, FilterTest.randomKeys(), FilterTest.randomKeys2());
    }

    @Test
    public void testWords()
    {
        if (KeyGenerator.WordGenerator.WORDS == 0)
        {
            return;
        }
        BloomFilter bf2 = new BloomFilter(KeyGenerator.WordGenerator.WORDS / 2, FilterTest.spec.bucketsPerElement);
        int skipEven = KeyGenerator.WordGenerator.WORDS % 2 == 0 ? 0 : 2;
        FilterTest.testFalsePositives(bf2,
                                      new KeyGenerator.WordGenerator(skipEven, 2),
                                      new KeyGenerator.WordGenerator(1, 2));
    }

    @Test
    public void testSerialize() throws IOException
    {
        FilterTest.testSerialize(bf);
    }

    /* TODO move these into a nightly suite (they take 5-10 minutes each)
    @Test
    // run with -mx1G
    public void testBigInt() {
        int size = 100 * 1000 * 1000;
        bf = new BloomFilter(size, FilterTest.spec.bucketsPerElement);
        FilterTest.testFalsePositives(bf,
                                      new KeyGenerator.IntGenerator(size),
                                      new KeyGenerator.IntGenerator(size, size * 2));
    }

    @Test
    public void testBigRandom() {
        int size = 100 * 1000 * 1000;
        bf = new BloomFilter(size, FilterTest.spec.bucketsPerElement);
        FilterTest.testFalsePositives(bf,
                                      new KeyGenerator.RandomStringGenerator(new Random().nextInt(), size),
                                      new KeyGenerator.RandomStringGenerator(new Random().nextInt(), size));
    }

    @Test
    public void timeit() {
        int size = 300 * FilterTest.ELEMENTS;
        bf = new BloomFilter(size, FilterTest.spec.bucketsPerElement);
        for (int i = 0; i < 10; i++) {
            FilterTest.testFalsePositives(bf,
                                          new KeyGenerator.RandomStringGenerator(new Random().nextInt(), size),
                                          new KeyGenerator.RandomStringGenerator(new Random().nextInt(), size));
            bf.clear();
        }
    }
    */
}
