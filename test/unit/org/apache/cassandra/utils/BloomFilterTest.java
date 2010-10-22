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
import java.nio.ByteBuffer;

import org.junit.Before;
import org.junit.Test;

public class BloomFilterTest
{
    public BloomFilter bf;

    public BloomFilterTest()
    {
        bf = BloomFilter.getFilter(FilterTest.ELEMENTS, FilterTest.MAX_FAILURE_RATE);
    }

    @Before
    public void clear()
    {
        bf.clear();
    }

    @Test(expected=UnsupportedOperationException.class)
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
        bf.add(ByteBuffer.wrap("a".getBytes()));
        assert bf.isPresent(ByteBuffer.wrap("a".getBytes()));
        assert !bf.isPresent(ByteBuffer.wrap("b".getBytes()));
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
        BloomFilter bf2 = BloomFilter.getFilter(KeyGenerator.WordGenerator.WORDS / 2, FilterTest.MAX_FAILURE_RATE);
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
}
