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
package org.apache.cassandra.utils;

import java.io.DataInput;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.obs.IBitSet;
import org.apache.cassandra.utils.obs.OffHeapBitSet;
import org.apache.cassandra.utils.obs.OpenBitSet;

public class FilterFactory
{
    public static final IFilter AlwaysPresent = new AlwaysPresentFilter();

    private static final Logger logger = LoggerFactory.getLogger(FilterFactory.class);
    private static final long BITSET_EXCESS = 20;

    public static void serialize(IFilter bf, DataOutputPlus output) throws IOException
    {
        BloomFilterSerializer.serialize((BloomFilter) bf, output);
    }

    public static IFilter deserialize(DataInput input, boolean offheap, boolean oldBfHashOrder) throws IOException
    {
        return BloomFilterSerializer.deserialize(input, offheap, oldBfHashOrder);
    }

    /**
     * @return A BloomFilter with the lowest practical false positive
     *         probability for the given number of elements.
     */
    public static IFilter getFilter(long numElements, int targetBucketsPerElem, boolean offheap, boolean oldBfHashOrder)
    {
        int maxBucketsPerElement = Math.max(1, BloomCalculations.maxBucketsPerElement(numElements));
        int bucketsPerElement = Math.min(targetBucketsPerElem, maxBucketsPerElement);
        if (bucketsPerElement < targetBucketsPerElem)
        {
            logger.warn("Cannot provide an optimal BloomFilter for {} elements ({}/{} buckets per element).", numElements, bucketsPerElement, targetBucketsPerElem);
        }
        BloomCalculations.BloomSpecification spec = BloomCalculations.computeBloomSpec(bucketsPerElement);
        return createFilter(spec.K, numElements, spec.bucketsPerElement, offheap, oldBfHashOrder);
    }

    /**
     * @return The smallest BloomFilter that can provide the given false
     *         positive probability rate for the given number of elements.
     *
     *         Asserts that the given probability can be satisfied using this
     *         filter.
     */
    public static IFilter getFilter(long numElements, double maxFalsePosProbability, boolean offheap, boolean oldBfHashOrder)
    {
        assert maxFalsePosProbability <= 1.0 : "Invalid probability";
        if (maxFalsePosProbability == 1.0)
            return new AlwaysPresentFilter();
        int bucketsPerElement = BloomCalculations.maxBucketsPerElement(numElements);
        BloomCalculations.BloomSpecification spec = BloomCalculations.computeBloomSpec(bucketsPerElement, maxFalsePosProbability);
        return createFilter(spec.K, numElements, spec.bucketsPerElement, offheap, oldBfHashOrder);
    }

    @SuppressWarnings("resource")
    private static IFilter createFilter(int hash, long numElements, int bucketsPer, boolean offheap, boolean oldBfHashOrder)
    {
        long numBits = (numElements * bucketsPer) + BITSET_EXCESS;
        IBitSet bitset = offheap ? new OffHeapBitSet(numBits) : new OpenBitSet(numBits);
        return new BloomFilter(hash, bitset, oldBfHashOrder);
    }
}
