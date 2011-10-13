/**
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

import java.nio.ByteBuffer;
import java.util.BitSet;

import org.apache.cassandra.io.IVersionedSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LegacyBloomFilter extends Filter
{
    private static final int EXCESS = 20;
    private static final Logger logger = LoggerFactory.getLogger(LegacyBloomFilter.class);
    static LegacyBloomFilterSerializer serializer_ = new LegacyBloomFilterSerializer();

    public static LegacyBloomFilterSerializer serializer()
    {
        return serializer_;
    }

    private BitSet filter_;

    LegacyBloomFilter(int hashes, BitSet filter)
    {
        hashCount = hashes;
        filter_ = filter;
    }

    private static BitSet bucketsFor(long numElements, int bucketsPer)
    {
        long numBits = numElements * bucketsPer + EXCESS;
        return new BitSet((int)Math.min(Integer.MAX_VALUE, numBits));
    }

    /**
     * @return A LegacyBloomFilter with the lowest practical false positive probability
     * for the given number of elements.
     */
    public static LegacyBloomFilter getFilter(long numElements, int targetBucketsPerElem)
    {
        int maxBucketsPerElement = Math.max(1, BloomCalculations.maxBucketsPerElement(numElements));
        int bucketsPerElement = Math.min(targetBucketsPerElem, maxBucketsPerElement);
        if (bucketsPerElement < targetBucketsPerElem)
        {
            logger.warn(String.format("Cannot provide an optimal LegacyBloomFilter for %d elements (%d/%d buckets per element).",
                                      numElements, bucketsPerElement, targetBucketsPerElem));
        }
        BloomCalculations.BloomSpecification spec = BloomCalculations.computeBloomSpec(bucketsPerElement);
        return new LegacyBloomFilter(spec.K, bucketsFor(numElements, spec.bucketsPerElement));
    }

    /**
     * @return The smallest LegacyBloomFilter that can provide the given false positive
     * probability rate for the given number of elements.
     *
     * Asserts that the given probability can be satisfied using this filter.
     */
    public static LegacyBloomFilter getFilter(long numElements, double maxFalsePosProbability)
    {
        assert maxFalsePosProbability <= 1.0 : "Invalid probability";
        int bucketsPerElement = BloomCalculations.maxBucketsPerElement(numElements);
        BloomCalculations.BloomSpecification spec = BloomCalculations.computeBloomSpec(bucketsPerElement, maxFalsePosProbability);
        return new LegacyBloomFilter(spec.K, bucketsFor(numElements, spec.bucketsPerElement));
    }

    public void clear()
    {
        filter_.clear();
    }

    int buckets()
    {
        return filter_.size();
    }

    public boolean isPresent(ByteBuffer key)
    {
        for (int bucketIndex : getHashBuckets(key))
        {
            if (!filter_.get(bucketIndex))
            {
                return false;
            }
        }
        return true;
    }

    /*
     @param key -- value whose hash is used to fill
     the filter_.
     This is a general purpose API.
     */
    public void add(ByteBuffer key)
    {
        for (int bucketIndex : getHashBuckets(key))
        {
            filter_.set(bucketIndex);
        }
    }

    public String toString()
    {
        return filter_.toString();
    }

    int emptyBuckets()
    {
        int n = 0;
        for (int i = 0; i < buckets(); i++)
        {
            if (!filter_.get(i))
            {
                n++;
            }
        }
        return n;
    }

    /** @return a LegacyBloomFilter that always returns a positive match, for testing */
    public static LegacyBloomFilter alwaysMatchingBloomFilter()
    {
        BitSet set = new BitSet(64);
        set.set(0, 64);
        return new LegacyBloomFilter(1, set);
    }

    public int[] getHashBuckets(ByteBuffer key)
    {
        return LegacyBloomFilter.getHashBuckets(key, hashCount, buckets());
    }

    // Murmur is faster than an SHA-based approach and provides as-good collision
    // resistance.  The combinatorial generation approach described in
    // http://www.eecs.harvard.edu/~kirsch/pubs/bbbf/esa06.pdf
    // does prove to work in actual tests, and is obviously faster
    // than performing further iterations of murmur.
    static int[] getHashBuckets(ByteBuffer b, int hashCount, int max)
    {
        int[] result = new int[hashCount];
        int hash1 = MurmurHash.hash32(b, b.position(), b.remaining(), 0);
        int hash2 = MurmurHash.hash32(b, b.position(), b.remaining(), hash1);
        for (int i = 0; i < hashCount; i++)
        {
            result[i] = Math.abs((hash1 + i * hash2) % max);
        }
        return result;
    }

    public BitSet getBitSet(){
      return filter_;
    }
}