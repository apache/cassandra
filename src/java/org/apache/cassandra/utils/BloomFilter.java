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

import java.nio.ByteBuffer;

import org.apache.cassandra.utils.obs.OpenBitSet;

public abstract class BloomFilter extends Filter
{
    private static final int EXCESS = 20;

    public final OpenBitSet bitset;

    BloomFilter(int hashes, long numElements, int bucketsPer)
    {
        hashCount = hashes;
        bitset = new OpenBitSet(numElements * bucketsPer + EXCESS);
    }

    BloomFilter(int hashes, OpenBitSet bitset)
    {
        this.hashCount = hashes;
        this.bitset = bitset;
    }

    private long[] getHashBuckets(ByteBuffer key)
    {
        return getHashBuckets(key, hashCount, bitset.size());
    }

    protected abstract long[] hash(ByteBuffer b, int position, int remaining, long seed);

    // Murmur is faster than an SHA-based approach and provides as-good collision
    // resistance.  The combinatorial generation approach described in
    // http://www.eecs.harvard.edu/~kirsch/pubs/bbbf/esa06.pdf
    // does prove to work in actual tests, and is obviously faster
    // than performing further iterations of murmur.
    long[] getHashBuckets(ByteBuffer b, int hashCount, long max)
    {
        long[] result = new long[hashCount];
        long[] hash = this.hash(b, b.position(), b.remaining(), 0L);
        for (int i = 0; i < hashCount; ++i)
        {
            result[i] = Math.abs((hash[0] + (long)i * hash[1]) % max);
        }
        return result;
    }

    public void add(ByteBuffer key)
    {
        for (long bucketIndex : getHashBuckets(key))
        {
            bitset.set(bucketIndex);
        }
    }

    public boolean isPresent(ByteBuffer key)
    {
      for (long bucketIndex : getHashBuckets(key))
      {
          if (!bitset.get(bucketIndex))
          {
              return false;
          }
      }
      return true;
    }

    public void clear()
    {
        bitset.clear(0, bitset.size());
    }
}
