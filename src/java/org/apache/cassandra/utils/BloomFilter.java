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

import java.io.IOException;

import com.google.common.annotations.VisibleForTesting;

import io.netty.util.concurrent.FastThreadLocal;
import net.nicoulaj.compilecommand.annotations.Inline;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.concurrent.WrappedSharedCloseable;
import org.apache.cassandra.utils.obs.IBitSet;

public class BloomFilter extends WrappedSharedCloseable implements IFilter
{
    private final static FastThreadLocal<long[]> reusableIndexes = new FastThreadLocal<long[]>()
    {
        @Override
        protected long[] initialValue()
        {
            return new long[21];
        }
    };

    public final IBitSet bitset;
    public final int hashCount;

    BloomFilter(int hashCount, IBitSet bitset)
    {
        super(bitset);
        this.hashCount = hashCount;
        this.bitset = bitset;
    }

    private BloomFilter(BloomFilter copy)
    {
        super(copy);
        this.hashCount = copy.hashCount;
        this.bitset = copy.bitset;
    }

    public long serializedSize(boolean old)
    {
        return BloomFilterSerializer.forVersion(old).serializedSize(this);
    }

    @Override
    public void serialize(DataOutputStreamPlus out, boolean old) throws IOException
    {
        BloomFilterSerializer.forVersion(old).serialize(this, out);
    }

    // Murmur is faster than an SHA-based approach and provides as-good collision
    // resistance.  The combinatorial generation approach described in
    // https://www.eecs.harvard.edu/~michaelm/postscripts/tr-02-05.pdf
    // does prove to work in actual tests, and is obviously faster
    // than performing further iterations of murmur.

    // tests ask for ridiculous numbers of hashes so here is a special case for them
    // rather than using the threadLocal like we do in production
    @VisibleForTesting
    public long[] getHashBuckets(FilterKey key, int hashCount, long max)
    {
        long[] hash = new long[2];
        key.filterHash(hash);
        long[] indexes = new long[hashCount];
        setIndexes(hash[1], hash[0], hashCount, max, indexes);
        return indexes;
    }

    // note that this method uses the threadLocal that may be longer than hashCount
    // to avoid generating a lot of garbage since stack allocation currently does not support stores
    // (CASSANDRA-6609).  it returns the array so that the caller does not need to perform
    // a second threadlocal lookup.
    @Inline
    private long[] indexes(FilterKey key)
    {
        // we use the same array both for storing the hash result, and for storing the indexes we return,
        // so that we do not need to allocate two arrays.
        long[] indexes = reusableIndexes.get();

        key.filterHash(indexes);
        setIndexes(indexes[1], indexes[0], hashCount, bitset.capacity(), indexes);
        return indexes;
    }

    @Inline
    private void setIndexes(long base, long inc, int count, long max, long[] results)
    {
        for (int i = 0; i < count; i++)
        {
            results[i] = FBUtilities.abs(base % max);
            base += inc;
        }
    }

    @Override
    public void add(FilterKey key)
    {
        long[] indexes = indexes(key);
        for (int i = 0; i < hashCount; i++)
        {
            bitset.set(indexes[i]);
        }
    }

    @Override
    public final boolean isPresent(FilterKey key)
    {
        long[] indexes = indexes(key);
        for (int i = 0; i < hashCount; i++)
        {
            if (!bitset.get(indexes[i]))
            {
                return false;
            }
        }
        return true;
    }

    @Override
    public void clear()
    {
        bitset.clear();
    }

    @Override
    public BloomFilter sharedCopy()
    {
        return new BloomFilter(this);
    }

    @Override
    public long offHeapSize()
    {
        return bitset.offHeapSize();
    }

    @Override
    public boolean isInformative()
    {
        return bitset.offHeapSize() > 0;
    }

    @Override
    public String toString()
    {
        return "BloomFilter[hashCount=" + hashCount + ";capacity=" + bitset.capacity() + ']';
    }

    @Override
    public void addTo(Ref.IdentityCollection identities)
    {
        super.addTo(identities);
        bitset.addTo(identities);
    }
}
