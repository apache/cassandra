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

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.obs.IBitSet;
import org.apache.cassandra.utils.obs.OffHeapBitSet;
import org.apache.cassandra.utils.obs.OpenBitSet;

final class BloomFilterSerializer
{
    private BloomFilterSerializer()
    {
    }

    public static void serialize(BloomFilter bf, DataOutputPlus out) throws IOException
    {
        out.writeInt(bf.hashCount);
        bf.bitset.serialize(out);
    }

    public static BloomFilter deserialize(DataInput in, boolean oldBfHashOrder) throws IOException
    {
        return deserialize(in, false, oldBfHashOrder);
    }

    @SuppressWarnings("resource")
    public static BloomFilter deserialize(DataInput in, boolean offheap, boolean oldBfHashOrder) throws IOException
    {
        int hashes = in.readInt();
        IBitSet bs = offheap ? OffHeapBitSet.deserialize(in) : OpenBitSet.deserialize(in);

        return new BloomFilter(hashes, bs, oldBfHashOrder);
    }

    /**
     * Calculates a serialized size of the given Bloom Filter
     * @param bf Bloom filter to calculate serialized size
     * @see org.apache.cassandra.io.ISerializer#serialize(Object, org.apache.cassandra.io.util.DataOutputPlus)
     *
     * @return serialized size of the given bloom filter
     */
    public static long serializedSize(BloomFilter bf)
    {
        int size = TypeSizes.sizeof(bf.hashCount); // hash count
        size += bf.bitset.serializedSize();
        return size;
    }
}
