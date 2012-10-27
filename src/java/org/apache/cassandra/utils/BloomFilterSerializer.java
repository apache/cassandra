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
import java.io.DataOutput;
import java.io.IOException;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.utils.obs.IBitSet;
import org.apache.cassandra.utils.obs.OffHeapBitSet;
import org.apache.cassandra.utils.obs.OpenBitSet;

abstract class BloomFilterSerializer implements ISerializer<BloomFilter>
{
    public void serialize(BloomFilter bf, DataOutput dos) throws IOException
    {
        dos.writeInt(bf.hashCount);
        bf.bitset.serialize(dos);
    }

    public BloomFilter deserialize(DataInput dis) throws IOException
    {
        return deserialize(dis, false);
    }

    public BloomFilter deserialize(DataInput dis, boolean offheap) throws IOException
    {
        int hashes = dis.readInt();
        IBitSet bs = offheap ? OffHeapBitSet.deserialize(dis) : OpenBitSet.deserialize(dis);
        return createFilter(hashes, bs);
    }

    protected abstract BloomFilter createFilter(int hashes, IBitSet bs);

    /**
     * Calculates a serialized size of the given Bloom Filter
     * @see BloomFilterSerializer#serialize(BloomFilter, DataOutput)
     *
     * @param bf Bloom filter to calculate serialized size
     *
     * @return serialized size of the given bloom filter
     */
    public long serializedSize(BloomFilter bf, TypeSizes typeSizes)
    {
        int size = typeSizes.sizeof(bf.hashCount); // hash count
        size += bf.bitset.serializedSize(typeSizes);
        return size;
    }
}
