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

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IGenericSerializer;
import org.apache.cassandra.io.util.DataInputPlus.DataInputStreamPlus;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.utils.obs.IBitSet;
import org.apache.cassandra.utils.obs.OffHeapBitSet;

public final class BloomFilterSerializer implements IGenericSerializer<BloomFilter, DataInputStreamPlus, DataOutputStreamPlus>
{
    public final static BloomFilterSerializer newFormatInstance = new BloomFilterSerializer(false);
    public final static BloomFilterSerializer oldFormatInstance = new BloomFilterSerializer(true);

    private final boolean oldFormat;

    private <T> BloomFilterSerializer(boolean oldFormat)
    {
        this.oldFormat = oldFormat;
    }

    public static BloomFilterSerializer forVersion(boolean oldSerializationFormat)
    {
        if (oldSerializationFormat)
            return oldFormatInstance;

        return newFormatInstance;
    }

    @Override
    public void serialize(BloomFilter bf, DataOutputStreamPlus out) throws IOException
    {
        assert !oldFormat : "Filter should not be serialized in old format";
        out.writeInt(bf.hashCount);
        bf.bitset.serialize(out);
    }

    /**
     * Calculates a serialized size of the given Bloom Filter
     *
     * @param bf Bloom filter to calculate serialized size
     * @return serialized size of the given bloom filter
     * @see org.apache.cassandra.io.ISerializer#serialize(Object, org.apache.cassandra.io.util.DataOutputPlus)
     */
    @Override
    public long serializedSize(BloomFilter bf)
    {
        int size = TypeSizes.sizeof(bf.hashCount); // hash count
        size += bf.bitset.serializedSize();
        return size;
    }

    @Override
    public BloomFilter deserialize(DataInputStreamPlus in) throws IOException
    {
        int hashes = in.readInt();
        IBitSet bs = OffHeapBitSet.deserialize(in, oldFormat);

        return new BloomFilter(hashes, bs);
    }
}
