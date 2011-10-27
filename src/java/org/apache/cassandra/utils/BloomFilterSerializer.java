package org.apache.cassandra.utils;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.cassandra.db.DBConstants;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.utils.obs.OpenBitSet;

public class BloomFilterSerializer implements ISerializer<BloomFilter>
{
    public void serialize(BloomFilter bf, DataOutput dos) throws IOException
    {
        int bitLength = bf.bitset.getNumWords();
        int pageSize = bf.bitset.getPageSize();
        int pageCount = bf.bitset.getPageCount();

        dos.writeInt(bf.getHashCount());
        dos.writeInt(bitLength);

        for (int p = 0; p < pageCount; p++)
        {
            long[] bits = bf.bitset.getPage(p);
            for (int i = 0; i < pageSize && bitLength-- > 0; i++)
                dos.writeLong(bits[i]);
        }
    }

    public BloomFilter deserialize(DataInput dis) throws IOException
    {
        int hashes = dis.readInt();
        long bitLength = dis.readInt();
        OpenBitSet bs = new OpenBitSet(bitLength << 6);
        int pageSize = bs.getPageSize();
        int pageCount = bs.getPageCount();

        for (int p = 0; p < pageCount; p++)
        {
            long[] bits = bs.getPage(p);
            for (int i = 0; i < pageSize && bitLength-- > 0; i++)
                bits[i] = dis.readLong();
        }

        return new BloomFilter(hashes, bs);
    }

    /**
     * Calculates a serialized size of the given Bloom Filter
     * @see this.serialize(BloomFilter, DataOutput)
     *
     * @param bf Bloom filter to calculate serialized size
     *
     * @return serialized size of the given bloom filter
     */
    public long serializedSize(BloomFilter bf)
    {
        return DBConstants.intSize // hash count
               + DBConstants.intSize // length
               + bf.bitset.getNumWords() * DBConstants.longSize; // buckets
    }
}
