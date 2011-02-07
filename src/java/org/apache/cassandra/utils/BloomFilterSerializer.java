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

import org.apache.cassandra.io.ICompactSerializer2;
import org.apache.cassandra.utils.obs.OpenBitSet;

class BloomFilterSerializer implements ICompactSerializer2<BloomFilter>
{
    public void serialize(BloomFilter bf, DataOutput dos) throws IOException
    {
        long[] bits = bf.bitset.getBits();
        int bitLength = bits.length;

        dos.writeInt(bf.getHashCount());
        dos.writeInt(bitLength);

        for (int i = 0; i < bitLength; i++)
            dos.writeLong(bits[i]);
    }

    public BloomFilter deserialize(DataInput dis) throws IOException
    {
        int hashes = dis.readInt();
        int bitLength = dis.readInt();
        long[] bits = new long[bitLength];
        for (int i = 0; i < bitLength; i++)
            bits[i] = dis.readLong();
        OpenBitSet bs = new OpenBitSet(bits, bitLength);
        return new BloomFilter(hashes, bs);
    }
}


