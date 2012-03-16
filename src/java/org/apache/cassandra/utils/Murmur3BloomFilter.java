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

import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.utils.obs.OpenBitSet;

public class Murmur3BloomFilter extends BloomFilter
{
    public static final ISerializer<BloomFilter> serializer = new Murmur3BloomFilterSerializer();

    Murmur3BloomFilter(int hashes, long numElements, int bucketsPer)
    {
        super(hashes, numElements, bucketsPer);
    }

    private Murmur3BloomFilter(int hashes, OpenBitSet bs)
    {
        super(hashes, bs);
    }

    protected long[] hash(ByteBuffer b, int position, int remaining, long seed)
    {
        return MurmurHash.hash3_x64_128(b, b.position(), b.remaining(), seed);
    }

    private static class Murmur3BloomFilterSerializer extends BloomFilterSerializer
    {
        protected BloomFilter createFilter(int hashes, OpenBitSet bs)
        {
            return new Murmur3BloomFilter(hashes, bs);
        }
    }
}