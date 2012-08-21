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
package org.apache.cassandra.dht;

import java.math.BigInteger;
import java.nio.ByteBuffer;

import org.apache.cassandra.utils.MurmurHash;

/**
 * This class generates a BigIntegerToken using a Murmur3 hash.
 */
public class Murmur3Partitioner extends AbstractHashedPartitioner
{
    protected BigInteger hash(ByteBuffer buffer)
    {
        long[] bufferHash = MurmurHash.hash3_x64_128(buffer, buffer.position(), buffer.remaining(), 0);
        byte[] hashBytes = new byte[16];

        writeLong(bufferHash[0], hashBytes, 0);
        writeLong(bufferHash[1], hashBytes, 8);
        // make sure it's positive, this isn't the same as abs() but doesn't effect distribution
        hashBytes[0] = (byte) (hashBytes[0] & 0x7F);
        return new BigInteger(hashBytes);
    }

    public static void writeLong(long src, byte[] dest, int offset)
    {
        dest[offset] = (byte) (src >> 56);
        dest[offset + 1] = (byte) (src >> 48);
        dest[offset + 2] = (byte) (src >> 40);
        dest[offset + 3] = (byte) (src >> 32);
        dest[offset + 4] = (byte) (src >> 24);
        dest[offset + 5] = (byte) (src >> 16);
        dest[offset + 6] = (byte) (src >> 8);
        dest[offset + 7] = (byte) (src);
    }
}
