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

package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import java.util.Arrays;

import com.google.common.hash.Hashing;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Hex;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUIDAsBytes;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class DigestTest
{
    private static final Logger logger = LoggerFactory.getLogger(DigestTest.class);

    @Test
    public void hashEmptyBytes() throws Exception {
        Assert.assertArrayEquals(Hex.hexToBytes("d41d8cd98f00b204e9800998ecf8427e"),
                                 Digest.forReadResponse().update(ByteBufferUtil.EMPTY_BYTE_BUFFER).digest());
    }

    @Test
    public void hashBytesFromTinyDirectByteBuffer() throws Exception {
        ByteBuffer directBuf = ByteBuffer.allocateDirect(8);
        directBuf.putLong(5L).position(0);
        directBuf.position(0);
        assertArrayEquals(Hex.hexToBytes("aaa07454fa93ed2d37b4c5da9f2f87fd"),
                                         Digest.forReadResponse().update(directBuf).digest());
    }

    @Test
    public void hashBytesFromLargerDirectByteBuffer() throws Exception {
        ByteBuffer directBuf = ByteBuffer.allocateDirect(1024);
        for (int i = 0; i < 100; i++) {
            directBuf.putInt(i);
        }
        directBuf.position(0);
        assertArrayEquals(Hex.hexToBytes("daf10ea8894783b1b2618309494cde21"),
                          Digest.forReadResponse().update(directBuf).digest());
    }

    @Test
    public void hashBytesFromTinyOnHeapByteBuffer() throws Exception {
        ByteBuffer onHeapBuf = ByteBuffer.allocate(8);
        onHeapBuf.putLong(5L);
        onHeapBuf.position(0);
        assertArrayEquals(Hex.hexToBytes("aaa07454fa93ed2d37b4c5da9f2f87fd"),
                          Digest.forReadResponse().update(onHeapBuf).digest());
    }

    @Test
    public void hashBytesFromLargerOnHeapByteBuffer() throws Exception {
        ByteBuffer onHeapBuf = ByteBuffer.allocate(1024);
        for (int i = 0; i < 100; i++) {
            onHeapBuf.putInt(i);
        }
        onHeapBuf.position(0);
        assertArrayEquals(Hex.hexToBytes("daf10ea8894783b1b2618309494cde21"),
                          Digest.forReadResponse().update(onHeapBuf).digest());
    }

    @Test
    public void testValidatorDigest()
    {
        Digest[] digests = new Digest[]
                           {
                           Digest.forValidator(),
                           new Digest(Hashing.murmur3_128(1000).newHasher()),
                           new Digest(Hashing.murmur3_128(2000).newHasher())
                           };
        byte [] random = nextTimeUUIDAsBytes();

        for (Digest digest : digests)
        {
            digest.updateWithByte((byte) 33)
                  .update(random, 0, random.length)
                  .update(ByteBuffer.wrap(random))
                  .update(random, 0, 3)
                  .updateWithBoolean(false)
                  .updateWithInt(77)
                  .updateWithLong(101);
        }

        long len = Byte.BYTES
                   + random.length * 2 // both the byte[] and the ByteBuffer
                   + 3 // 3 bytes from the random byte[]
                   + Byte.BYTES
                   + Integer.BYTES
                   + Long.BYTES;

        assertEquals(len, digests[0].inputBytes());
        byte[] h = digests[0].digest();
        assertArrayEquals(digests[1].digest(), Arrays.copyOfRange(h, 0, 16));
        assertArrayEquals(digests[2].digest(), Arrays.copyOfRange(h, 16, 32));
    }

}
