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

import com.google.common.hash.Hasher;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HashingUtilsTest
{
    private static final Logger logger = LoggerFactory.getLogger(HashingUtilsTest.class);

    @Test
    public void hashEmptyBytes() throws Exception {
        Hasher hasher = HashingUtils.CURRENT_HASH_FUNCTION.newHasher();
        HashingUtils.updateBytes(hasher, ByteBuffer.wrap(new byte[]{}));
        String md5HashInHexOfEmptyByteArray = ByteBufferUtil.bytesToHex(ByteBuffer.wrap(hasher.hash().asBytes()));
        Assert.assertEquals("d41d8cd98f00b204e9800998ecf8427e", md5HashInHexOfEmptyByteArray);
    }

    @Test
    public void hashBytesFromTinyDirectByteBuffer() throws Exception {
        Hasher hasher = HashingUtils.CURRENT_HASH_FUNCTION.newHasher();
        ByteBuffer directBuf = ByteBuffer.allocateDirect(8);
        directBuf.putLong(5L);
        directBuf.position(0);
        HashingUtils.updateBytes(hasher, directBuf);

        String md5HashInHexOfDirectByteBuffer = ByteBufferUtil.bytesToHex(ByteBuffer.wrap(hasher.hash().asBytes()));
        Assert.assertEquals("aaa07454fa93ed2d37b4c5da9f2f87fd", md5HashInHexOfDirectByteBuffer);
    }

    @Test
    public void hashBytesFromLargerDirectByteBuffer() throws Exception {
        Hasher hasher = HashingUtils.CURRENT_HASH_FUNCTION.newHasher();
        ByteBuffer directBuf = ByteBuffer.allocateDirect(1024);
        for (int i = 0; i < 100; i++) {
            directBuf.putInt(i);
        }
        directBuf.position(0);
        HashingUtils.updateBytes(hasher, directBuf);

        String md5HashInHexOfDirectByteBuffer = ByteBufferUtil.bytesToHex(ByteBuffer.wrap(hasher.hash().asBytes()));
        Assert.assertEquals("daf10ea8894783b1b2618309494cde21", md5HashInHexOfDirectByteBuffer);
    }

    @Test
    public void hashBytesFromTinyOnHeapByteBuffer() throws Exception {
        Hasher hasher = HashingUtils.CURRENT_HASH_FUNCTION.newHasher();
        ByteBuffer onHeapBuf = ByteBuffer.allocate(8);
        onHeapBuf.putLong(5L);
        onHeapBuf.position(0);
        HashingUtils.updateBytes(hasher, onHeapBuf);

        String md5HashInHexOfDirectByteBuffer = ByteBufferUtil.bytesToHex(ByteBuffer.wrap(hasher.hash().asBytes()));
        Assert.assertEquals("aaa07454fa93ed2d37b4c5da9f2f87fd", md5HashInHexOfDirectByteBuffer);
    }

    @Test
    public void hashBytesFromLargerOnHeapByteBuffer() throws Exception {
        Hasher hasher = HashingUtils.CURRENT_HASH_FUNCTION.newHasher();
        ByteBuffer onHeapBuf = ByteBuffer.allocate(1024);
        for (int i = 0; i < 100; i++) {
            onHeapBuf.putInt(i);
        }
        onHeapBuf.position(0);
        HashingUtils.updateBytes(hasher, onHeapBuf);

        String md5HashInHexOfDirectByteBuffer = ByteBufferUtil.bytesToHex(ByteBuffer.wrap(hasher.hash().asBytes()));
        Assert.assertEquals("daf10ea8894783b1b2618309494cde21", md5HashInHexOfDirectByteBuffer);
    }
}
