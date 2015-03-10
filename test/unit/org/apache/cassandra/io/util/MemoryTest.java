/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.io.util;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Test;

import junit.framework.Assert;
import org.apache.cassandra.utils.memory.MemoryUtil;

public class MemoryTest
{

    @Test
    public void testByteBuffers()
    {
        byte[] bytes = new byte[1000];
        ThreadLocalRandom.current().nextBytes(bytes);
        final Memory memory = Memory.allocate(bytes.length);
        memory.setBytes(0, bytes, 0, bytes.length);
        ByteBuffer canon = ByteBuffer.wrap(bytes).order(ByteOrder.nativeOrder());
        test(canon, memory);
        memory.setBytes(0, new byte[1000], 0, 1000);
        memory.setBytes(0, canon.duplicate());
        test(canon, memory);
    }

    private static void test(ByteBuffer canon, Memory memory)
    {
        ByteBuffer hollow = MemoryUtil.getHollowDirectByteBuffer();
        test(canon, hollow, memory, 0, 1000);
        test(canon, hollow, memory, 33, 100);
        test(canon, hollow, memory, 77, 77);
        test(canon, hollow, memory, 903, 96);
    }

    private static void test(ByteBuffer canon, ByteBuffer hollow, Memory memory, int offset, int length)
    {
        canon = canon.duplicate();
        canon.position(offset).limit(offset + length);
        canon = canon.slice().order(ByteOrder.nativeOrder());
        test(canon, memory.asByteBuffer(offset, length));
        memory.setByteBuffer(hollow, offset, length);
        test(canon, hollow);
    }

    private static void test(ByteBuffer canon, ByteBuffer test)
    {
        Assert.assertEquals(canon, test);
        for (int i = 0 ; i <= canon.limit() - 4 ; i += 4)
            Assert.assertEquals(canon.getInt(i), test.getInt(i));
        for (int i = 0 ; i <= canon.limit() - 8 ; i += 8)
            Assert.assertEquals(canon.getLong(i), test.getLong(i));
    }

}
