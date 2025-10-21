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

package org.apache.cassandra.utils.memory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.junit.Assert;
import org.junit.Test;

public class NativeEndianMemoryUtilTest
{
    private static final int TEST_BUFFER_LENGTH = 8;
    private final ByteBuffer directBuffer = ByteBuffer.allocateDirect(TEST_BUFFER_LENGTH);
    {
        directBuffer.order(ByteOrder.nativeOrder());
    }
    private final long address = NativeEndianMemoryUtil.getAddress(directBuffer);

    @Test
    public void testGetSetLong()
    {
        long originalValue = 0xAB_CD_EF_12_34_56_78_90L;
        directBuffer.putLong(originalValue);
        Assert.assertEquals(originalValue, NativeEndianMemoryUtil.getLong(address));

        directBuffer.rewind();
        directBuffer.putLong(0);
        NativeEndianMemoryUtil.setLong(address, originalValue);

        Assert.assertEquals(originalValue, directBuffer.getLong(0));
        Assert.assertEquals(originalValue, NativeEndianMemoryUtil.getLong(address));

    }

    @Test
    public void testGetSetInt()
    {
        int originalValue = 0xAB_CD_EF_12;
        directBuffer.putInt(originalValue);
        Assert.assertEquals(originalValue, NativeEndianMemoryUtil.getInt(address));

        directBuffer.rewind();
        directBuffer.putInt(0);
        NativeEndianMemoryUtil.setInt(address, originalValue);

        Assert.assertEquals(originalValue, directBuffer.getInt(0));
        Assert.assertEquals(originalValue, NativeEndianMemoryUtil.getInt(address));

    }

    @Test
    public void testGetSetUnsighedShort()
    {
        short originalValue = (short) 0xAB_CD;
        directBuffer.putShort(originalValue);
        Assert.assertEquals(originalValue & 0xffff, NativeEndianMemoryUtil.getUnsignedShort(address));

        directBuffer.rewind();
        directBuffer.putShort((short) 0);
        NativeEndianMemoryUtil.setShort(address, originalValue);

        Assert.assertEquals(originalValue, directBuffer.getShort(0));
        Assert.assertEquals(originalValue & 0xffff, NativeEndianMemoryUtil.getUnsignedShort(address));
    }

    @Test
    public void testGetSetLongByBytes()
    {
        long originalValue = 0xAB_CD_EF_12_34_56_78_90L;
        directBuffer.putLong(originalValue);
        Assert.assertEquals(originalValue, NativeEndianMemoryUtil.getLongByByte(address));

        directBuffer.rewind();
        directBuffer.putLong(0);
        NativeEndianMemoryUtil.putLongByByte(address, originalValue);

        Assert.assertEquals(originalValue, directBuffer.getLong(0));
        Assert.assertEquals(originalValue, NativeEndianMemoryUtil.getLongByByte(address));

    }

    @Test
    public void testGetSetIntByBytes()
    {
        int originalValue = 0xAB_CD_EF_12;
        directBuffer.putInt(originalValue);
        Assert.assertEquals(originalValue, NativeEndianMemoryUtil.getIntByByte(address));

        directBuffer.rewind();
        directBuffer.putInt(0);
        NativeEndianMemoryUtil.putIntByByte(address, originalValue);

        Assert.assertEquals(originalValue, directBuffer.getInt(0));
        Assert.assertEquals(originalValue, NativeEndianMemoryUtil.getIntByByte(address));

    }

    @Test
    public void testGetSetShortByBytes()
    {
        short originalValue = (short) 0xAB_CD;
        directBuffer.putShort(originalValue);
        Assert.assertEquals(originalValue, NativeEndianMemoryUtil.getShortByByte(address));

        directBuffer.rewind();
        directBuffer.putShort((short) 0);
        NativeEndianMemoryUtil.putShortByByte(address, originalValue);

        Assert.assertEquals(originalValue, directBuffer.getShort(0));
        Assert.assertEquals(originalValue, NativeEndianMemoryUtil.getShortByByte(address));
    }


    @Test
    public void testGetHollowDirectByteBuffer()
    {
        ByteBuffer byteBuffer = NativeEndianMemoryUtil.getHollowDirectByteBuffer();
        Assert.assertEquals(directBuffer.getClass(), byteBuffer.getClass());
        Assert.assertEquals(ByteOrder.nativeOrder(), byteBuffer.order());
    }

    @Test
    public void testGetByteBuffer()
    {
        ByteBuffer byteBuffer = NativeEndianMemoryUtil.getByteBuffer(address, TEST_BUFFER_LENGTH);
        Assert.assertEquals(directBuffer.getClass(), byteBuffer.getClass());
        Assert.assertEquals(ByteOrder.nativeOrder(), byteBuffer.order());
        Assert.assertEquals(TEST_BUFFER_LENGTH, byteBuffer.capacity());
        Assert.assertEquals(0, byteBuffer.position());
    }
}
