package org.apache.cassandra.db.marshal;
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


import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;
import java.util.UUID;

import org.apache.cassandra.utils.ByteBufferUtil;

import org.junit.Test;

public class TypeCompareTest
{
    @Test
    public void testAscii()
    {
        AsciiType comparator = new AsciiType();
        assert comparator.compare(ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.bytes("asdf")) < 0;
        assert comparator.compare(ByteBufferUtil.bytes("asdf"), ByteBufferUtil.EMPTY_BYTE_BUFFER) > 0;
        assert comparator.compare(ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.EMPTY_BYTE_BUFFER) == 0;
        assert comparator.compare(ByteBufferUtil.bytes("z"), ByteBufferUtil.bytes("a")) > 0;
        assert comparator.compare(ByteBufferUtil.bytes("a"), ByteBufferUtil.bytes("z")) < 0;
        assert comparator.compare(ByteBufferUtil.bytes("asdf"), ByteBufferUtil.bytes("asdf")) == 0;
        assert comparator.compare(ByteBufferUtil.bytes("asdz"), ByteBufferUtil.bytes("asdf")) > 0;
    }

    @Test
    public void testBytes()
    {
        BytesType comparator = new BytesType();
        assert comparator.compare(ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.bytes("asdf")) < 0;
        assert comparator.compare(ByteBufferUtil.bytes("asdf"), ByteBufferUtil.EMPTY_BYTE_BUFFER) > 0;
        assert comparator.compare(ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.EMPTY_BYTE_BUFFER) == 0;
        assert comparator.compare(ByteBufferUtil.bytes("z"), ByteBufferUtil.bytes("a")) > 0;
        assert comparator.compare(ByteBufferUtil.bytes("a"), ByteBufferUtil.bytes("z")) < 0;
        assert comparator.compare(ByteBufferUtil.bytes("asdf"), ByteBufferUtil.bytes("asdf")) == 0;
        assert comparator.compare(ByteBufferUtil.bytes("asdz"), ByteBufferUtil.bytes("asdf")) > 0;
    }

    @Test
    public void testUTF8()
    {
        UTF8Type comparator = new UTF8Type();
        assert comparator.compare(ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.bytes("asdf")) < 0;
        assert comparator.compare(ByteBufferUtil.bytes("asdf"), ByteBufferUtil.EMPTY_BYTE_BUFFER) > 0;
        assert comparator.compare(ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.EMPTY_BYTE_BUFFER) == 0;
        assert comparator.compare(ByteBufferUtil.bytes("z"), ByteBufferUtil.bytes("a")) > 0;
        assert comparator.compare(ByteBufferUtil.bytes("z"), ByteBufferUtil.bytes("z")) == 0;
        assert comparator.compare(ByteBufferUtil.bytes("a"), ByteBufferUtil.bytes("z")) < 0;
    }

    @Test
    public void testLong()
    {
        Random rng = new Random();
        ByteBuffer[] data = new ByteBuffer[1000];
        for (int i = 0; i < data.length; i++)
        {
            data[i] = ByteBuffer.allocate(8);
            rng.nextBytes(data[i].array());
        }

        Arrays.sort(data, LongType.instance);

        for (int i = 1; i < data.length; i++)
        {

            long l0 = data[i - 1].getLong(data[i - 1].position());
            long l1 = data[i].getLong(data[i].position());
            assert l0 <= l1;
        }
    }

    @Test
    public void testByte()
    {
        Random rng = new Random();
        ByteBuffer[] data = new ByteBuffer[Byte.MAX_VALUE];
        for (int i = 0; i < data.length; i++)
        {
            data[i] = ByteBuffer.allocate(1);
            rng.nextBytes(data[i].array());
        }

        Arrays.sort(data, ByteType.instance);

        for (int i = 1; i < data.length; i++)
        {
            byte b0 = data[i - 1].get(data[i - 1].position());
            byte b1 = data[i].get(data[i].position());
            assert b0 <= b1;
        }
    }

    @Test
    public void testShort()
    {
        Random rng = new Random();
        ByteBuffer[] data = new ByteBuffer[1000];
        for (int i = 0; i < data.length; i++)
        {
            data[i] = ByteBuffer.allocate(2);
            rng.nextBytes(data[i].array());
        }

        Arrays.sort(data, ShortType.instance);

        for (int i = 1; i < data.length; i++)
        {
            short s0 = data[i - 1].getShort(data[i - 1].position());
            short s1 = data[i].getShort(data[i].position());
            assert s0 <= s1;
        }
    }

    @Test
    public void testInt()
    {
        Random rng = new Random();
        ByteBuffer[] data = new ByteBuffer[1000];
        for (int i = 0; i < data.length; i++)
        {
            data[i] = ByteBuffer.allocate(4);
            rng.nextBytes(data[i].array());
        }

        Arrays.sort(data, Int32Type.instance);

        for (int i = 1; i < data.length; i++)
        {

            int l0 = data[i - 1].getInt(data[i - 1].position());
            int l1 = data[i].getInt(data[i].position());
            assert l0 <= l1;
        }
    }

    @Test
    public void testTimeUUID()
    {
        // two different UUIDs w/ the same timestamp
        UUID uuid1 = UUID.fromString("1077e700-c7f2-11de-86d5-f5bcc793a028");
        byte[] bytes1 = new byte[16];
        ByteBuffer bb1 = ByteBuffer.wrap(bytes1);
        bb1.putLong(uuid1.getMostSignificantBits());  bb1.putLong(uuid1.getLeastSignificantBits());

        UUID uuid2 = UUID.fromString("1077e700-c7f2-11de-982e-6fad363d5f29");
        byte[] bytes2 = new byte[16];
        ByteBuffer bb2 = ByteBuffer.wrap(bytes2);
        bb2.putLong(uuid2.getMostSignificantBits());  bb2.putLong(uuid2.getLeastSignificantBits());

        assert new TimeUUIDType().compare(ByteBuffer.wrap(bytes1), ByteBuffer.wrap(bytes2)) != 0;
    }
}
