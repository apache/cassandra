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


import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;
import java.util.UUID;

import com.google.common.base.Charsets;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.junit.Test;

public class TypeCompareTest
{
    @Test
    public void testAscii()
    {
        AsciiType comparator = new AsciiType();
        assert comparator.compare(FBUtilities.EMPTY_BYTE_BUFFER, ByteBuffer.wrap("asdf".getBytes())) < 0;
        assert comparator.compare(ByteBuffer.wrap("asdf".getBytes()), FBUtilities.EMPTY_BYTE_BUFFER) > 0;
        assert comparator.compare(FBUtilities.EMPTY_BYTE_BUFFER, FBUtilities.EMPTY_BYTE_BUFFER) == 0;
        assert comparator.compare(ByteBuffer.wrap("z".getBytes()), ByteBuffer.wrap("a".getBytes())) > 0;
        assert comparator.compare(ByteBuffer.wrap("a".getBytes()), ByteBuffer.wrap("z".getBytes())) < 0;
        assert comparator.compare(ByteBuffer.wrap("asdf".getBytes()), ByteBuffer.wrap("asdf".getBytes())) == 0;
        assert comparator.compare(ByteBuffer.wrap("asdz".getBytes()), ByteBuffer.wrap("asdf".getBytes())) > 0;
    }

    @Test
    public void testBytes()
    {
        BytesType comparator = new BytesType();
        assert comparator.compare(FBUtilities.EMPTY_BYTE_BUFFER, ByteBuffer.wrap("asdf".getBytes())) < 0;
        assert comparator.compare(ByteBuffer.wrap("asdf".getBytes()), FBUtilities.EMPTY_BYTE_BUFFER) > 0;
        assert comparator.compare(FBUtilities.EMPTY_BYTE_BUFFER, FBUtilities.EMPTY_BYTE_BUFFER) == 0;
        assert comparator.compare(ByteBuffer.wrap("z".getBytes()), ByteBuffer.wrap("a".getBytes())) > 0;
        assert comparator.compare(ByteBuffer.wrap("a".getBytes()), ByteBuffer.wrap("z".getBytes())) < 0;
        assert comparator.compare(ByteBuffer.wrap("asdf".getBytes()), ByteBuffer.wrap("asdf".getBytes())) == 0;
        assert comparator.compare(ByteBuffer.wrap("asdz".getBytes()), ByteBuffer.wrap("asdf".getBytes())) > 0;
    }

    @Test
    public void testUTF8()
    {
        UTF8Type comparator = new UTF8Type();
        assert comparator.compare(FBUtilities.EMPTY_BYTE_BUFFER, ByteBuffer.wrap("asdf".getBytes())) < 0;
        assert comparator.compare(ByteBuffer.wrap("asdf".getBytes()), FBUtilities.EMPTY_BYTE_BUFFER) > 0;
        assert comparator.compare(FBUtilities.EMPTY_BYTE_BUFFER, FBUtilities.EMPTY_BYTE_BUFFER) == 0;
        assert comparator.compare(ByteBuffer.wrap("z".getBytes(Charsets.UTF_8)), ByteBuffer.wrap("a".getBytes(Charsets.UTF_8))) > 0;
        assert comparator.compare(ByteBuffer.wrap("z".getBytes(Charsets.UTF_8)), ByteBuffer.wrap("z".getBytes(Charsets.UTF_8))) == 0;
        assert comparator.compare(ByteBuffer.wrap("a".getBytes(Charsets.UTF_8)), ByteBuffer.wrap("z".getBytes(Charsets.UTF_8))) < 0;
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
        	
            long l0 = data[i - 1].getLong(data[i - 1].position()+data[i - 1].arrayOffset());
            long l1 = data[i].getLong(data[i].position()+data[i].arrayOffset());
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
