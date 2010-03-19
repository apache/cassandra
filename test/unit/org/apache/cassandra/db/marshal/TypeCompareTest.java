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
import java.util.UUID;

import org.apache.commons.lang.ArrayUtils;
import org.junit.Test;

public class TypeCompareTest
{
    @Test
    public void testAscii()
    {
        AsciiType comparator = new AsciiType();
        assert comparator.compare(ArrayUtils.EMPTY_BYTE_ARRAY, "asdf".getBytes()) < 0;
        assert comparator.compare("asdf".getBytes(), ArrayUtils.EMPTY_BYTE_ARRAY) > 0;
        assert comparator.compare(ArrayUtils.EMPTY_BYTE_ARRAY, ArrayUtils.EMPTY_BYTE_ARRAY) == 0;
        assert comparator.compare("z".getBytes(), "a".getBytes()) > 0;
        assert comparator.compare("a".getBytes(), "z".getBytes()) < 0;
        assert comparator.compare("asdf".getBytes(), "asdf".getBytes()) == 0;
        assert comparator.compare("asdz".getBytes(), "asdf".getBytes()) > 0;
    }

    @Test
    public void testBytes()
    {
        BytesType comparator = new BytesType();
        assert comparator.compare(ArrayUtils.EMPTY_BYTE_ARRAY, "asdf".getBytes()) < 0;
        assert comparator.compare("asdf".getBytes(), ArrayUtils.EMPTY_BYTE_ARRAY) > 0;
        assert comparator.compare(ArrayUtils.EMPTY_BYTE_ARRAY, ArrayUtils.EMPTY_BYTE_ARRAY) == 0;
        assert comparator.compare("z".getBytes(), "a".getBytes()) > 0;
        assert comparator.compare("a".getBytes(), "z".getBytes()) < 0;
        assert comparator.compare("asdf".getBytes(), "asdf".getBytes()) == 0;
        assert comparator.compare("asdz".getBytes(), "asdf".getBytes()) > 0;
    }

    @Test
    public void testUTF8() throws UnsupportedEncodingException
    {
        UTF8Type comparator = new UTF8Type();
        assert comparator.compare(ArrayUtils.EMPTY_BYTE_ARRAY, "asdf".getBytes()) < 0;
        assert comparator.compare("asdf".getBytes(), ArrayUtils.EMPTY_BYTE_ARRAY) > 0;
        assert comparator.compare(ArrayUtils.EMPTY_BYTE_ARRAY, ArrayUtils.EMPTY_BYTE_ARRAY) == 0;
        assert comparator.compare("z".getBytes("UTF-8"), "a".getBytes("UTF-8")) > 0;
        assert comparator.compare("z".getBytes("UTF-8"), "z".getBytes("UTF-8")) == 0;
        assert comparator.compare("a".getBytes("UTF-8"), "z".getBytes("UTF-8")) < 0;
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

        assert new TimeUUIDType().compare(bytes1, bytes2) != 0;
    }
}
