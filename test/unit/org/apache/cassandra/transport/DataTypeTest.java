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

package org.apache.cassandra.transport;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.utils.Pair;

import static org.junit.Assert.assertEquals;

public class DataTypeTest
{
    @Test
    public void TestSimpleDataTypeSerialization()
    {
        for (DataType type : DataType.values())
        {
            if (isComplexType(type))
                continue;

            Pair<DataType, Object> options = Pair.create(type, (Object)type.toString());
            for (ProtocolVersion version : ProtocolVersion.SUPPORTED)
                testEncodeDecode(type, options, version);
        }
    }

    @Test
    public void TestListDataTypeSerialization()
    {
        DataType type = DataType.LIST;
        Pair<DataType, Object> options = Pair.create(type, (Object)LongType.instance);
        for (ProtocolVersion version : ProtocolVersion.SUPPORTED)
            testEncodeDecode(type, options, version);
    }

    @Test
    public void TestMapDataTypeSerialization()
    {
        DataType type = DataType.MAP;
        List<AbstractType> value = new ArrayList<>();
        value.add(LongType.instance);
        value.add(AsciiType.instance);
        Pair<DataType, Object> options = Pair.create(type, (Object)value);
        for (ProtocolVersion version : ProtocolVersion.SUPPORTED)
            testEncodeDecode(type, options, version);
    }

    private void testEncodeDecode(DataType type, Pair<DataType, Object> options, ProtocolVersion version)
    {
        int optLength = DataType.codec.oneSerializedSize(options, version);
        ByteBuf dest = Unpooled.buffer(optLength);
        DataType.codec.writeOne(options, dest, version);
        Pair<DataType, Object> result = DataType.codec.decodeOne(dest, version);

        System.out.println(result + "version " + version);
        int ssize = type.serializedValueSize(result.right, version);
        int esize = version.isSmallerThan(type.getProtocolVersion()) ? 2 + TypeSizes.encodedUTF8Length(result.right.toString()) : 0;
        switch (type)
        {
            case LIST:
            case SET:
                esize += 2;
                break;
            case MAP:
                esize += 4;
                break;
            case CUSTOM:
                esize = 8;
                break;
        }
        assertEquals(esize, ssize);

        DataType expected = version.isSmallerThan(type.getProtocolVersion())
            ? DataType.CUSTOM
            : type;
        assertEquals(expected, result.left);
   }

    private boolean isComplexType(DataType type)
    {
        return type.getId(ProtocolVersion.CURRENT) >= 32;
    }
}
