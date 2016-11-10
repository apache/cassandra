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
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.LongType;

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

            Map<DataType, Object> options = Collections.singletonMap(type, (Object)type.toString());
            for (int version = 1; version < 5; version++)
                testEncodeDecode(type, options, version);
        }
    }

    @Test
    public void TestListDataTypeSerialization()
    {
        DataType type = DataType.LIST;
        Map<DataType, Object> options =  Collections.singletonMap(type, (Object)LongType.instance);
        for (int version = 1; version < 5; version++)
            testEncodeDecode(type, options, version);
    }

    @Test
    public void TestMapDataTypeSerialization()
    {
        DataType type = DataType.MAP;
        List<AbstractType> value = new ArrayList<>();
        value.add(LongType.instance);
        value.add(AsciiType.instance);
        Map<DataType, Object> options = Collections.singletonMap(type, (Object)value);
        for (int version = 1; version < 5; version++)
            testEncodeDecode(type, options, version);
    }

    private void testEncodeDecode(DataType type, Map<DataType, Object> options, int version)
    {
        ByteBuf dest = type.codec.encode(options, version);
        Map<DataType, Object> results = type.codec.decode(dest, version);

        for (DataType key : results.keySet())
        {
            int ssize = type.serializedValueSize(results.get(key), version);
            int esize = version < type.getProtocolVersion() ? 2 + TypeSizes.encodedUTF8Length(results.get(key).toString()) : 0;
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

            DataType expected = version < type.getProtocolVersion()
                ? DataType.CUSTOM
                : type;
            assertEquals(expected, key);
        }
    }

    private boolean isComplexType(DataType type)
    {
        return type.getId(Server.CURRENT_VERSION) >= 32;
    }
}
