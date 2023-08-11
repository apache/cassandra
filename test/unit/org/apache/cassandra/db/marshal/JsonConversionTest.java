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
package org.apache.cassandra.db.marshal;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;

import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.JsonUtils;

import org.junit.Test;

public class JsonConversionTest
{
    @Test
    public void testMap() throws Exception
    {
        String type = "FrozenType(MapType(TupleType(ListType(Int32Type), ListType(Int32Type)), ListType(Int32Type)))";
        String json = "{"
                + "\"[[1, 2, 3], [1, 2, 3]]\": [1, 2, 3], "
                + "\"[[1, 2, 3, 4], [1, 2, 3, 4]]\": [1, 2, 3, 4], "
                + "\"[[1, 2, 3, 4, 5], [1, 2, 3, 4, 5]]\": [1, 2, 3, 4, 5]"
                + "}";
        assertBytebufferPositionAndOutput(json, type);
    }

    @Test
    public void testUDT() throws Exception
    {
        // 6161-> aa
        // 78 -> x
        String type = "UserType(ks,6161,78:TupleType(ListType(Int32Type), ListType(Int32Type)))";
        String json = "{"
                + "\"x\": [[1, 2, 3], [1, 2, 3]]"
                + "}";
        assertBytebufferPositionAndOutput(json, type);
    }

    @Test
    public void testSimpleDate() throws Exception
    {
        String type = "SimpleDateType";
        String json = "\"1991-06-20\"";
        assertBytebufferPositionAndOutput(json, type);
    }

    @Test
    public void testDate() throws Exception
    {
        String type = "DateType";
        String json = "\"1991-06-20 18:00:00.000Z\"";
        assertBytebufferPositionAndOutput(json, type);
    }

    @Test
    public void testCounterColumn() throws Exception
    {
        Long value = 1L;
        String json = "1";
        assertBytebufferPositionAndOutput(json, value, CounterColumnType.instance);
    }

    @Test
    public void testTimestamp() throws Exception
    {
        String type = "TimestampType";
        String json = "\"1991-06-20 18:00:00.000Z\"";
        assertBytebufferPositionAndOutput(json, type);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testDynamicCompositeType() throws Exception
    {
        String type = "DynamicCompositeType(a=>Int32Type, b=>Int32Type)";
        // not supported
        String json = "{"
                + "\"a\":1,"
                + "\"b\":2"
                + "}";
        assertBytebufferPositionAndOutput(json, type);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testCompositeType() throws Exception
    {
        String type = "CompositeType(Int32Type, Int32Type)";
        // not supported
        String json = "{"
                + "\"a\":1,"
                + "\"b\":2"
                + "}";
        assertBytebufferPositionAndOutput(json, type);
    }

    @Test
    public void testList() throws Exception
    {
        String type = "FrozenType(ListType(TupleType(ListType(Int32Type), ListType(Int32Type))))";
        String json = "["
                + "[[1, 2, 3], [1, 2, 3]], "
                + "[[1, 2, 3, 4], [1, 2, 3, 4]], "
                + "[[1, 2, 3, 4, 5], [1, 2, 3, 4, 5]]"
                + "]";
        assertBytebufferPositionAndOutput(json, type);
    }

    @Test
    public void testSet() throws Exception
    {
        String type = "FrozenType(SetType(TupleType(Int32Type, Int32Type)))";
        String json = "[[1, 2], [1, 3], [2, 3]]";
        assertBytebufferPositionAndOutput(json, type);
    }

    @Test
    public void testTuple() throws Exception
    {
        String type = "FrozenType(TupleType(TupleType(ListType(Int32Type), ListType(Int32Type))))";
        String json = "["
                + "[[1, 2, 3], [1, 2, 3]]"
                + "]";
        assertBytebufferPositionAndOutput(json, type);
    }

    @Test
    public void testInt32() throws Exception
    {
        String type = "Int32Type";
        String json = "10000000";
        assertBytebufferPositionAndOutput(json, type);
    }

    @Test
    public void testReversed() throws Exception
    {
        String type = "ReversedType(Int32Type)";
        String json = "10000000";
        assertBytebufferPositionAndOutput(json, type);
    }

    @Test
    public void testInteger() throws Exception
    {
        String type = "IntegerType";
        String json = "10000000";
        assertBytebufferPositionAndOutput(json, type);
    }

    @Test
    public void testDecimal() throws Exception
    {
        String type = "DecimalType";
        String json = "100000.01";
        assertBytebufferPositionAndOutput(json, type);
    }

    @Test
    public void testEmpty() throws Exception
    {
        String type = "EmptyType";
        String json = "\"\"";
        assertBytebufferPositionAndOutput(json, type);
    }

    @Test
    public void testDouble() throws Exception
    {
        String type = "DoubleType";
        String json = "100000.01";
        assertBytebufferPositionAndOutput(json, type);
    }

    @Test
    public void testFloat() throws Exception
    {
        String type = "FloatType";
        String json = "100000.01";
        assertBytebufferPositionAndOutput(json, type);
    }

    @Test
    public void testShort() throws Exception
    {
        String type = "ShortType";
        String json = "100";
        assertBytebufferPositionAndOutput(json, type);
    }

    @Test
    public void testByte() throws Exception
    {
        String type = "ByteType";
        String json = "0";
        assertBytebufferPositionAndOutput(json, type);
    }

    @Test
    public void testTime() throws Exception
    {
        String type = "TimeType";
        String json = "\"00:00:00.000001991\"";
        assertBytebufferPositionAndOutput(json, type);
    }

    @Test
    public void testInetAddress() throws Exception
    {
        String type = "InetAddressType";
        String json = "\"127.0.0.1\"";
        assertBytebufferPositionAndOutput(json, type);
    }

    @Test
    public void testBoolean() throws Exception
    {
        String type = "BooleanType";
        String json = "false";
        assertBytebufferPositionAndOutput(json, type);
    }

    @Test
    public void testLong() throws Exception
    {
        String type = "LongType";
        String json = "10000000000";
        assertBytebufferPositionAndOutput(json, type);
    }

    @Test
    public void testAscii() throws Exception
    {
        String type = "AsciiType";
        String json = "\"aaa\"";
        assertBytebufferPositionAndOutput(json, type);
    }

    @Test
    public void testBytes() throws Exception
    {
        String type = "BytesType";
        String json = "\"0x00000001\"";
        assertBytebufferPositionAndOutput(json, type);
    }

    @Test
    public void testUUID() throws Exception
    {
        String type = "UUIDType";
        String json = "\"6bddc89a-5644-11e4-97fc-56847afe9799\"";
        assertBytebufferPositionAndOutput(json, type);
    }

    @Test
    public void testLexicalUUID() throws Exception
    {
        String type = "LexicalUUIDType";
        String json = "\"6bddc89a-5644-11e4-97fc-56847afe9799\"";
        assertBytebufferPositionAndOutput(json, type);
    }

    @Test
    public void testTimeUUID() throws Exception
    {
        String type = "TimeUUIDType";
        String json = "\"" + nextTimeUUID() + "\"";
        assertBytebufferPositionAndOutput(json, type);
    }

    @Test
    public void testUtf8() throws Exception
    {
        String type = "UTF8Type";
        String json = "\"abc\"";
        assertBytebufferPositionAndOutput(json, type);
    }

    // for those only supports toJson, eg. Counter
    private static <T> void assertBytebufferPositionAndOutput(String json, T value, AbstractType<T> type)
            throws Exception
    {
        ByteBuffer bb = type.getSerializer().serialize(value);
        int position = bb.position();

        String output = type.toJSONString(bb, ProtocolVersion.CURRENT);
        assertEquals(position, bb.position());
        assertEquals(json, output);
    }

    // test fromJSONObject and toJSONString
    private static void assertBytebufferPositionAndOutput(String json, String typeString) throws Exception
    {
        AbstractType<?> type = TypeParser.parse(typeString);
        Object jsonObject = JsonUtils.JSON_OBJECT_MAPPER.readValue(json, Object.class);
        ByteBuffer bb = type.fromJSONObject(jsonObject).bindAndGet(QueryOptions.DEFAULT);
        int position = bb.position();

        String output = type.toJSONString(bb, ProtocolVersion.CURRENT);
        assertEquals(position, bb.position());
        assertEquals(json, output);
    }
}
