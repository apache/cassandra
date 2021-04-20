/**
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

package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.db.marshal.geometry.LineString;
import org.apache.cassandra.db.marshal.geometry.Point;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.db.marshal.GeometricTypeTests.*;
import static org.apache.cassandra.db.marshal.GeometricTypeTests.p;
import static org.apache.cassandra.db.marshal.GeometricTypeTests.padBuffer;

public class PointTypeTest
{
    private static final Logger logger = LoggerFactory.getLogger(PointTypeTest.class);

    PointType type = PointType.instance;

    @Test
    public void successCase2d()
    {
        ByteBuffer actual = type.fromString("point(1.1 2.2)");

        ByteBuffer expected = ByteBuffer.allocate(1024).order(ByteOrder.nativeOrder());
        expected.position(0);

        expected.put((byte) (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN ? 1 : 0)); // endianness
        expected.putInt(1);  // type
        expected.putDouble(1.1); // x
        expected.putDouble(2.2); // y
        expected.flip();

        logger.debug("expected: {}", ByteBufferUtil.bytesToHex(expected));
        logger.debug("actual:   {}", ByteBufferUtil.bytesToHex(actual));
        String failMsg = String.format("%s != %s", ByteBufferUtil.bytesToHex(actual), ByteBufferUtil.bytesToHex(expected));
        Assert.assertEquals(failMsg, expected, actual);

        Point point = type.getSerializer().deserialize(actual);
        Assert.assertEquals(p(1.1, 2.2), point);
    }

    @Test(expected=MarshalException.class)
    public void parseFailure()
    {
        type.fromString("superpoint(1.1 2.2 3.3)");
    }

    @Test
    public void jsonWktInput()
    {
        Constants.Value value = (Constants.Value) type.fromJSONObject("point(1 2)");
        Assert.assertEquals(p(1, 2), type.getSerializer().deserialize(value.bytes));
    }

    @Test
    public void geoJsonInput()
    {
        String json = "{\"type\":\"Point\",\"coordinates\":[1.0,2.0]}";
        Constants.Value value = (Constants.Value) type.fromJSONObject(json);
        Assert.assertEquals(p(1, 2), type.getSerializer().deserialize(value.bytes));
    }

    @Test
    public void geoJsonInputWithoutPrecision()
    {
        String json = "{\"type\":\"Point\",\"coordinates\":[1,2]}";
        Constants.Value value = (Constants.Value) type.fromJSONObject(json);
        Assert.assertEquals(p(1, 2), type.getSerializer().deserialize(value.bytes));
    }

    @Test
    public void geoJsonOutput()
    {
        String json = type.toJSONString(type.getSerializer().serialize(p(1, 2)), ProtocolVersion.CURRENT);
        Assert.assertEquals("{\"type\":\"Point\",\"coordinates\":[1,2]}", json);
        logger.debug(json);
    }

    /**
     * Use of absolute indexing in deserializers shouldn't cause problems
     */
    @Test
    public void bufferOffset()
    {
        Point expected = p(1, 2);
        ByteBuffer bb = padBuffer(type.getSerializer().serialize(expected));
        type.getSerializer().validate(bb);
        Point actual = type.getSerializer().deserialize(bb);
        Assert.assertEquals(expected, actual);
    }

    private static ByteBuffer getExpectedSerialization(Point point, ByteOrder order)
    {
        ByteBuffer expected = ByteBuffer.allocate(1024).order(order);
        expected.put((byte) (order == ByteOrder.LITTLE_ENDIAN ? 1 : 0)); // endianness
        expected.putInt(1);  // type
        expected.putDouble(point.getOgcPoint().X()); // x
        expected.putDouble(point.getOgcPoint().Y()); // y
        expected.flip();
        return expected;
    }

    @Test
    public void bufferBigEndianess()
    {
        Point expected = p(1, 2);
        ByteBuffer bb = padBuffer(type.getSerializer().serialize(expected));
        Assert.assertEquals(ByteOrder.BIG_ENDIAN, bb.order());
    }
}
