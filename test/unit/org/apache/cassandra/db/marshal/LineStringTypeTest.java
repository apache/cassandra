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
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.db.marshal.GeometricTypeTests.lineString;
import static org.apache.cassandra.db.marshal.GeometricTypeTests.p;
import static org.apache.cassandra.db.marshal.GeometricTypeTests.padBuffer;

public class LineStringTypeTest
{
    private static final Logger logger = LoggerFactory.getLogger(LineStringTypeTest.class);

    LineStringType type = LineStringType.instance;

    @Test
    public void successCase2d()
    {
        ByteBuffer actual = type.fromString("linestring(30 10, 10 30, 40 40)");

        ByteBuffer expected = ByteBuffer.allocate(1024).order(ByteOrder.nativeOrder());
        expected.position(0);

        expected.put((byte) (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN ? 1 : 0)); // endianness
        expected.putInt(2);  // type
        expected.putInt(3);  // num points
        expected.putDouble(30);  // x1
        expected.putDouble(10);  // y1
        expected.putDouble(10);  // x2
        expected.putDouble(30);  // y2
        expected.putDouble(40);  // x3
        expected.putDouble(40);  // y3
        expected.flip();

        logger.debug("expected: {}", ByteBufferUtil.bytesToHex(expected));
        logger.debug("actual:   {}", ByteBufferUtil.bytesToHex(actual));
        String failMsg = String.format("%s != %s", ByteBufferUtil.bytesToHex(actual), ByteBufferUtil.bytesToHex(expected));
        Assert.assertEquals(failMsg, expected, actual);

        LineString expectedGeometry = lineString(p(30, 10), p(10, 30), p(40, 40));
        LineString actualGeometry = type.getSerializer().deserialize(actual);
        logger.debug("expected: {}", expectedGeometry);
        logger.debug("actual:   {}", actualGeometry);
        Assert.assertEquals(expectedGeometry, actualGeometry);
    }

    @Test(expected=MarshalException.class)
    public void emptyFailure()
    {
        type.fromString("linestring()");
    }

    @Test(expected=MarshalException.class)
    public void failure3d()
    {
        type.fromString("linestring(30 10 20, 10 30 20)");
    }

    /**
     * Line strings that cross themselves shouldn't validate
     */
    @Test(expected=MarshalException.class)
    public void simpleFailure()
    {
        type.fromString("linestring(0 0, 1 1, 0 1, 1 0)");
    }

    @Test(expected=MarshalException.class)
    public void parseFailure()
    {
        type.fromString("superlinestring(30 10, 10 30, 40 40)");
    }

    @Test
    public void jsonWktInput()
    {
        Constants.Value value = (Constants.Value) type.fromJSONObject("linestring(30 10, 10 30, 40 40)");
        Assert.assertEquals(lineString(p(30, 10), p(10, 30), p(40, 40)), type.getSerializer().deserialize(value.bytes));
    }

    @Test
    public void geoJsonInput()
    {
        String json = "{\"type\":\"LineString\",\"coordinates\":[[30.0,10.0],[10.0,30.0],[40.0,40.0]]}";
        Constants.Value value = (Constants.Value) type.fromJSONObject(json);
        Assert.assertEquals(lineString(p(30, 10), p(10, 30), p(40, 40)), type.getSerializer().deserialize(value.bytes));
    }

    @Test
    public void geoJsonOutput()
    {
        String json = type.toJSONString(type.getSerializer().serialize(lineString(p(30, 10), p(10, 30), p(40, 40))), ProtocolVersion.CURRENT);
        Assert.assertEquals("{\"type\":\"LineString\",\"coordinates\":[[30,10],[10,30],[40,40]]}", json);
        logger.debug(json);
    }

    /**
     * Use of absolute indexing in deserializers shouldn't cause problems
     */
    @Test
    public void bufferOffset()
    {
        LineString expected = lineString(p(30, 10), p(10, 30), p(40, 40));
        ByteBuffer bb = padBuffer(type.getSerializer().serialize(expected));
        type.getSerializer().validate(bb);
        LineString actual = type.getSerializer().deserialize(bb);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void bufferBigEndianess()
    {
        LineString expected = lineString(p(30, 10), p(10, 30), p(40, 40));
        ByteBuffer bb = padBuffer(type.getSerializer().serialize(expected));
        Assert.assertEquals(ByteOrder.BIG_ENDIAN, bb.order());
    }

}

