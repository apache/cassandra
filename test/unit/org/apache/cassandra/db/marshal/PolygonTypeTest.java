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

import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCPolygon;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.db.marshal.geometry.Polygon;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.db.marshal.GeometricTypeTests.p;
import static org.apache.cassandra.db.marshal.GeometricTypeTests.padBuffer;
import static org.apache.cassandra.db.marshal.GeometricTypeTests.polygon;

public class PolygonTypeTest
{
    private static final Logger logger = LoggerFactory.getLogger(LineStringTypeTest.class);

    private static final PolygonType type = PolygonType.instance;

    @Test
    public void successCase()
    {
        ByteBuffer actualBB = type.fromString("polygon((30 10, 40 40, 20 40, 10 20, 30 10))");

        ByteBuffer expectedBB = ByteBuffer.allocate(1024).order(ByteOrder.nativeOrder());
        expectedBB.position(0);

        expectedBB.put((byte) (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN ? 1 : 0)); // endianness
        expectedBB.putInt(3);  // type
        expectedBB.putInt(1);  // num rings
        expectedBB.putInt(5);  // num points (ring 1/1)
        expectedBB.putDouble(30);  // x1
        expectedBB.putDouble(10);  // y1
        expectedBB.putDouble(40);  // x2
        expectedBB.putDouble(40);  // y2
        expectedBB.putDouble(20);  // x3
        expectedBB.putDouble(40);  // y3
        expectedBB.putDouble(10);  // x4
        expectedBB.putDouble(20);  // y4
        expectedBB.putDouble(30);  // x5
        expectedBB.putDouble(10);  // y5
        expectedBB.flip();

        logger.debug("expected: {}", ByteBufferUtil.bytesToHex(expectedBB));
        logger.debug("actual:   {}", ByteBufferUtil.bytesToHex(actualBB));
        String failMsg = String.format("%s != %s", ByteBufferUtil.bytesToHex(actualBB), ByteBufferUtil.bytesToHex(expectedBB));
        Assert.assertEquals(failMsg, expectedBB, actualBB);

        Polygon expectedGeometry = polygon(p(30, 10), p(10, 20), p(20, 40), p(40, 40));
        Polygon actualGeometry = type.getSerializer().deserialize(actualBB);
        Assert.assertEquals(expectedGeometry, actualGeometry);
    }

    @Test(expected=MarshalException.class)
    public void emptyFailure()
    {
        type.fromString("polygon(())");
    }

    @Test(expected=MarshalException.class)
    public void failure3d()
    {
        type.fromString("polygon((30 10 1, 40 40 1, 20 40 1, 10 20 1, 30 10 1))");
    }

    /**
     * Line strings that cross themselves shouldn't validate
     */
    @Test(expected=MarshalException.class)
    public void simpleFailure()
    {
        type.fromString("polygon((0 0, 1 1, 0 1, 1 0, 0 0))");
    }

    @Test(expected=MarshalException.class)
    public void parseFailure()
    {
        type.fromString("polygon123((30 10, 40 40, 20 40, 10 20, 30 10))");
    }

    @Test
    public void jsonWktInput()
    {
        Constants.Value value = (Constants.Value) type.fromJSONObject("polygon((30 10, 40 40, 20 40, 10 20, 30 10))");
        Assert.assertEquals(polygon(p(30, 10), p(10, 20), p(20, 40), p(40, 40)), type.getSerializer().deserialize(value.bytes));
    }

    @Test
    public void geoJsonInput()
    {
        String json = "{\"type\":\"Polygon\",\"coordinates\":[[[30.0,10.0],[10.0,20.0],[20.0,40.0],[40.0,40.0],[30.0,10.0]]]}";
        Constants.Value value = (Constants.Value) type.fromJSONObject(json);
        Assert.assertEquals(polygon(p(30, 10), p(10, 20), p(20, 40), p(40, 40)), type.getSerializer().deserialize(value.bytes));
    }

    @Test
    public void geoJsonInputNoPrecision()
    {
        String json = "{\"type\":\"Polygon\",\"coordinates\":[[[30,10],[10,20],[20,40],[40,40],[30,10]]]}";
        Constants.Value value = (Constants.Value) type.fromJSONObject(json);
        Assert.assertEquals(polygon(p(30, 10), p(10, 20), p(20, 40), p(40, 40)), type.getSerializer().deserialize(value.bytes));
    }

    @Test
    public void geoJsonOutputWithDoubles()
    {
        String json = type.toJSONString(type.getSerializer().serialize(polygon(p(30.1111, 10.2), p(10.3, 20.4), p(20.5, 40.6), p(40.7, 40.8))), ProtocolVersion.CURRENT);
        logger.debug(json);
        Assert.assertEquals("{\"type\":\"Polygon\",\"coordinates\":[[[30.1111,10.2],[40.7,40.8],[20.5,40.6],[10.3,20.4],[30.1111,10.2]]]}", json);
    }

    @Test
    public void geoJsonOutput()
    {
        String json = type.toJSONString(type.getSerializer().serialize(polygon(p(30, 10), p(10, 20), p(20, 40), p(40, 40))), ProtocolVersion.CURRENT);
        logger.debug(json);
        Assert.assertEquals("{\"type\":\"Polygon\",\"coordinates\":[[[30,10],[40,40],[20,40],[10,20],[30,10]]]}", json);
    }

    /**
     * Use of absolute indexing in deserializers shouldn't cause problems
     */
    @Test
    public void bufferOffset()
    {
        Polygon expected = polygon(p(30, 10), p(10, 20), p(20, 40), p(40, 40));
        ByteBuffer bb = padBuffer(type.getSerializer().serialize(expected));
        type.getSerializer().validate(bb);
        Polygon actual = type.getSerializer().deserialize(bb);
        Assert.assertEquals(expected, actual);
    }

    /**
     * Duplicates DSP-10070
     * There are some cases where esri can parse wkt into an invalid geometry object, but
     * can't then convert the invalid geometry to a wkt string. We should catch these cases
     * and throw a MarshalException
     */
    @Test(expected=MarshalException.class)
    public void invalidInnerRingWkt()
    {
        String wkt = "POLYGON ((0.0 0.0, 0.0 10.0, 10.0 10.0, 10.0 0.0, 0.0 0.0), (1.0 10.0, 9.0 0.0, 9.0 9.0, 0.0 9.0, 0.0 0.0))";
        OGCGeometry geometry = OGCGeometry.fromText(wkt);
        Assert.assertTrue(geometry instanceof OGCPolygon);
        new Polygon((OGCPolygon) geometry);
    }

    /**
     * Duplicates DSP-10070
     * There are some cases where esri can parse wkb into an invalid geometry object, but
     * can't then convert the invalid geometry back to wkb. We should catch these cases
     * and throw a MarshalException
     */
    @Test(expected=MarshalException.class)
    public void invalidInnerRingWkb()
    {
        ByteBuffer bb = ByteBuffer.allocate(1024).order(ByteOrder.nativeOrder());
        bb.position(0);

        bb.put((byte) (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN ? 1 : 0)); // endianness
        bb.putInt(3);  // type
        bb.putInt(2);  // num rings
        bb.putInt(5);  // num points (ring 1/2)
        bb.putDouble(0).putDouble(0);
        bb.putDouble(0).putDouble(10);
        bb.putDouble(10).putDouble(10);
        bb.putDouble(10).putDouble(0);
        bb.putDouble(0).putDouble(0);
        bb.putInt(5);  // num points (ring 1/2)
        bb.putDouble(1).putDouble(10);
        bb.putDouble(9).putDouble(0);
        bb.putDouble(9).putDouble(9);
        bb.putDouble(0).putDouble(9);
        bb.putDouble(0).putDouble(0);
        bb.flip();

        type.getSerializer().validate(bb);
    }

    /**
     * DSP-10092
     * Tests that a polygon serialized with a clockwise outer ring and no closing point fails validation, since
     * it's normalized form has the closing point with points defined counterclockwise.
     *
     * Esri 'helps' us by normalizing the geometries it deserializes. Since some values are reserialized from the
     * deserialized objects, and some aren't, we need to make sure that binary data we get can be reserialized into
     * equal bytes. Otherwise, users can run into issues with data appearing to disappear.
     */
    @Test(expected=MarshalException.class)
    public void denormalizedPolygon()
    {
        ByteBuffer bb = ByteBuffer.allocate(1024).order(ByteOrder.nativeOrder());
        bb.position(0);

        bb.put((byte) (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN ? 1 : 0)); // endianness
        bb.putInt(3);  // type
        bb.putInt(1);  // num rings
        bb.putInt(3);  // num points (ring 1/1)
        bb.putDouble(0);  // x1
        bb.putDouble(0);  // y1
        bb.putDouble(1);  // x2
        bb.putDouble(1);  // y2
        bb.putDouble(1);  // x3
        bb.putDouble(0);  // y3
        bb.flip();

        type.getSerializer().validate(bb);
    }

    @Test
    public void bufferBigEndianess()
    {
        Polygon expected = polygon(p(30, 10), p(10, 20), p(20, 40), p(40, 40));
        ByteBuffer bb = padBuffer(type.getSerializer().serialize(expected));
        Assert.assertEquals(ByteOrder.BIG_ENDIAN, bb.order());
    }
}
