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

package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Test;

import com.datastax.driver.core.ProtocolVersion;
import junit.framework.TestCase;
import org.apache.cassandra.db.marshal.geometry.OgcGeometry;
import org.apache.cassandra.db.marshal.geometry.Point;

public class GeometryCodecTest extends TestCase
{
    private final GeometryCodec<Point> codec = new GeometryCodec<>(PointType.instance);

    @Test
    public void testFormat()
    {
        Assert.assertEquals("NULL", codec.format(null));
        Assert.assertEquals("POINT (5.4 1)", codec.format(new Point(5.4, 1.0)));
    }

    @Test
    public void testParse()
    {
        Assert.assertEquals(null, codec.parse(null));
        Assert.assertEquals(new Point(5.4, 1.0), codec.parse("POINT (5.4 1)"));
    }

    @Test
    public void testSerializationRoundTrip()
    {
        Point point = new Point(5.4, 1.0);
        ByteBuffer serialized = codec.serialize(point, ProtocolVersion.NEWEST_SUPPORTED);
        OgcGeometry deserialized = codec.deserialize(serialized, ProtocolVersion.NEWEST_SUPPORTED);
        Assert.assertEquals(point, deserialized);
    }

    @Test
    public void testEmptyValuesSerialization()
    {
        Assert.assertEquals(null, codec.serialize(null, ProtocolVersion.NEWEST_SUPPORTED));
        Assert.assertEquals(null, codec.deserialize(null, ProtocolVersion.NEWEST_SUPPORTED));
    }
}