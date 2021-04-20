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

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import org.apache.cassandra.db.marshal.geometry.LineString;
import org.apache.cassandra.db.marshal.geometry.OgcGeometry;
import org.apache.cassandra.db.marshal.geometry.Point;
import org.apache.cassandra.db.marshal.geometry.Polygon;

public class GeometryCodec<T extends OgcGeometry> extends TypeCodec<T>
{
    public static final TypeCodec<Point> pointCodec = new GeometryCodec<>(PointType.instance);
    public static final TypeCodec<LineString> lineStringCodec = new GeometryCodec<>(LineStringType.instance);
    public static final TypeCodec<Polygon> polygonCodec = new GeometryCodec<>(PolygonType.instance);

    private final OgcGeometry.Serializer<T> serializer;

    public GeometryCodec(AbstractGeometricType type)
    {
        super(DataType.custom(type.getClass().getName()), (Class<T>) type.getGeoType().getGeoClass());
        this.serializer = (OgcGeometry.Serializer<T>) type.getGeoType().getSerializer();
    }

    @Override
    public T deserialize(ByteBuffer bb, ProtocolVersion protocolVersion) throws InvalidTypeException
    {
        return bb == null || bb.remaining() == 0 ? null : serializer.fromWellKnownBinary(bb);
    }

    @Override
    public ByteBuffer serialize(T geometry, ProtocolVersion protocolVersion) throws InvalidTypeException
    {
        return geometry == null ? null : geometry.asWellKnownBinary();
    }

    @Override
    public T parse(String s) throws InvalidTypeException
    {
        if (s == null || s.isEmpty() || s.equalsIgnoreCase("NULL"))
            return null;
        return serializer.fromWellKnownText(s);
    }

    @Override
    public String format(T geometry) throws InvalidTypeException
    {
        return geometry == null ? "NULL" : geometry.asWellKnownText();
    }
}
