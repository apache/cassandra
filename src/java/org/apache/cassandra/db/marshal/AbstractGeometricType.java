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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Json;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.db.marshal.geometry.GeometricType;
import org.apache.cassandra.db.marshal.geometry.OgcGeometry;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.transport.ProtocolVersion;

public abstract class AbstractGeometricType<T extends OgcGeometry> extends AbstractType<T>
{
    private final TypeSerializer<T> serializer = new TypeSerializer<T>()
    {
        @Override
        public ByteBuffer serialize(T geometry)
        {
            return geoSerializer.toWellKnownBinary(geometry);
        }

        @Override
        public <V> T deserialize(V value, ValueAccessor<V> accessor)
        {
            // OGCGeometry does not respect the current position of the buffer, so you need to use slice()
            ByteBuffer byteBuffer = accessor.toBuffer(value);
            return geoSerializer.fromWellKnownBinary(byteBuffer.slice());
        }

        @Override
        public <V> void validate(V value, ValueAccessor<V> accessor) throws MarshalException
        {
            ByteBuffer byteBuffer = accessor.toBuffer(value);
            int pos = byteBuffer.position();
            // OGCGeometry does not respect the current position of the buffer, so you need to use slice()
            geoSerializer.fromWellKnownBinary(byteBuffer.slice()).validate();
            byteBuffer.position(pos);
        }

        @Override
        public String toString(T geometry)
        {
            return geoSerializer.toWellKnownText(geometry);
        }

        @Override
        public Class<T> getType()
        {
            return klass;
        }
    };

    private final GeometricType type;
    private final Class<T> klass;
    private final OgcGeometry.Serializer<T> geoSerializer;

    public AbstractGeometricType(GeometricType type)
    {
        super(ComparisonType.BYTE_ORDER);
        this.type = type;
        this.klass = (Class<T>) type.getGeoClass();
        this.geoSerializer = type.getSerializer();
    }

    public GeometricType getGeoType()
    {
        return type;
    }

    @Override
    public ByteBuffer fromString(String s) throws MarshalException
    {
        try
        {
            T geometry = geoSerializer.fromWellKnownText(s);
            geometry.validate();
            return geoSerializer.toWellKnownBinary(geometry);
        }
        catch (Exception e)
        {
            String parentMsg = e.getMessage() != null ? " " + e.getMessage() : "";
            String msg = String.format("Unable to make %s from '%s'", getClass().getSimpleName(), s) + parentMsg;
            throw new MarshalException(msg, e);
        }
    }

    @Override
    public Term fromJSONObject(Object parsed) throws MarshalException
    {
        if (!(parsed instanceof String))
        {
            try
            {
                parsed = Json.JSON_OBJECT_MAPPER.writeValueAsString(parsed);
            }
            catch (IOException e)
            {
                throw new MarshalException(e.getMessage());
            }
        }

        T geometry;
        try
        {
            geometry = geoSerializer.fromGeoJson((String) parsed);
        }
        catch (MarshalException e)
        {
            try
            {
                geometry = geoSerializer.fromWellKnownText((String) parsed);
            }
            catch (MarshalException ignored)
            {
                throw new MarshalException(e.getMessage());
            }
        }
        geometry.validate();
        return new Constants.Value(geoSerializer.toWellKnownBinary(geometry));
    }

    @Override
    public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion)
    {
        // OGCGeometry does not respect the current position of the buffer, so you need to use slice()
        return geoSerializer.toGeoJson(geoSerializer.fromWellKnownBinary(buffer.slice()));
    }

    @Override
    public TypeSerializer<T> getSerializer()
    {
        return serializer;
    }
}
