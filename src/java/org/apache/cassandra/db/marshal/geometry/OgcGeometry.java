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

package org.apache.cassandra.db.marshal.geometry;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.esri.core.geometry.GeometryException;
import com.esri.core.geometry.JsonGeometryException;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.ogc.OGCGeometry;
import org.apache.cassandra.serializers.MarshalException;

public abstract class OgcGeometry
{

    // default spatial reference for wkt/wkb
    public static final SpatialReference SPATIAL_REFERENCE_4326 = SpatialReference.create(4326);

    public interface Serializer<T extends OgcGeometry>
    {
        String toWellKnownText(T geometry);

        // We need to return a Big Endian ByteBuffer as that's required by org.apache.cassandra.db.NativeDecoratedKey
        // when the memtable allocation type is "offheap_objects". See https://datastax.jira.com/browse/DSP-16302
        // Note that the order set here may not match the actual endianess. OGC serialization encodes actual endianess
        // and discards BB order set here.
        default ByteBuffer toWellKnownBinary(T geometry)
        {
            return toWellKnownBinaryNativeOrder(geometry).order(ByteOrder.BIG_ENDIAN);
        }

        ByteBuffer toWellKnownBinaryNativeOrder(T geometry);

        String toGeoJson(T geometry);

        T fromWellKnownText(String source);

        T fromWellKnownBinary(ByteBuffer source);

        T fromGeoJson(String source);
    }

    public abstract GeometricType getType();

    public abstract void validate() throws MarshalException;

    public abstract Serializer getSerializer();

    static void validateType(OGCGeometry geometry, Class<? extends OGCGeometry> klass)
    {
        if (!geometry.getClass().equals(klass))
        {
            throw new MarshalException(String.format("%s is not of type %s",
                    geometry.getClass().getSimpleName(),
                    klass.getSimpleName()));
        }
    }

    static ByteBuffer getWkb(OGCGeometry geometry)
    {
        try
        {
            return geometry.asBinary();
        }
        catch (GeometryException | IllegalArgumentException e)
        {
            throw new MarshalException("Invalid Geometry", e);
        }
    }

    static String getWkt(OGCGeometry geometry)
    {
        try
        {
            return geometry.asText();
        }
        catch (GeometryException | IllegalArgumentException e)
        {
            throw new MarshalException("Invalid Geometry", e);
        }
    }

    static void validateNormalization(OGCGeometry geometry, ByteBuffer source)
    {
        ByteBuffer normalized = getWkb(geometry);
        ByteBuffer inputCopy = source.slice();

        // since the data we get is sometimes part of a longer string of bytes, we set the limit to the normalized
        // buffer length. Normalization only ever adds and rearranges points though, so this should be ok
        if (inputCopy.remaining() > normalized.remaining())
        {
            inputCopy.limit(normalized.remaining());
        }

        if (!normalized.equals(inputCopy))
        {
            String klass = geometry.getClass().getSimpleName();
            String msg = String.format("%s is not normalized. %s should be defined/serialized as: %s", klass, klass, getWkt(geometry));
            throw new MarshalException(msg);
        }
    }

    static <T extends OGCGeometry> T fromOgcWellKnownText(String source, Class<T> klass)
    {
        OGCGeometry geometry;
        try
        {
            geometry = OGCGeometry.fromText(source);
        }
        catch (IllegalArgumentException e)
        {
            throw new MarshalException(e.getMessage());
        }
        validateType(geometry, klass);
        return (T) geometry;
    }

    static <T extends OGCGeometry> T fromOgcWellKnownBinary(ByteBuffer source, Class<T> klass)
    {
        OGCGeometry geometry;
        try
        {
            geometry = OGCGeometry.fromBinary(source);
        }
        catch (IllegalArgumentException e)
        {
            throw new MarshalException(e.getMessage());
        }
        validateType(geometry, klass);
        validateNormalization(geometry, source);
        return (T) geometry;
    }

    static <T extends OGCGeometry> T fromOgcGeoJson(String source, Class<T> klass)
    {
        OGCGeometry geometry;
        try
        {
            geometry = OGCGeometry.fromGeoJson(source);
        }
        catch (IllegalArgumentException | JsonGeometryException e)
        {
            throw new MarshalException(e.getMessage());
        }
        validateType(geometry, klass);
        return (T) geometry;
    }

    public boolean contains(OgcGeometry geometry)
    {
        if (!(geometry instanceof OgcGeometry))
        {
            throw new UnsupportedOperationException(String.format("%s is not compatible with %s.contains",
                    geometry.getClass().getSimpleName(), getClass().getSimpleName()));
        }

        OGCGeometry thisGeometry = getOgcGeometry();
        OGCGeometry thatGeometry = ((OgcGeometry) geometry).getOgcGeometry();
        if (thisGeometry != null && thatGeometry != null)
        {
            return thisGeometry.contains(thatGeometry);
        }
        else
        {
            return false;
        }
    }

    protected abstract OGCGeometry getOgcGeometry();

    static void validateOgcGeometry(OGCGeometry geometry)
    {
        try
        {
            if (geometry.is3D())
            {
                throw new MarshalException(String.format("'%s' is not 2D", getWkt(geometry)));
            }

            if (!geometry.isSimple())
            {
                throw new MarshalException(String.format("'%s' is not simple. Points and edges cannot self-intersect.", getWkt(geometry)));
            }
        }
        catch (GeometryException e)
        {
            throw new MarshalException("Invalid geometry", e);
        }
    }

    public String asWellKnownText()
    {
        return getSerializer().toWellKnownText(this);
    }

    public ByteBuffer asWellKnownBinary()
    {
        return getSerializer().toWellKnownBinary(this);
    }

    public String asGeoJson()
    {
        return getSerializer().toGeoJson(this);
    }
}
