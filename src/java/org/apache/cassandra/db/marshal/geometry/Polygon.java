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

import com.esri.core.geometry.GeoJsonExportFlags;
import com.esri.core.geometry.Operator;
import com.esri.core.geometry.OperatorExportToGeoJson;
import com.esri.core.geometry.OperatorFactoryLocal;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCPolygon;
import org.apache.cassandra.serializers.MarshalException;

public class Polygon extends OgcGeometry
{
    public static final Serializer<Polygon> serializer = new Serializer<Polygon>()
    {
        @Override
        public String toWellKnownText(Polygon geometry)
        {
            return geometry.polygon.asText();
        }

        @Override
        public ByteBuffer toWellKnownBinaryNativeOrder(Polygon geometry)
        {
            return geometry.polygon.asBinary();
        }

        @Override
        public String toGeoJson(Polygon geometry)
        {
            OperatorExportToGeoJson op = (OperatorExportToGeoJson) OperatorFactoryLocal.getInstance().getOperator(Operator.Type.ExportToGeoJson);
            return op.execute(GeoJsonExportFlags.geoJsonExportSkipCRS, geometry.polygon.esriSR, geometry.polygon.getEsriGeometry());
        }

        @Override
        public Polygon fromWellKnownText(String source)
        {
            return new Polygon(fromOgcWellKnownText(source, OGCPolygon.class));
        }

        @Override
        public Polygon fromWellKnownBinary(ByteBuffer source)
        {
            return new Polygon(fromOgcWellKnownBinary(source, OGCPolygon.class));
        }

        @Override
        public Polygon fromGeoJson(String source)
        {
            return new Polygon(fromOgcGeoJson(source, OGCPolygon.class));
        }
    };

    OGCPolygon polygon;

    public Polygon(OGCPolygon polygon)
    {
        this.polygon = polygon;
        validate();
    }

    @Override
    protected OGCGeometry getOgcGeometry()
    {
        return polygon;
    }

    @Override
    public GeometricType getType()
    {
        return GeometricType.POLYGON;
    }

    @Override
    public void validate() throws MarshalException
    {
        validateOgcGeometry(polygon);
    }

    @Override
    public Serializer getSerializer()
    {
        return serializer;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Polygon polygon1 = (Polygon) o;

        return !(polygon != null ? !polygon.equals(polygon1.polygon) : polygon1.polygon != null);

    }

    @Override
    public int hashCode()
    {
        return polygon != null ? polygon.hashCode() : 0;
    }

    @Override
    public String toString()
    {
        return asWellKnownText();
    }
}
