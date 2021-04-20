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
import com.esri.core.geometry.ogc.OGCLineString;
import org.apache.cassandra.serializers.MarshalException;

public class LineString extends OgcGeometry
{
    public static final Serializer<LineString> serializer = new Serializer<LineString>()
    {
        @Override
        public String toWellKnownText(LineString geometry)
        {
            return geometry.lineString.asText();
        }

        @Override
        public ByteBuffer toWellKnownBinaryNativeOrder(LineString geometry)
        {
            return geometry.lineString.asBinary();
        }

        @Override
        public String toGeoJson(LineString geometry)
        {
            OperatorExportToGeoJson op = (OperatorExportToGeoJson) OperatorFactoryLocal.getInstance().getOperator(Operator.Type.ExportToGeoJson);
            return op.execute(GeoJsonExportFlags.geoJsonExportSkipCRS, geometry.lineString.esriSR, geometry.lineString.getEsriGeometry());
        }

        @Override
        public LineString fromWellKnownText(String source)
        {
            return new LineString(fromOgcWellKnownText(source, OGCLineString.class));
        }

        @Override
        public LineString fromWellKnownBinary(ByteBuffer source)
        {
            return new LineString(fromOgcWellKnownBinary(source, OGCLineString.class));
        }

        @Override
        public LineString fromGeoJson(String source)
        {
            return new LineString(fromOgcGeoJson(source, OGCLineString.class));
        }
    };

    private final OGCLineString lineString;

    public LineString(OGCLineString lineString)
    {
        this.lineString = lineString;
        validate();
    }

    @Override
    public GeometricType getType()
    {
        return GeometricType.LINESTRING;
    }

    @Override
    public void validate() throws MarshalException
    {
        validateOgcGeometry(lineString);
    }

    @Override
    public Serializer getSerializer()
    {
        return serializer;
    }

    @Override
    protected OGCGeometry getOgcGeometry()
    {
        return lineString;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LineString that = (LineString) o;

        return !(lineString != null ? !lineString.equals(that.lineString) : that.lineString != null);

    }

    @Override
    public int hashCode()
    {
        return lineString != null ? lineString.hashCode() : 0;
    }

    @Override
    public String toString()
    {
        return asWellKnownText();
    }
}
