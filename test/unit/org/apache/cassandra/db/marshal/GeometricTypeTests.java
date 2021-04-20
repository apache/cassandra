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

import com.esri.core.geometry.Polyline;
import com.esri.core.geometry.ogc.OGCLineString;
import com.esri.core.geometry.ogc.OGCPolygon;
import org.apache.cassandra.db.marshal.geometry.LineString;
import org.apache.cassandra.db.marshal.geometry.OgcGeometry;
import org.apache.cassandra.db.marshal.geometry.Point;
import org.apache.cassandra.db.marshal.geometry.Polygon;

public class GeometricTypeTests
{
    public static Point p(double x, double y)
    {
        return new Point(x, y);
    }

    public static com.esri.core.geometry.Point ep(double x, double y)
    {
        return new com.esri.core.geometry.Point(x, y);
    }

    public static com.esri.core.geometry.Point ep(Point p)
    {
        return new com.esri.core.geometry.Point(p.getOgcPoint().X(), p.getOgcPoint().Y());
    }

    public static LineString lineString(Point p1, Point p2, Point... pn)
    {
        Polyline polyline = new Polyline(ep(p1), ep(p2));
        for (Point p : pn)
        {
            polyline.lineTo(ep(p));
        }

        return new LineString(new OGCLineString(polyline, 0, OgcGeometry.SPATIAL_REFERENCE_4326));
    }

    public static Polygon polygon(Point p1, Point p2, Point p3, Point... pn)
    {
        com.esri.core.geometry.Polygon polygon = new com.esri.core.geometry.Polygon();
        polygon.startPath(ep(p1));
        polygon.lineTo(ep(p2));
        polygon.lineTo(ep(p3));
        for (Point p : pn)
        {
            polygon.lineTo(ep(p));
        }
        return new Polygon(new OGCPolygon(polygon, OgcGeometry.SPATIAL_REFERENCE_4326));
    }

    /**
     * pads the buffer with some leading and trailing data to aid testing
     * proper deserialization from continuous buffers
     */
    public static ByteBuffer padBuffer(ByteBuffer bb)
    {
        ByteBuffer padded = ByteBuffer.allocate(8 + bb.limit()).putInt(49).put(bb).putInt(50);
        padded.position(4);
        return padded;
    }
}
