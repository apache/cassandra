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

import java.io.IOException;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.marshal.geometry.LineString;
import org.apache.cassandra.db.marshal.geometry.OgcGeometry;
import org.apache.cassandra.db.marshal.geometry.Point;
import org.apache.cassandra.db.marshal.geometry.Polygon;
import org.apache.cassandra.transport.ProtocolVersion;

import static org.apache.cassandra.db.marshal.GeometricTypeTests.lineString;
import static org.apache.cassandra.db.marshal.GeometricTypeTests.p;
import static org.apache.cassandra.db.marshal.GeometricTypeTests.polygon;

public class GeometryIntegrationTest extends CQLTester
{

    @BeforeClass
    public static void setupCluster() throws IOException
    {
        CodecRegistry.DEFAULT_INSTANCE.register(GeometryCodec.pointCodec,
                                                GeometryCodec.lineStringCodec,
                                                GeometryCodec.polygonCodec);
    }

    @Before
    public void setUpKeyspace() throws Throwable
    {
        executeNet("CREATE KEYSPACE ks WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 1}");
    }

    @After
    public void teardown() throws Throwable
    {
        executeNet("DROP KEYSPACE ks;");
    }

    private <T extends OgcGeometry> void testType(T expected, Class<T> klass, AbstractGeometricType<T> type, String tableName, String wkt, String columnType) throws Throwable
    {
        executeNet(String.format("CREATE TABLE ks.%s (k INT PRIMARY KEY, g '%s')", tableName, columnType));
        executeNet(String.format("INSERT INTO ks.%s (k, g) VALUES (1, '%s')", tableName, wkt));

        ResultSet results = executeNet(String.format("SELECT * FROM ks.%s", tableName));
        List<Row> rows = results.all();
        Assert.assertEquals(1, rows.size());
        Row row = rows.get(0);
        T actual = row.get("g", klass);
        Assert.assertEquals(expected, actual);
        results = executeNet(String.format("SELECT toJson(g) FROM ks.%s", tableName));
        rows = results.all();
        Assert.assertEquals(1, rows.size());
        row = rows.get(0);
        String actualJson = row.getString("system.tojson(g)");
        String expectedJson = type.toJSONString(type.getSerializer().serialize(expected), ProtocolVersion.CURRENT);
        Assert.assertEquals(expectedJson, actualJson);
    }

    @Test
    public void pointTest() throws Throwable
    {
        executeNet("CREATE TABLE ks.point (k INT PRIMARY KEY, g 'PointType')");
        String wkt = "POINT(1.1 2.2)";
        executeNet(String.format("INSERT INTO ks.point (k, g) VALUES (1, '%s')", wkt));

        ResultSet results = executeNet("SELECT * FROM ks.point");
        List<Row> rows = results.all();
        Assert.assertEquals(1, rows.size());
        Row row = rows.get(0);
        Point point = row.get("g", Point.class);
        Assert.assertEquals(new Point(1.1, 2.2), point);
    }

    @Test
    public void lineStringTest() throws Throwable
    {
        LineString expected = lineString(p(30, 10), p(10, 30), p(40, 40));
        String wkt = "linestring(30 10, 10 30, 40 40)";
        testType(expected, LineString.class, LineStringType.instance, "linestring", wkt, "LineStringType");
    }

    @Test
    public void polygonTest() throws Throwable
    {
        Polygon expected = polygon(p(30, 10), p(10, 20), p(20, 40), p(40, 40));
        String wkt = "polygon((30 10, 40 40, 20 40, 10 20, 30 10))";
        testType(expected, Polygon.class, PolygonType.instance, "polygon", wkt, "PolygonType");
    }

    @Test
    public void primaryKeyTest() throws Throwable
    {
        executeNet("CREATE TABLE ks.geo (k 'PointType', c 'LineStringType', g 'PointType', PRIMARY KEY (k, c))");
        executeNet("INSERT INTO ks.geo (k, c, g) VALUES ('POINT(1 2)', 'linestring(30 10, 10 30, 40 40)', 'POINT(1.1 2.2)')");
        ResultSet results = executeNet("SELECT * FROM ks.geo");
        List<Row> rows = results.all();
        Assert.assertEquals(1, rows.size());
        Row row = rows.get(0);

        Point point1 = row.get("k", Point.class);
        Assert.assertEquals(new Point(1, 2), point1);

        LineString lineString = row.get("c", LineString.class);
        Assert.assertEquals(lineString(p(30, 10), p(10, 30), p(40, 40)), lineString);

        Point point = row.get("g", Point.class);
        Assert.assertEquals(new Point(1.1, 2.2), point);
    }
}