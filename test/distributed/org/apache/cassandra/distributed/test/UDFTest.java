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

package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.concurrent.ExecutionException;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

public class UDFTest extends TestBaseImpl
{
    private void testData(Cluster cluster, String city, String timestamp, double temp) {
        String stmt = "INSERT INTO "+KEYSPACE+".current (city, timestamp, location, measurement)\n" +
                      "VALUES ('%s', '%s', {coordinate: {latitude: %f, longitude: %f}, elevation: %f}, %f)";
        double lat;
        double lng;
        double el;

        if (city.equals("Helsinki")) {
            lat = 60.1699;
            lng = 24.9384;
            el = 51.0;
        } else {
            lat = 42.3601;
            lng = -71.0589;
            el = 43.0;
        }
        cluster.coordinator(1).execute(String.format(stmt, city, timestamp, lat, lng, el, temp), ConsistencyLevel.ALL);
    }

    @Test
    public void testReloadAfterStop() throws IOException, ExecutionException, InterruptedException
    {
        String[] createStmts = {
        "CREATE TYPE "+KEYSPACE+".coordinate (latitude decimal, longitude decimal)",
        "CREATE TYPE "+KEYSPACE+".location (coordinate FROZEN<coordinate>, elevation double)",
        "CREATE TABLE "+KEYSPACE+".current (city text, timestamp timestamp, location location, measurement double, t_uuid timeuuid, PRIMARY KEY (city, timestamp))",
        "CREATE TYPE IF NOT EXISTS "+KEYSPACE+".aggst (lt int, ge int)",
        "CREATE FUNCTION "+KEYSPACE+".city_measurements_sfunc (state map<text, frozen<aggst>>, city text, measurement double, threshold double)\n" +
        "RETURNS NULL ON NULL INPUT\n" +
        "RETURNS map<text, frozen<aggst>> LANGUAGE java AS $$\n" +
        "UDTValue a = (UDTValue)state.get(city);\n" +
        "if (a == null) {\n" +
        "    a = udfContext.newUDTValue(\"aggst\");\n" +
        "    if (measurement < threshold) {\n" +
        "        a.setInt(\"lt\", 1);\n" +
        "        a.setInt(\"ge\", 0);\n" +
        "    }\n" +
        "    else {\n" +
        "        a.setInt(\"lt\", 0);\n" +
        "        a.setInt(\"ge\", 1);\n" +
        "    }\n" +
        "}\n" +
        "else {\n" +
        "    if (measurement < threshold) {\n" +
        "        a.setInt(\"lt\", a.getInt(\"lt\") + 1);\n" +
        "    }\n" +
        "    else {\n" +
        "        a.setInt(\"ge\", a.getInt(\"ge\") + 1);\n" +
        "    }\n" +
        "}\n" +
        "state.put(city, a);\n" +
        "return state;\n" +
        "$$;",
        "CREATE AGGREGATE "+KEYSPACE+".city_measurements (text, double, double)\n" +
        "SFUNC city_measurements_sfunc\n" +
        "STYPE map<text, frozen<aggst>>\n" +
        "INITCOND {};\n"
        };
        ByteBuffer result = ByteBuffer.wrap(new byte[]{ 0, 0, 0, 2, 0, 0, 0, 6, 66, 111, 115, 116, 111, 110, 0, 0, 0, 16, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 24, 0, 0, 0, 8, 72, 101, 108, 115, 105, 110, 107, 105, 0, 0, 0, 16, 0, 0, 0, 4, 0, 0, 0, 8, 0, 0, 0, 4, 0, 0, 0, 16});
        try (Cluster cluster = init(Cluster.create(3, config -> config.set("enable_user_defined_functions", "true"))))
        {
            for (String stmt : createStmts) {
                cluster.schemaChange(stmt);
            }
            testData(cluster, "Helsinki", "2018-09-13 21:00:00", 19.2);
            testData(cluster, "Boston", "2018-09-13 11:00:00", 27.2);
            testData(cluster, "Boston", "2018-09-13 20:00:00", 29.0);
            testData(cluster, "Boston", "2018-09-13 19:00:00", 28.8);
            testData(cluster, "Helsinki", "2018-09-13 23:00:00", 19.6);
            testData(cluster, "Boston", "2018-09-13 00:00:00", 25.0);
            testData(cluster, "Helsinki", "2018-09-13 06:00:00", 16.2);
            testData(cluster, "Boston", "2018-09-13 04:00:00", 25.8);
            testData(cluster, "Boston", "2018-09-13 01:00:00", 25.2);
            testData(cluster, "Boston", "2018-09-13 10:00:00", 27.0);
            testData(cluster, "Boston", "2018-09-13 21:00:00", 29.2);
            testData(cluster, "Helsinki", "2018-09-13 12:00:00", 17.4);
            testData(cluster, "Helsinki", "2018-09-13 20:00:00", 19.0);
            testData(cluster, "Helsinki", "2018-09-13 07:00:00", 16.4);
            testData(cluster, "Helsinki", "2018-09-13 08:00:00", 16.6);
            testData(cluster, "Helsinki", "2018-09-13 17:00:00", 18.4);
            testData(cluster, "Helsinki", "2018-09-13 18:00:00", 18.6);
            testData(cluster, "Helsinki", "2018-09-13 11:00:00", 17.2);
            testData(cluster, "Boston", "2018-09-13 12:00:00", 27.4);
            testData(cluster, "Helsinki", "2018-09-13 15:00:00", 18.0);
            testData(cluster, "Helsinki", "2018-09-13 01:00:00", 15.2);
            testData(cluster, "Helsinki", "2018-09-13 19:00:00", 18.8);
            testData(cluster, "Boston", "2018-09-13 15:00:00", 28.0);
            testData(cluster, "Helsinki", "2018-09-13 09:00:00", 16.8);
            testData(cluster, "Helsinki", "2018-09-13 05:00:00", 16.0);
            testData(cluster, "Helsinki", "2018-09-13 13:00:00", 17.6);
            testData(cluster, "Helsinki", "2018-09-13 22:00:00", 19.4);
            testData(cluster, "Boston", "2018-09-13 14:00:00", 27.8);
            testData(cluster, "Boston", "2018-09-13 22:00:00", 29.4);
            testData(cluster, "Boston", "2018-09-13 18:00:00", 28.6);
            testData(cluster, "Boston", "2018-09-13 23:00:00", 29.6);
            testData(cluster, "Boston", "2018-09-13 02:00:00", 25.4);
            testData(cluster, "Boston", "2018-09-13 05:00:00", 26.0);
            testData(cluster, "Boston", "2018-09-13 09:00:00", 26.8);
            testData(cluster, "Helsinki", "2018-09-13 04:00:00", 15.8);
            testData(cluster, "Helsinki", "2018-09-13 00:00:00", 15.0);
            testData(cluster, "Boston", "2018-09-13 17:00:00", 28.4);
            testData(cluster, "Helsinki", "2018-09-13 14:00:00", 17.8);
            testData(cluster, "Helsinki", "2018-09-13 02:00:00", 15.4);
            testData(cluster, "Helsinki", "2018-09-13 03:00:00", 15.6);
            testData(cluster, "Boston", "2018-09-13 08:00:00", 26.6);
            testData(cluster, "Boston", "2018-09-13 07:00:00", 26.4);
            testData(cluster, "Helsinki", "2018-09-13 16:00:00", 18.2);
            testData(cluster, "Boston", "2018-09-13 03:00:00", 25.6);
            testData(cluster, "Boston", "2018-09-13 16:00:00", 28.2);
            testData(cluster, "Boston", "2018-09-13 06:00:00", 26.2);
            testData(cluster, "Boston", "2018-09-13 13:00:00", 27.6);
            testData(cluster, "Helsinki", "2018-09-13 10:00:00", 17.0);


            //Object[] rows = cluster.coordinator(1).execute( "SELECT "+KEYSPACE+".city_measurements(city, measurement, 16.5) AS m FROM "+KEYSPACE+".current", ConsistencyLevel.ALL);
            //Object x = ((Object[])rows[0])[0];
            ByteBuffer bostonValue = result.duplicate();
            bostonValue.position(18).limit(34);
            ByteBuffer helsinkiValue = result.duplicate();
            helsinkiValue.position(50).limit(66);

            LinkedHashMap<String,ByteBuffer> map = new LinkedHashMap();
            map.put("Boston", bostonValue);
            map.put("Helsinki",helsinkiValue);
            assertRows(cluster.coordinator(1).execute( "SELECT "+KEYSPACE+".city_measurements(city, measurement, 16.5) AS m FROM "+KEYSPACE+".current", ConsistencyLevel.ALL), row(map));
            cluster.get(1).shutdown().get();
            cluster.get(1).startup();
            assertRows(cluster.coordinator(1).execute( "SELECT "+KEYSPACE+".city_measurements(city, measurement, 16.5) AS m FROM "+KEYSPACE+".current", ConsistencyLevel.ALL), row(map));
        }
    }
}