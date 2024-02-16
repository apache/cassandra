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
package org.apache.cassandra.cql3;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.metrics.ClientRequestsMetrics;
import org.apache.cassandra.metrics.ClientRequestsMetricsProvider;

import static org.apache.cassandra.db.ConsistencyLevel.NODE_LOCAL;
import static org.junit.Assert.assertEquals;

public class NodeLocalConsistencyTest extends CQLTester
{
    @BeforeClass
    public static void setUp() throws Exception
    {
        CassandraRelevantProperties.ENABLE_NODELOCAL_QUERIES.setBoolean(true);
    }

    @Test
    public void testModify()
    {
        ClientRequestsMetrics metrics = ClientRequestsMetricsProvider.instance.metrics(null);
        createTable("CREATE TABLE %s (key text, val int, PRIMARY KEY(key));");

        long beforeLevel  = metrics.writeMetricsForLevel(NODE_LOCAL).executionTimeMetrics.latency.getCount();
        long beforeGlobal = metrics.writeMetrics.executionTimeMetrics.latency.getCount();

        QueryProcessor.process(formatQuery("INSERT INTO %s (key, val) VALUES ('key', 0);"), NODE_LOCAL);

        long afterLevel  = metrics.writeMetricsForLevel(NODE_LOCAL).executionTimeMetrics.latency.getCount();
        long afterGlobal = metrics.writeMetrics.executionTimeMetrics.latency.getCount();

        assertEquals(1, afterLevel - beforeLevel);
        assertEquals(1, afterGlobal - beforeGlobal);
    }

    @Test
    public void testBatch()
    {
        ClientRequestsMetrics metrics = ClientRequestsMetricsProvider.instance.metrics(null);
        createTable("CREATE TABLE %s (key text, val int, PRIMARY KEY(key));");

        long beforeLevel  = metrics.writeMetricsForLevel(NODE_LOCAL).executionTimeMetrics.latency.getCount();
        long beforeGlobal = metrics.writeMetrics.executionTimeMetrics.latency.getCount();

        QueryProcessor.process(formatQuery("BEGIN BATCH INSERT INTO %s (key, val) VALUES ('key', 0); APPLY BATCH;"), NODE_LOCAL);

        long afterLevel  = metrics.writeMetricsForLevel(NODE_LOCAL).executionTimeMetrics.latency.getCount();
        long afterGlobal = metrics.writeMetrics.executionTimeMetrics.latency.getCount();

        assertEquals(1, afterLevel - beforeLevel);
        assertEquals(1, afterGlobal - beforeGlobal);
    }

    @Test
    public void testSelect()
    {
        ClientRequestsMetrics metrics = ClientRequestsMetricsProvider.instance.metrics(null);
        createTable("CREATE TABLE %s (key text, val int, PRIMARY KEY(key));");

        long beforeLevel  = metrics.readMetricsForLevel(NODE_LOCAL).executionTimeMetrics.latency.getCount();
        long beforeGlobal = metrics.readMetrics.executionTimeMetrics.latency.getCount();

        QueryProcessor.process(formatQuery("SELECT * FROM %s;"), NODE_LOCAL);

        long afterLevel  = metrics.readMetricsForLevel(NODE_LOCAL).executionTimeMetrics.latency.getCount();
        long afterGlobal = metrics.readMetrics.executionTimeMetrics.latency.getCount();

        assertEquals(1, afterLevel - beforeLevel);
        assertEquals(1, afterGlobal - beforeGlobal);
    }
}