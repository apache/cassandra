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

import java.util.Arrays;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;

public class ReadDigestConsistencyTest extends TestBaseImpl
{
    public static final String TABLE_NAME = "tbl";
    public static final String CREATE_TABLE = String.format("CREATE TABLE %s.%s (" +
                                                            "   k int, " +
                                                            "   c int, " +
                                                            "   s1 int static, " +
                                                            "   s2 set<int> static, " +
                                                            "   v1 int, " +
                                                            "   v2 set<int>, " +
                                                            "   PRIMARY KEY (k, c))", KEYSPACE, TABLE_NAME);

    public static final String INSERT = String.format("INSERT INTO %s.%s (k, c, s1, s2, v1, v2) VALUES (?, ?, ?, ?, ?, ?)", KEYSPACE, TABLE_NAME);


    public static final String SELECT_TRACE = "SELECT activity FROM system_traces.events where session_id = ? and source = ? ALLOW FILTERING;";

    @Test
    public void testDigestConsistency() throws Exception
    {
        try (Cluster cluster = init(builder().withNodes(2).start()))
        {
            cluster.schemaChange(CREATE_TABLE);
            insertData(cluster.coordinator(1));
            testDigestConsistency(cluster.coordinator(1));
            testDigestConsistency(cluster.coordinator(2));
        }
    }

    public static void checkTraceForDigestMismatch(ICoordinator coordinator, String query, Object... boundValues)
    {
        UUID sessionId = UUID.randomUUID();
        coordinator.executeWithTracing(sessionId, query, ConsistencyLevel.ALL, boundValues);
        Object[][] results = coordinator.execute(SELECT_TRACE,
                                                 ConsistencyLevel.ALL,
                                                 sessionId,
                                                 coordinator.instance().broadcastAddress().getAddress());
        for (Object[] result : results)
        {
            String activity = (String) result[0];
            Assert.assertFalse(String.format("Found Digest Mismatch while executing query: %s with bound values %s on %s/%s",
                                             query,
                                             Arrays.toString(boundValues),
                                             coordinator.instance().broadcastAddress(),
                                             coordinator.instance().getReleaseVersionString()),
                               activity.toLowerCase().contains("mismatch for key"));
        }
    }

    public static void insertData(ICoordinator coordinator)
    {
        coordinator.execute(String.format("INSERT INTO %s.%s (k, c, s1, s2, v1, v2) VALUES (1, 1, 2, {1, 2, 3, 4, 5}, 3, {6, 7, 8, 9, 10})", KEYSPACE, TABLE_NAME), ConsistencyLevel.ALL);
        coordinator.execute(String.format("INSERT INTO %s.%s (k, c, s1, s2, v1, v2) VALUES (1, 2, 3, {2, 3, 4, 5, 6}, 4, {7, 8, 9, 10, 11})", KEYSPACE, TABLE_NAME), ConsistencyLevel.ALL);
    }

    public static void testDigestConsistency(ICoordinator coordinator)
    {
        String queryPattern = "SELECT %s FROM %s.%s WHERE %s";
        String[] columnss1 = {
        "k, c",
        "*",
        "v1",
        "v2",
        "v1, s1",
        "v1, s2"
        };

        String[] columnss2 = {
        "s1",
        "s2"
        };

        for (String columns : columnss1)
        {
            checkTraceForDigestMismatch(coordinator, String.format(queryPattern, columns, KEYSPACE, TABLE_NAME, "k = 1"));
            checkTraceForDigestMismatch(coordinator, String.format(queryPattern, columns, KEYSPACE, TABLE_NAME, "k = 1 AND c = 2"));
        }
        for (String columns : columnss2)
        {
            checkTraceForDigestMismatch(coordinator, String.format(queryPattern, columns, KEYSPACE, TABLE_NAME, "k = 1"));
        }
    }
}
