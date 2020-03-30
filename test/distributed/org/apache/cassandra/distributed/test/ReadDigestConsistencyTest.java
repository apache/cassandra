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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.TimeUUID;

public class ReadDigestConsistencyTest extends TestBaseImpl
{
    private final static Logger logger = LoggerFactory.getLogger(ReadDigestConsistencyTest.class);

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
        UUID sessionId = TimeUUID.Generator.nextTimeUUID().asUUID();
        try
        {
            coordinator.executeWithTracing(sessionId, query, ConsistencyLevel.ALL, boundValues);
        }
        catch (RuntimeException ex)
        {
            if (Throwables.isCausedBy(ex, t -> t.getClass().getName().equals(SyntaxException.class.getName())))
            {
                if (coordinator.instance().getReleaseVersionString().startsWith("3.") && query.contains("["))
                {
                    logger.warn("Query {} is not supported on node {} version {}",
                                query,
                                coordinator.instance().broadcastAddress().getAddress().getHostAddress(),
                                coordinator.instance().getReleaseVersionString());

                    // we can forgive SyntaxException for C* < 4.0 if the query contains collection element selection
                    return;
                }
            }
            logger.error("Failing for coordinator {} and query {}", coordinator.instance().getReleaseVersionString(), query);
            throw ex;
        }
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
        "v1, s2",
        "v2[3]",
        "v2[2..4]",
        "v1, s2[7]",
        "v1, s2[6..8]"
        };

        String[] columnss2 = {
        "s1",
        "s2",
        "s2[7]",
        "s2[6..8]"
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
