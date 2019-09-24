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
import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.impl.IsolatedExecutor;
import org.apache.cassandra.distributed.impl.TracingUtil;
import org.apache.cassandra.utils.UUIDGen;

public class MessageForwardingTest extends DistributedTestBase
{
    @Test
    public void mutationsForwardedToAllReplicasTest()
    {
        String originalTraceTimeout = TracingUtil.setWaitForTracingEventTimeoutSecs("1");
        final int numInserts = 100;
        Map<InetAddress,Integer> commitCounts = new HashMap<>();

        try (Cluster cluster = init(Cluster.build()
                                           .withDC("dc0", 1)
                                           .withDC("dc1", 3)
                                           .start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v text, PRIMARY KEY (pk, ck))");

            cluster.forEach(instance -> commitCounts.put(instance.broadcastAddressAndPort().address, 0));
            final UUID sessionId = UUIDGen.getTimeUUID();
            Stream<Future<Object[][]>> inserts = IntStream.range(0, numInserts).mapToObj((idx) ->
                cluster.coordinator(1).asyncExecuteWithTracing(sessionId,
                                                         "INSERT INTO " + KEYSPACE + ".tbl(pk,ck,v) VALUES (1, 1, 'x')",
                                                         ConsistencyLevel.ALL)
            );

            // Wait for each of the futures to complete before checking the traces, don't care
            // about the result so
            //noinspection ResultOfMethodCallIgnored
            inserts.map(IsolatedExecutor::waitOn).count();

            cluster.forEach(instance -> commitCounts.put(instance.broadcastAddressAndPort().address, 0));
            List<TracingUtil.TraceEntry> traces = TracingUtil.getTrace(cluster, sessionId, ConsistencyLevel.ALL);
            traces.forEach(traceEntry -> {
                if (traceEntry.activity.contains("Appending to commitlog"))
                {
                    commitCounts.compute(traceEntry.source, (k, v) -> (v != null ? v : 0) + 1);
                }
            });

            // Check that each node received the forwarded messages once (and only once)
            commitCounts.forEach((source, count) ->
                                 Assert.assertEquals(source + " appending to commitlog traces",
                                                     (long) numInserts, (long) count));
        }
        catch (IOException e)
        {
            Assert.fail("Threw exception: " + e);
        }
        finally
        {
            TracingUtil.setWaitForTracingEventTimeoutSecs(originalTraceTimeout);
        }
    }
}
