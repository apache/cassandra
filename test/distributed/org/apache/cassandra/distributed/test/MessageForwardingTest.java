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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.impl.IsolatedExecutor;
import org.apache.cassandra.distributed.impl.TracingUtil;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.config.CassandraRelevantProperties.WAIT_FOR_TRACING_EVENTS_TIMEOUT_SECS;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;

public class MessageForwardingTest extends TestBaseImpl
{
    @Test
    public void mutationsForwardedToAllReplicasTest()
    {
        final int numInserts = 100;
        Map<InetAddress, Integer> forwardFromCounts = new HashMap<>();
        Map<InetAddress, Integer> commitCounts = new HashMap<>();

        try (WithProperties properties = new WithProperties().set(WAIT_FOR_TRACING_EVENTS_TIMEOUT_SECS, 1);
             Cluster cluster = (Cluster) init(builder()
                                              .withDC("dc0", 1)
                                              .withDC("dc1", 3)
                                              .start());
             )
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v text, PRIMARY KEY (pk, ck))");

            cluster.forEach(instance -> commitCounts.put(instance.broadcastAddress().getAddress(), 0));
            final TimeUUID sessionId = nextTimeUUID();
            Stream<Future<Object[][]>> inserts = IntStream.range(0, numInserts).mapToObj((idx) -> {
                return cluster.coordinator(1).asyncExecuteWithTracing(sessionId.asUUID(),
                                                                      "INSERT INTO " + KEYSPACE + ".tbl(pk,ck,v) VALUES (1, 1, 'x')",
                                                                      ConsistencyLevel.ALL);
            });

            // Wait for each of the futures to complete before checking the traces, don't care
            // about the result so
            //noinspection ResultOfMethodCallIgnored
            inserts.map(IsolatedExecutor::waitOn).collect(Collectors.toList());

            // Tracing is async with respect to queries, just because the query has completed it does not mean
            // all tracing updates have completed. The tracing executor serializes work, so run a task through
            // and everthing submitted before must have completed.
            cluster.forEach(instance -> instance.runOnInstance(() -> {
                Future<?> result = Stage.TRACING.submit(() -> null);
                try
                {
                    result.get(30, TimeUnit.SECONDS);
                }
                catch (ExecutionException | InterruptedException | TimeoutException ex)
                {
                    throw new RuntimeException(ex);
                }
            }));

            cluster.stream("dc1").forEach(instance -> forwardFromCounts.put(instance.broadcastAddress().getAddress(), 0));
            cluster.forEach(instance -> commitCounts.put(instance.broadcastAddress().getAddress(), 0));
            List<TracingUtil.TraceEntry> traces = TracingUtil.getTrace(cluster, sessionId.asUUID(), ConsistencyLevel.ALL);
            traces.forEach(traceEntry -> {
                if (traceEntry.activity.contains("Appending to commitlog"))
                {
                    commitCounts.compute(traceEntry.source, (k, v) -> (v != null ? v : 0) + 1);
                }
                else if (traceEntry.activity.contains("Enqueuing forwarded write to "))
                {
                    forwardFromCounts.compute(traceEntry.source, (k, v) -> (v != null ? v : 0) + 1);
                }
            });

            // Check that each node in dc1 was the forwarder at least once.  There is a (1/3)^numInserts chance
            // that the same node will be picked, but the odds of that are ~2e-48.
            forwardFromCounts.forEach((source, count) -> Assert.assertTrue(source + " should have been randomized to forward messages", count > 0));

            // Check that each node received the forwarded messages once (and only once)
            commitCounts.forEach((source, count) ->
                                 Assert.assertEquals(source + " appending to commitlog traces",
                                                     (long) numInserts, (long) count));
        }
        catch (IOException e)
        {
            Assert.fail("Threw exception: " + e);
        }
    }
}
