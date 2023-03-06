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

package org.apache.cassandra.distributed.test.log;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.log.LogStorage;
import org.apache.cassandra.tcm.log.Entry;
import org.apache.cassandra.tcm.transformations.CustomTransformation;
import org.apache.cassandra.utils.concurrent.CountDownLatch;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DistributedLogTest extends TestBaseImpl
{
    @Test
    public void testSequentialDelivery() throws Throwable
    {
        try (Cluster cluster = builder().withDC("DC1", 2)
                                        .withDC("DC2", 2)
                                        .withConfig(config -> config.with(NETWORK, GOSSIP).set("metadata_snapshot_frequency", Integer.MAX_VALUE)) // test assumes that all nodes have all events, that is not true if snapshotting
                                                                                                                                                  // todo; add config param to disable snapshotting
                                        .start())
        {
            List<String> expected = new ArrayList<>();

            final int iterations = 100;
            for (int i = 0; i < iterations; i++)
            {
                for (int j = 1; j <= cluster.size(); j++)
                {
                    int node = j;
                    int counter = i;

                    String s = cluster.get(j).callOnInstance(() -> {
                        try
                        {
                            return (String) ClusterMetadataService.instance()
                                                                  .commit(CustomTransformation.make(String.format("test payload %d %d", node, counter)))
                                   .extensions.get(CustomTransformation.PokeString.METADATA_KEY).getValue();
                        }
                        catch (Throwable t)
                        {
                            throw new AssertionError(t);
                        }
                    });
                    expected.add(s);
                }
            }

            for (int counter = 0; counter < iterations; counter++)
            {
                for (int node = 1; node <= cluster.size(); node++)
                {
                    Assert.assertTrue(expected.contains(String.format("test payload %d %d", node, counter)));
                }
            }

            assertEquals(iterations * cluster.size(),
                                expected.size());

            cluster.forEach(node -> {
                List<String> actual = node.callOnInstance(() -> {
                    List<String> res = new ArrayList<>();

                    for (Entry entry : LogStorage.SystemKeyspace.getLogState(Epoch.FIRST).transformations.entries())
                    {
                        if (entry.transform instanceof CustomTransformation)
                        {
                            CustomTransformation.PokeString e = (CustomTransformation.PokeString) ((CustomTransformation) entry.transform).child();
                            res.add(e.str);
                        }
                    }
                    return res;
                });
                assertEquals(expected, actual);
            });
        }
    }

    @Test
    public void testConcurrentCommit() throws Throwable
    {
        final int threadsPerNode = 10;
        final int iterations = 100;

        try (Cluster cluster = builder().withNodes(3)
                                        // test assumes that all nodes have all events, that is not true if snapshotting
                                        .withConfig(config -> config.set("metadata_snapshot_frequency", Integer.MAX_VALUE)
                                                                    .set("default_retry_max_tries", 100))
                                        // todo; add config param to disable snapshotting
                                        .start())
        {
            Set<String> expected = ConcurrentHashMap.newKeySet();
            List<Thread> threads = new ArrayList<>();

            CountDownLatch waitForStart = CountDownLatch.newCountDownLatch(1);
            CountDownLatch waitForFinish = CountDownLatch.newCountDownLatch(threadsPerNode * cluster.size());

            for (int j = 1; j <= cluster.size(); j++)
            {
                int node = j;
                for (int i = 0; i < threadsPerNode; i++)
                {
                    int thread = i;
                    threads.add(new Thread(() -> {
                        waitForStart.awaitUninterruptibly();
                        try
                        {
                            for (int k = 0; k < iterations; k++)
                            {
                                int iteration = k;
                                String str = String.format("test payload %d %d %d", node, thread, iteration);
                                cluster.get(node).runOnInstance(() -> {
                                    try
                                    {
                                        ClusterMetadataService.instance().commit(CustomTransformation.make(str));
                                    }
                                    catch (Throwable t)
                                    {
                                        throw new AssertionError(t);
                                    }
                                });
                                expected.add(str);
                            }
                        }
                        catch (Throwable t)
                        {
                            t.printStackTrace();
                            throw t;
                        }
                        finally
                        {
                            waitForFinish.decrement();
                        }
                    }, String.format("Writer#%d_%d", node, i)));
                }
            }

            for (Thread thread : threads)
                thread.start();

            waitForStart.decrement();
            waitForFinish.awaitUninterruptibly();

            assertEquals(expected.size(), iterations * threadsPerNode * cluster.size());

            cluster.forEach(node -> {
                Set<String> actual = node.callOnInstance(() -> {
                    ClusterMetadataService.instance().replayAndWait();

                    Set<String> res = new HashSet<>();

                    for (Entry entry : LogStorage.SystemKeyspace.getLogState(Epoch.FIRST).transformations.entries())
                    {
                        if (entry.transform instanceof CustomTransformation)
                        {
                            CustomTransformation.PokeString e = (CustomTransformation.PokeString) ((CustomTransformation) entry.transform).child();
                            res.add(e.str);
                        }
                    }
                    return res;
                });
                HashSet<String> diff = new HashSet<>(expected);
                diff.removeAll(actual);
                assertTrue(diff.toString(), diff.isEmpty());
            });
        }
    }
    // TODO: ensure exactly once delivery / callback?
    // TODO: concurrent submission?
}
