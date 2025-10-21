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

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import com.google.monitoring.runtime.instrumentation.common.util.concurrent.Uninterruptibles;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.concurrent.ExecutorFactory;
import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.GossipDigest;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.utils.concurrent.Future;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertEquals;

public class GossipDeadlockTest extends TestBaseImpl
{
    @Test
    public void testPendingDeadlock() throws IOException, ExecutionException, InterruptedException
    {

        try (Cluster cluster = init(builder().withNodes(4)
                                             .withConfig(config -> config.with(NETWORK, GOSSIP)
                                                                         .set("progress_barrier_default_consistency_level", "NODE_LOCAL")
                                                                         .set("progress_barrier_min_consistency_level", "NODE_LOCAL"))
                                             .withoutVNodes()
                                             .withInstanceInitializer(BB::install) // make GossipTask.run slower to increase the deadlock window
                                             .start()))
        {
            cluster.schemaChange(withKeyspace("alter keyspace %s with replication = {'class': 'SimpleStrategy', 'replication_factor':1 }"));
            cluster.schemaChange("alter keyspace system_distributed with replication = {'class': 'SimpleStrategy', 'replication_factor':1 }");
            cluster.schemaChange("alter keyspace system_traces with replication = {'class': 'SimpleStrategy', 'replication_factor':1 }");

            ExecutorPlus e = ExecutorFactory.Global.executorFactory().pooled("BounceMove", 2);
            long startToken = cluster.get(2).callOnInstance(() -> {
                NodeId nodeId = ClusterMetadata.current().myNodeId();
                return ((Murmur3Partitioner.LongToken)ClusterMetadata.current().tokenMap.tokens(nodeId).get(0)).token;
            });
            AtomicBoolean stop = new AtomicBoolean(false);
            Future<Integer> moves = e.submit(() -> {
                long token = startToken;
                while (!stop.get())
                {
                    token++;
                    cluster.get(2).nodetoolResult("move", String.valueOf(token)).asserts().success();
                }
                return (int)(token - startToken);
            });
            Future<Integer> bounces = e.submit(() -> {
                int iters = 0;
                while (!stop.get())
                {
                    cluster.get(4).nodetoolResult("disablegossip").asserts().success();
                    Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
                    cluster.get(4).nodetoolResult("enablegossip").asserts().success();
                    Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
                    iters++;
                }
                return iters;
            });

            Uninterruptibles.sleepUninterruptibly(120, TimeUnit.SECONDS);
            stop.set(true);
            System.out.println("MOVES = " + moves.get());
            System.out.println("BOUNCES = " + bounces.get());

            long epoch = cluster.get(1).callOnInstance(() -> ClusterMetadata.current().epoch.getEpoch());
            for (int i = 1; i <= 4; i++)
                assertEquals(epoch, (long)cluster.get(i).callOnInstance(() -> ClusterMetadata.current().epoch.getEpoch()));
        }
    }

    public static class BB
    {
        public static void install(ClassLoader cl, int i)
        {
            new ByteBuddy().rebase(Gossiper.class)
                           .method(named("makeRandomGossipDigest")).intercept(MethodDelegation.to(BB.class))
                           .method(named("unsafeUpdateEpStates")).intercept(MethodDelegation.to(BB.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        public static void makeRandomGossipDigest(List<GossipDigest> digests, @SuperCall Callable<Void> zuper)
        {
            Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
            try
            {
                zuper.call();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }

        public static void unsafeUpdateEpStates(InetAddressAndPort endpoint, EndpointState epstate, @SuperCall Callable<Void> zuper)
        {
            Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
            try
            {
                zuper.call();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }
    }
}
