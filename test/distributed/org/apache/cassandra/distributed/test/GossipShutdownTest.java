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
import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.util.concurrent.Uninterruptibles;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.gms.GossipShutdown;
import org.apache.cassandra.gms.GossipShutdownVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.utils.concurrent.Condition;

import org.junit.Test;

import static org.apache.cassandra.distributed.action.GossipHelper.statusToBootstrap;
import static org.apache.cassandra.distributed.action.GossipHelper.withProperty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.StorageService;


import static java.lang.Thread.sleep;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.gms.Gossiper.instance;
import static org.apache.cassandra.net.Verb.GOSSIP_DIGEST_ACK;
import static org.apache.cassandra.net.Verb.GOSSIP_DIGEST_SYN;
import static org.apache.cassandra.utils.concurrent.Condition.newOneTimeCondition;

public class GossipShutdownTest extends TestBaseImpl
{
    /**
     * Makes sure that a node that has shutdown doesn't come back as live (without being restarted)
     */
    @Test
    public void shutdownStayDownTest() throws IOException, InterruptedException, ExecutionException
    {
        ExecutorService es = Executors.newSingleThreadExecutor();
        try (Cluster cluster = init(builder().withNodes(2)
                                             .withConfig(config -> config.with(GOSSIP)
                                                                         .with(NETWORK))
                                             .start()))
        {
            cluster.schemaChange("create table "+KEYSPACE+".tbl (id int primary key, v int)");

            for (int i = 0; i < 10; i++)
                cluster.coordinator(1).execute("insert into "+KEYSPACE+".tbl (id, v) values (?,?)", ALL, i, i);

            Condition timeToShutdown = newOneTimeCondition();
            Condition waitForShutdown = newOneTimeCondition();
            AtomicBoolean signalled = new AtomicBoolean(false);
            Future f = es.submit(() -> {
                await(timeToShutdown);

                cluster.get(1).runOnInstance(() -> {
                    instance.register(new EPChanges());
                });

                cluster.get(2).runOnInstance(() -> {
                    StorageService.instance.setIsShutdownUnsafeForTests(true);
                    instance.stop();
                });
                waitForShutdown.signalAll();
            });

            cluster.filters().outbound().from(2).to(1).verbs(GOSSIP_DIGEST_SYN.id).messagesMatching((from, to, message) -> true).drop();
            cluster.filters().outbound().from(2).to(1).verbs(GOSSIP_DIGEST_ACK.id).messagesMatching((from, to, message) ->
                                                                                                         {
                                                                                                             if (signalled.compareAndSet(false, true))
                                                                                                             {
                                                                                                                 timeToShutdown.signalAll();
                                                                                                                 await(waitForShutdown);
                                                                                                                 return false;
                                                                                                             }
                                                                                                             return true;
                                                                                                         }).drop();

            sleep(10000); // wait for gossip to exchange a few messages
            f.get();
        }
        finally
        {
            es.shutdown();
        }
    }

    private static void await(Condition sc)
    {
        try
        {
            sc.await();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static class EPChanges implements IEndpointStateChangeSubscriber, Serializable
    {
        private volatile boolean wasDead = false;
        public void onAlive(InetAddressAndPort endpoint, EndpointState state)
        {
            if (wasDead)
                throw new RuntimeException("Node should not go live after it has been dead.");
        }
        public void onDead(InetAddressAndPort endpoint, EndpointState state)
        {
            wasDead = true;
        }
    };

    public static class BB
    {
        static ExecutorService es = Executors.newSingleThreadExecutor();
        public static void install(ClassLoader classLoader, Integer num)
        {
            new ByteBuddy().rebase(GossipShutdownVerbHandler.class)
                           .method(named("doVerb"))
                           .intercept(MethodDelegation.to(GossipShutdownTest.BB.class))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);

        }

        public static void doVerb(Message<GossipShutdown> message, @SuperCall Callable<Void> zuper)
        {
            es.submit(() -> {
                // sleep 10 second to simulate a long network delay of the gossip shutdown message
                Uninterruptibles.sleepUninterruptibly(30, TimeUnit.SECONDS);
                try
                {
                    zuper.call();
                }
                catch (Exception e)
                {
                    throw new RuntimeException(e);
                }
            });

        }
    }

    @Test
    public void shutdownGenerationCheckTest() throws IOException
    {
        try (Cluster cluster = init(builder().withNodes(2)
                                             .withConfig(config -> config.with(GOSSIP)
                                                                         .with(NETWORK))
                                             .withInstanceInitializer(GossipShutdownTest.BB::install)
                                             .start()))
        {
            IInvokableInstance node1 = cluster.get(1);
            IInvokableInstance node2 = cluster.get(2);
            NodeToolResult result =  node2.nodetoolResult("gossipinfo");
            result.asserts().success();
            String node1Address = node1.broadcastAddress().getAddress().toString();
            int oldGeneration = Integer.parseInt(getValueFromGossipinfo(result.getStdout(), "generation:", node1Address));
            //restart node1
            ClusterUtils.stopUnchecked(node1);
            node1.startup();
            //check node2 get the new generation of node 1 and verify that the node is normal
            result =  node2.nodetoolResult("gossipinfo");
            result.asserts().success();
            int newGeneration = Integer.parseInt(getValueFromGossipinfo(result.getStdout(), "generation:", node1Address));
            assertTrue(oldGeneration < newGeneration);
            String status = getValueFromGossipinfo(result.getStdout(), "STATUS_WITH_PORT:", node1Address);
            assertTrue(status.contains("NORMAL"));

            // the shutdown message arrives and should be ignored because node2 has the latest generation of node1
            Uninterruptibles.sleepUninterruptibly(32, TimeUnit.SECONDS);
            result =  node2.nodetoolResult("gossipinfo");
            result.asserts().success();
            int latestGeneration = Integer.parseInt(getValueFromGossipinfo(result.getStdout(), "generation:", node1Address));
            assertEquals(newGeneration, latestGeneration);
            status = getValueFromGossipinfo(result.getStdout(), "STATUS_WITH_PORT:", node1Address);
            assertTrue(status.contains("NORMAL"));
        }
    }

    private String getValueFromGossipinfo(String gossipInfoOutput, String key, String nodeIPAddress)
    {
        String[] lines = gossipInfoOutput.split("\\r?\\n");
        String currentNodeIP = null;
        for (String line : lines)
        {
            line = line.trim();

            if (line.startsWith("/") && line.equals(nodeIPAddress))
            {
                // Extract the IP address (skipping the leading "/")
                currentNodeIP = line;
            }
            else if (line.startsWith(key))
            {
                // Extract the generation value
                String value = line.substring(key.length()).trim();

                // Put into the map if we have an IP
                if (currentNodeIP != null)
                {
                    return value;
                }
            }
        }
        return "";
    }

    @Test
    public void forceShutdownNodeTest() throws IOException, TimeoutException, InterruptedException
    {
        int originalNodeCount = 2;
        int expandedNodeCount = originalNodeCount + 1;

        try (Cluster cluster = builder().withNodes(originalNodeCount)
                                        .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(expandedNodeCount))
                                        .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(expandedNodeCount, "dc0", "rack0"))
                                        .withConfig(config -> config.with(NETWORK, GOSSIP))
                                        .start())
        {
            IInstanceConfig config = cluster.newInstanceConfig();
            IInvokableInstance newInstance = cluster.bootstrap(config);
            withProperty("cassandra.join_ring", false,
                         () -> newInstance.startup(cluster));

            cluster.forEach(statusToBootstrap(newInstance));
            cluster.setUncaughtExceptionsFilter(t ->
                                                t.getMessage().contains("is in silient shutdown state(e.g. the node is joining) or endpoint is not an owner of the token ring")
            || t.getMessage().contains("Not able to find endpoint state from gossip endpoint state map for endpoint"));

            // test force shutdown a bootstrapping node should fail
            IInvokableInstance node1 = cluster.get(1);
            IInvokableInstance node2 = cluster.get(2);
            NodeToolResult result =  node1.nodetoolResult("shutdown", newInstance.broadcastAddress().getHostString() + ':' + newInstance.broadcastAddress().getPort(), "-f");
            result.asserts().errorContains("is in silient shutdown state(e.g. the node is joining) or endpoint is not an owner of the token ring");

            // test force shutdown a non-existing node should fail
            result = node1.nodetoolResult("shutdown", "2.2.2.2", "-f");
            result.asserts().errorContains("Not able to find endpoint state from gossip endpoint state map for endpoint");
        }
    }
}
