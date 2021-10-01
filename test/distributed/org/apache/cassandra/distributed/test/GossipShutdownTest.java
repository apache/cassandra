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
import java.net.InetAddress;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.concurrent.SimpleCondition;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

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
                cluster.coordinator(1).execute("insert into "+KEYSPACE+".tbl (id, v) values (?,?)", ConsistencyLevel.ALL, i, i);

            SimpleCondition timeToShutdown = new SimpleCondition();
            SimpleCondition waitForShutdown = new SimpleCondition();
            AtomicBoolean signalled = new AtomicBoolean(false);
            Future f = es.submit(() -> {
                await(timeToShutdown);

                cluster.get(1).runOnInstance(() -> {
                    Gossiper.instance.register(new EPChanges());
                });

                cluster.get(2).runOnInstance(() -> {
                    StorageService.instance.setIsShutdownUnsafeForTests(true);
                    Gossiper.instance.stop();
                });
                waitForShutdown.signalAll();
            });

            cluster.filters().outbound().from(2).to(1).verbs(MessagingService.Verb.GOSSIP_DIGEST_SYN.ordinal()).messagesMatching((from, to, message) -> true).drop();
            cluster.filters().outbound().from(2).to(1).verbs(MessagingService.Verb.GOSSIP_DIGEST_ACK.ordinal()).messagesMatching((from, to, message) ->
                                                                                                         {
                                                                                                             if (signalled.compareAndSet(false, true))
                                                                                                             {
                                                                                                                 timeToShutdown.signalAll();
                                                                                                                 await(waitForShutdown);
                                                                                                                 return false;
                                                                                                             }
                                                                                                             return true;
                                                                                                         }).drop();

            Thread.sleep(10000); // wait for gossip to exchange a few messages
            f.get();
        }
        finally
        {
            es.shutdown();
        }
    }

    private static void await(SimpleCondition sc)
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
        public void onAlive(InetAddress endpoint, EndpointState state)
        {
            if (wasDead)
                throw new RuntimeException("Node should not go live after it has been dead.");
        }
        public void onDead(InetAddress endpoint, EndpointState state)
        {
            wasDead = true;
        }
    };
}
