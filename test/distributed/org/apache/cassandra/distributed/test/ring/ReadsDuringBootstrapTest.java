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

package org.apache.cassandra.distributed.test.ring;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.FieldValue;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.EndpointsForRange;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

public class ReadsDuringBootstrapTest extends TestBaseImpl
{

    @Test
    public void readsDuringBootstrapTest() throws IOException, ExecutionException, InterruptedException, TimeoutException
    {
        int originalNodeCount = 3;
        int expandedNodeCount = originalNodeCount + 1;
        ExecutorService es = Executors.newSingleThreadExecutor();
        try (Cluster cluster = builder().withNodes(originalNodeCount)
                                        .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(expandedNodeCount))
                                        .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(expandedNodeCount, "dc0", "rack0"))
                                        .withConfig(config -> config.with(NETWORK, GOSSIP)
                                                                    .set("read_request_timeout", String.format("%dms", Integer.MAX_VALUE))
                                                                    .set("request_timeout", String.format("%dms", Integer.MAX_VALUE)))
                                        .withInstanceInitializer(BB::install)
                                        .start())
        {
            String query = withKeyspace("SELECT * FROM %s.tbl WHERE id = ?");
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2};"));
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (id int PRIMARY KEY)"));
            cluster.get(1).runOnInstance(() -> BB.block.set(true));
            Future<?> read = es.submit(() -> cluster.coordinator(1).execute(query, ConsistencyLevel.QUORUM, 3));
            long mark = cluster.get(1).logs().mark();
            bootstrapAndJoinNode(cluster);
            cluster.get(1).logs().watchFor(mark, "New node /127.0.0.4");
            cluster.get(1).runOnInstance(() -> BB.block.set(false));
            // populate cache
            for (int i = 0; i < 10; i++)
                cluster.coordinator(1).execute(query, ConsistencyLevel.QUORUM, i);
            cluster.get(1).runOnInstance(() -> BB.latch.countDown());
            read.get();
        }
        finally
        {
            es.shutdown();
        }
    }

    public static class BB
    {
        public static final AtomicBoolean block = new AtomicBoolean();
        public static final CountDownLatch latch = new CountDownLatch(1);

        private static void install(ClassLoader cl, Integer instanceId)
        {
            if (instanceId != 1)
                return;
            new ByteBuddy().rebase(AbstractReplicationStrategy.class)
                           .method(named("getCachedReplicas"))
                           .intercept(MethodDelegation.to(BB.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        public static EndpointsForRange getCachedReplicas(long ringVersion, Token t,
                                                          @FieldValue("keyspaceName") String keyspaceName,
                                                          @SuperCall Callable<EndpointsForRange> zuper) throws Exception
        {
            if (keyspaceName.equals(KEYSPACE) && block.get())
                latch.await();
            return zuper.call();
        }
    }
}
