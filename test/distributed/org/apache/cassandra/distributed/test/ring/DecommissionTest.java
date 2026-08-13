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
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;

import org.junit.Test;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.db.SystemKeyspace.TRANSFERRED_RANGES_V2;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.test.ring.BootstrapTest.populate;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DecommissionTest extends TestBaseImpl
{
    @Test
    public void testAbortingDecommissionRestreams() throws Exception
    {
        // https://issues.apache.org/jira/browse/CASSANDRA-16290
        // We demonstrate here that decommissioning and then aborting decommission is unsafe
        // if we've persisted transferred ranges and then skip them for something which was delivered after we aborted the decommission but before we resumed
        try (Cluster cluster = builder().withNodes(4)
                                        .withConfig(config -> config.with(NETWORK, GOSSIP)
                                                                    // disable hints to simplify test
                                                                    .set("hinted_handoff_enabled", false)
                                        )
                                        // only install on the first generation of node2: a restarted instance gets a
                                        // fresh classloader, so any static "already failed once" state would be reset
                                        // and the resumed decommission would fail again
                                        .withInstanceInitializer((cl, threadGroup, num, generation) -> {
                                            if (num == 2 && generation == 0)
                                                BB.streamHintsInstall(cl);
                                        })
                                        .start())
        {
            // We need blob columns here so later we can do Murmur3Partitioner.LongToken.keyForToken(token);
            populate(cluster, 0, 100, 1, 2, ConsistencyLevel.QUORUM, "pk blob, ck blob, v blob");

            IInvokableInstance leavingNode = cluster.get(2);

            leavingNode.nodetoolResult("decommission").asserts().failure();

            // abort the decommission
            ClusterUtils.stopUnchecked(leavingNode);
            ClusterUtils.start(leavingNode, props -> {});
            ClusterUtils.awaitRingHealthy(leavingNode);

            // Stop the non leaving nodes so we can write at ONE and fail to stream that datum
            ClusterUtils.stopUnchecked(cluster.get(1));
            ClusterUtils.stopUnchecked(cluster.get(3));
            ClusterUtils.stopUnchecked(cluster.get(4));

            List<Murmur3Partitioner.LongToken> tokens = ClusterUtils.getLocalTokens(leavingNode).stream().map(t -> new Murmur3Partitioner.LongToken(Long.parseLong(t))).collect(Collectors.toList());
            for (Murmur3Partitioner.LongToken token : tokens)
            {
                ByteBuffer key = Murmur3Partitioner.LongToken.keyForToken(token);
                leavingNode.coordinator().execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (?, ?, ?)", ConsistencyLevel.ONE, key, key, key);
            }

            ClusterUtils.start(cluster.get(1), props -> {});
            ClusterUtils.start(cluster.get(3), props -> {});
            ClusterUtils.start(cluster.get(4), props -> {});

            ClusterUtils.awaitRingHealthy(leavingNode);

            Object[][] ranges = leavingNode.executeInternal("SELECT keyspace_name from system." + TRANSFERRED_RANGES_V2);

            assertTrue("transferred ranges missing entirely", ranges.length > 0);
            assertTrue("transferred ranges present for keyspace", Arrays.stream(ranges).anyMatch(x -> x[0].equals(KEYSPACE)));

            // Resume decomm
            leavingNode.nodetoolResult("decommission").asserts().success();

            // Try and read data we wrote at ONE at ALL
            for (Murmur3Partitioner.LongToken token : tokens)
            {
                ByteBuffer key = Murmur3Partitioner.LongToken.keyForToken(token);
                Object[][] resp = cluster.get(1).coordinator().execute("SELECT pk from " + KEYSPACE + ".tbl where pk=?", ConsistencyLevel.ALL, key);
                assertTrue("We should get a response for this key we wrote it at ONE", resp.length > 0);
                assertEquals(key, resp[0][0]);
            }
        }
    }

    public static class BB
    {
        static void streamHintsInstall(ClassLoader cl)
        {
            new ByteBuddy().rebase(StorageService.class)
                           .method(named("streamHints"))
                           .intercept(MethodDelegation.to(BB.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        @SuppressWarnings({ "unused", "rawtypes" })
        public static Future streamHints(@SuperCall Callable<Future> zuper)
        {
            // this is only installed on the first startup of the leaving node, so every invocation
            // here belongs to the decommission attempt we want to fail at the last moment possible
            return ImmediateFuture.failure(new IOException("failing hints so that decomm fails at last moment possible"));
        }
    }
}
