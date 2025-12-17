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

package org.apache.cassandra.distributed.test.gossip;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.gms.GossipExpiryHelper;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.listeners.LegacyStateListener;
import org.apache.cassandra.tcm.membership.NodeId;
import org.awaitility.Awaitility;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.config.CassandraRelevantProperties.VERY_LONG_TIME_MS;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.gms.ApplicationState.STATUS_WITH_PORT;
import static org.apache.cassandra.gms.VersionedValue.STATUS_LEFT;

public abstract class GossipExpiryTestBase extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(GossipExpiryTestBase.class);

    abstract void doRemoval(Cluster cluster, IInvokableInstance toRemove);

    @Test
    public void testExpiryOfLeftStateWithoutQuarantine() throws IOException
    {
        doTest(true);
    }

    @Test
    public void testExpiryOfLeftStateWithQuarantine() throws IOException
    {
        doTest(false);
    }

    private void doTest(boolean withQuarantineDisabled) throws IOException
    {
        // This test verifies that when a node leaves the cluster, the expiry time for its state in gossip is
        // recorded on each node and then expunged when that deadline is reached. By default, the expiry time for
        // a left peer is calculated on each node independently, but if the gossip_quarantine_disabled config
        // option is set to true it will converge and become consistent across the remaining members.
        //
        // * First we set the property that controls expiry time to 10s. This interval is added to the current wall
        //   clock time to calculate the expiry deadline.
        // * Use bytebuddy to inject some jitter into the local expiry time calculation. Each node will individually
        //   calculate an expiry based on when it processes the completion of the operation that removes the node. If
        //   the config option is set the cluster should converge on the expiry time calculated by the node coordinating
        //   the operation. For decommission, this will be the leaving node itself and for removenode/assassinate it
        //   will be the coordinator. The jitter is to make sure that in the test, the nodes start off with differing
        //   expiry times.
        // * Remove one peer via decommission, removenode or assassinate.
        // * After maybe verifying the convergence, check that the state for the left node does in fact get removed.
        try (WithProperties ignored = new WithProperties().set(VERY_LONG_TIME_MS, 11000);
             Cluster cluster = builder().withNodes(5)
                                        .withInstanceInitializer(GossipExpiryTestBase.BB::install)
                                        .withConfig(config -> config.with(NETWORK, GOSSIP)
                                                                    .set("gossip_quarantine_disabled", withQuarantineDisabled))
                                        .start())
        {
            cluster.forEach(i -> i.runOnInstance(() -> BB.injectDelay.set(true)));

            IInvokableInstance toRemove = cluster.get(5);
            String gossipStateKey = toRemove.config().broadcastAddress().getAddress().toString();
            doRemoval(cluster, toRemove);
            if (withQuarantineDisabled)
            {
                // STATUS_WITH_PORT for the left node should converge to share the same expiry time across all nodes
                Awaitility.waitAtMost(10, TimeUnit.SECONDS)
                          .pollInterval(1, TimeUnit.SECONDS)
                          .until(() -> {
                              Set<String> endpointStates = new HashSet<>();
                              cluster.forEach(i -> {
                                  if (!i.equals(toRemove))
                                  {
                                      Map<String, String> instanceState = ClusterUtils.gossipInfo(i).get(gossipStateKey);
                                      if (instanceState != null && instanceState.containsKey(STATUS_WITH_PORT.name()))
                                          endpointStates.add(instanceState.get(STATUS_WITH_PORT.name()));
                                  }
                              });
                              logger.info("Collected STATUS_WITH_PORT values: {}", endpointStates);
                              return endpointStates.size() == 1 && endpointStates.iterator()
                                                                                 .next()
                                                                                 .contains(STATUS_LEFT);
                          });
            }

            // Once the expiry time is reached, gossip state for the left node is purged
            Awaitility.waitAtMost(30, TimeUnit.SECONDS)
                      .pollInterval(1, TimeUnit.SECONDS)
                      .until(() -> {
                          AtomicBoolean purged = new AtomicBoolean(true);
                          cluster.forEach(i -> {
                              if (!i.equals(toRemove))
                              {
                                  // Expiry happens during periodic gossip tasks. Sometimes these may not run in a
                                  // timely fashion, so for tests we can trigger the status check artificially.
                                  i.runOnInstance(GossipExpiryHelper.evictExpiredFromGossip(toRemove));
                                  if (ClusterUtils.gossipInfo(i).containsKey(gossipStateKey))
                                      purged.set(false);
                              }
                          });
                          return purged.get();
                      });
        }
    }

    public static class BB
    {
        static void install(ClassLoader cl, int nodeNumber)
        {
            if (nodeNumber != 2)
                return;
            new ByteBuddy().rebase(LegacyStateListener.class)
                           .method(named("processChangesToRemotePeers"))
                           .intercept(MethodDelegation.to(BB.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        static AtomicBoolean injectDelay = new AtomicBoolean(false);
        static Random random = new Random(System.nanoTime());

        public static void processChangesToRemotePeers(ClusterMetadata prev,
                                                       ClusterMetadata next,
                                                       Set<NodeId> changed,
                                                       @SuperCall Callable<Void> zuper) throws Exception
        {
            if (injectDelay.get())
                TimeUnit.MILLISECONDS.sleep(random.nextInt(1000));
            zuper.call();
        }
    }
}
