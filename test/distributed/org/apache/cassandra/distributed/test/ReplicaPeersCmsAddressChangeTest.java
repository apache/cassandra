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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Startup;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.ownership.ReplicaPeers;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Replica peers while a CMS member is restarting with a new broadcast address which it has not yet committed. In that
 * window the CMS placement names the member by its new address, which the directory does not know yet.
 */
public class ReplicaPeersCmsAddressChangeTest extends TestBaseImpl
{
    private static final String KEYSPACE = "replica_peers_cms_address";
    private static final String NEW_ADDRESS = "127.0.0.4";

    @Test
    public void testPeersOfCmsMemberWithUncommittedAddressChange() throws Throwable
    {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (WithProperties properties = new WithProperties().set(CassandraRelevantProperties.DTEST_ACCORD_ENABLED, false);
             Cluster cluster = builder().withNodes(3)
                                        .withConfig(c -> c.with(NETWORK, GOSSIP))
                                        .withInstanceInitializer(BB::install)
                                        .start())
        {
            IInvokableInstance n1 = cluster.get(1);
            IInvokableInstance n2 = cluster.get(2);
            n1.nodetoolResult("cms", "reconfigure", "3").asserts().success();
            ClusterUtils.waitForCMSToQuiesce(cluster, n1);
            cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}");

            int moved = ClusterUtils.getNodeId(n2).id();
            String oldAddress = n2.config().broadcastAddress().getAddress().getHostAddress();
            // Taken now: node ids are looked up by broadcast address, which node 2 cannot resolve during the window
            TreeSet<Integer> expectedCmsPeers = cmsOthers(cluster, moved);

            // Restart node 2 at a new address, holding it before it commits the change. It surveys the CMS at startup,
            // finds itself at the new address and installs an override for it, so its CMS placement names the new
            // address while its directory still holds the old one.
            n2.shutdown().get();
            ClusterUtils.updateAddress(n2, NEW_ADDRESS);
            Future<?> startup = executor.submit(() -> n2.startup());
            Map<String, Object> during;
            try
            {
                // ignoring exceptions while the restarting instance is not yet created
                Awaitility.waitAtMost(2, TimeUnit.MINUTES)
                          .ignoreExceptions()
                          .until(() -> n2.callOnInstance(() -> BB.reached.getCount() == 0));

                during = n2.callOnInstance(() -> peers(moved));
                // Precondition: the window is open
                assertThat(during.get("lookup_active")).isEqualTo(true);
                assertThat(during.get("directory_address")).isEqualTo(oldAddress);
                // Every CMS member is a peer of the other two, and the moved member is shown at its new address
                assertThat(during.get("cms_peers_of_moved")).isEqualTo(expectedCmsPeers);
                assertThat(during.get("endpoint_of_moved")).isEqualTo(NEW_ADDRESS);
            }
            finally
            {
                // Always let node 2 finish starting, else closing the cluster waits on it forever
                n2.runOnInstance(() -> BB.release.countDown());
            }
            startup.get(2, TimeUnit.MINUTES);
            ClusterUtils.waitForCMSToQuiesce(cluster, n1);

            // Once committed, the directory agrees with the address reported during the window
            Map<String, Object> after = n2.callOnInstance(() -> peers(moved));
            assertThat(after.get("directory_address")).isEqualTo(NEW_ADDRESS);
            assertThat(after.get("cms_peers_of_moved")).isEqualTo(during.get("cms_peers_of_moved"));
            assertThat(after.get("endpoint_of_moved")).isEqualTo(NEW_ADDRESS);
        }
        finally
        {
            executor.shutdownNow();
        }
    }

    private static TreeSet<Integer> cmsOthers(Cluster cluster, int moved)
    {
        TreeSet<Integer> others = new TreeSet<>();
        for (IInvokableInstance instance : cluster)
            others.add(ClusterUtils.getNodeId(instance).id());
        others.remove(moved);
        return others;
    }

    /** Runs on an instance: replica peers over every keyspace, as seen by that instance. */
    private static Map<String, Object> peers(int movedId)
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        NodeId moved = new NodeId(movedId);
        ReplicaPeers peers = ReplicaPeers.compute(metadata, ReplicaPeers.Options.resolve(metadata, null, null, null,
                                                                                          null));
        ReplicaPeers cmsPeers = ReplicaPeers.compute(metadata, ReplicaPeers.Options.resolve(metadata, null, null,
                                                                                             Set.of(SchemaConstants.METADATA_KEYSPACE_NAME),
                                                                                             null));
        TreeSet<Integer> cmsPeersOfMoved = new TreeSet<>();
        cmsPeers.peersOf(moved).read().values().forEach(ids -> ids.forEach(id -> cmsPeersOfMoved.add(id.id())));

        Map<String, Object> result = new HashMap<>();
        result.put("lookup_active", metadata.cmsLookup.isActive());
        result.put("directory_address", metadata.directory.endpoint(moved).getAddress().getHostAddress());
        result.put("cms_peers_of_moved", cmsPeersOfMoved);
        result.put("endpoint_of_moved", ((Map<?, ?>) peers.toMap().get("endpoints")).get(movedId).toString().split(":")[0]);
        return result;
    }

    public static class BB
    {
        static final CountDownLatch reached = new CountDownLatch(1);
        static final CountDownLatch release = new CountDownLatch(1);

        public static void install(ClassLoader cl, int instance)
        {
            if (instance != 2)
                return;

            new ByteBuddy().rebase(Startup.class)
                           .method(named("initializeCMSLookup"))
                           .intercept(MethodDelegation.to(BB.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        /**
         * Only reached by a node restarting at a new address. Hold it once the CMS lookup is built, before it commits
         * the change.
         */
        @SuppressWarnings("unused")
        public static ClusterMetadata initializeCMSLookup(NodeId nodeId, ClusterMetadata replayed, @SuperCall Callable<ClusterMetadata> zuper) throws Exception
        {
            ClusterMetadata result = zuper.call();
            reached.countDown();
            release.await();
            return result;
        }
    }
}
