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
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;

import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.tcm.transformations.Startup;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.FBUtilities;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.test.ring.BootstrapTest.populate;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DecommissionTest extends TestBaseImpl
{
    @Test
    public void testResumableDecom() throws IOException
    {
        try (Cluster cluster = builder().withNodes(3)
                                        .withConfig(config -> config.with(NETWORK, GOSSIP))
                                        .withInstanceInitializer(BB::install)
                                        .start())
        {
            populate(cluster, 0, 100, 1, 2, ConsistencyLevel.QUORUM);
            cluster.get(2).nodetoolResult("decommission", "--force").asserts().failure();
            cluster.get(2).nodetoolResult("decommission", "--force").asserts().success();
        }
    }

    public static class BB
    {
        static void install(ClassLoader cl, int nodeNumber)
        {
            if (nodeNumber != 2)
                return;
            new ByteBuddy().rebase(StreamSession.class)
                           .method(named("startStreamingFiles"))
                           .intercept(MethodDelegation.to(BB.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }
        static AtomicBoolean first = new AtomicBoolean();

        public static void startStreamingFiles(@Nullable StreamSession.PrepareDirection prepareDirection, @SuperCall Callable<Void> zuper) throws Exception
        {
            if (!first.get())
            {
                first.set(true);
                throw new RuntimeException("Triggering streaming error");
            }
            zuper.call();
        }
    }

    @Test
    public void testDecomDirectoryMinMaxVersions() throws IOException {
        try (Cluster cluster = builder()
                               .withConfig(cfg -> cfg.with(GOSSIP))
                               .withNodes(3)
                .start())
        {
            cluster.get(3).nodetoolResult("decommission", "--force").asserts().success();

            cluster.get(1).runOnInstance(() -> {
                ClusterMetadata metadata = ClusterMetadata.current();
                ClusterMetadataService.instance().commit(new Startup(metadata.myNodeId(),
                                                                     metadata.directory.getNodeAddresses(metadata.myNodeId()),
                                                                     new NodeVersion(new CassandraVersion("6.0.0"),
                                                                                     NodeVersion.CURRENT_METADATA_VERSION)));
            });

            cluster.get(2).runOnInstance(() -> {
                ClusterMetadata metadata = ClusterMetadata.current();
                ClusterMetadataService.instance().commit(new Startup(metadata.myNodeId(),
                                                                     metadata.directory.getNodeAddresses(metadata.myNodeId()),
                                                                     new NodeVersion(new CassandraVersion("5.0.0"),
                                                                                     NodeVersion.CURRENT_METADATA_VERSION)));
            });

            for (int i = 1; i <= 2; i++)
            {
                cluster.get(i).runOnInstance(() -> {
                    ClusterMetadata metadata = ClusterMetadata.current();
                    assertEquals(new CassandraVersion("5.0.0"), metadata.directory.clusterMinVersion.cassandraVersion);
                    assertEquals(new CassandraVersion("6.0.0"), metadata.directory.clusterMaxVersion.cassandraVersion);
                    assertTrue(metadata.directory.versions.containsValue(NodeVersion.CURRENT));
                });
            }
        }
    }

    @Test
    public void testMixedVersionBlockDecom() throws IOException {
        try (Cluster cluster = builder().withNodes(3)
                                        .withConfig(config -> config.with(GOSSIP, NETWORK))
                                        .start())
        {
            cluster.get(3).nodetoolResult("decommission", "--force").asserts().success();

            // make node2 run V0:
            cluster.get(2).runOnInstance(() -> {
                ClusterMetadata metadata = ClusterMetadata.current();

                ClusterMetadataService.instance().commit(new Startup(metadata.myNodeId(),
                                                                     metadata.directory.getNodeAddresses(metadata.myNodeId()),
                                                                     new NodeVersion(new CassandraVersion("4.0.0"),
                                                                                     Version.V0)));
            });

            // make node1 run V1:
            cluster.get(1).runOnInstance(() -> {
                ClusterMetadata metadata = ClusterMetadata.current();

                ClusterMetadataService.instance().commit(new Startup(metadata.myNodeId(),
                                                                     metadata.directory.getNodeAddresses(metadata.myNodeId()),
                                                                     new NodeVersion(new CassandraVersion("6.0.0"),
                                                                                     NodeVersion.CURRENT_METADATA_VERSION)));
            });
            ClusterUtils.waitForCMSToQuiesce(cluster, cluster.get(1), 3);
            NodeToolResult res = cluster.get(2).nodetoolResult("decommission", "--force");
            res.asserts().failure();
            assertTrue(res.getStdout().contains("Upgrade in progress"));
            cluster.get(2).runOnInstance(() -> {
                ClusterMetadata metadata = ClusterMetadata.current();

                ClusterMetadataService.instance().commit(new Startup(metadata.myNodeId(),
                                                                     metadata.directory.getNodeAddresses(metadata.myNodeId()),
                                                                     new NodeVersion(new CassandraVersion("6.0.0"),
                                                                                     NodeVersion.CURRENT_METADATA_VERSION)));
            });
            cluster.get(2).nodetoolResult("decommission", "--force").asserts().success();
        }
    }

    @Test
    public void testPeersPostDecom() throws IOException
    {
        try (Cluster cluster = builder().withNodes(4)
                                        .withConfig(config -> config.with(NETWORK, GOSSIP))
                                        .start())
        {
            populate(cluster, 0, 100, 1, 2, ConsistencyLevel.QUORUM);
            cluster.get(3).nodetoolResult("decommission", "--force").asserts().success();

            int[] remainingNodes = {1, 2, 4};
            Set<String> expectedPeers = new HashSet<>();
            for (int i : remainingNodes)
                expectedPeers.add(cluster.get(i).config().broadcastAddress().getAddress().toString());

            // Decommission should remove from both the peers & peers_v2 system tables
            for (int i : remainingNodes)
            {
                cluster.get(i).runOnInstance(() -> {
                    for (String table : new String[] {"peers", "peers_v2"})
                    {
                        Set<String> values = new HashSet<>();
                        QueryProcessor.executeInternal(String.format("SELECT peer from system.%s;", table))
                                      .forEach(r -> values.add(r.getInetAddress("peer").toString()));
                        assertEquals(2, values.size());
                        for (String e : expectedPeers)
                            if (!e.equals(FBUtilities.getJustBroadcastAddress().toString()))
                                assertTrue(values.contains(e));
                    }
                });
            }
        }
    }


}
