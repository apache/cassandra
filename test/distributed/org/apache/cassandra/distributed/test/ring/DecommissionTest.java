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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.google.common.util.concurrent.Uninterruptibles;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.Constants;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.transformations.PrepareLeave;
import org.apache.cassandra.tcm.transformations.Startup;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.db.SystemKeyspace.TRANSFERRED_RANGES_V2;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.ClusterUtils.pauseBeforeCommit;
import static org.apache.cassandra.distributed.shared.ClusterUtils.unpauseCommits;
import static org.apache.cassandra.distributed.shared.NetworkTopology.dcAndRack;
import static org.apache.cassandra.distributed.shared.NetworkTopology.networkTopology;
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

    @Test
    public void testOperationModeOnDecomResume() throws Exception
    {
        // Node 2's first decomission attempt fails mid-stream (injected by BB), leaving it in
        // DECOMISSION_FAILED. On resume, operationMode() must transition back to LEAVING before
        // MID_LEAVE is committed. We pause the CMS just before that commit to assert the mode
        // in the window where streaming has finished but the epoch has not yet advanced.
        try (Cluster cluster = builder().withNodes(3)
                                        .withConfig(config -> config.with(NETWORK, GOSSIP))
                                        .withInstanceInitializer(BB::install)
                                        .start())
        {
            populate(cluster, 0, 100, 1, 2, ConsistencyLevel.QUORUM);

            IInvokableInstance cmsNode = cluster.get(1);
            IInvokableInstance leavingNode = cluster.get(2);

            // --force required: system_distributed keyspace has RF=3, decommission would fail replication check otherwise
            leavingNode.nodetoolResult("decommission", "--force").asserts().failure();
            leavingNode.runOnInstance(() -> assertEquals(StorageService.Mode.DECOMMISSION_FAILED, StorageService.instance.operationMode()));

            Callable<Epoch> midLeavePaused = pauseBeforeCommit(cmsNode, e -> e instanceof PrepareLeave.MidLeave);

            Thread resumeThread = new Thread(() -> leavingNode.nodetoolResult("decommission").asserts().success());
            resumeThread.start();
            midLeavePaused.call();

            leavingNode.runOnInstance(() ->
                                      assertEquals("operationMode during resumed decommission streaming should be LEAVING, not DECOMMISSION_FAILED",
                                                   StorageService.Mode.LEAVING,
                                                   StorageService.instance.operationMode()));

            unpauseCommits(cmsNode);
            resumeThread.join(TimeUnit.MINUTES.toMillis(2));
        }
    }

    @Test
    public void testOperationModeOnFreshDecomAfterRejectedAttempt() throws Exception
    {
        try (Cluster cluster = builder().withNodes(3)
                                        .withConfig(config -> config.with(NETWORK, GOSSIP))
                                        .start())
        {
            populate(cluster, 0, 100, 1, 2, ConsistencyLevel.QUORUM);

            IInvokableInstance cmsNode = cluster.get(1);
            IInvokableInstance leavingNode = cluster.get(2);

            leavingNode.nodetoolResult("decommission").asserts().failure();
            leavingNode.runOnInstance(() -> assertEquals(StorageService.Mode.DECOMMISSION_FAILED,
                                                         StorageService.instance.operationMode()));

            Callable<Epoch> midLeavePaused = pauseBeforeCommit(cmsNode, e -> e instanceof PrepareLeave.MidLeave);

            Thread decomThread = new Thread(() -> leavingNode.nodetoolResult("decommission", "--force").asserts().success());
            decomThread.start();
            midLeavePaused.call();

            leavingNode.runOnInstance(() ->
                assertEquals("operationMode during re-decommission after rejected attempt should be LEAVING, not DECOMMISSION_FAILED",
                             StorageService.Mode.LEAVING,
                             StorageService.instance.operationMode()));

            unpauseCommits(cmsNode);
            decomThread.join(TimeUnit.MINUTES.toMillis(2));
        }
    }

    @Test
    public void testAddressReuseAfterDecommission() throws IOException, ExecutionException, InterruptedException
    {
        // Initially, all nodes should be in dc1/rack1. Node 3 will be decommissioned and a new node added re-using
        // node 3's address. When the new node registers, it should be in dc2/rack2.
        // For now, this requires the accord service to disabled. See CASSANDRA-21026
        try (Cluster cluster = builder().withNodes(3)
                                        .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(4))
                                        .withConfig(config -> config.with(NETWORK, GOSSIP)
                                                                    .set("accord.enabled", false))
                                        .withNodeIdTopology(networkTopology(3, (id) -> dcAndRack("dc1", "rack1")))
                                        .start())
        {
            assertEquals("dc1/rack1", cluster.get(1).callOnInstance(() -> DatabaseDescriptor.getLocator().local().toString()));
            assertEquals("dc1/rack1", cluster.get(2).callOnInstance(() -> DatabaseDescriptor.getLocator().local().toString()));
            assertEquals("dc1/rack1", cluster.get(3).callOnInstance(() -> DatabaseDescriptor.getLocator().local().toString()));

            IInvokableInstance toRemove = cluster.get(3);
            toRemove.nodetoolResult("decommission", "--force").asserts().success();
            toRemove.shutdown().get();
            ClusterUtils.getDirectories(toRemove).forEach(File::tryDeleteRecursive);
            cluster.unsafeRemoveNode(toRemove);

            // Now add a new node, using the same address as the one we just removed. This new node should register
            // itself in dc2/rack2 and not inherit the location of its predecessor.
            // Note: because we have removed the original node3 from the cluster completely, which is necessary because
            // the cluster will complain about an id clash otherwise, this new node will also be "node3". However, it is
            // completely distinct from the original one.
            cluster.unsafeUpdateNodeIdTopology(toRemove.config().num(), dcAndRack("dc2", "rack2"));
            IInstanceConfig config = cluster.newInstanceConfig()
                                            .set("auto_bootstrap", true)
                                            .set(Constants.KEY_DTEST_FULL_STARTUP, true);
            IInvokableInstance newInstance = cluster.bootstrap(config);
            newInstance.startup();

            assertEquals("dc1/rack1", cluster.get(1).callOnInstance(() -> DatabaseDescriptor.getLocator().local().toString()));
            assertEquals("dc1/rack1", cluster.get(2).callOnInstance(() -> DatabaseDescriptor.getLocator().local().toString()));
            assertEquals("dc2/rack2", newInstance.callOnInstance(() -> DatabaseDescriptor.getLocator().local().toString()));
        }
    }

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
                                        .withInstanceInitializer(BB::streamHintsInstall)
                                        .start())
        {
            // We need blob columns here so later we can do Murmur3Partitioner.LongToken.keyForToken(token);
            populate(cluster, 0, 100, 1, 2, ConsistencyLevel.QUORUM, "pk blob, ck blob, v blob");

            IInvokableInstance leavingNode = cluster.get(2);

            leavingNode.nodetoolResult("decommission").asserts().failure();
            leavingNode.runOnInstance(() -> assertEquals(StorageService.Mode.DECOMMISSION_FAILED, StorageService.instance.operationMode()));

            // abort the decommission
            leavingNode.nodetoolResult("abortdecommission").asserts().success();

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
            ClusterUtils.waitForCMSToQuiesce(cluster, cluster.get(1));

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

        static void streamHintsInstall(ClassLoader cl, int nodeNumber)
        {
            if (nodeNumber != 2)
                return;
            new ByteBuddy().rebase(StorageService.class)
                           .method(named("streamHints"))
                           .intercept(MethodDelegation.to(BB.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        static AtomicBoolean first = new AtomicBoolean();

        public static Future<?> streamHints(@SuperCall Callable<Future<?>> zuper) throws Exception
        {
            if (!first.get())
            {
                first.set(true);
                return ImmediateFuture.failure(new IOException("failing hints so that decomm fails at last moment possible" ));
            }
            return zuper.call();
        }

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
    public void testAbortDecom() throws IOException
    {
        try (Cluster cluster = builder().withNodes(3)
                                        .withConfig(config -> config.with(NETWORK, GOSSIP))
                                        .withInstanceInitializer(BB::install)
                                        .start())
        {
            populate(cluster, 0, 100, 1, 2, ConsistencyLevel.QUORUM);
            cluster.get(2).nodetoolResult("decommission", "--force").asserts().failure();
            cluster.get(2).nodetoolResult("abortdecommission").asserts().success();
            cluster.get(2).runOnInstance(() -> {
                assertEquals(StorageService.Mode.NORMAL, StorageService.instance.operationMode());
                assertTrue(ClusterMetadata.current().inProgressSequences.isEmpty());
            });
            cluster.get(2).nodetoolResult("decommission", "--force").asserts().success();
        }
    }

    @Test
    public void testAbortDecomRemote() throws IOException, ExecutionException, InterruptedException
    {
        try (Cluster cluster = builder().withNodes(3)
                                        .withConfig(config -> config.with(NETWORK, GOSSIP))
                                        .withInstanceInitializer(BB::install)
                                        .start())
        {
            populate(cluster, 0, 100, 1, 2, ConsistencyLevel.QUORUM);
            int nodeId = cluster.get(2).callOnInstance(() -> {
                return ClusterMetadata.current().myNodeId().id();
            });
            cluster.get(2).nodetoolResult("decommission", "--force").asserts().failure();
            cluster.get(2).shutdown().get();
            cluster.get(3).runOnInstance(() -> {
                while (Gossiper.instance.isAlive(ClusterMetadata.current().directory.endpoint(new NodeId(nodeId))))
                    Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
            });
            cluster.get(3).nodetoolResult("abortdecommission", "--node", String.valueOf(nodeId)).asserts().success();
            cluster.get(2).startup();
            cluster.get(2).runOnInstance(() -> {
                assertEquals(StorageService.Mode.NORMAL, StorageService.instance.operationMode());
                assertTrue(ClusterMetadata.current().inProgressSequences.isEmpty());
            });
            cluster.get(2).runOnInstance(() -> {
                BB.first.set(true);
            });
            cluster.get(2).nodetoolResult("decommission", "--force").asserts().success();
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

    @Test
    public void testDontReplicateToLeftNodes() throws IOException, TimeoutException
    {
        try (Cluster cluster = init(builder().withNodes(3)
                                        .withConfig(config -> config.with(NETWORK, GOSSIP))
                                        .start()))
        {
            cluster.get(2).nodetoolResult("decommission", "--force").asserts().success();
            long mark = cluster.get(1).logs().mark();
            cluster.coordinator(1).execute(withKeyspace("create table %s.tbl (id int primary key)"), ConsistencyLevel.ONE);
            cluster.get(3).logs().watchFor("Enacted.*CreateTableStatement.*tbl");
            assertTrue(cluster.get(1).logs().grep(mark, "Replicating newly committed transformations up to.*127.0.0.2.*").getResult().isEmpty());
        }
    }


}
