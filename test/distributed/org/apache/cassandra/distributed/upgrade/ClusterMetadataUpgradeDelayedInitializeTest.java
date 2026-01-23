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

package org.apache.cassandra.distributed.upgrade;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;

import org.awaitility.Awaitility;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IUpgradeableInstance;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.shared.Versions;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Shared;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.junit.Assert.assertEquals;

public class ClusterMetadataUpgradeDelayedInitializeTest extends UpgradeTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(ClusterMetadataUpgradeDelayedInitializeTest.class);

    @Test
    public void delayedInitializationTest() throws Throwable
    {
        // In this test we force the initiator to pause between committing the PreInitialize and Initialize, then verify
        // that read/write/gossip messages exchanged whilst in this state do not cause the other nodes to fetch the
        // PreInitialize log entry. We inject that pause using ByteBuddy to add a wait on a CountdownLatch controlled
        // from the main test code.
        final int nodeCount = 3;
        Versions dtestVersions = Versions.find();
        Versions.Version earliestVersion = dtestVersions.getLatest(v41);
        Consumer<IInstanceConfig> configUpdater = config -> config.with(Feature.NETWORK, Feature.GOSSIP);
        Consumer<UpgradeableCluster.Builder> builderUpdater = builder -> builder.withInstanceInitializer(ClusterMetadataUpgradeDelayedInitializeTest.BBInstaller::installUpgradeVersionBB);
        try (UpgradeableCluster cluster = UpgradeableCluster.create(nodeCount, earliestVersion, configUpdater, builderUpdater))
        {
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor':3}"));
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (k int PRIMARY KEY)");

            // Upgrade all instances
            Versions.Version upgradeVersion = dtestVersions.getLatest(CURRENT);
            for (IUpgradeableInstance i : cluster)
                upgradeInstance(i, upgradeVersion);
            logger.info(String.format("Upgrade complete. Current version: %s", upgradeVersion));
            ((IInvokableInstance)cluster.get(2)).runOnInstance(() -> {
                KeyspaceMetadata ksm = ClusterMetadata.current().schema.getKeyspaceMetadata("distributed_test_keyspace");
                TableMetadata tm = ksm.tables.getNullable("tbl");
                System.out.println(tm.epoch);
            });
            // Start the CMS initialization, which should pause after committing the PreInitialize transform
            assertEquals(1, BBState.latch.getCount());
            ExecutorService initExecutor = Executors.newSingleThreadExecutor();
            IUpgradeableInstance initiator = cluster.get(1);
            Future<NodeToolResult> result = initExecutor.submit(() -> initiator.nodetoolResult("cms", "initialize"));
            logger.info("Waiting for initiator to pause");
            Awaitility.waitAtMost(30, TimeUnit.SECONDS).until(() -> {
                // wait until the intiator has committed the PreInitialize entry
                NodeToolResult res = initiator.nodetoolResult("cms");
                return res.getStdout().contains("Epoch: 1");
            });
            logger.info("Initiator paused");

            // internode messages for reads/writes should not trigger catchup between node1 and its peers
            for (int i = 1; i <= 3; i++)
            {
                cluster.coordinator(i).execute(withKeyspace("INSERT INTO %s.tbl (k) VALUES (0)"), ConsistencyLevel.ALL);
                cluster.coordinator(i).execute(withKeyspace("SELECT * FROM %s.tbl WHERE k=0"), ConsistencyLevel.ALL);
            }
            // nor should gossip messages
            awaitGossipUpdateFromPeer(cluster.get(2), initiator);
            awaitGossipUpdateFromPeer(cluster.get(3), initiator);

            // Assert that nodes 2 & 3 did not catch up from node 1
            cluster.get(2).nodetoolResult("cms").asserts().success().stdoutContains("Epoch: -9223372036854775807");
            cluster.get(3).nodetoolResult("cms").asserts().success().stdoutContains("Epoch: -9223372036854775807");

            logger.info("Allowing initiator to continue");
            BBState.latch.countDown();
            NodeToolResult initRes = result.get(30, TimeUnit.SECONDS);
            initRes.asserts().success();

            // Verify that the initialization completes
            Awaitility.waitAtMost(30, TimeUnit.SECONDS)
                      .until(() -> cluster.get(1).nodetoolResult("cms").getStdout().contains("Epoch: 3") &&
                                   cluster.get(2).nodetoolResult("cms").getStdout().contains("Epoch: 3") &&
                                   cluster.get(3).nodetoolResult("cms").getStdout().contains("Epoch: 3"));
        }
    }

    /**
     * Inspects gossip state on one peer and waits for the heartbeat state of another peer to increment
     * (which is not an absolute guarantee of direct communication between the two nodes, but should be good enough).
     *
     * @param waiter the node doing the checking & waiting to observe an update
     * @param waitingFor the node who's heartbeat being observed
     */
    private void awaitGossipUpdateFromPeer(IUpgradeableInstance waiter, IUpgradeableInstance waitingFor)
    {
        String targetEndpoint = waitingFor.config().getString("broadcast_address");
        ((IInvokableInstance) waiter).runOnInstance(() -> {
            InetAddressAndPort endpoint = InetAddressAndPort.getByNameUnchecked(targetEndpoint);
            final int before = Gossiper.instance.getEndpointStateForEndpoint(endpoint)
                                                .getHeartBeatState()
                                                .getHeartBeatVersion();

            long deadline = System.nanoTime() + TimeUnit.NANOSECONDS.convert(30, TimeUnit.SECONDS);
            while (Gossiper.instance.getEndpointStateForEndpoint(endpoint)
                                    .getHeartBeatState()
                                    .getHeartBeatVersion() < before + 1)
            {
                FBUtilities.sleepQuietly(100);
                if (System.nanoTime() > deadline)
                    throw new RuntimeException("Timed out waiting for gossip state to update");
            }
        });
    }

    private void upgradeInstance(IUpgradeableInstance instance, Versions.Version upgradeTo) throws ExecutionException, InterruptedException, TimeoutException
    {
        int instanceId = instance.config().num();
        logger.info("Shutting down instance {} to upgrade to {}", instanceId, upgradeTo.version);
        instance.shutdown(true).get(60, TimeUnit.SECONDS);
        logger.info("Starting instanceId {} on version {}", instanceId, upgradeTo.version);
        instance.setVersion(upgradeTo);
        instance.startup();
        logger.info("Started instanceId {}", instanceId);
    }


    @Shared
    public static class BBState
    {
        public static CountDownLatch latch = new CountDownLatch(1);
    }

    public static class BBInstaller
    {
        public static void installUpgradeVersionBB(ClassLoader classLoader, Integer num)
        {
            try
            {
                new ByteBuddy().rebase(ClusterMetadata.class)
                               .method(named("forceInitializedState"))
                               .intercept(MethodDelegation.to(ClusterMetadataUpgradeDelayedInitializeTest.BBInterceptor.class))
                               .make()
                               .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
            }
            catch (NoClassDefFoundError noClassDefFoundError)
            {
                logger.info("... but no class def", noClassDefFoundError);
            }
            catch (Throwable tr)
            {
                logger.info("Unable to intercept upgradeFromVersion method", tr);
                throw tr;
            }
        }
    }

    public static class BBInterceptor
    {
        @SuppressWarnings("unused")
        public static ClusterMetadata forceInitializedState(@SuperCall Callable<ClusterMetadata> zuper)
        {
            try
            {
                logger.info("forceInitializedState waiting...");
                BBState.latch.await(60, TimeUnit.SECONDS);
                logger.info("forceInitializedState continuing...");
                return zuper.call();
            }
            catch (Throwable e)
            {
                logger.info("error:", e);
                throw new RuntimeException(e);
            }
        }
    }
}
