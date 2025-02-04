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

import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.StreamSession;

import static java.util.Arrays.asList;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.config.CassandraRelevantProperties.RESET_BOOTSTRAP_PROGRESS;
import static org.apache.cassandra.distributed.action.GossipHelper.bootstrap;
import static org.apache.cassandra.distributed.action.GossipHelper.pullSchemaFrom;
import static org.apache.cassandra.distributed.action.GossipHelper.statusToBootstrap;
import static org.apache.cassandra.distributed.action.GossipHelper.withProperty;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

public class BootstrapTest extends TestBaseImpl
{
    private long savedMigrationDelay;

    @Before
    public void beforeTest()
    {
        // MigrationCoordinator schedules schema pull requests immediatelly when the node is just starting up, otherwise
        // the first pull request is sent in 60 seconds. Whether we are starting up or not is detected by examining
        // the node up-time and if it is lower than MIGRATION_DELAY, we consider the server is starting up.
        // When we are running multiple test cases in the class, where each starts a node but in the same JVM, the
        // up-time will be more or less relevant only for the first test. In order to enforce the startup-like behaviour
        // for each test case, the MIGRATION_DELAY time is adjusted accordingly
        savedMigrationDelay = CassandraRelevantProperties.MIGRATION_DELAY.getLong();
        CassandraRelevantProperties.MIGRATION_DELAY.setLong(ManagementFactory.getRuntimeMXBean().getUptime() + savedMigrationDelay);
    }

    @After
    public void afterTest()
    {
        CassandraRelevantProperties.MIGRATION_DELAY.setLong(savedMigrationDelay);
    }

    @Test
    public void bootstrapTest() throws Throwable
    {
        int originalNodeCount = 2;
        int expandedNodeCount = originalNodeCount + 1;

        try (Cluster cluster = builder().withNodes(originalNodeCount)
                                        .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(expandedNodeCount))
                                        .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(expandedNodeCount, "dc0", "rack0"))
                                        .withConfig(config -> config.with(NETWORK, GOSSIP))
                                        .start())
        {
            populate(cluster, 0, 100);

            IInstanceConfig config = cluster.newInstanceConfig();
            IInvokableInstance newInstance = cluster.bootstrap(config);
            withProperty("cassandra.join_ring", false,
                         () -> newInstance.startup(cluster));

            cluster.forEach(statusToBootstrap(newInstance));

            cluster.run(asList(pullSchemaFrom(cluster.get(1)),
                               bootstrap()),
                        newInstance.config().num());

            for (Map.Entry<Integer, Long> e : count(cluster).entrySet())
                Assert.assertEquals("Node " + e.getKey() + " has incorrect row state",
                                    100L,
                                    e.getValue().longValue());
        }
    }

    @Test
    public void readWriteDuringBootstrapTest() throws Throwable
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

            populate(cluster, 0, 100);

            Assert.assertEquals(100, newInstance.executeInternal("SELECT *FROM " + KEYSPACE + ".tbl").length);
        }
    }

    public static void populate(ICluster cluster, int from, int to)
    {
        populate(cluster, from, to, 1, 3, ConsistencyLevel.QUORUM);
    }

    public static void populate(ICluster cluster, int from, int to, int coord, int rf, ConsistencyLevel cl)
    {
        cluster.schemaChange("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + rf + "};");
        cluster.schemaChange("CREATE TABLE IF NOT EXISTS " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
        for (int i = from; i < to; i++)
        {
            cluster.coordinator(coord).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (?, ?, ?)",
                                               cl,
                                               i, i, i);
        }
    }

    public static Map<Integer, Long> count(ICluster cluster)
    {
        return IntStream.rangeClosed(1, cluster.size())
                        .boxed()
                        .collect(Collectors.toMap(nodeId -> nodeId,
                                                  nodeId -> (Long) cluster.get(nodeId).executeInternal("SELECT count(*) FROM " + KEYSPACE + ".tbl")[0][0]));
    }

    public static class BBStreamFailure
    {
        public static final AtomicBoolean failStream = new AtomicBoolean();
        public static void install(ClassLoader cl, Integer i)
        {
            new ByteBuddy().rebase(StreamSession.class)
                           .method(named("startStreamingFiles"))
                           .intercept(MethodDelegation.to(BootstrapTest.BBStreamFailure.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        public static void startStreamingFiles(StreamSession.PrepareDirection prepareDirection, @SuperCall Callable<Boolean> zuper) throws Exception
        {
            if (failStream.get())
            {
                throw new RuntimeException("Trigger stream failure");
            }
            zuper.call();
        }
    }

    @Test
    public void bootstrapStreamFailedResumeTestWithResetBootstrapProgress() throws Throwable
    {
        bootstrapStreamFailedAndResumeTest(true);
    }

    @Test
    public void bootstrapStreamFailedResumeTestWithoutResetBootstrapProgress() throws Throwable
    {
        bootstrapStreamFailedAndResumeTest(false);
    }

    private void bootstrapStreamFailedAndResumeTest(boolean resetBootstrapProgress) throws Throwable
    {
        RESET_BOOTSTRAP_PROGRESS.setBoolean(resetBootstrapProgress);

        int originalNodeCount = 2;
        int expandedNodeCount = originalNodeCount + 1;

        try (Cluster cluster = builder().withNodes(originalNodeCount)
                                        .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(expandedNodeCount, 1))
                                        .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(expandedNodeCount, "dc0", "rack0"))
                                        .withConfig(config -> config.with(NETWORK, GOSSIP))
                                        .withInstanceInitializer(BootstrapTest.BBStreamFailure::install)
                                        .start())
        {
            populate(cluster, 0, 100);

            // Make node 1 stream fail
            cluster.get(1).runOnInstance(
            ()-> {
                BBStreamFailure.failStream.set(true);
            }
            );

            IInstanceConfig config = cluster.newInstanceConfig();
            config.set("auto_bootstrap", true);
            IInvokableInstance newInstance = cluster.bootstrap(config);
            newInstance.startup(cluster);
            newInstance.logs().watchFor("Stream failed");

            // Make node 1 stream normal
            cluster.get(1).runOnInstance(
            ()-> {
                BBStreamFailure.failStream.set(false);
            }
            );
            // verify that there is some data streamed to the new node and start bootstrap resume
            newInstance.runOnInstance(
            () -> {
                ColumnFamilyStore populatedStore = Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl");
                // received some sstable from node2
                Assert.assertTrue(populatedStore.getLiveSSTables().size() > 0);

                // Bootstrap resume
                StorageService.instance.resumeBootstrap();
            }
            );
            // Existing sstables are deleted
            if (resetBootstrapProgress)
                newInstance.logs().watchFor(String.format("Truncating %s.%s", KEYSPACE, "tbl"));

            // Wait for bootstrap to complete
            newInstance.logs().watchFor("Resume complete");

            for (Map.Entry<Integer, Long> e : count(cluster).entrySet())
                Assert.assertEquals("Node " + e.getKey() + " has incorrect row state",
                                    100L,
                                    e.getValue().longValue());
        }
    }
}
