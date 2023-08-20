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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.After;
import org.junit.Assert;
import org.junit.AssumptionViolatedException;
import org.junit.Before;
import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.distributed.test.DecommissionTest;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.StorageService;

import static java.util.Arrays.asList;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.config.CassandraRelevantProperties.JOIN_RING;
import static org.apache.cassandra.config.CassandraRelevantProperties.MIGRATION_DELAY;
import static org.apache.cassandra.config.CassandraRelevantProperties.RESET_BOOTSTRAP_PROGRESS;
import static org.apache.cassandra.distributed.action.GossipHelper.bootstrap;
import static org.apache.cassandra.distributed.action.GossipHelper.pullSchemaFrom;
import static org.apache.cassandra.distributed.action.GossipHelper.statusToBootstrap;
import static org.apache.cassandra.distributed.action.GossipHelper.withProperty;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BootstrapTest extends TestBaseImpl
{
    private long savedMigrationDelay;

    static WithProperties properties;

    @Before
    public void beforeTest()
    {
        // MigrationCoordinator schedules schema pull requests immediatelly when the node is just starting up, otherwise
        // the first pull request is sent in 60 seconds. Whether we are starting up or not is detected by examining
        // the node up-time and if it is lower than MIGRATION_DELAY, we consider the server is starting up.
        // When we are running multiple test cases in the class, where each starts a node but in the same JVM, the
        // up-time will be more or less relevant only for the first test. In order to enforce the startup-like behaviour
        // for each test case, the MIGRATION_DELAY time is adjusted accordingly
        properties = new WithProperties().set(MIGRATION_DELAY, ManagementFactory.getRuntimeMXBean().getUptime() + savedMigrationDelay);
    }

    @After
    public void afterTest()
    {
        properties.close();
    }

    @Test
    public void bootstrapWithResumeTest() throws Throwable
    {
        RESET_BOOTSTRAP_PROGRESS.setBoolean(false);
        bootstrapTest();
    }

    @Test
    public void bootstrapWithoutResumeTest() throws Throwable
    {
        RESET_BOOTSTRAP_PROGRESS.setBoolean(true);
        bootstrapTest();
    }

    /**
     * Confirm that a normal, non-resumed bootstrap without the reset_bootstrap_progress param specified works without issue.
     * @throws Throwable
     */
    @Test
    public void bootstrapUnspecifiedResumeTest() throws Throwable
    {
        RESET_BOOTSTRAP_PROGRESS.clearValue(); // checkstyle: suppress nearby 'clearValueSystemPropertyUsage'
        bootstrapTest();
    }

    /**
     * Confirm that, in the absence of the reset_bootstrap_progress param being set and in the face of a found prior
     * partial bootstrap, we error out and don't complete our bootstrap.
     *
     * Test w/out vnodes only; logic is identical for both run env but the token alloc in this test doesn't work for
     * vnode env and it's not worth the lift to update it to work in both env.
     *
     * @throws Throwable
     */
    @Test
    public void bootstrapUnspecifiedFailsOnResumeTest() throws Throwable
    {
        RESET_BOOTSTRAP_PROGRESS.clearValue(); // checkstyle: suppress nearby 'clearValueSystemPropertyUsage'

        // Need our partitioner active for rangeToBytes conversion below
        Config c = DatabaseDescriptor.loadConfig();
        DatabaseDescriptor.daemonInitialization(() -> c);

        int originalNodeCount = 2;
        int expandedNodeCount = originalNodeCount + 1;

        boolean sawException = false;
        try (Cluster cluster = builder().withNodes(originalNodeCount)
                                        .withoutVNodes()
                                        .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(expandedNodeCount))
                                        .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(expandedNodeCount, "dc0", "rack0"))
                                        .withConfig(config -> config.with(NETWORK, GOSSIP))
                                        .start())
        {
            populate(cluster, 0, 100);

            IInstanceConfig config = cluster.newInstanceConfig();
            IInvokableInstance newInstance = cluster.bootstrap(config);
            withProperty(JOIN_RING, false, () -> newInstance.startup(cluster));
            cluster.forEach(statusToBootstrap(newInstance));

            List<Token> tokens = cluster.tokens();
            assert tokens.size() >= 3;

            /*
            Our local tokens:
            Tokens in cluster tokens: [-3074457345618258603, 3074457345618258601, 9223372036854775805]

            From the bootstrap process:
            fetchReplicas in our test keyspace:
            [FetchReplica
                {local=Full(/127.0.0.3:7012,(-3074457345618258603,3074457345618258601]),
                remote=Full(/127.0.0.1:7012,(-3074457345618258603,3074457345618258601])},
            FetchReplica
                {local=Full(/127.0.0.3:7012,(9223372036854775805,-3074457345618258603]),
                remote=Full(/127.0.0.1:7012,(9223372036854775805,-3074457345618258603])},
            FetchReplica
                {local=Full(/127.0.0.3:7012,(3074457345618258601,9223372036854775805]),
                remote=Full(/127.0.0.1:7012,(3074457345618258601,9223372036854775805])}]
             */

            // Insert some bogus ranges in the keyspace to be bootstrapped to trigger the check on available ranges on bootstrap.
            // Note: these have to precisely overlap with the token ranges hit during streaming or they won't trigger the
            // availability logic on bootstrap to then except out; we can't just have _any_ range for a keyspace, but rather,
            // must have a range that overlaps with what we're trying to stream.
            Set<Range<Token>> fullSet = new HashSet<>();
            fullSet.add(new Range<>(tokens.get(0), tokens.get(1)));
            fullSet.add(new Range<>(tokens.get(1), tokens.get(2)));
            fullSet.add(new Range<>(tokens.get(2), tokens.get(0)));

            // Should be fine to trigger on full ranges only but add a partial for good measure.
            Set<Range<Token>> partialSet = new HashSet<>();
            partialSet.add(new Range<>(tokens.get(2), tokens.get(1)));

            String cql = String.format("INSERT INTO %s.%s (keyspace_name, full_ranges, transient_ranges) VALUES (?, ?, ?)",
                                       SchemaConstants.SYSTEM_KEYSPACE_NAME,
                                       SystemKeyspace.AVAILABLE_RANGES_V2);

            newInstance.executeInternal(cql,
                                        KEYSPACE,
                                        fullSet.stream().map(SystemKeyspace::rangeToBytes).collect(Collectors.toSet()),
                                        partialSet.stream().map(SystemKeyspace::rangeToBytes).collect(Collectors.toSet()));

            // We expect bootstrap to throw an exception on node3 w/the seen ranges we've inserted
            cluster.run(asList(pullSchemaFrom(cluster.get(1)),
                               bootstrap()),
                        newInstance.config().num());
        }
        catch (AssumptionViolatedException ave)
        {
            // We get an AssumptionViolatedException if we're in a test job configured w/vnodes
            throw ave;
        }
        catch (RuntimeException rte)
        {
            if (rte.getMessage().contains("Discovered existing bootstrap data"))
                sawException = true;
        }
        Assert.assertTrue("Expected to see a RuntimeException w/'Discovered existing bootstrap data' in the error message; did not.",
                          sawException);
    }

    private void bootstrapTest() throws Throwable
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
            withProperty(JOIN_RING, false,
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
            withProperty(JOIN_RING, false,
                         () -> newInstance.startup(cluster));

            cluster.forEach(statusToBootstrap(newInstance));

            populate(cluster, 0, 100);

            Assert.assertEquals(100, newInstance.executeInternal("SELECT * FROM " + KEYSPACE + ".tbl").length);
        }
    }

    @Test
    public void bootstrapJMXStatus() throws Throwable
    {
        int originalNodeCount = 2;
        int expandedNodeCount = originalNodeCount + 1;

        RESET_BOOTSTRAP_PROGRESS.setBoolean(false);
        try (Cluster cluster = builder().withNodes(originalNodeCount)
                                        .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(expandedNodeCount))
                                        .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(expandedNodeCount, "dc0", "rack0"))
                                        .withConfig(config -> config.with(NETWORK, GOSSIP))
                                        .withInstanceInitializer(BootstrapTest.BB::install)
                                        .start())
        {
            bootstrapAndJoinNode(cluster);

            IInvokableInstance joiningInstance = cluster.get(3);

            joiningInstance.runOnInstance(() -> {
                assertEquals("IN_PROGRESS", StorageService.instance.getBootstrapState());
                assertTrue(StorageService.instance.isBootstrapFailed());
            });

            joiningInstance.nodetoolResult("bootstrap", "resume").asserts().success();
            joiningInstance.runOnInstance(() -> {
                assertEquals("COMPLETED", StorageService.instance.getBootstrapState());
                assertFalse(StorageService.instance.isBootstrapFailed());
            });
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

    public static class BB
    {
        public static void install(ClassLoader classLoader, Integer num)
        {
            if (num != 3)
            {
                return;
            }
            new ByteBuddy().rebase(StorageService.class)
                           .method(named("bootstrapFinished"))
                           .intercept(MethodDelegation.to(DecommissionTest.BB.class))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
        }

        private static int invocations = 0;

        @SuppressWarnings("unused")
        public static void bootstrapFinished(@SuperCall Callable<Void> zuper)
        {
            ++invocations;

            if (invocations == 1)
                throw new RuntimeException("simulated error in bootstrapFinished");

            try
            {
                zuper.call();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }
    }

}
