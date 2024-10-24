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

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;

import net.bytebuddy.implementation.bind.annotation.Argument;
import org.apache.cassandra.db.SystemKeyspace;
import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.Constants;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.JMXUtil;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.metrics.DefaultNameFactory;
import org.apache.cassandra.service.StorageService;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.config.CassandraRelevantProperties.RESET_BOOTSTRAP_PROGRESS;
import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_WRITE_SURVEY;
import static org.apache.cassandra.distributed.action.GossipHelper.withProperty;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.JMX;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class BootstrapTest extends TestBaseImpl
{
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

    private void bootstrapTest() throws Throwable
    {
        // This test simply asserts that the value of the cassandra.reset_bootstrap_progress flag
        // has no impact on a normal, uninterrupted bootstrap
        int originalNodeCount = 2;
        int expandedNodeCount = originalNodeCount + 1;

        try (Cluster cluster = builder().withNodes(originalNodeCount)
                                        .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(expandedNodeCount))
                                        .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(expandedNodeCount, "dc0", "rack0"))
                                        .withConfig(config -> config.with(NETWORK, GOSSIP))
                                        .start())
        {
            populate(cluster, 0, 100);

            IInstanceConfig config = cluster.newInstanceConfig()
                                            .set("auto_bootstrap", true)
                                            .set(Constants.KEY_DTEST_FULL_STARTUP, true);
            IInvokableInstance newInstance = cluster.bootstrap(config);
            newInstance.startup(cluster);

            for (Map.Entry<Integer, Long> e : count(cluster).entrySet())
                assertEquals("Node " + e.getKey() + " has incorrect row state",
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
            withProperty(TEST_WRITE_SURVEY, true,
                         () -> newInstance.startup(cluster));
            populate(cluster, 0, 100);
            assertEquals(100, newInstance.executeInternal("SELECT * FROM " + KEYSPACE + ".tbl").length);
        }
    }

    @Test
    public void bootstrapJMXStatus() throws Throwable
    {
        int originalNodeCount = 2;
        int expandedNodeCount = originalNodeCount + 1;

        try (Cluster cluster = builder().withNodes(originalNodeCount)
                                        .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(expandedNodeCount))
                                        .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(expandedNodeCount, "dc0", "rack0"))
                                        .withConfig(config -> config.with(NETWORK, GOSSIP, JMX))
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

            assertEquals(Long.valueOf(0L), getMetricGaugeValue(joiningInstance, "BootstrapFilesTotal", Long.class));
            assertEquals(Long.valueOf(0L), getMetricGaugeValue(joiningInstance, "BootstrapFilesReceived", Long.class));
            assertEquals("Bootstrap streaming success", getMetricGaugeValue(joiningInstance, "BootstrapLastSeenStatus", String.class));
            assertEquals("", getMetricGaugeValue(joiningInstance, "BootstrapLastSeenError", String.class));
        }
    }

    @Test
    public void resumeBootstrapTest() throws Throwable {
        int originalNodeCount = 2;
        int expandedNodeCount = originalNodeCount + 1;
        try (Cluster cluster = builder().withNodes(originalNodeCount)
                                        .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(expandedNodeCount))
                                        .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(expandedNodeCount, "dc0", "rack0"))
                                        .withConfig(config -> config.with(NETWORK, GOSSIP, JMX))
                                        .withInstanceInitializer(BB::installSetBootstrapStateWithDelay)
                                        .start()) {
            populate(cluster, 0, 100);
            IInstanceConfig config = cluster.newInstanceConfig()
                                    .set("auto_bootstrap", true);
            IInvokableInstance newInstance = cluster.bootstrap(config);
            IInvokableInstance joiningInstance = cluster.get(3);
            newInstance.startup(cluster);

            joiningInstance.runOnInstance(() -> {
                assertEquals("IN_PROGRESS", StorageService.instance.getBootstrapState());
            });

            joiningInstance.runOnInstance(() -> {
                RESET_BOOTSTRAP_PROGRESS.setBoolean(false);
            });

            joiningInstance.runOnInstance(() -> {
                assertEquals("IN_PROGRESS", StorageService.instance.getBootstrapState());
            });
            assertEquals("Node is already bootstrapped or bootstrap is in progress.\n", joiningInstance.nodetoolResult("bootstrap", "resume").getStdout());
            // Releasing the lock.
            BB.latch.countDown();

            if (BB.isLatchreleased) {
                //Assuming node is still bootstrapping.
                joiningInstance.runOnInstance(() -> {
                    assertEquals("COMPLETED", StorageService.instance.getBootstrapState());
                });
            }
        }
    }

    public static <T> T getMetricGaugeValue(IInvokableInstance instance, String metricName, Class<T> gaugeReturnType) {
        return gaugeReturnType.cast(getMetricAttribute(instance, metricName, "Value"));
    }

    public static long getMetricMeterRate(IInvokableInstance instance, String metricName)
    {
        Object raw = getMetricAttribute(instance, metricName, "Count");
        return raw == null ? 0 : (Long) raw;
    }

    public static Object getMetricAttribute(IInvokableInstance instance, String metricName, String attributeName)
    {
        if (instance.isShutdown())
            throw new IllegalStateException("Instance is shutdown");

        try (JMXConnector jmxc = JMXUtil.getJmxConnector(instance.config()))
        {
            MBeanServerConnection mbsc = jmxc.getMBeanServerConnection();
            ObjectName metric = mbsc.queryNames(null, null)
                                    .stream()
                                    .filter(objectName -> objectName.getDomain().equals(DefaultNameFactory.GROUP_NAME))
                                    .filter(objectName -> Objects.nonNull(objectName.getKeyProperty("name")))
                                    .filter(objectName -> metricName.equals(objectName.getKeyProperty("name")))
                                    .findFirst()
                                    .orElse(null);

            if (metric == null)
                return null;

            MBeanInfo info = mbsc.getMBeanInfo(metric);
            for (MBeanAttributeInfo a : info.getAttributes())
            {
                if (a.getName().equals(attributeName))
                    return mbsc.getAttribute(metric, a.getName());
            }

            return null;
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
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
        populateExistingTable(cluster, from, to, coord, cl);
        for (int i = from; i < to; i++)
        {
            cluster.coordinator(coord).executeWithRetries("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (?, ?, ?)",
                                                          cl,
                                                          i, i, i);
        }
    }

    public static void populateExistingTable(ICluster cluster, int from, int to, int coord, ConsistencyLevel cl)
    {
        for (int i = from; i < to; i++)
        {
            cluster.coordinator(coord).executeWithRetries("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (?, ?, ?)",
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

    public static class BB {
        private static CountDownLatch latch = new CountDownLatch(1);
        private static final Object lock = new Object();
        private static boolean isLatchreleased = false;

        public static void install(ClassLoader classLoader, Integer num) {
            if (num != 3) {
                return;
            }
            new ByteBuddy().rebase(StorageService.class)
                           .method(named("markViewsAsBuilt"))
                           .intercept(MethodDelegation.to(BB.class))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
        }

        private static int invocations = 0;

        @SuppressWarnings("unused")
        public static void markViewsAsBuilt(@SuperCall Callable<Void> zuper)
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

        public static void installSetBootstrapStateWithDelay(ClassLoader classLoader, Integer num) {
            // Apply ByteBuddy modification for setBootstrapState only on node 3
            if (num == 3) {
                new ByteBuddy().rebase(SystemKeyspace.class)
                        .method(named("setBootstrapState"))
                        .intercept(MethodDelegation.to(BB.class))
                        .make()
                        .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        @SuppressWarnings("unused")
        public static void setBootStrapState(@SuperCall Callable<Void> zuper, @Argument(0) SystemKeyspace.BootstrapState state) {
            if (!(state == SystemKeyspace.BootstrapState.COMPLETED))
            {
                // Calling original method without latch for state other than COMPLETED
                try
                {
                    zuper.call();
                }
                catch (Exception e)
                {
                    throw new RuntimeException(e);
                }
                return;
            }
            new Thread(() -> {
                try {
                    synchronized (lock) {
                        latch.await(); // This will block until the latch is released
                    }
                    // Once the latch is released, call the original method
                    try {
                        zuper.call(); // Call the original method now
                        isLatchreleased = true;
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while waiting for latch to proceed", e);
                }
            }).start();

        }
    }
}