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

package org.apache.cassandra.distributed.test.guardrails;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.util.Auth;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.disk.usage.DiskUsageBroadcaster;
import org.apache.cassandra.service.disk.usage.DiskUsageMonitor;
import org.apache.cassandra.service.disk.usage.DiskUsageState;
import org.assertj.core.api.Assertions;

import static net.bytebuddy.matcher.ElementMatchers.named;

/**
 * Tests the guardrails for disk usage, {@link Guardrails#localDataDiskUsage} and {@link Guardrails#replicaDiskUsage}.
 */
public class GuardrailDiskUsageTest extends GuardrailTester
{
    private static final int NUM_ROWS = 100;

    private static final String WARN_MESSAGE = "Replica disk usage exceeds warning threshold";
    private static final String FAIL_MESSAGE = "Write request failed because disk usage exceeds failure threshold";

    private static Cluster cluster;
    private static com.datastax.driver.core.Cluster driverCluster;
    private static Session driverSession;

    @BeforeClass
    public static void setupCluster() throws IOException
    {
        // speed up the task that calculates and propagates the disk usage info
        CassandraRelevantProperties.DISK_USAGE_MONITOR_INTERVAL_MS.setInt(100);

        // build a 2-node cluster with RF=1
        cluster = init(Cluster.build(2)
                              .withInstanceInitializer(DiskStateInjection::install)
                              .withConfig(c -> c.with(Feature.GOSSIP, Feature.NATIVE_PROTOCOL)
                                                .set("data_disk_usage_percentage_warn_threshold", 98)
                                                .set("data_disk_usage_percentage_fail_threshold", 99)
                                                .set("authenticator", "PasswordAuthenticator"))
                              .start(), 1);

        Auth.waitForExistingRoles(cluster.get(1));

        // create a regular user, since the default superuser is excluded from guardrails
        com.datastax.driver.core.Cluster.Builder builder = com.datastax.driver.core.Cluster.builder().addContactPoint("127.0.0.1");
        try (com.datastax.driver.core.Cluster c = builder.withCredentials("cassandra", "cassandra").build();
             Session session = c.connect())
        {
            session.execute("CREATE USER test WITH PASSWORD 'test'");
        }

        // connect using that superuser, we use the driver to get access to the client warnings
        driverCluster = builder.withCredentials("test", "test").build();
        driverSession = driverCluster.connect();
    }

    @AfterClass
    public static void teardownCluster()
    {
        if (driverSession != null)
            driverSession.close();

        if (driverCluster != null)
            driverCluster.close();

        if (cluster != null)
            cluster.close();
    }

    @Override
    protected Cluster getCluster()
    {
        return cluster;
    }

    @Test
    public void testDiskUsage() throws Throwable
    {
        schemaChange("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        String insert = format("INSERT INTO %s(k, v) VALUES (?, 0)");

        // With both nodes in SPACIOUS state, we can write without warnings nor failures
        for (int i = 0; i < NUM_ROWS; i++)
        {
            ResultSet rs = driverSession.execute(insert, i);
            Assertions.assertThat(rs.getExecutionInfo().getWarnings()).isEmpty();
        }

        // If the disk usage information about one node becomes unavailable, we can still write without warnings
        DiskStateInjection.setState(getCluster(), 2, DiskUsageState.NOT_AVAILABLE);
        for (int i = 0; i < NUM_ROWS; i++)
        {
            ResultSet rs = driverSession.execute(insert, i);
            Assertions.assertThat(rs.getExecutionInfo().getWarnings()).isEmpty();
        }

        // If one node becomes STUFFED, the writes targeting that node will raise a warning, while the writes targetting
        // the node that remains SPACIOUS will keep succeeding without warnings
        DiskStateInjection.setState(getCluster(), 2, DiskUsageState.STUFFED);
        int numWarnings = 0;
        for (int i = 0; i < NUM_ROWS; i++)
        {
            ResultSet rs = driverSession.execute(insert, i);

            List<String> warnings = rs.getExecutionInfo().getWarnings();
            if (!warnings.isEmpty())
            {
                Assertions.assertThat(warnings).hasSize(1).anyMatch(s -> s.contains(WARN_MESSAGE));
                numWarnings++;
            }
        }
        Assertions.assertThat(numWarnings).isGreaterThan(0).isLessThan(NUM_ROWS);

        // If the STUFFED node becomes FULL, the writes targeting that node will fail, while the writes targeting
        // the node that remains SPACIOUS will keep succeeding without warnings
        DiskStateInjection.setState(getCluster(), 2, DiskUsageState.FULL);
        int numFailures = 0;
        for (int i = 0; i < NUM_ROWS; i++)
        {
            try
            {
                ResultSet rs = driverSession.execute(insert, i);
                Assertions.assertThat(rs.getExecutionInfo().getWarnings()).isEmpty();
            }
            catch (InvalidQueryException e)
            {
                Assertions.assertThat(e).hasMessageContaining(FAIL_MESSAGE);
                numFailures++;
            }
        }
        Assertions.assertThat(numFailures).isGreaterThan(0).isLessThan(NUM_ROWS);

        // If both nodes are FULL, all queries will fail
        DiskStateInjection.setState(getCluster(), 1, DiskUsageState.FULL);
        for (int i = 0; i < NUM_ROWS; i++)
        {
            try
            {
                driverSession.execute(insert, i);
                Assertions.fail("Should have failed");
            }
            catch (InvalidQueryException e)
            {
                numFailures++;
            }
        }

        // Finally, if both nodes go back to SPACIOUS, all queries will succeed again
        DiskStateInjection.setState(getCluster(), 1, DiskUsageState.SPACIOUS);
        DiskStateInjection.setState(getCluster(), 2, DiskUsageState.SPACIOUS);
        for (int i = 0; i < NUM_ROWS; i++)
        {
            ResultSet rs = driverSession.execute(insert, i);
            Assertions.assertThat(rs.getExecutionInfo().getWarnings()).isEmpty();
        }
    }

    /**
     * ByteBuddy rule to override the disk usage state of each node.
     */
    public static class DiskStateInjection
    {
        public static volatile DiskUsageState state = DiskUsageState.SPACIOUS;

        private static void install(ClassLoader cl, int node)
        {
            new ByteBuddy().rebase(DiskUsageMonitor.class)
                           .method(named("getState"))
                           .intercept(MethodDelegation.to(DiskStateInjection.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        public static void setState(Cluster cluster, int node, DiskUsageState state)
        {
            IInvokableInstance instance = cluster.get(node);
            instance.runOnInstance(() -> DiskStateInjection.state = state);

            // wait for disk usage state propagation, all nodes must see it
            InetAddressAndPort enpoint = InetAddressAndPort.getByAddress(instance.broadcastAddress());
            cluster.forEach(n -> n.runOnInstance(() -> Util.spinAssertEquals(state, () -> DiskUsageBroadcaster.instance.state(enpoint), 60)));
        }

        @SuppressWarnings("unused")
        public static DiskUsageState getState(long usagePercentage, @SuperCall Callable<DiskUsageState> zuper)
        {
            return state;
        }
    }
}
