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

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;

import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.distributed.util.Auth;
import org.apache.cassandra.service.disk.usage.DiskUsageBroadcaster;
import org.apache.cassandra.service.disk.usage.DiskUsageState;

import static org.apache.cassandra.config.CassandraRelevantProperties.BOOTSTRAP_SKIP_SCHEMA_CHECK;
import static org.apache.cassandra.distributed.shared.ClusterUtils.awaitRingJoin;
import static org.apache.cassandra.distributed.shared.ClusterUtils.replaceHostAndStart;

public class GuardrailDataDiskUsageKeyspaceWideProtectionTest extends TestBaseImpl
{
    public static final int NODE_TO_MARK_AS_FULL = 2;
    private static final int NUM_ROWS = 100;
    private static final String NTS_KEYSPACE_NAME = "nts_keyspace1";
    private static Cluster cluster;
    private static com.datastax.driver.core.Cluster driverCluster;
    private static Session driverSession;

    @Before
    public void setupCluster() throws IOException
    {
        // speed up the task that calculates and propagates the disk usage info
        CassandraRelevantProperties.DISK_USAGE_MONITOR_INTERVAL_MS.setInt(100);
        TokenSupplier even = TokenSupplier.evenlyDistributedTokens(2);
        // build a 2-node cluster with RF=1
        cluster = init(Cluster.build(2)
                              .withInstanceInitializer(GuardrailDiskUsageTest.DiskStateInjection::install)
                              .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK, Feature.NATIVE_PROTOCOL)
                                                .set("data_disk_usage_max_disk_size", "10GiB")
                                                .set("data_disk_usage_percentage_warn_threshold", 98)
                                                .set("data_disk_usage_percentage_fail_threshold", 99)
                                                .set("data_disk_usage_keyspace_wide_protection_enabled", true)
                                                .set("authenticator", "PasswordAuthenticator")
                                                .set("initial_location_provider", "SimpleLocationProvider"))
                              .withTokenSupplier(node -> even.token(node == 3 ? 2 : node))
                              .start(), 1);

        Auth.waitForExistingRoles(cluster.get(1));

        // create a regular user, since the default superuser is excluded from guardrails
        com.datastax.driver.core.Cluster.Builder builder = com.datastax.driver.core.Cluster.builder().addContactPoint("127.0.0.1");
        try (com.datastax.driver.core.Cluster c = builder.withCredentials("cassandra", "cassandra").build();
             Session session = c.connect())
        {
            session.execute("CREATE USER test WITH PASSWORD 'test'");
            session.execute("CREATE KEYSPACE " + NTS_KEYSPACE_NAME + " WITH REPLICATION={'class': 'NetworkTopologyStrategy', 'datacenter1': 1}");
        }

        // connect using that superuser, we use the driver to get access to the client warnings
        driverCluster = builder.withCredentials("test", "test").build();
        driverSession = driverCluster.connect();
    }

    @After
    public void cleanup() throws IOException
    {
        if (driverSession != null)
            driverSession.close();

        if (driverCluster != null)
            driverCluster.close();

        if (cluster != null)
            cluster.close();
    }

    @Test
    public void testDiskUsageWithStopWritesForKeyspaceOnFail() throws Throwable
    {
        String tableName = KEYSPACE + ".guardrail_disk_usage_tbl";
        testDataDiskUsageKeyspaceWideProtectionGuardrailCommon(tableName);
    }

    @Test
    public void testDataDiskUsageKeyspaceWideProtectionGuardrailDatacenterFullAndNetworkTopologyStrategyUsedShouldBlockWrites()
    {
        String tableName = NTS_KEYSPACE_NAME + ".guardrail_disk_usage_tbl";
        testDataDiskUsageKeyspaceWideProtectionGuardrailCommon(tableName);
    }

    private static void testDataDiskUsageKeyspaceWideProtectionGuardrailCommon(String tableName)
    {
        cluster.schemaChange(String.format("CREATE TABLE %s (k int PRIMARY KEY, v int)", tableName));
        String insert = String.format("INSERT INTO %s(k, v) VALUES (?, 0)", tableName);
        ensureGuardrailCommon(insert, NODE_TO_MARK_AS_FULL);

        // Finally, if both nodes go back to SPACIOUS, all queries will succeed again
        GuardrailDiskUsageTest.DiskStateInjection.setState(cluster, NODE_TO_MARK_AS_FULL, DiskUsageState.SPACIOUS);
        for (int i = 0; i < NUM_ROWS; i++)
        {
            ResultSet rs = driverSession.execute(insert, i);
            Assertions.assertThat(rs.getExecutionInfo().getWarnings()).isEmpty();
        }
    }



    @Test
    public void testDiskUsageWithStopWriteForKeyspaceWhenFullNodeReplaceWithSpaciousShouldNotBlock() throws Throwable
    {
        String tableName = KEYSPACE + ".guardrail_disk_usage_tbl";
        cluster.schemaChange(String.format("CREATE TABLE %s (k int PRIMARY KEY, v int)", tableName));
        String insert = String.format("INSERT INTO %s(k, v) VALUES (?, 0)", tableName);

        ensureGuardrailCommon(insert, NODE_TO_MARK_AS_FULL);
        // When we replace the node with a SPACIOUS node, then we should succeed again.
        IInvokableInstance nodeToRemove = cluster.get(NODE_TO_MARK_AS_FULL);
        ClusterUtils.stopUnchecked(nodeToRemove);
        IInvokableInstance replacingNode = replaceHostAndStart(cluster, nodeToRemove, props -> {
            // since we have a downed host there might be a schema version which is old show up but
            // can't be fetched since the host is down...
            props.set(BOOTSTRAP_SKIP_SCHEMA_CHECK, true);
        });
        awaitRingJoin(cluster.get(1), replacingNode);
        awaitRingJoin(replacingNode, cluster.get(1));
        for (int i = 0; i < NUM_ROWS; i++)
        {
            ResultSet rs = driverSession.execute(insert, i);
            Assertions.assertThat(rs.getExecutionInfo().getWarnings()).isEmpty();
        }

    }

    @Test
    public void testDiskUsageWithStopWriteForKeyspaceWhenFullNodeIpChangeAndBecomesSpaciousShouldNotBlock() throws Throwable
    {
        String tableName = KEYSPACE + ".guardrail_disk_usage_tbl";
        cluster.schemaChange(String.format("CREATE TABLE %s (k int PRIMARY KEY, v int)", tableName));
        String insert = String.format("INSERT INTO %s(k, v) VALUES (?, 0)", tableName);
        ensureGuardrailCommon(insert, NODE_TO_MARK_AS_FULL);

        // If the node goes offline then comes online with a different IP, then we should still fail.
        IInvokableInstance nodeToChangeIp = cluster.get(NODE_TO_MARK_AS_FULL);
        ClusterUtils.stopUnchecked(nodeToChangeIp);
        ClusterUtils.updateAddress(nodeToChangeIp, "127.0.0.4");
        nodeToChangeIp.startup();
        GuardrailDiskUsageTest.DiskStateInjection.setState(cluster, NODE_TO_MARK_AS_FULL, DiskUsageState.FULL);
        int numFailures = 0;
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
        Assertions.assertThat(numFailures).isEqualTo(NUM_ROWS);

        // If the node then becomes SPACIOUS then we should succeed again.
        GuardrailDiskUsageTest.DiskStateInjection.setState(cluster, NODE_TO_MARK_AS_FULL, DiskUsageState.SPACIOUS);
        for (int i = 0; i < NUM_ROWS; i++)
        {
            ResultSet rs = driverSession.execute(insert, i);
            Assertions.assertThat(rs.getExecutionInfo().getWarnings()).isEmpty();
        }
    }


    @Test
    public void testDiskUsageWithStopWritesForKeyspaceOnFailWhenFullNodeLeavesShouldStopBlocking()
    {
        String tableName = KEYSPACE + ".guardrail_disk_usage_tbl";
        cluster.schemaChange(String.format("CREATE TABLE %s (k int PRIMARY KEY, v int)", tableName));
        String insert = String.format("INSERT INTO %s(k, v) VALUES (?, 0)", tableName);
        ensureGuardrailCommon(insert, NODE_TO_MARK_AS_FULL);

        // If the FULL node leaves the cluster, then writes should succeed again.
        IInvokableInstance nodeToRemove = cluster.get(NODE_TO_MARK_AS_FULL);
        ClusterUtils.decommission(nodeToRemove);
        for (int i = 0; i < NUM_ROWS; i++)
        {
            ResultSet rs = driverSession.execute(insert, i);
            Assertions.assertThat(rs.getExecutionInfo().getWarnings()).isEmpty();
        }
    }

    @Test
    public void testDiskUsageWithStopWritesForKeyspaceWhenFlagDisabledShouldNotBlockEntireKeyspace()
    {
        String tableName = KEYSPACE + ".guardrail_disk_usage_tbl";
        cluster.schemaChange(String.format("CREATE TABLE %s (k int PRIMARY KEY, v int)", tableName));
        String insert = String.format("INSERT INTO %s(k, v) VALUES (?, 0)", tableName);
        ensureGuardrailCommon(insert, NODE_TO_MARK_AS_FULL);

        // Disable the stopWritesForKeyspacesOnFail flag.
        // Writes which were destined for the FULL node should fail, but others should succeed.
        cluster.get(1).runOnInstance(() -> Guardrails.instance.setDataDiskUsageKeyspaceWideProtectionEnabled(false));
        int numFailures = 0;
        for (int i = 0; i < NUM_ROWS; i++)
        {
            try
            {
                driverSession.execute(insert, i);
            }
            catch (InvalidQueryException e)
            {
                numFailures++;
            }
        }
        Assertions.assertThat(numFailures).isBetween(1, NUM_ROWS - 1);
    }

    @Test
    public void testDiskUsageWithStopWritesForKeyspaceWhenFlagToggledAndNodeLeavesShouldReEnableWrites()
    {
        String tableName = KEYSPACE + ".guardrail_disk_usage_tbl";
        cluster.schemaChange(String.format("CREATE TABLE %s (k int PRIMARY KEY, v int)", tableName));
        String insert = String.format("INSERT INTO %s(k, v) VALUES (?, 0)", tableName);
        ensureGuardrailCommon(insert, NODE_TO_MARK_AS_FULL);
        cluster.get(1).runOnInstance(() -> Guardrails.instance.setDataDiskUsageKeyspaceWideProtectionEnabled(false));
        // If the FULL node leaves the cluster, then writes should succeed again.
        IInvokableInstance nodeToRemove = cluster.get(NODE_TO_MARK_AS_FULL);
        ClusterUtils.decommission(nodeToRemove);
        for (int i = 0; i < NUM_ROWS; i++)
        {
            ResultSet rs = driverSession.execute(insert, i);
            Assertions.assertThat(rs.getExecutionInfo().getWarnings()).isEmpty();
        }
        cluster.get(1).runOnInstance(() -> Guardrails.instance.setDataDiskUsageKeyspaceWideProtectionEnabled(true));
        for (int i = 0; i < NUM_ROWS; i++)
        {
            ResultSet rs = driverSession.execute(insert, i);
            Assertions.assertThat(rs.getExecutionInfo().getWarnings()).isEmpty();
        }
        cluster.get(1).runOnInstance(() -> Assertions.assertThat(DiskUsageBroadcaster.instance.isDatacenterFull("datacenter1")).isFalse());
        cluster.get(1).runOnInstance(() -> Assertions.assertThat(DiskUsageBroadcaster.instance.isDatacenterStuffed("datacenter1")).isFalse());
    }

    @Test
    public void testDiskUsageWithStopWritesForKeyspaceWhenFlagToggledAndNodeBecomesSpaciousShouldReEnableWrites()
    {
        String tableName = KEYSPACE + ".guardrail_disk_usage_tbl";
        cluster.schemaChange(String.format("CREATE TABLE %s (k int PRIMARY KEY, v int)", tableName));
        String insert = String.format("INSERT INTO %s(k, v) VALUES (?, 0)", tableName);
        ensureGuardrailCommon(insert, NODE_TO_MARK_AS_FULL);
        cluster.get(1).runOnInstance(() -> Guardrails.instance.setDataDiskUsageKeyspaceWideProtectionEnabled(false));
        GuardrailDiskUsageTest.DiskStateInjection.setState(cluster, NODE_TO_MARK_AS_FULL, DiskUsageState.SPACIOUS);
        cluster.get(1).runOnInstance(() -> Guardrails.instance.setDataDiskUsageKeyspaceWideProtectionEnabled(true));
        for (int i = 0; i < NUM_ROWS; i++)
        {
            ResultSet rs = driverSession.execute(insert, i);
            Assertions.assertThat(rs.getExecutionInfo().getWarnings()).isEmpty();
        }
        cluster.get(1).runOnInstance(() -> Assertions.assertThat(DiskUsageBroadcaster.instance.isDatacenterFull("datacenter1")).isFalse());
        cluster.get(1).runOnInstance(() -> Assertions.assertThat(DiskUsageBroadcaster.instance.isDatacenterStuffed("datacenter1")).isFalse());
    }

    /**
     * Ensures that the guardrail works in the common scenario across all tests (i.e., with both nodes SPACIOUS we
     * succeed and with one FULL we fail).
     *
     * @param insert The insert statement which we will use to test the guardrail.
     * @param node The node which we will mark as full.
     */
    private static void ensureGuardrailCommon(String insert, int node)
    {
        // With both nodes in SPACIOUS state, we can write without warnings nor failures
        for (int i = 0; i < NUM_ROWS; i++)
        {
            ResultSet rs = driverSession.execute(insert, i);
            Assertions.assertThat(rs.getExecutionInfo().getWarnings()).isEmpty();
        }

        // If the STUFFED node becomes full, but the data_disk_usage_keyspace_wide_protection_enabled is set,
        // then all writes will fail regardless of node.
        GuardrailDiskUsageTest.DiskStateInjection.setState(cluster, node, DiskUsageState.FULL);
        int numFailures = 0;
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
        Assertions.assertThat(numFailures).isEqualTo(NUM_ROWS);
    }
}
