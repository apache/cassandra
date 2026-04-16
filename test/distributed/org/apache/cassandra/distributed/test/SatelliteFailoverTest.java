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

import java.io.IOException;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.NodeToolResult;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Distributed tests for satellite failover: transfers the primary DC role
 * from dc1 to dc2 using nodetool satellite_admin and verifies the process
 * completes without errors and data remains accessible.
 *
 * Uses a shared 12-node cluster (dc1:3, dc2:3, sat1:3, sat2:3) to avoid
 * repeated cluster startup overhead.
 */
public class SatelliteFailoverTest extends TestBaseImpl
{
    private static Cluster SHARED_CLUSTER;

    private static final String SRS_OPTIONS =
        "'class': 'SatelliteReplicationStrategy', " +
        "'dc1': '3', " +
        "'dc1.satellite.sat1': '3/3', " +
        "'dc2': '3', " +
        "'dc2.satellite.sat2': '3/3'";

    @BeforeClass
    public static void setupClass() throws IOException
    {
        // 4 DCs: 2 full (dc1, dc2) + 2 satellite (sat1, sat2), 3 nodes each
        SHARED_CLUSTER = Cluster.build()
                                .withDC("dc1", 3)
                                .withDC("dc2", 3)
                                .withDC("sat1", 3)
                                .withDC("sat2", 3)
                                .withConfig(cfg -> cfg.with(Feature.NETWORK)
                                                      .with(Feature.GOSSIP)
                                                      .set("transient_replication_enabled", "true")
                                                      .set("paxos_variant", "v2"))
                                .start();
    }

    private void createKeyspaceAndTable(String keyspace, String primary)
    {
        SHARED_CLUSTER.schemaChange("CREATE KEYSPACE " + keyspace + " WITH replication = {" +
                                    SRS_OPTIONS + ", " +
                                    "'primary': '" + primary + "'" +
                                    "} AND replication_type = 'tracked'");

        SHARED_CLUSTER.schemaChange("CREATE TABLE " + keyspace + ".tbl (k int PRIMARY KEY, v text)");
    }

    private void alterPrimary(String keyspace, String newPrimary)
    {
        SHARED_CLUSTER.schemaChange("ALTER KEYSPACE " + keyspace + " WITH replication = {" +
                                    SRS_OPTIONS + ", " +
                                    "'primary': '" + newPrimary + "'" +
                                    "} AND replication_type = 'tracked'");
    }

    private void assertTransferComplete(String keyspace)
    {
        NodeToolResult status = SHARED_CLUSTER.get(1).nodetoolResult("satellite_admin", "status", keyspace);
        status.asserts().success();
        String out = status.getStdout();
        assertTrue("Transfer should be complete, got: " + out,
                   out.contains("Complete: true") || out.contains("No active satellite failover"));
    }

    /**
     * LWTs must work on an SRS keyspace with no failover in progress. Paxos consensus runs in the primary DC only,
     * so the commit must not be sent to satellite or secondary DC replicas — they reject paxos operations, and those
     * rejections get counted against the primary DC electorate. They receive the committed mutation as a plain
     * mutation alongside the paxos commit instead.
     */
    @Test
    public void testLwtWithoutFailover() throws Exception
    {
        String ks = "lwt_no_failover_test";
        String ksTbl = ks + ".tbl";

        createKeyspaceAndTable(ks, "dc1");

        Object[][] applied = SHARED_CLUSTER.coordinator(1).execute(
            "INSERT INTO " + ksTbl + " (k, v) VALUES (1, 'lwt') IF NOT EXISTS", ConsistencyLevel.QUORUM);
        assertTrue("LWT should have been applied", (boolean) applied[0][0]);

        // the same LWT must now be rejected, proving the first one committed
        Object[][] reapplied = SHARED_CLUSTER.coordinator(1).execute(
            "INSERT INTO " + ksTbl + " (k, v) VALUES (1, 'again') IF NOT EXISTS", ConsistencyLevel.QUORUM);
        assertFalse("LWT should not reapply over an existing row", (boolean) reapplied[0][0]);

        assertEquals(1, SHARED_CLUSTER.coordinator(1).execute("SELECT * FROM " + ksTbl, ConsistencyLevel.QUORUM).length);

        // and it must work when coordinated from outside the primary DC, which routes the commit through
        // PaxosCommit#forwardPaxos2Commit — that picks a coordinator from the same live set
        Object[][] fromDc2 = SHARED_CLUSTER.coordinator(4).execute(
            "INSERT INTO " + ksTbl + " (k, v) VALUES (2, 'lwt') IF NOT EXISTS", ConsistencyLevel.QUORUM);
        assertTrue("LWT coordinated outside the primary DC should have been applied", (boolean) fromDc2[0][0]);

        assertEquals(2, SHARED_CLUSTER.coordinator(1).execute("SELECT * FROM " + ksTbl, ConsistencyLevel.QUORUM).length);
    }

    @Test
    public void testForceTransferPrimary() throws Exception
    {
        String ks = "force_transfer_test";
        String ksTbl = ks + ".tbl";

        createKeyspaceAndTable(ks, "dc1");

        // Write data via dc1
        SHARED_CLUSTER.coordinator(1).execute("INSERT INTO " + ksTbl + " (k, v) VALUES (1, 'before')", ConsistencyLevel.QUORUM);
        SHARED_CLUSTER.coordinator(1).execute("INSERT INTO " + ksTbl + " (k, v) VALUES (2, 'before')", ConsistencyLevel.QUORUM);

        // Verify readable
        Object[][] rows = SHARED_CLUSTER.coordinator(1).execute("SELECT * FROM " + ksTbl, ConsistencyLevel.QUORUM);
        assertEquals(2, rows.length);

        // Transfer primary to dc2
        alterPrimary(ks, "dc2");

        // Check status shows active transfer
        NodeToolResult statusBefore = SHARED_CLUSTER.get(1).nodetoolResult("satellite_admin", "status", ks);
        statusBefore.asserts().success();
        assertTrue("Should show active transfer from dc1", statusBefore.getStdout().contains("From DC: dc1"));

        // Force advance (skip gates)
        SHARED_CLUSTER.get(1).nodetoolResult("satellite_admin", "advance", ks, "--force").asserts().success();

        // Verify complete
        assertTransferComplete(ks);

        // Read from dc2 coordinator (node 4 = first dc2 node)
        Object[][] dc2Rows = SHARED_CLUSTER.coordinator(4).execute("SELECT * FROM " + ksTbl, ConsistencyLevel.QUORUM);
        assertEquals(2, dc2Rows.length);
    }

    @Test
    public void testTransferWithGates() throws Exception
    {
        String ks = "gated_transfer_test";
        String ksTbl = ks + ".tbl";

        createKeyspaceAndTable(ks, "dc1");

        // Write data via dc1
        SHARED_CLUSTER.coordinator(1).execute("INSERT INTO " + ksTbl + " (k, v) VALUES (1, 'regular')", ConsistencyLevel.QUORUM);
        SHARED_CLUSTER.coordinator(1).execute("INSERT INTO " + ksTbl + " (k, v) VALUES (2, 'regular')", ConsistencyLevel.QUORUM);
        SHARED_CLUSTER.coordinator(1).execute("INSERT INTO " + ksTbl + " (k, v) VALUES (3, 'regular')", ConsistencyLevel.QUORUM);

        // Verify all 3 rows present
        Object[][] rows = SHARED_CLUSTER.coordinator(1).execute("SELECT * FROM " + ksTbl, ConsistencyLevel.QUORUM);
        assertEquals(3, rows.length);

        // === Transfer primary to dc2 — enters TRANSITION_ACK ===
        alterPrimary(ks, "dc2");

        // Status should show all ranges in TRANSITION_ACK
        NodeToolResult ackStatus = SHARED_CLUSTER.get(1).nodetoolResult("satellite_admin", "status", ks);
        ackStatus.asserts().success();
        String ackOut = ackStatus.getStdout();
        assertTrue("Should show active transfer from dc1, got: " + ackOut, ackOut.contains("From DC: dc1"));
        assertTrue("Should have TRANSITION_ACK ranges, got: " + ackOut, !ackOut.contains("TRANSITION_ACK ranges (0)"));
        assertTrue("Should have no TRANSITION ranges yet, got: " + ackOut, ackOut.contains("TRANSITION ranges (0)"));

        // LWT should be rejected during TRANSITION_ACK (paxos blocked on old primary)
        try
        {
            SHARED_CLUSTER.coordinator(1).execute(
                "INSERT INTO " + ksTbl + " (k, v) VALUES (10, 'should_fail') IF NOT EXISTS", ConsistencyLevel.QUORUM);
            fail("LWT should be rejected during TRANSITION_ACK");
        }
        catch (Exception e)
        {
            // Expected — paxos is blocked during TRANSITION_ACK
        }

        // Regular reads should still work during TRANSITION_ACK
        Object[][] readsDuringAck = SHARED_CLUSTER.coordinator(1).execute("SELECT * FROM " + ksTbl, ConsistencyLevel.QUORUM);
        assertEquals("Regular reads should work during TRANSITION_ACK", 3, readsDuringAck.length);

        // === Advance with --ack (epoch ack + paxos repair → TRANSITION) ===
        SHARED_CLUSTER.get(1).nodetoolResult("satellite_admin", "advance", ks, "--ack").asserts().success();

        // Status should show ranges in TRANSITION (no longer TRANSITION_ACK)
        NodeToolResult midStatus = SHARED_CLUSTER.get(1).nodetoolResult("satellite_admin", "status", ks);
        midStatus.asserts().success();
        String midOut = midStatus.getStdout();
        assertTrue("TRANSITION_ACK ranges should be 0, got: " + midOut, midOut.contains("TRANSITION_ACK ranges (0)"));
        assertTrue("TRANSITION ranges should be non-zero, got: " + midOut, !midOut.contains("TRANSITION ranges (0)"));

        // Regular reads/writes should work during TRANSITION
        SHARED_CLUSTER.coordinator(4).execute("INSERT INTO " + ksTbl + " (k, v) VALUES (4, 'during_transition')", ConsistencyLevel.QUORUM);
        Object[][] readsDuringTransition = SHARED_CLUSTER.coordinator(4).execute("SELECT * FROM " + ksTbl, ConsistencyLevel.QUORUM);
        assertEquals("Reads/writes should work during TRANSITION", 4, readsDuringTransition.length);

        // Running --ack again should be idempotent (no TRANSITION_ACK ranges left to process)
        SHARED_CLUSTER.get(1).nodetoolResult("satellite_admin", "advance", ks, "--ack").asserts().success();

        // === Advance with --force to complete (MT barrier hangs in dtest env — needs investigation) ===
        SHARED_CLUSTER.get(1).nodetoolResult("satellite_admin", "advance", ks, "--force").asserts().success();

        // Verify complete
        assertTransferComplete(ks);

        // Verify all data accessible from dc2 (including row written during TRANSITION)
        Object[][] dc2Rows = SHARED_CLUSTER.coordinator(4).execute("SELECT * FROM " + ksTbl, ConsistencyLevel.QUORUM);
        assertEquals(4, dc2Rows.length);

        // LWT should work on new primary (dc2) after transfer completes
        Object[][] lwtResult = SHARED_CLUSTER.coordinator(4).execute(
            "INSERT INTO " + ksTbl + " (k, v) VALUES (5, 'after_transfer') IF NOT EXISTS", ConsistencyLevel.QUORUM);
        assertNotNull(lwtResult);
        assertTrue("LWT should succeed on new primary", (boolean) lwtResult[0][0]);

        // Verify final state
        Object[][] finalRows = SHARED_CLUSTER.coordinator(4).execute("SELECT * FROM " + ksTbl, ConsistencyLevel.QUORUM);
        assertEquals(5, finalRows.length);
    }
}
