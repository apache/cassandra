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

import java.util.Collections;
import java.util.Arrays;
import java.util.Comparator;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.exceptions.UnavailableException;

import static org.apache.cassandra.distributed.shared.ClusterUtils.stopUnchecked;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

public class RemoteQuorumReadWriteTest extends TestBaseImpl
{
    /**
     * Minimal 2-DC validation for REMOTE_QUORUM writes.
     * Verifies success at 3/3 and 2/3 remote replicas, and Unavailable at 1/3.
     */
    @Test
    public void testRemoteQuorumWritesTwoDatacenters() throws Exception
    {
        try (Cluster cluster = Cluster.build()
                                      .withRacks(2, 3, 1) // nodes 1-3 in datacenter1, 4-6 in datacenter2
                                      .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK))
                                      .start())
        {
            // Create NTS keyspace replicated to both DCs
            cluster.schemaChange("CREATE KEYSPACE ks_remote_quorum WITH replication = "
                                 + "{'class':'NetworkTopologyStrategy','datacenter1':3,'datacenter2':3}");
            cluster.schemaChange("CREATE TABLE ks_remote_quorum.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

            IInvokableInstance dc1Node = cluster.get(1); // coordinator in datacenter1

            // Configure REMOTE_QUORUM mapping on the coordinator: local DC -> datacenter2
            dc1Node.runOnInstance(() -> {
                String local = DatabaseDescriptor.getLocalDataCenter(); // "datacenter1"
                DatabaseDescriptor.setRemoteQuorumTargetDcs(Collections.singletonMap(local, "datacenter2"));
            });

            // All 3 replicas up in remote DC => success
            dc1Node.coordinator().execute(
                "INSERT INTO ks_remote_quorum.tbl (pk, ck, v) VALUES (1, 1, 1)",
                ConsistencyLevel.REMOTE_QUORUM);

            // Kill one remote node (DC2) => still success (2/3 quorum)
            stopUnchecked(cluster.get(6));
            dc1Node.coordinator().execute(
                "INSERT INTO ks_remote_quorum.tbl (pk, ck, v) VALUES (2, 2, 2)",
                ConsistencyLevel.REMOTE_QUORUM);

            // Kill a second remote node (DC2) => now only 1/3 => expect Unavailable
            stopUnchecked(cluster.get(5));
            try
            {
                dc1Node.coordinator().execute(
                    "INSERT INTO ks_remote_quorum.tbl (pk, ck, v) VALUES (3, 3, 3)",
                    ConsistencyLevel.REMOTE_QUORUM);
                fail("Expected UnavailableException at REMOTE_QUORUM with only 1/3 remote replicas");
            }
            catch (Exception e)
            {
                Assert.assertEquals(UnavailableException.class.getName(), e.getClass().getName());
                Assert.assertEquals("Cannot achieve consistency level REMOTE_QUORUM", e.getMessage());
            }
        }
    }

    /**
     * Read (single-partition) at REMOTE_QUORUM.
     */
    @Test
    public void testRemoteQuorumReadsTwoDatacenters() throws Exception
    {
        try (Cluster cluster = Cluster.build()
                                      .withRacks(2, 3, 1)
                                      .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK))
                                      .start())
        {
            cluster.schemaChange("CREATE KEYSPACE ks_remote_quorum WITH replication = "
                                 + "{'class':'NetworkTopologyStrategy','datacenter1':3,'datacenter2':3}");
            cluster.schemaChange("CREATE TABLE ks_remote_quorum.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

            IInvokableInstance dc1Node = cluster.get(1);
            dc1Node.runOnInstance(() -> {
                String local = DatabaseDescriptor.getLocalDataCenter();
                DatabaseDescriptor.setRemoteQuorumTargetDcs(Collections.singletonMap(local, "datacenter2"));
            });

            // Write the row at ALL so both DCs have it
            dc1Node.coordinator().execute(
                "INSERT INTO ks_remote_quorum.tbl (pk, ck, v) VALUES (100, 1, 999)",
                ConsistencyLevel.ALL);

            Object[][] rows = dc1Node.coordinator().execute(
                "SELECT pk, ck, v FROM ks_remote_quorum.tbl WHERE pk = 100",
                ConsistencyLevel.REMOTE_QUORUM);
            assertRows(rows, row(100, 1, 999));
        }
    }

    /**
     * Partition range read at REMOTE_QUORUM.
     */
    @Test
    public void testRemoteQuorumRangeReadTwoDatacenters() throws Exception
    {
        try (Cluster cluster = Cluster.build()
                .withRacks(2, 3, 1)
                .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK))
                .start())
        {
            cluster.schemaChange("CREATE KEYSPACE ks_remote_quorum WITH replication = "
                    + "{'class':'NetworkTopologyStrategy','datacenter1':3,'datacenter2':3}");
            cluster.schemaChange("CREATE TABLE ks_remote_quorum.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

            IInvokableInstance dc1Node = cluster.get(1);
            dc1Node.runOnInstance(() -> {
                String local = DatabaseDescriptor.getLocalDataCenter();
                DatabaseDescriptor.setRemoteQuorumTargetDcs(Collections.singletonMap(local, "datacenter2"));
            });

            // Write multiple partitions at ALL so both DCs have them
            for (int pk = 1; pk <= 5; pk++)
            {
                dc1Node.coordinator().execute(
                        "INSERT INTO ks_remote_quorum.tbl (pk, ck, v) VALUES (?, 0, ?)",
                        ConsistencyLevel.ALL, pk, pk * 10);
            }

            // Token-range across pk 1..5
            Object[][] rows = dc1Node.coordinator().execute(
                    "SELECT pk, ck, v FROM ks_remote_quorum.tbl WHERE pk >= 1 and pk <= 5 ALLOW FILTERING",
                    ConsistencyLevel.REMOTE_QUORUM);
            // Expect five rows
            assertEquals(5, rows.length);
            // Order-agnostic values validation, sort by pk to make order deterministic
            Arrays.sort(rows, Comparator.comparingInt(r -> (Integer) r[0]));
            assertRows(rows,
                    row(1, 0, 10),
                    row(2, 0, 20),
                    row(3, 0, 30),
                    row(4, 0, 40),
                    row(5, 0, 50));
        }
    }

    /**
     * Write at REMOTE_QUORUM when local replicas are unavailable.
     */
    @Test
    public void testRemoteQuorumOverwriteLocalQuorumWritesTwoDatacenters() throws Exception
    {
        try (Cluster cluster = Cluster.build()
                .withRacks(2, 3, 1)
                .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK))
                .start())
        {
            cluster.schemaChange("CREATE KEYSPACE ks_remote_quorum WITH replication = "
                    + "{'class':'NetworkTopologyStrategy','datacenter1':3,'datacenter2':3}");
            cluster.schemaChange("CREATE TABLE ks_remote_quorum.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

            IInvokableInstance dc1Node = cluster.get(1);
            dc1Node.runOnInstance(() -> DatabaseDescriptor.setRemoteQuorumWriteOverrideEnabled(false));

            // Kill two local nodes.
            stopUnchecked(cluster.get(2));
            stopUnchecked(cluster.get(3));

            // With override disabled, LOCAL_QUORUM write should fail due to insufficient local replicas
            try
            {
                dc1Node.coordinator().execute(
                        "INSERT INTO ks_remote_quorum.tbl (pk, ck, v) VALUES (100, 1, 999)",
                        ConsistencyLevel.LOCAL_QUORUM);
                fail("Expected UnavailableException at LOCAL_QUORUM with two local replicas down");
            }
            catch (Exception e)
            {
                Assert.assertEquals(UnavailableException.class.getName(), e.getClass().getName());
                Assert.assertEquals("Cannot achieve consistency level LOCAL_QUORUM", e.getMessage());
            }

            // Enable override and retry; now it should succeed by falling back to REMOTE_QUORUM
            dc1Node.runOnInstance(() -> {
                String local = DatabaseDescriptor.getLocalDataCenter();
                DatabaseDescriptor.setRemoteQuorumTargetDcs(Collections.singletonMap(local, "datacenter2"));
                DatabaseDescriptor.setRemoteQuorumWriteOverrideEnabled(true);
            });
            dc1Node.coordinator().execute(
                    "INSERT INTO ks_remote_quorum.tbl (pk, ck, v) VALUES (100, 1, 999)",
                    ConsistencyLevel.LOCAL_QUORUM);

            // Validating Write succeed in remote DC, Read with Remote quorum should return records.
            Object[][] rows = dc1Node.coordinator().execute(
                    "SELECT pk, ck, v FROM ks_remote_quorum.tbl WHERE pk = 100",
                    ConsistencyLevel.REMOTE_QUORUM);
            assertRows(rows, row(100, 1, 999));
        }
    }

    /**
     * Read (single-partition) at REMOTE_QUORUM when local replicas are unavailable.
     */
    @Test
    public void testRemoteQuorumOverwriteLocalQuorumReadsTwoDatacenters() throws Exception
    {
        try (Cluster cluster = Cluster.build()
                .withRacks(2, 3, 1)
                .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK))
                .start())
        {
            cluster.schemaChange("CREATE KEYSPACE ks_remote_quorum WITH replication = "
                    + "{'class':'NetworkTopologyStrategy','datacenter1':3,'datacenter2':3}");
            cluster.schemaChange("CREATE TABLE ks_remote_quorum.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

            IInvokableInstance dc1Node = cluster.get(1);
            dc1Node.runOnInstance(() -> DatabaseDescriptor.setRemoteQuorumReadOverrideEnabled(false));

            // Write the row at ConsistencyLevel.ALL so both DCs have them
            dc1Node.coordinator().execute(
                    "INSERT INTO ks_remote_quorum.tbl (pk, ck, v) VALUES (100, 1, 999)",
                    ConsistencyLevel.REMOTE_QUORUM);

            // Kill two local nodes.
            stopUnchecked(cluster.get(2));
            stopUnchecked(cluster.get(3));

            // With override disabled, LOCAL_QUORUM read should fail due to insufficient local replicas
            try
            {
                dc1Node.coordinator().execute(
                        "SELECT pk, ck, v FROM ks_remote_quorum.tbl WHERE pk = 100",
                        ConsistencyLevel.LOCAL_QUORUM);
                fail("Expected UnavailableException at LOCAL_QUORUM with two local replicas down");
            }
            catch (Exception e)
            {
                Assert.assertEquals(UnavailableException.class.getName(), e.getClass().getName());
                Assert.assertEquals("Cannot achieve consistency level LOCAL_QUORUM", e.getMessage());
            }

            // Enable override and provide mapping; now LOCAL_QUORUM should be overwritten to REMOTE_QUORUM
            dc1Node.runOnInstance(() -> {
                String local = DatabaseDescriptor.getLocalDataCenter();
                DatabaseDescriptor.setRemoteQuorumTargetDcs(Collections.singletonMap(local, "datacenter2"));
                DatabaseDescriptor.setRemoteQuorumReadOverrideEnabled(true);
            });

            Object[][] rows = dc1Node.coordinator().execute(
                    "SELECT pk, ck, v FROM ks_remote_quorum.tbl WHERE pk = 100",
                    ConsistencyLevel.LOCAL_QUORUM);
            assertRows(rows, row(100, 1, 999));
        }
    }

    /**
     * Partition range read at REMOTE_QUORUM.
     */
    @Test
    public void testRemoteQuorumOverwriteLocalQuorumRangeReadTwoDatacenters() throws Exception
    {
        try (Cluster cluster = Cluster.build()
                .withRacks(2, 3, 1)
                .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK))
                .start())
        {
            cluster.schemaChange("CREATE KEYSPACE ks_remote_quorum WITH replication = "
                    + "{'class':'NetworkTopologyStrategy','datacenter1':3,'datacenter2':3}");
            cluster.schemaChange("CREATE TABLE ks_remote_quorum.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

            IInvokableInstance dc1Node = cluster.get(1);
            dc1Node.runOnInstance(() -> DatabaseDescriptor.setRemoteQuorumReadOverrideEnabled(false));

            // Write multiple partitions at ALL so both DCs have them
            for (int pk = 1; pk <= 5; pk++)
            {
                dc1Node.coordinator().execute(
                        "INSERT INTO ks_remote_quorum.tbl (pk, ck, v) VALUES (?, 0, ?)",
                        ConsistencyLevel.ALL, pk, pk * 10);
            }

            // Token-range across pk 1..5
            Object[][] rows = dc1Node.coordinator().execute(
                    "SELECT pk, ck, v FROM ks_remote_quorum.tbl WHERE pk >= 1 and pk <= 5 ALLOW FILTERING",
                    ConsistencyLevel.LOCAL_QUORUM);
            // Expect 5 rows
            assertEquals(5, rows.length);

            // Kill two local nodes.
            stopUnchecked(cluster.get(2));
            stopUnchecked(cluster.get(3));

            // With override disabled, LOCAL_QUORUM range read should fail due to insufficient local replicas
            try
            {
                dc1Node.coordinator().execute(
                        "SELECT pk, ck, v FROM ks_remote_quorum.tbl WHERE pk >= 1 and pk <= 5 ALLOW FILTERING",
                        ConsistencyLevel.LOCAL_QUORUM);
                fail("Expected UnavailableException at LOCAL_QUORUM with two local replicas down");
            }
            catch (Exception e)
            {
                Assert.assertEquals(UnavailableException.class.getName(), e.getClass().getName());
                Assert.assertEquals("Cannot achieve consistency level LOCAL_QUORUM", e.getMessage());
            }

            // Enable override and provide mapping; now LOCAL_QUORUM should be overwritten to REMOTE_QUORUM
            dc1Node.runOnInstance(() -> {
                String local = DatabaseDescriptor.getLocalDataCenter();
                DatabaseDescriptor.setRemoteQuorumTargetDcs(Collections.singletonMap(local, "datacenter2"));
                DatabaseDescriptor.setRemoteQuorumReadOverrideEnabled(true);
            });
            rows = dc1Node.coordinator().execute(
                    "SELECT pk, ck, v FROM ks_remote_quorum.tbl WHERE pk >= 1 and pk <= 5 ALLOW FILTERING",
                    ConsistencyLevel.LOCAL_QUORUM);

            // Expect five rows
            assertEquals(5, rows.length);
            // Order-agnostic values validation, sort by pk to make order deterministic
            Arrays.sort(rows, Comparator.comparingInt(r -> (Integer) r[0]));
            assertRows(rows,
                    row(1, 0, 10),
                    row(2, 0, 20),
                    row(3, 0, 30),
                    row(4, 0, 40),
                    row(5, 0, 50));
        }
    }

    /**
     * LOCAL_QUORUM should not fall back to REMOTE_QUORUM when the target remote DC has RF=0.
     * Example: phx:3, dca:0 with targetDc mapping local->dca.
     */
    @Test
    public void testRemoteQuorumFallsBackWhenRemoteRFZero() throws Exception
    {
        try (Cluster cluster = Cluster.build()
                .withRacks(2, 3, 1)
                .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK))
                .start())
        {
            cluster.schemaChange("CREATE KEYSPACE ks_remote_quorum WITH replication = "
                    + "{'class':'NetworkTopologyStrategy','datacenter1':3,'datacenter2':0}");
            cluster.schemaChange("CREATE TABLE ks_remote_quorum.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

            IInvokableInstance dc1Node = cluster.get(1);
            dc1Node.runOnInstance(() -> {
                String local = DatabaseDescriptor.getLocalDataCenter();
                DatabaseDescriptor.setRemoteQuorumTargetDcs(Collections.singletonMap(local, "datacenter2"));
                DatabaseDescriptor.setRemoteQuorumWriteOverrideEnabled(true);
                DatabaseDescriptor.setRemoteQuorumReadOverrideEnabled(true);
            });

            // Kill two local nodes.
            stopUnchecked(cluster.get(2));
            stopUnchecked(cluster.get(3));

            try
            {
                // Write the row at Local Quorum should NOT be overwrited to Remote quorum when remote RF=0.
                dc1Node.coordinator().execute(
                        "INSERT INTO ks_remote_quorum.tbl (pk, ck, v) VALUES (100, 1, 999)",
                        ConsistencyLevel.LOCAL_QUORUM);
                fail("Expected UnavailableException at LOCAL_QUORUM with only 1/3 remote replicas");
            }
            catch (Exception e)
            {
                Assert.assertEquals(UnavailableException.class.getName(), e.getClass().getName());
                Assert.assertEquals("Cannot achieve consistency level LOCAL_QUORUM", e.getMessage());
            }

            try
            {
                // Read the row at Local Quorum should NOT be overwrited to Remote quorum when remote RF=0.
                dc1Node.coordinator().execute(
                        "SELECT pk, ck, v FROM ks_remote_quorum.tbl WHERE pk = 100",
                        ConsistencyLevel.LOCAL_QUORUM);
                fail("Expected UnavailableException at LOCAL_QUORUM with only 1/3 remote replicas");
            }
            catch (Exception e)
            {
                Assert.assertEquals(UnavailableException.class.getName(), e.getClass().getName());
                Assert.assertEquals("Cannot achieve consistency level LOCAL_QUORUM", e.getMessage());
            }
        }
    }
}
