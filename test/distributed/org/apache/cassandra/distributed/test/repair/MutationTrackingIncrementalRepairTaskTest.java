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
package org.apache.cassandra.distributed.test.repair;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.repair.MutationTrackingIncrementalRepairTask;

import static org.junit.Assert.*;

/**
 * Tests for MutationTrackingIncrementalRepairTask.
 * Tests the decision logic for when to use mutation tracking repair.
 *
 * Uses a shared cluster across all tests to minimize overhead.
 */
public class MutationTrackingIncrementalRepairTaskTest extends TestBaseImpl
{
    private static Cluster CLUSTER;
    private static final AtomicInteger ksCounter = new AtomicInteger();

    @BeforeClass
    public static void setupCluster() throws IOException
    {
        CLUSTER = Cluster.build()
                         .withNodes(3)
                         .withConfig(cfg -> cfg.with(Feature.NETWORK, Feature.GOSSIP)
                                               .set("mutation_tracking_enabled", true))
                         .start();
    }

    @AfterClass
    public static void teardownCluster()
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }

    private static String nextKsName()
    {
        return "mtirt_ks" + ksCounter.incrementAndGet();
    }

    @Test
    public void testShouldUseMutationTrackingRepairForTrackedKeyspace() throws Throwable
    {
        String ksName = nextKsName();
        CLUSTER.schemaChange("CREATE KEYSPACE " + ksName + " WITH replication = " +
                             "{'class': 'SimpleStrategy', 'replication_factor': 3} " +
                             "AND replication_type='tracked'");

        Boolean shouldUse = CLUSTER.get(1).callOnInstance(() -> MutationTrackingIncrementalRepairTask.shouldUseMutationTrackingRepair(ksName));

        assertTrue("Tracked keyspace should use mutation tracking repair", shouldUse);
    }

    @Test
    public void testShouldNotUseMutationTrackingRepairForUntrackedKeyspace() throws Throwable
    {
        String ksName = nextKsName();
        CLUSTER.schemaChange("CREATE KEYSPACE " + ksName + " WITH replication = " +
                             "{'class': 'SimpleStrategy', 'replication_factor': 3} " +
                             "AND replication_type='untracked'");

        Boolean shouldUse = CLUSTER.get(1).callOnInstance(() -> MutationTrackingIncrementalRepairTask.shouldUseMutationTrackingRepair(ksName));

        assertFalse("Untracked keyspace should not use mutation tracking repair", shouldUse);
    }

    @Test
    public void testRequiresTraditionalRepairReturnsFalseForNonMigratingKeyspace() throws Throwable
    {
        String ksName = nextKsName();
        CLUSTER.schemaChange("CREATE KEYSPACE " + ksName + " WITH replication = " +
                             "{'class': 'SimpleStrategy', 'replication_factor': 3} " +
                             "AND replication_type='tracked'");

        Boolean requiresTraditional = CLUSTER.get(1).callOnInstance(() -> MutationTrackingIncrementalRepairTask.requiresTraditionalRepair(ksName));

        assertFalse("Non-migrating keyspace should not require traditional repair", requiresTraditional);
    }

    @Test
    public void testShouldUseMutationTrackingRepairForNonexistentKeyspace() throws Throwable
    {
        Boolean shouldUse = CLUSTER.get(1).callOnInstance(() -> MutationTrackingIncrementalRepairTask.shouldUseMutationTrackingRepair("nonexistent_ks_xyz"));

        assertFalse("Nonexistent keyspace should return false", shouldUse);
    }

    @Test
    public void testMigrationFromUntrackedToTracked() throws Throwable
    {
        String ksName = nextKsName();
        CLUSTER.schemaChange("CREATE KEYSPACE " + ksName + " WITH replication = " +
                             "{'class': 'SimpleStrategy', 'replication_factor': 3} " +
                             "AND replication_type='untracked'");
        CLUSTER.schemaChange("CREATE TABLE " + ksName + ".tbl (k int PRIMARY KEY, v int)");

        // Verify initial state
        Boolean shouldUseBefore = CLUSTER.get(1).callOnInstance(() -> MutationTrackingIncrementalRepairTask.shouldUseMutationTrackingRepair(ksName));
        assertFalse("Untracked keyspace should not use mutation tracking repair", shouldUseBefore);

        Boolean requiresBefore = CLUSTER.get(1).callOnInstance(() -> MutationTrackingIncrementalRepairTask.requiresTraditionalRepair(ksName));
        assertFalse("Non-migrating keyspace should not require traditional repair", requiresBefore);

        // Trigger migration by altering to tracked
        CLUSTER.schemaChange("ALTER KEYSPACE " + ksName + " WITH replication = " +
                             "{'class': 'SimpleStrategy', 'replication_factor': 3} " +
                             "AND replication_type='tracked'");

        // Verify migration state - both methods should now return true
        Boolean shouldUseAfter = CLUSTER.get(1).callOnInstance(() -> MutationTrackingIncrementalRepairTask.shouldUseMutationTrackingRepair(ksName));
        assertTrue("Migrating keyspace should use mutation tracking repair", shouldUseAfter);

        Boolean requiresAfter = CLUSTER.get(1).callOnInstance(() -> MutationTrackingIncrementalRepairTask.requiresTraditionalRepair(ksName));
        assertTrue("Migrating keyspace should require traditional repair", requiresAfter);
    }

    @Test
    public void testMigrationFromTrackedToUntracked() throws Throwable
    {
        String ksName = nextKsName();
        CLUSTER.schemaChange("CREATE KEYSPACE " + ksName + " WITH replication = " +
                             "{'class': 'SimpleStrategy', 'replication_factor': 3} " +
                             "AND replication_type='tracked'");
        CLUSTER.schemaChange("CREATE TABLE " + ksName + ".tbl (k int PRIMARY KEY, v int)");

        // Verify initial state
        Boolean shouldUseBefore = CLUSTER.get(1).callOnInstance(() -> MutationTrackingIncrementalRepairTask.shouldUseMutationTrackingRepair(ksName));
        assertTrue("Tracked keyspace should use mutation tracking repair", shouldUseBefore);

        Boolean requiresBefore = CLUSTER.get(1).callOnInstance(() -> MutationTrackingIncrementalRepairTask.requiresTraditionalRepair(ksName));
        assertFalse("Non-migrating tracked keyspace should not require traditional repair", requiresBefore);

        // Migrate back to untracked
        CLUSTER.schemaChange("ALTER KEYSPACE " + ksName + " WITH replication = " +
                             "{'class': 'SimpleStrategy', 'replication_factor': 3} " +
                             "AND replication_type='untracked'");

        // During reverse migration, both should still apply
        Boolean shouldUseAfter = CLUSTER.get(1).callOnInstance(() -> MutationTrackingIncrementalRepairTask.shouldUseMutationTrackingRepair(ksName));
        assertTrue("Keyspace migrating from tracked should still use mutation tracking repair", shouldUseAfter);

        Boolean requiresAfter = CLUSTER.get(1).callOnInstance(() -> MutationTrackingIncrementalRepairTask.requiresTraditionalRepair(ksName));
        assertTrue("Keyspace migrating from tracked should require traditional repair", requiresAfter);
    }
}
