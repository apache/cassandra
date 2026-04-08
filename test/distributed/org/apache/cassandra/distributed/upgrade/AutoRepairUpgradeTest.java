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

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.repair.autorepair.AutoRepairConfig;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Upgrade test for auto-repair verifying that it runs successfully before and after
 * upgrading from 5.0 to current. The first repair round executes on 5.0 nodes
 * (automatically during startup) and the second round after all nodes are upgraded.
 *
 * Host IDs change across the upgrade (from random UUIDs to NodeId-derived UUIDs).
 * The migration in {@code AutoRepairUtils.migrateAutoRepairHistoryForUpgrade()} re-keys
 * entries under the new host IDs, preserving repair timestamps. The test verifies that:
 * <ol>
 *   <li>3 repair history entries exist before the upgrade (on 5.0)</li>
 *   <li>3 entries exist after upgrade, keyed by new host IDs, retaining per-node pre-upgrade timestamps</li>
 *   <li>After repair runs, each entry's timestamp exceeds its own pre-upgrade value</li>
 * </ol>
 *
 * Auto-repair is started automatically during node startup via
 * {@code StorageService.doAutoRepairSetup()} when the config is enabled.
 * In 5.0, the JVM property {@code cassandra.autorepair.enable=true} is also required.
 */
public class AutoRepairUpgradeTest extends UpgradeTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(AutoRepairUpgradeTest.class);

    @Test
    public void testAutoRepairAcrossUpgrade() throws Throwable
    {
        // 5.0 requires this JVM property to enable auto-repair (schema tables, JMX, scheduler).
        // Trunk does not use this property.
        System.setProperty("cassandra.autorepair.enable", "true"); // checkstyle: suppress nearby 'blockSystemPropertyUsage'

        // Maps pre-upgrade host ID -> finish timestamp, captured right before upgrade
        Map<String, Long> preUpgradeTimestamps = new ConcurrentHashMap<>();

        new TestCase()
        .nodes(3)
        .singleUpgradeToCurrentFrom(v50)
        .withConfig(cfg -> cfg.with(Feature.NETWORK, Feature.GOSSIP)
                              .set("auto_repair",
                                   ImmutableMap.of(
                                   "repair_type_overrides",
                                   ImmutableMap.of(AutoRepairConfig.RepairType.FULL.getConfigName(),
                                                   ImmutableMap.of(
                                                   "initial_scheduler_delay", "60s",
                                                   "enabled", "true",
                                                   "parallel_repair_count", "3",
                                                   "allow_parallel_replica_repair", "true",
                                                   "min_repair_interval", "60s"))))
                              .set("auto_repair.enabled", "true")
                              .set("auto_repair.global_settings.repair_by_keyspace", "true")
                              .set("auto_repair.global_settings.repair_retry_backoff", "5s")
                              .set("auto_repair.repair_task_min_duration", "0s")
                              .set("auto_repair.repair_check_interval", "60s"))
        .setup(cluster -> {
            cluster.schemaChange("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE +
                                 " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};");
            cluster.schemaChange("CREATE TABLE IF NOT EXISTS " + KEYSPACE +
                                 ".tbl (pk int, ck text, v1 int, v2 int, PRIMARY KEY (pk, ck))");

            // Wait for auto-repair to complete on all 5.0 nodes.
            waitForNEntries(cluster, 3);

            assertEquals("Expected repair history for all 3 nodes on 5.0",
                         3, captureFinishTimestamps(cluster).size());
        })
        .runBeforeClusterUpgrade(cluster -> {
            // Wait for any in-flight repair to complete before capturing timestamps.
            waitForNoInFlightRepairs(cluster);
            preUpgradeTimestamps.putAll(captureFinishTimestamps(cluster));
            logger.info("Pre-upgrade timestamps: {}", preUpgradeTimestamps);

            // Seed auto_repair_priority with pre-upgrade host IDs to test priority migration.
            String hostIdSet = preUpgradeTimestamps.keySet().stream()
                                                   .map(id -> id.toString())
                                                   .collect(Collectors.joining(", "));
            cluster.coordinator(1).execute(
                String.format("INSERT INTO system_distributed.auto_repair_priority (repair_type, repair_priority) VALUES ('%s', {%s})",
                              AutoRepairConfig.RepairType.FULL.toString(), hostIdSet),
                ConsistencyLevel.QUORUM);
            logger.info("Seeded auto_repair_priority with pre-upgrade host IDs: {}", preUpgradeTimestamps.keySet());
        })
        .runAfterClusterUpgrade(cluster -> {
            // Phase 1: Verify migration — old entries replaced by new entries with
            // different host IDs but preserved per-node timestamps
            Map<String, Long> migratedTimestamps = captureFinishTimestamps(cluster);
            logger.info("Pre-upgrade entries: {}, post-migration entries: {}", preUpgradeTimestamps, migratedTimestamps);
            assertEquals("Expected exactly 3 migrated entries", 3, migratedTimestamps.size());

            // Host IDs must have changed — new entries should not use pre-upgrade IDs
            for (String id : migratedTimestamps.keySet())
                assertFalse("Migrated entry should use new host ID, not pre-upgrade ID " + id,
                            preUpgradeTimestamps.containsKey(id));

            // Each migrated entry should retain its exact original per-node timestamp.
            // Since host IDs changed, we compare by value: every migrated timestamp must
            // exist in the pre-upgrade set (values preserved exactly during migration).
            for (Long ts : migratedTimestamps.values())
                assertTrue("Migrated timestamp " + ts + " should match a pre-upgrade timestamp",
                           preUpgradeTimestamps.containsValue(ts));

            // Verify auto_repair_priority migration: old host IDs should be replaced by new ones,
            // and the total count should remain the same (3 entries seeded before upgrade).
            Set<String> priorityIds = capturePriorityHostIds(cluster);
            logger.info("Post-migration priority IDs: {}", priorityIds);
            assertEquals("Priority set should have same number of entries after migration",
                         preUpgradeTimestamps.size(), priorityIds.size());
            for (String id : priorityIds)
                assertFalse("Priority should not contain pre-upgrade host ID " + id,
                            preUpgradeTimestamps.containsKey(id));

            // Phase 2: Wait for repair to run (after initial_scheduler_delay expires),
            // then verify each entry's timestamp exceeds its own migrated value.
            Map<String, Long> migratedSnapshot = new HashMap<>(migratedTimestamps);
            waitForAllTimestampsExceeded(cluster, migratedSnapshot);

            Map<String, Long> postRepairTimestamps = captureFinishTimestamps(cluster);
            assertEquals("Expected 3 entries after repair", 3, postRepairTimestamps.size());
            assertEquals("Post-repair entries should use same host IDs as migrated",
                         migratedSnapshot.keySet(), postRepairTimestamps.keySet());
            for (Map.Entry<String, Long> entry : postRepairTimestamps.entrySet())
                assertTrue("Post-repair timestamp for " + entry.getKey() + " should exceed migrated timestamp",
                           entry.getValue() > migratedSnapshot.get(entry.getKey()));

            // Priority table should be cleared after repair completes
            Set<String> postRepairPriorityIds = capturePriorityHostIds(cluster);
            assertTrue("Priority set should be empty after post-upgrade repair completes, but was: " + postRepairPriorityIds,
                       postRepairPriorityIds.isEmpty());
        })
        .run();
    }

    private void waitForNEntries(UpgradeableCluster cluster, int expected)
    {
        await().atMost(5, TimeUnit.MINUTES)
               .pollInterval(2, TimeUnit.SECONDS)
               .until(() -> captureFinishTimestamps(cluster).size() >= expected);
    }

    private void waitForAllTimestampsExceeded(UpgradeableCluster cluster, Map<String, Long> baseline)
    {
        await().atMost(5, TimeUnit.MINUTES)
               .pollInterval(2, TimeUnit.SECONDS)
               .until(() -> {
                   Map<String, Long> current = captureFinishTimestamps(cluster);
                   if (current.size() < baseline.size())
                       return false;
                   for (Map.Entry<String, Long> entry : baseline.entrySet())
                   {
                       Long currentTs = current.get(entry.getKey());
                       if (currentTs == null || currentTs <= entry.getValue())
                           return false;
                   }
                   return true;
               });
    }

    private Set<String> capturePriorityHostIds(UpgradeableCluster cluster)
    {
        Object[][] rows = cluster.coordinator(1).execute(
            String.format("SELECT repair_priority FROM system_distributed.auto_repair_priority WHERE repair_type='%s'",
                          AutoRepairConfig.RepairType.FULL.toString()),
            ConsistencyLevel.QUORUM);
        if (rows.length == 0 || rows[0][0] == null)
            return Set.of();
        @SuppressWarnings("unchecked")
        Set<UUID> uuids = (Set<UUID>) rows[0][0];
        return uuids.stream().map(UUID::toString).collect(Collectors.toSet());
    }

    private void waitForNoInFlightRepairs(UpgradeableCluster cluster)
    {
        await().atMost(2, TimeUnit.MINUTES)
               .pollInterval(1, TimeUnit.SECONDS)
               .until(() -> {
                   Object[][] rows = cluster.coordinator(1).execute(
                       String.format("SELECT host_id, repair_start_ts, repair_finish_ts FROM system_distributed.auto_repair_history WHERE repair_type='%s'",
                                     AutoRepairConfig.RepairType.FULL.toString()),
                       ConsistencyLevel.QUORUM);
                   for (Object[] row : rows)
                   {
                       long startTs = ((Date) row[1]).getTime();
                       long finishTs = ((Date) row[2]).getTime();
                       if (startTs > finishTs)
                           return false; // repair still in flight
                   }
                   return true;
               });
    }

    private Map<String, Long> captureFinishTimestamps(UpgradeableCluster cluster)
    {
        Object[][] rows = cluster.coordinator(1).execute(
            String.format("SELECT host_id, repair_finish_ts FROM system_distributed.auto_repair_history WHERE repair_type='%s'",
                          AutoRepairConfig.RepairType.FULL.toString()),
            ConsistencyLevel.QUORUM);
        Map<String, Long> timestamps = new HashMap<>();
        for (Object[] row : rows)
        {
            String hostId = row[0].toString();
            long finishTs = ((Date) row[1]).getTime();
            timestamps.put(hostId, finishTs);
        }
        return timestamps;
    }
}
