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
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import org.apache.cassandra.Util;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.repair.autorepair.AutoRepair;
import org.apache.cassandra.repair.autorepair.AutoRepairConfig;
import org.apache.cassandra.service.AutoRepairService;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.cassandra.schema.SchemaConstants.DISTRIBUTED_KEYSPACE_NAME;
import static org.apache.cassandra.schema.SystemDistributedKeyspace.AUTO_REPAIR_HISTORY;
import static org.junit.Assert.assertEquals;

/**
 * Test that verifies orphan nodes are cleaned up from auto_repair_history even when repairs
 * are skipped due to min_repair_interval constraints.
 */
public class AutoRepairOrphanCleanupTest extends TestBaseImpl
{
    private static Cluster cluster;

    @BeforeClass
    public static void init() throws IOException
    {
        // Configure a 3-node cluster with auto_repair enabled but with a very high min_repair_interval
        // This ensures that when we test, repairs will be skipped due to "too soon to repair"
        cluster = Cluster.build(3)
                         .withTokenCount(4)
                         .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(3, 4))
                         .withConfig(config -> config
                                               .set("num_tokens", 4)
                                               .set("auto_repair",
                                                    ImmutableMap.of(
                                                    "repair_check_interval", "1s",
                                                    "repair_type_overrides",
                                                    ImmutableMap.of(AutoRepairConfig.RepairType.FULL.getConfigName(),
                                                                    ImmutableMap.builder()
                                                                                .put("initial_scheduler_delay", "0s")
                                                                                .put("enabled", "true")
                                                                                // Set very high min_repair_interval
                                                                                // to ensure repairs are skipped
                                                                                .put("min_repair_interval", "24h")
                                                                                .put("allow_parallel_replica_repair", "true")
                                                                                .put("repair_by_keyspace", "true")
                                                                                .build())))
                                               .set("auto_repair.enabled", "true"))
                         .start();
    }

    @AfterClass
    public static void tearDown()
    {
        cluster.close();
    }

    @Test
    public void testOrphanNodeCleanupWhenRepairSkipped()
    {
        // Insert 3 auto-repair records for each live node and 1 for the orphan node
        List<UUID> liveHostIds = new ArrayList<>();
        for (int i = 1; i <= 3; i++)
        {
            liveHostIds.add(
            cluster.get(i).callOnInstance(() ->
                                          StorageService.instance.getHostIdForEndpoint(
                                          FBUtilities.getBroadcastAddressAndPort())));
        }
        UUID orphanHostId = UUID.randomUUID();

        long currentTime = System.currentTimeMillis();
        // Orphan node: oldest finish time, so it is next in line to run repair
        long orphanStart = currentTime - TimeUnit.HOURS.toMillis(4);   // 4 hours ago
        long orphanFinish = currentTime - TimeUnit.HOURS.toMillis(3);  // 3 hours ago

        // Live nodes: more recent finish times
        long[] liveStart = {
        currentTime - TimeUnit.HOURS.toMillis(3), // 3 hours ago
        currentTime - TimeUnit.HOURS.toMillis(2), // 2 hours ago
        currentTime - TimeUnit.HOURS.toMillis(1)  // 1 hour ago
        };
        long[] liveFinish = {
        currentTime - TimeUnit.HOURS.toMillis(2), // 2 hours ago
        currentTime - TimeUnit.HOURS.toMillis(1), // 1 hour ago
        currentTime                              // now
        };

        // Insert live node records
        for (int i = 0; i < 3; i++)
        {
            cluster.coordinator(1).execute(String.format(
            "INSERT INTO %s.%s (repair_type, host_id, repair_start_ts, repair_finish_ts, repair_turn) " +
            "VALUES ('%s', %s, %d, %d, '%s')",
            DISTRIBUTED_KEYSPACE_NAME,
            AUTO_REPAIR_HISTORY,
            AutoRepairConfig.RepairType.FULL,
            liveHostIds.get(i),
            liveStart[i],
            liveFinish[i],
            "NOT_MY_TURN"
            ), ConsistencyLevel.QUORUM);
        }

        // Insert orphan node record (should be next in line to run repair)
        cluster.coordinator(1).execute(String.format(
        "INSERT INTO %s.%s (repair_type, host_id, repair_start_ts, repair_finish_ts, repair_turn) " +
        "VALUES ('%s', %s, %d, %d, '%s')",
        DISTRIBUTED_KEYSPACE_NAME,
        AUTO_REPAIR_HISTORY,
        AutoRepairConfig.RepairType.FULL,
        orphanHostId,
        orphanStart,
        orphanFinish,
        "NOT_MY_TURN"
        ), ConsistencyLevel.QUORUM);

        // Validate that all 4 records (3 live nodes + 1 orphan) are present before starting auto-repair
        Object[][] initialRows = cluster.coordinator(1).execute(
        String.format("SELECT host_id FROM %s.%s WHERE repair_type = '%s'",
                      DISTRIBUTED_KEYSPACE_NAME,
                      AUTO_REPAIR_HISTORY,
                      AutoRepairConfig.RepairType.FULL),
        ConsistencyLevel.QUORUM
        );
        assertEquals("Expected 4 records (3 live nodes + 1 orphan) before auto-repair starts", 4, initialRows.length);
        boolean orphanFound = false;
        int liveNodesFound = 0;
        for (Object[] row : initialRows)
        {
            UUID hostId = (UUID) row[0];
            if (hostId.equals(orphanHostId))
                orphanFound = true;
            else if (liveHostIds.contains(hostId))
                liveNodesFound++;
        }
        assertEquals("All 3 live nodes should be present", 3, liveNodesFound);
        assertEquals("Orphan node should be present", true, orphanFound);

        // Once the auto_repair_history table is prepared, initialize auto-repair service on all nodes
        cluster.forEach(i -> i.runOnInstance(() -> {
            try
            {
                AutoRepairService.setup();
                AutoRepair.instance.setup();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }));

        // Wait for at least one auto-repair cycle to allow orphan cleanup to run
        // (auto_repair.repair_check_interval is set to 1s, so 10s is plenty)
        Util.spinAssertEquals(true, () -> {
            Object[][] rows = cluster.coordinator(1).execute(
            String.format("SELECT host_id FROM %s.%s WHERE repair_type = '%s'",
                          DISTRIBUTED_KEYSPACE_NAME,
                          AUTO_REPAIR_HISTORY,
                          AutoRepairConfig.RepairType.FULL),
            ConsistencyLevel.QUORUM
            );
            // The orphanHostId should not be present in the results
            for (Object[] row : rows)
            {
                UUID hostId = (UUID) row[0];
                if (hostId.equals(orphanHostId))
                    return false;
            }
            return true;
        }, 10_000);

        // Query all records for this repair type
        Object[][] rows = cluster.coordinator(1).execute(
        String.format("SELECT host_id, repair_start_ts, repair_finish_ts FROM %s.%s WHERE repair_type = '%s'",
                      DISTRIBUTED_KEYSPACE_NAME,
                      AUTO_REPAIR_HISTORY,
                      AutoRepairConfig.RepairType.FULL),
        ConsistencyLevel.QUORUM
        );

        // Check that the orphan node's record is gone, and live nodes' records remain with unchanged timestamps
        Map<UUID, long[]> actualLiveTimestamps = new HashMap<>();
        for (Object[] row : rows)
        {
            UUID hostId = (UUID) row[0];
            long startTs = ((Date) row[1]).getTime();
            long finishTs = ((Date) row[2]).getTime();
            if (hostId.equals(orphanHostId))
            {
                throw new AssertionError("Orphan node record was not cleaned up");
            }
            actualLiveTimestamps.put(hostId, new long[]{startTs, finishTs});
        }

        // All live nodes should still be present with their original timestamps
        assertEquals("Unexpected number of live node records", 3, actualLiveTimestamps.size());
        for (int i = 0; i < 3; i++)
        {
            UUID hostId = liveHostIds.get(i);
            long[] expected = new long[]{liveStart[i], liveFinish[i]};
            long[] actual = actualLiveTimestamps.get(hostId);
            if (actual == null)
                throw new AssertionError("Live node record missing for hostId: " + hostId);
            assertEquals("Live node repair_start_ts changed for hostId: " + hostId, expected[0], actual[0]);
            assertEquals("Live node repair_finish_ts changed for hostId: " + hostId, expected[1], actual[1]);
        }
    }
}
