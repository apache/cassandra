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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.metrics.AutoRepairMetricsManager;
import org.apache.cassandra.repair.AutoRepairConfig;
import org.apache.cassandra.repair.AutoRepairKeyspace;
import org.apache.cassandra.repair.AutoRepairV2;

import static org.apache.cassandra.schema.SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME;
import static org.junit.Assert.assertEquals;

public class AutoRepairSchedulerTest extends TestBaseImpl
{
    static final Logger logger = LoggerFactory.getLogger(AutoRepairSchedulerTest.class);
    private static Cluster cluster;
    static SimpleDateFormat sdf;

    @BeforeClass
    public static void init() throws IOException
    {
        // Define the expected date format pattern
        String pattern = "EEE MMM dd HH:mm:ss z yyyy";
        // Create SimpleDateFormat object with the given pattern
        sdf = new SimpleDateFormat(pattern);
        sdf.setLenient(false);
        cluster = Cluster.build(3).withConfig(config -> config
                                                        .set("auto_repair",
                                                             ImmutableMap.of(
                                                             "repair_type_overrides",
                                                             ImmutableMap.of(AutoRepairConfig.RepairType.full.toString(),
                                                                             ImmutableMap.of(
                                                                             "initial_scheduler_delay_in_sec", "5",
                                                                             "enabled", "true",
                                                                             "parallel_repair_count_in_group", "3",
                                                                             "parallel_repair_percentage_in_group", "0",
                                                                             "min_repair_interval_in_hours", "-1"),
                                                                             AutoRepairConfig.RepairType.incremental.toString(),
                                                                             ImmutableMap.of(
                                                                             "initial_scheduler_delay_in_sec", "5",
                                                                             "enabled", "true",
                                                                             "parallel_repair_count_in_group", "3",
                                                                             "parallel_repair_percentage_in_group", "0",
                                                                             "min_repair_interval_in_hours", "-1"),
                                                                             AutoRepairConfig.RepairType.preview_repaired.toString(),
                                                                             ImmutableMap.of(
                                                                             "initial_scheduler_delay_in_sec", "5",
                                                                             "enabled", "true",
                                                                             "parallel_repair_count_in_group", "3",
                                                                             "parallel_repair_percentage_in_group", "0",
                                                                             "min_repair_interval_in_hours", "-1"),
                                                                             AutoRepairConfig.RepairType.paxos_cleanup.toString(),
                                                                             ImmutableMap.of(
                                                                             "initial_scheduler_delay_in_sec", "5",
                                                                             "enabled", "true",
                                                                             "parallel_repair_count_in_group", "3",
                                                                             "parallel_repair_percentage_in_group", "0",
                                                                             "min_repair_interval_in_hours", "-1"),
                                                                             AutoRepairConfig.RepairType.bootstrap.toString(),
                                                                             ImmutableMap.of(
                                                                             "initial_scheduler_delay_in_sec", "5",
                                                                             "enabled", "true",
                                                                             "parallel_repair_count_in_group", "3",
                                                                             "parallel_repair_percentage_in_group", "0",
                                                                             "min_repair_interval_in_hours", "-1")
                                                             )))
                                                        .set("auto_repair.enabled", "true")
                                                        .set("auto_repair.repair_check_interval_in_sec", "10")
                                                        .set("auto_repair.repair_task_min_duration", "0s")).start();

        cluster.schemaChange("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};");
        cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck text, v1 int, v2 int, PRIMARY KEY (pk, ck)) WITH read_repair='NONE'"));
    }

    @Test
    public void testScheduler() throws ParseException
    {
        // ensure there was no history of previous repair runs through the scheduler
        Object[][] rows = cluster.coordinator(1).execute(String.format("SELECT repair_type, host_id, repair_start_ts, repair_finish_ts, repair_turn FROM %s.%s", AUTO_REPAIR_KEYSPACE_NAME, AutoRepairKeyspace.AUTO_REPAIR_HISTORY_V2), ConsistencyLevel.QUORUM);
        assertEquals(0, rows.length);

        cluster.forEach(i -> i.runOnInstance(() -> {
            try
            {
                AutoRepairV2.instance.setup();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }));
        logger.info("Repair setup done");

        // For AutoRepairConfig.RepairType.bootstrap, we do not insert any entries to the
        // AUTO_REPAIR_KEYSPACE_NAME.AUTO_REPAIR_HISTORY_V2
        // as it bypasses all the complex checks to determine whose turn is next, etc.
        List<AutoRepairConfig.RepairType> repairTypes =
        Arrays.stream(AutoRepairConfig.RepairType.values()).
              filter(type -> type != AutoRepairConfig.RepairType.bootstrap).
              collect(Collectors.toList());

        // validate that the repair ran on all nodes
        cluster.forEach(i -> i.runOnInstance(() -> {
            for (AutoRepairConfig.RepairType repairType : repairTypes)
            {
                while (AutoRepairMetricsManager.getMetrics(repairType).nodeRepairTimeInSec.getValue().longValue() <= 0)
                {
                    try
                    {
                        Thread.sleep(1000);
                    }
                    catch (InterruptedException e)
                    {
                        throw new RuntimeException(e);
                    }
                }
                logger.info("AutoRepair has completed one {} repair cycle", repairType);
            }
        }));
        for (AutoRepairConfig.RepairType repairType : repairTypes)
        {
            validate(repairType.toString());
        }
    }

    private void validate(String repairType) throws ParseException
    {
        Object[][] rows = cluster.coordinator(1).execute(String.format("SELECT repair_type, host_id, repair_start_ts, repair_finish_ts, repair_turn FROM %s.%s WHERE pid=0 AND repair_type='%s'", AUTO_REPAIR_KEYSPACE_NAME, AutoRepairKeyspace.AUTO_REPAIR_HISTORY_V2, repairType), ConsistencyLevel.QUORUM);
        assertEquals(3, rows.length);
        for (int node = 0; node < rows.length; node++)
        {
            Object[] row = rows[node];
            Assert.assertEquals(repairType, row[0].toString());
            Assert.assertEquals(String.format("00000000-0000-4000-8000-%012d", node + 1), row[1].toString());
            // ensure there is a legit repair_start_ts and repair_finish_ts time
            sdf.parse(row[2].toString());
            sdf.parse(row[3].toString());
            // the reason why the repair was scheduled
            Assert.assertEquals("MY_TURN", row[4].toString());
            for (Object col : row)
            {
                System.out.println("Data:" + col);
            }
            System.out.println("=====================================");
        }
    }
}
