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
import java.util.concurrent.TimeUnit;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.repair.autorepair.AutoRepair;
import org.apache.cassandra.repair.autorepair.AutoRepairConfig;
import org.apache.cassandra.repair.autorepair.AutoRepairKeyspace;

import static org.apache.cassandra.schema.SchemaConstants.DISTRIBUTED_KEYSPACE_NAME;
import static org.junit.Assert.assertEquals;

public class AutoRepairSchedulerTest extends TestBaseImpl
{

    private static Cluster cluster;
    static SimpleDateFormat sdf;

    @BeforeClass
    public static void init() throws IOException
    {
        System.setProperty("cassandra.streaming.requires_view_build_during_repair", "false");

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
                                                                                 "initial_scheduler_delay", "5s",
                                                                                 "enabled", "true",
                                                                                 "parallel_repair_count", "1",
                                                                                 "parallel_repair_percentage", "0",
                                                                                 "min_repair_interval", "1s"),
                                                                             AutoRepairConfig.RepairType.incremental.toString(),
                                                                                 ImmutableMap.of(
                                                                                 "initial_scheduler_delay", "5s",
                                                                                 "enabled", "true",
                                                                                 "parallel_repair_count", "1",
                                                                                 "parallel_repair_percentage", "0",
                                                                                 "min_repair_interval", "1s"))))
                                                        .set("auto_repair.enabled", "true")
                                                        .set("auto_repair.repair_check_interval", "10s")).start();

        cluster.schemaChange("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};");
        cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck text, v1 int, v2 int, PRIMARY KEY (pk, ck)) WITH read_repair='NONE'"));
    }

    @AfterClass
    public static void afterClass()
    {
        System.clearProperty("cassandra.streaming.requires_view_build_during_repair");
    }

    @Test
    public void testScheduler() throws ParseException
    {
        // ensure there was no history of previous repair runs through the scheduler
        Object[][] rows = cluster.coordinator(1).execute(String.format("SELECT repair_type, host_id, repair_start_ts, repair_finish_ts, repair_turn FROM %s.%s", DISTRIBUTED_KEYSPACE_NAME, AutoRepairKeyspace.AUTO_REPAIR_HISTORY), ConsistencyLevel.QUORUM);
        assertEquals(0, rows.length);

        cluster.forEach(i -> i.runOnInstance(() -> {
            try
            {
                DatabaseDescriptor.setCDCOnRepairEnabled(false);
                AutoRepair.instance.setup();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }));
        // wait for a couple of minutes for repair to go through on all three nodes
        Uninterruptibles.sleepUninterruptibly(2, TimeUnit.MINUTES);

        validate(AutoRepairConfig.RepairType.full.toString());
        validate(AutoRepairConfig.RepairType.incremental.toString());
    }

    private void validate(String repairType) throws ParseException
    {
        Object[][] rows = cluster.coordinator(1).execute(String.format("SELECT repair_type, host_id, repair_start_ts, repair_finish_ts, repair_turn FROM %s.%s where repair_type='%s'", DISTRIBUTED_KEYSPACE_NAME, AutoRepairKeyspace.AUTO_REPAIR_HISTORY, repairType), ConsistencyLevel.QUORUM);
        assertEquals(3, rows.length);
        for (int node = 0; node < rows.length; node++)
        {
            Object[] row = rows[node];
            // repair_type
            Assert.assertEquals(repairType, row[0].toString());
            // host_id
            UUID.fromString(row[1].toString());
            // ensure there is a legit repair_start_ts and repair_finish_ts
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
