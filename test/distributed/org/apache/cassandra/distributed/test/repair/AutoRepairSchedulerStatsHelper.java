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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;

import org.junit.Assert;

import org.apache.cassandra.Util;
import org.apache.cassandra.auth.AuthKeyspace;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.metrics.AutoRepairMetrics;
import org.apache.cassandra.metrics.AutoRepairMetricsManager;
import org.apache.cassandra.repair.autorepair.AutoRepair;
import org.apache.cassandra.repair.autorepair.AutoRepairConfig;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SystemDistributedKeyspace;
import org.apache.cassandra.service.AutoRepairService;

import static org.apache.cassandra.schema.SchemaConstants.DISTRIBUTED_KEYSPACE_NAME;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Helper class to test {@code totalBytesToRepair}, {@code bytesAlreadyRepaired}, {@code totalKeyspaceRepairPlansToRepair},
 * and {@code keyspaceRepairPlansAlreadyRepaired}
 * for {@link org.apache.cassandra.repair.autorepair.AutoRepairState} scheduler
 */
public class AutoRepairSchedulerStatsHelper extends TestBaseImpl
{
    private static Cluster cluster;
    static SimpleDateFormat sdf;
    private static final String KEYSPACE1 = "ks1";
    private static final String KEYSPACE2 = "ks2";
    private static final String TABLE1 = "tbl1";
    private static final String TABLE2 = "tbl2";

    public static void init(int numTokens) throws IOException
    {
        // Define the expected date format pattern
        String pattern = "EEE MMM dd HH:mm:ss z yyyy";
        // Create SimpleDateFormat object with the given pattern
        sdf = new SimpleDateFormat(pattern);
        sdf.setLenient(false);
        CassandraRelevantProperties.SYSTEM_DISTRIBUTED_DEFAULT_RF.setInt(1);
        cluster = Cluster.build(1)
                         .withTokenCount(numTokens)
                         .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(1, numTokens))
                         .withConfig(config -> config
                                               .set("num_tokens", numTokens)
                                               .set("auto_repair",
                                                    ImmutableMap.of(
                                                    "repair_type_overrides",
                                                    ImmutableMap.of(AutoRepairConfig.RepairType.FULL.getConfigName(),
                                                                    ImmutableMap.of(
                                                                    "initial_scheduler_delay", "5s",
                                                                    "enabled", "true",
                                                                    "parallel_repair_count", "1",
                                                                    // Allow parallel replica repair to allow replicas
                                                                    // to execute full repair at same time.
                                                                    "allow_parallel_replica_repair", "true",
                                                                    // Set min_repair_interval to a higher number to
                                                                    // run only one round of AutoRepair
                                                                    "min_repair_interval", "48h"))))
                                               .set("auto_repair.enabled", "true")
                                               .set("auto_repair.global_settings.repair_retry_backoff", "5s")
                                               .set("auto_repair.repair_task_min_duration", "0s")
                                               .set("auto_repair.repair_check_interval", "5s"))
                         .start();

        cluster.schemaChange("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE1 + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");
        cluster.schemaChange("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE2 + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");
        // disable the compression to calculate an accurate expected repair bytes because with compression enabled,
        // we only get estimated bytes, which hinders the ability to do actual vs. expected checks in the test case
        cluster.schemaChange(String.format("CREATE TABLE %s.%s (pk int, ck int, v int, PRIMARY KEY (pk, ck))  WITH compression = { 'enabled' : false }", KEYSPACE1, TABLE1));
        cluster.schemaChange(String.format("CREATE TABLE %s.%s (pk int, ck int, v int, PRIMARY KEY (pk, ck))  WITH compression = { 'enabled' : false }", KEYSPACE1, TABLE2));
        cluster.schemaChange(String.format("CREATE TABLE %s.%s (pk int, ck int, v int, PRIMARY KEY (pk, ck))  WITH compression = { 'enabled' : false }", KEYSPACE2, TABLE1));
        cluster.schemaChange(String.format("CREATE TABLE %s.%s (pk int, ck int, v int, PRIMARY KEY (pk, ck))  WITH compression = { 'enabled' : false }", KEYSPACE2, TABLE2));
    }

    public static void tearDown()
    {
        cluster.close();
    }

    public static void testSchedulerStats() throws ParseException
    {
        // disabling AutoRepair for system_distributed and system_auth tables to avoid
        // interfering with the repaired bytes/plans calculation
        disableAutoRepair(SystemDistributedKeyspace.NAME, SystemDistributedKeyspace.TABLE_NAMES);
        disableAutoRepair(SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.TABLE_NAMES);

        insertData();

        cluster.get(1).runOnInstance(() -> {
            try
            {
                AutoRepairService.setup();
                AutoRepair.instance.setup();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        });

        cluster.forEach(i -> i.runOnInstance(() -> {
            AutoRepair.SLEEP_IF_REPAIR_FINISHES_QUICKLY = new DurationSpec.IntSecondsBound("2s");

            AutoRepairMetrics fullMetrics = AutoRepairMetricsManager.getMetrics(AutoRepairConfig.RepairType.FULL);
            // Since the AutoRepair sleeps up to SLEEP_IF_REPAIR_FINISHES_QUICKLY if the repair finishes quickly,
            // so the "nodeRepairTimeInSec" metric should at least be greater than or equal to
            // SLEEP_IF_REPAIR_FINISHES_QUICKLY
            Util.spinAssert("AutoRepair has not yet completed one FULL repair cycle",
                            greaterThanOrEqualTo(2L),
                            () -> fullMetrics.nodeRepairTimeInSec.getValue().longValue(),
                            2,
                            TimeUnit.MINUTES);

            long expectedRepairBytes = calculateExpectedBytes(Arrays.asList(KEYSPACE1, KEYSPACE2));
            assertEquals(fullMetrics.totalKeyspaceRepairPlansToRepair.getValue(), fullMetrics.keyspaceRepairPlansAlreadyRepaired.getValue());
            // AutoRepair creates a repair plan per keyspace;
            // Since there are two separate keyspaces, KEYSPACE1 and KEYSPACE2, the total expected plans should be "2"
            assertEquals(2, fullMetrics.totalKeyspaceRepairPlansToRepair.getValue().intValue());
            assertEquals(fullMetrics.totalBytesToRepair.getValue().longValue(), fullMetrics.bytesAlreadyRepaired.getValue().longValue());
            assertEquals(expectedRepairBytes, fullMetrics.bytesAlreadyRepaired.getValue().longValue());
        }));
        validate(AutoRepairConfig.RepairType.FULL.toString());
    }

    private static long calculateExpectedBytes(List<String> keyspaces)
    {
        long totalBytes = 0;
        for (String keyspace : keyspaces)
        {
            for (String table : Arrays.asList(TABLE1, TABLE2))
            {
                ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(keyspace, table);
                assertNotNull(cfs);
                Iterable<SSTableReader> sstables = cfs.getTracker().getView().select(SSTableSet.CANONICAL);
                for (SSTableReader sstable : sstables)
                {
                    totalBytes += sstable.onDiskLength();
                }
            }
        }
        return totalBytes;
    }

    private static void insertData()
    {
        for (int i = 0; i < 100; i++)
        {
            cluster.coordinator(1).execute(String.format("INSERT INTO %s.%s (pk, ck, v) VALUES (?,?,?)", KEYSPACE1, TABLE1),
                                           ConsistencyLevel.ONE, i, i, i);
            cluster.coordinator(1).execute(String.format("INSERT INTO %s.%s (pk, ck, v) VALUES (?,?,?)", KEYSPACE1, TABLE2),
                                           ConsistencyLevel.ONE, i, i, i);
            cluster.coordinator(1).execute(String.format("INSERT INTO %s.%s (pk, ck, v) VALUES (?,?,?)", KEYSPACE2, TABLE1),
                                           ConsistencyLevel.ONE, i, i, i);
            cluster.coordinator(1).execute(String.format("INSERT INTO %s.%s (pk, ck, v) VALUES (?,?,?)", KEYSPACE2, TABLE2),
                                           ConsistencyLevel.ONE, i, i, i);
        }
        cluster.get(1).nodetool("flush", KEYSPACE1, TABLE1);
        cluster.get(1).nodetool("flush", KEYSPACE1, TABLE2);
        cluster.get(1).nodetool("flush", KEYSPACE2, TABLE1);
        cluster.get(1).nodetool("flush", KEYSPACE2, TABLE2);
    }

    private static void disableAutoRepair(String keyspaceName, Set<String> distributedSystemTables)
    {
        for (String tableName : distributedSystemTables)
        {
            cluster.coordinator(1).execute(String.format("ALTER TABLE %s.%s WITH auto_repair = {'full_enabled': 'false'}", keyspaceName, tableName),
                                           ConsistencyLevel.ONE);
        }
    }

    private static void validate(String repairType) throws ParseException
    {
        Object[][] rows = cluster.coordinator(1).execute(String.format("SELECT repair_type, host_id, repair_start_ts, repair_finish_ts, repair_turn FROM %s.%s where repair_type='%s'", DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.AUTO_REPAIR_HISTORY, repairType), ConsistencyLevel.QUORUM);
        assertEquals(1, rows.length);
        for (int node = 0; node < rows.length; node++)
        {
            Object[] row = rows[node];
            // repair_type
            Assert.assertEquals(repairType, row[0].toString());
            // host_id
            Assert.assertNotNull(UUID.fromString(row[1].toString()));
            // ensure there is a legit repair_start_ts and repair_finish_ts
            sdf.parse(row[2].toString());
            sdf.parse(row[3].toString());
            // the reason why the repair was scheduled
            Assert.assertNotNull(row[4]);
            Assert.assertEquals("MY_TURN", row[4].toString());
        }
    }
}
