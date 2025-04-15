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
import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import com.google.common.collect.ImmutableMap;
import org.apache.cassandra.metrics.AutoRepairMetricsManager;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.repair.AutoRepairConfig;
import org.apache.cassandra.repair.AutoRepairKeyspace;
import org.apache.cassandra.repair.AutoRepairV2;
import org.apache.cassandra.service.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.cassandra.schema.SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AutoRepairRunRepairOnBehalfTest extends TestBaseImpl
{
    static final Logger logger = LoggerFactory.getLogger(AutoRepairRunRepairOnBehalfTest.class);
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
                                                "parallel_repair_count_in_group", "1",
                                                "parallel_repair_percentage_in_group", "0",
                                                "min_repair_interval_in_hours", "-1"),
                                        AutoRepairConfig.RepairType.incremental.toString(),
                                        ImmutableMap.of(
                                                "initial_scheduler_delay_in_sec", "5",
                                                "enabled", "true",
                                                "parallel_repair_count_in_group", "1",
                                                "parallel_repair_percentage_in_group", "0",
                                                "min_repair_interval_in_hours", "-1"))))
                .set("auto_repair.enabled", "true")
                .set("auto_repair.repair_check_interval_in_sec", "10")
                .set("auto_repair.repair_task_min_duration", "0s")).start();

        cluster.schemaChange("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};");
        cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck text, v1 int, v2 int, PRIMARY KEY (pk, ck)) WITH read_repair='NONE'"));
    }

    @Test
    public void testRepairOnBehalfOfSomebody()
    {
        // ensure there was no history of previous repair runs through the scheduler
        Object[][] rows = cluster.coordinator(1).execute(String.format("SELECT repair_type, host_id, repair_start_ts, repair_finish_ts, repair_turn FROM %s.%s", AUTO_REPAIR_KEYSPACE_NAME, AutoRepairKeyspace.AUTO_REPAIR_HISTORY_V2), ConsistencyLevel.QUORUM);
        assertEquals(0, rows.length);

        InetSocketAddress node2Address = cluster.get(2).broadcastAddress();
        cluster.get(1).runOnInstance(() -> {
            try
            {
                DatabaseDescriptor.getAutoRepairConfig().setRepairTokenRangesForNode(AutoRepairConfig.RepairType.full, InetAddressAndPort.getByAddress(node2Address));
                DatabaseDescriptor.getAutoRepairConfig().setRepairTokenRangesForNode(AutoRepairConfig.RepairType.incremental, InetAddressAndPort.getByAddress(node2Address));
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        });

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

        List<AutoRepairConfig.RepairType> repairTypes =
                Arrays.asList(AutoRepairConfig.RepairType.full, AutoRepairConfig.RepairType.incremental);

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

        List<String> logsNode1 = cluster.get(1).logs().grep("Parent Token Left side").getResult();
        List<String> logsNode2 = cluster.get(2).logs().grep("Parent Token Left side").getResult();
        String tokensNode1 = buildExpectedLogMessageToShowcaseTheTokensBeingUsed(1);
        String tokensNode2 = buildExpectedLogMessageToShowcaseTheTokensBeingUsed(2);

        // node1's logs
        for (String log : logsNode1)
        {
            // node1's logs should not contain the tokens for node1 because we told node1 to repair on behalf of node2
            assertFalse("Token " + tokensNode1 + " found in logs" + logsNode1, log.contains(tokensNode1));
            // node1's logs should contain the tokens for node2 because we told node1 to repair on behalf of node2
            assertTrue("Token " + log.contains(tokensNode2) + " not found in logs", log.contains(tokensNode2));
        }

        // node2's logs
        for (String log : logsNode2)
        {
            // node2's logs should contain the tokens for node2 only because did not specify anything for node2; hence,
            // node2 should repair its own token ranges
            assertTrue("Token " + log.contains(tokensNode2) + " not found in logs", log.contains(tokensNode2));
        }
    }

    private String buildExpectedLogMessageToShowcaseTheTokensBeingUsed(int nodeId)
    {
        @SuppressWarnings("unchecked")
        List<Long> tokens = cluster.get(nodeId).callOnInstance(() -> {
            try
            {
                List<Long> allTokens = new ArrayList<>();
                Collection<Range<Token>> tokenRanges = StorageService.instance.getPrimaryRanges(AUTO_REPAIR_KEYSPACE_NAME);
                for (Range<Token> token : tokenRanges)
                {
                    Murmur3Partitioner.LongToken l = (Murmur3Partitioner.LongToken) (token.left);
                    Murmur3Partitioner.LongToken r = (Murmur3Partitioner.LongToken) (token.right);
                    long left = (Long) l.getTokenValue();
                    long right = (Long) r.getTokenValue();
                    allTokens.add(left);
                    allTokens.add(right);
                }
                return allTokens;
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        });
        assertEquals(2, tokens.size());
        return String.format("Parent Token Left side %d, right side %d", tokens.get(0), tokens.get(1));
    }
}
