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
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.ResultSet;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.repair.ValidationProgress;
import org.apache.cassandra.repair.messages.RepairMessage;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.StorageService;

@RunWith(Parameterized.class)
public class RepairTableTest extends DistributedTestBase implements Serializable
{
    private static final Logger logger = LoggerFactory.getLogger(RepairTableTest.class);

    private static Cluster CLUSTER;

    private final RepairParallelism parallelism;
    private final boolean incremental;

    public RepairTableTest(RepairParallelism parallelism, boolean incremental)
    {
        this.parallelism = parallelism;
        this.incremental = incremental;
    }

    @Parameterized.Parameters(name = "{0}/{1}")
    public static Collection<Object[]> tests()
    {
        List<Object[]> tests = new ArrayList<>();
        for (RepairParallelism parallelism : RepairParallelism.values())
            for (Boolean incremental : Arrays.asList(Boolean.TRUE, Boolean.FALSE))
                tests.add(new Object[]{ parallelism, incremental });
        return tests;
    }

    @BeforeClass
    public static void setupCluster() throws IOException
    {
        // streaming requires networking ATM
        // streaming also requires gossip or isn't setup properly
        CLUSTER = init(Cluster.build(2)
                              .withConfig(c -> c.with(Feature.NETWORK)
                                                .with(Feature.GOSSIP))
                              .start());
    }

    @AfterClass
    public static void teardownCluster()
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }

    @Before
    public void cleanupState()
    {
        for (int i = 1; i <= CLUSTER.size(); i++)
            CLUSTER.get(i).runOnInstance(() -> MessagingService.instance().inboundSink.clear());
    }

    @Test(timeout = 10 * 60 * 1000)
    public void validationTracking() throws IOException
    {
        final int coordinator = 2;
        String tableName = parallelism.getName().toLowerCase() + "_" + incremental;
        String fqtn = KEYSPACE + "." + tableName;

        CLUSTER.schemaChange("CREATE TABLE " + fqtn + " (k INT, PRIMARY KEY (k))");

        int numRows = 20;

        for (int i = 0; i < numRows; i++)
            CLUSTER.coordinator(1)
                   .execute("INSERT INTO " + fqtn + " (k) VALUES (?)", ConsistencyLevel.ONE, i);

        // override validation msg so the test can control the progress
        for (int i = 1; i <= CLUSTER.size(); i++)
            CLUSTER.get(i).runOnInstance(() -> {
                MessagingService.instance().inboundSink.add(msg -> {
                    if (msg.verb() != Verb.VALIDATION_REQ)
                        return true;

                    RepairJobDesc desc = ((RepairMessage) msg.payload).desc;
                    ActiveRepairService.instance.consistent.local.maybeSetRepairing(desc.parentSessionId);
                    ValidationProgress progress = new ValidationProgress();
                    ActiveRepairService.instance.trackValidation(desc, progress);

                    return false;
                });
            });

        // run a repair which is expected to fail
        int repairCmd = CLUSTER.get(coordinator).callOnInstance(() -> {
            Map<String, String> args = new HashMap<String, String>()
            {{
                put(RepairOption.PARALLELISM_KEY, parallelism.getName());
                put(RepairOption.INCREMENTAL_KEY, Boolean.toString(incremental));
                put(RepairOption.COLUMNFAMILIES_KEY, tableName);
            }};
            int cmd = StorageService.instance.repairAsync(KEYSPACE, args);
            Assert.assertFalse("repair return status was 0, expected non-zero return status, 0 indicates repair not submitted", cmd == 0);
            return cmd;
        });

        //TODO current table only shows once RepairJobs are registered; this is a limitation in the current data modeling
        // really should be able to show RepairRunnable as well
        UUID repairId;
        do
        {
            // can't push filtering to C*, so need to manually filter
            ResultSet rs = CLUSTER.get(coordinator).executeQueryInternal("SELECT id, table_name FROM system_views.repairs");
            if (!rs.hasNext())
                continue;
            String cfName = rs.getString("table_name");
            if (!tableName.equals(cfName))
                continue;
            repairId = rs.getUUID("id");
            break;
        }
        while (true);

        while (true)
        {
            ResultSet rs = CLUSTER.get(coordinator).executeQueryInternal("SELECT * FROM system_views.repairs WHERE id=?", repairId);
            if (rs.size() != 3) // wait for the repair jobs to start
                continue;

            while (rs.hasNext())
            {
                String state = rs.getString("state");
                logger.info("state: {}", state);
                if (!"validating".equals(state))
                    break;
            }
            // all are validating, kawlz
        }
    }
}
