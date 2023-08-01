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

package org.apache.cassandra.distributed.test.cdc;

import java.util.function.Consumer;

import org.junit.Test;

import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogSegment;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertTrue;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

public class ToggleCDCOnRepairEnabledTest extends TestBaseImpl
{
    @Test
    public void testCDCOnRepairIsEnabled() throws Exception
    {
        testCDCOnRepairEnabled(true, cluster -> {
            cluster.get(2).runOnInstance(() -> {
                boolean containCDCInLog = CommitLog.instance.segmentManager
                                              .getActiveSegments()
                                              .stream()
                                              .anyMatch(s -> s.getCDCState() == CommitLogSegment.CDCState.CONTAINS);
                assertTrue("Mutation should be added to commit log when cdc_on_repair_enabled is true",
                           containCDCInLog);
            });
        });
    }

    @Test
    public void testCDCOnRepairIsDisabled() throws Exception
    {
        testCDCOnRepairEnabled(false, cluster -> {
            cluster.get(2).runOnInstance(() -> {
                boolean containCDCInLog = CommitLog.instance.segmentManager
                                              .getActiveSegments()
                                              .stream()
                                              .allMatch(s -> s.getCDCState() != CommitLogSegment.CDCState.CONTAINS);
                assertTrue("No mutation should be added to commit log when cdc_on_repair_enabled is false",
                           containCDCInLog);
            });
        });
    }

    // test helper to repair data between nodes when cdc_on_repair_enabled is on or off.
    private void testCDCOnRepairEnabled(boolean enabled, Consumer<Cluster> assertion) throws Exception
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withConfig(c -> c.set("cdc_enabled", true)
                                                             .set("cdc_on_repair_enabled", enabled)
                                                             .with(Feature.NETWORK)
                                                             .with(Feature.GOSSIP))
                                           .start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (k INT PRIMARY KEY, v INT) WITH cdc=true"));

            // Data only in node1
            cluster.get(1).executeInternal(withKeyspace("INSERT INTO %s.tbl (k, v) VALUES (1, 1)"));
            Object[][] result = cluster.get(1).executeInternal(withKeyspace("SELECT * FROM %s.tbl WHERE k = 1"));
            assertRows(result, row(1, 1));
            result = cluster.get(2).executeInternal(withKeyspace("SELECT * FROM %s.tbl WHERE k = 1"));
            assertRows(result);

            // repair
            cluster.get(1).flush(KEYSPACE);
            cluster.get(2).nodetool("repair", KEYSPACE, "tbl");

            // verify node2 now have data
            result = cluster.get(2).executeInternal(withKeyspace("SELECT * FROM %s.tbl WHERE k = 1"));
            assertRows(result, row(1, 1));

            assertion.accept(cluster);
        }
    }
}
