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

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.tools.ToolRunner;

import static org.apache.cassandra.db.compaction.CompactionHistoryTabularData.COMPACTION_TYPE_PROPERTY;
import static org.apache.cassandra.tools.ToolRunner.invokeNodetoolJvmDtest;
import static org.apache.cassandra.tools.nodetool.CompactionHistoryTest.assertCompactionHistoryOutPut;

public class CompactionHistorySystemTableUpgradeTest extends UpgradeTestBase
{

    @Test
    public void compactionHistorySystemTableTest() throws Throwable
    {
        new TestCase()
        .nodes(1)
        .nodesToUpgrade(1)
        // all upgrades from v40 to current, excluding v50 -> v51
        .singleUpgradeToCurrentFrom(v40)
        .singleUpgradeToCurrentFrom(v41)
        .setup((cluster) -> {
            //create table
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tb (" +
                                 "pk text PRIMARY KEY," +
                                 "c1 text," +
                                 "c2 int," +
                                 "c3 int)");
            // disable auto compaction
            cluster.stream().forEach(node -> node.nodetool("disableautocompaction"));
            // generate sstables
            for (int i = 0; i != 10; ++i)
            {
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tb (pk, c1, c2, c3) VALUES ('pk" + i + "', 'c1" + i + "', " + i + ',' + i + ')', ConsistencyLevel.ALL);
                cluster.stream().forEach(node -> node.flush(KEYSPACE));
            }
            // force compact
            cluster.stream().forEach(node -> node.forceCompact(KEYSPACE, "tb"));
        }).runAfterClusterUpgrade((cluster) -> {
            // disable auto compaction at start up
            cluster.stream().forEach(node -> node.nodetool("disableautocompaction"));
            ToolRunner.ToolResult toolHistory = invokeNodetoolJvmDtest(cluster.get(1), "compactionhistory");
            toolHistory.assertOnCleanExit();
            // upgraded system.compaction_history data verify
            assertCompactionHistoryOutPut(toolHistory, KEYSPACE, "tb", ImmutableMap.of());

            // force compact
            cluster.stream().forEach(node -> node.nodetool("compact"));
            toolHistory = invokeNodetoolJvmDtest(cluster.get(1), "compactionhistory");
            toolHistory.assertOnCleanExit();
            assertCompactionHistoryOutPut(toolHistory, KEYSPACE, "tb", ImmutableMap.of(COMPACTION_TYPE_PROPERTY, OperationType.MAJOR_COMPACTION.type));
        })
        .run();
    }
}
