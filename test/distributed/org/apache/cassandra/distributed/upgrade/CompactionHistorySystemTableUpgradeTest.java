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
import com.google.common.collect.Lists;

import com.vdurmont.semver4j.Semver;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.tools.ToolRunner;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.ArrayList;

import static org.apache.cassandra.db.compaction.CompactionHistoryTabularData.COMPACTION_TYPE_PROPERTY;
import static org.apache.cassandra.tools.ToolRunner.invokeNodetoolJvmDtest;
import static org.apache.cassandra.tools.nodetool.CompactionHistoryTest.assertCompactionHistoryOutPut;

@RunWith(Parameterized.class)
public class CompactionHistorySystemTableUpgradeTest extends UpgradeTestBase
{
    @Parameter
    public Semver version;

    @Parameters()
    public static ArrayList<Semver> versions()
    {
        return Lists.newArrayList(v30, v3X, v40, v41);
    }

    @Test
    public void compactionHistorySystemTableTest() throws Throwable
    {
        new TestCase()
        .nodes(1)
        .nodesToUpgrade(1)
        .upgradesToCurrentFrom(version)
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
