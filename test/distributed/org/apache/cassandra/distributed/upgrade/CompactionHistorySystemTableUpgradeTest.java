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

import com.google.common.collect.Lists;
import com.vdurmont.semver4j.Semver;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.tools.ToolRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.apache.cassandra.tools.ToolRunner.invokeNodetoolJvmDtest;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class CompactionHistorySystemTableUpgradeTest extends UpgradeTestBase
{
    @Parameterized.Parameter
    public Semver version;

    @Parameterized.Parameters()
    public static ArrayList<Semver> versions()
    {
      return Lists.newArrayList(v30, v3X, v40, v41);
    }

  @Test
    public void compactionHistorySystemTableTest() throws Throwable
    {
        new TestCase()
            .nodes(2)
            .nodesToUpgrade(1, 2)
            .upgradesToCurrentFrom(version).setup((cluster) -> {
              //create table
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tb (" +
                "pk text PRIMARY KEY," +
                "c1 text," +
                "c2 int," +
                "c3 int)");
             // disable auto compaction
            cluster.stream().forEach(node -> node.nodetool("disableautocompaction"));
            // generate sstables
            for (int i = 0; i != 10; ++ i)
            {
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tb (pk, c1, c2, c3) VALUES ('pk" + i + "', 'c1" + i + "', " + i + ',' + i + ')'  , ConsistencyLevel.ALL);
                cluster.stream().forEach(node -> node.flush(KEYSPACE));
            }
            // force compact
            cluster.stream().forEach(node -> node.forceCompact(KEYSPACE, "tb"));
        }).runAfterClusterUpgrade((cluster) -> {
          String query = "SELECT compaction_type, keyspace_name, columnfamily_name FROM system.compaction_history where keyspace_name = '" + KEYSPACE + "' AND columnfamily_name = 'tb' LIMIT 1 ALLOW FILTERING";
          Object[][] expectedResult = {
              row(null, KEYSPACE, "tb")
          };
          assertRows(cluster.coordinator(1).execute(query, ConsistencyLevel.ALL), expectedResult);
          assertRows(cluster.coordinator(2).execute(query, ConsistencyLevel.ALL), expectedResult);

          ToolRunner.ToolResult toolHistory = invokeNodetoolJvmDtest(cluster.get(1), "compactionhistory");
          toolHistory.assertOnCleanExit();
          String stdout = toolHistory.getStdout();
          String[] resultArray = stdout.split("\n");
          assertTrue(Arrays.stream(resultArray)
              .anyMatch(result -> result.contains(OperationType.UNKNOWN.type)
                  && result.contains(KEYSPACE)
                  && result.contains("tb")));
      })
            .run();
    }
}
