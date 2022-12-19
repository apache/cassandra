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

import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.tools.ToolRunner;
import org.junit.Test;

import java.util.Arrays;

import static org.apache.cassandra.db.compaction.CompactionHistoryTabularData.UNKNOWN_TYPE;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.apache.cassandra.tools.ToolRunner.invokeNodetool;
import static org.junit.Assert.assertTrue;

public class CompactionHistorySystemTableUpgradeTest extends UpgradeTestBase
{
    @Test
    public void compactionHistorySystemTableTest() throws Throwable
    {
        new TestCase()
            .nodes(2)
            .nodesToUpgrade(2)
            .upgradesToCurrentFrom(v40).setup((cluster) -> {
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
        }).runAfterNodeUpgrade((cluster, node) -> {
          String query = "SELECT compaction_type FROM system.compaction_history where keyspace_name = '" + KEYSPACE + "' AND columnfamily_name = 'tb' ALLOW FILTERING";
          Object[][] expectedResult = {
              row(null)
          };
          assertRows(cluster.coordinator(1).execute(query, ConsistencyLevel.ALL), expectedResult);
          assertRows(cluster.coordinator(2).execute(query, ConsistencyLevel.ALL), expectedResult);

          ToolRunner.ToolResult toolHistory = invokeNodetool("compactionhistory");
          toolHistory.assertOnCleanExit();
          String stdout = toolHistory.getStdout();
          String [] resultArray = stdout.split("\n");
          assertTrue(Arrays.stream(resultArray)
              .anyMatch(result -> result.contains(UNKNOWN_TYPE)
                  && result.contains(KEYSPACE)
                  && result.contains("tb")));
      })
            .run();
    }
}
