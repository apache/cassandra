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

package org.apache.cassandra.distributed.test.fql;

import com.google.common.collect.Sets;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.tools.ToolRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

public class FqlTombstoneHandlingTest extends TestBaseImpl
{
    private static Cluster CLUSTER;

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        CLUSTER = init(Cluster.build(1).withConfig(updater -> updater.with(NETWORK, GOSSIP, NATIVE_PROTOCOL)).start());
    }
    
    @Test
    public void testNullCellBindingInBatch()
    {
        String tableName = "null_as_tombstone_in_batch";
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s." + tableName + " (k int, c int, s set<int>, primary key (k, c))"));
        CLUSTER.get(1).nodetool("enablefullquerylog", "--path", temporaryFolder.getRoot().getAbsolutePath());
        String insertTemplate = withKeyspace("INSERT INTO %s." + tableName + " (k, c, s) VALUES ( ?, ?, ?) USING TIMESTAMP 2");
        String select = withKeyspace("SELECT * FROM %s." + tableName + " WHERE k = 0 AND c = 0");

        com.datastax.driver.core.Cluster.Builder builder1 =com.datastax.driver.core.Cluster.builder().addContactPoint("127.0.0.1");

        // Use the driver to write this initial row, since otherwise we won't hit the dispatcher
        try (com.datastax.driver.core.Cluster cluster1 = builder1.build(); Session session1 = cluster1.connect())
        {
            BatchStatement batch = new BatchStatement(BatchStatement.Type.UNLOGGED);
            PreparedStatement preparedWrite = session1.prepare(insertTemplate);
            batch.add(preparedWrite.bind(0, 0, null));
            session1.execute(batch);
        }

        CLUSTER.get(1).nodetool("disablefullquerylog");

        // The dump should contain a null entry for our tombstone
        ToolRunner.ToolResult runner = ToolRunner.invokeClass("org.apache.cassandra.fqltool.FullQueryLogTool", 
                                                              "dump",
                                                              "--",
                                                              temporaryFolder.getRoot().getAbsolutePath());
        assertTrue(runner.getStdout().contains(insertTemplate));
        assertEquals(0, runner.getExitCode());

        Object[][] preReplayResult = CLUSTER.get(1).executeInternal(select);
        assertRows(preReplayResult, row(0, 0, null));

        // Make sure the row no longer exists after truncate...
        CLUSTER.get(1).executeInternal(withKeyspace("TRUNCATE %s." + tableName));
        assertRows(CLUSTER.get(1).executeInternal(select));

        // ...insert a new row with an actual value for the set at an earlier timestamp...
        CLUSTER.get(1).executeInternal(withKeyspace("INSERT INTO %s." + tableName + " (k, c, s) VALUES ( ?, ?, ?) USING TIMESTAMP 1"), 0, 0, Sets.newHashSet(1));
        assertRows(CLUSTER.get(1).executeInternal(select), row(0, 0, Sets.newHashSet(1)));

        runner = ToolRunner.invokeClass("org.apache.cassandra.fqltool.FullQueryLogTool",
                                        "replay",
                                        "--keyspace", KEYSPACE,
                                        "--target", "127.0.0.1",
                                        "--", temporaryFolder.getRoot().getAbsolutePath());
        assertEquals(0, runner.getExitCode());

        // ...then ensure the replayed row deletes the one we wrote before replay.
        Object[][] postReplayResult = CLUSTER.get(1).executeInternal(select);
        assertRows(postReplayResult, preReplayResult);
    }

    @AfterClass
    public static void afterClass()
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }
}
