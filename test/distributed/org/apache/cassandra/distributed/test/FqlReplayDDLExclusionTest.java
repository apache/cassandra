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

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.datastax.driver.core.Session;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.QueryResults;
import org.apache.cassandra.tools.ToolRunner;
import org.apache.cassandra.tools.ToolRunner.ToolResult;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FqlReplayDDLExclusionTest extends TestBaseImpl
{
    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Ignore
    @Test
    public void test() throws Throwable
    {
        try (final Cluster cluster = init(builder().withNodes(1)
                                                   .withConfig(updater -> updater.with(NETWORK, GOSSIP, NATIVE_PROTOCOL))
                                                   .start()))
        {
            final IInvokableInstance node = cluster.get(1);

            // using driver path is important because dtest API and query execution does not invoke code
            // in Cassandra where events are propagated to logger
            try (com.datastax.driver.core.Cluster c = com.datastax.driver.core.Cluster.builder().addContactPoint("127.0.0.1").build();
                 Session s = c.connect())
            {
                s.execute("CREATE KEYSPACE fql_ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");

                node.nodetool("enablefullquerylog", "--path", temporaryFolder.getRoot().getAbsolutePath());

                s.execute("CREATE TABLE fql_ks.fql_table (id int primary key);");
                s.execute("INSERT INTO fql_ks.fql_table (id) VALUES (1)");

                node.nodetool("disablefullquerylog");

                // here we are dropping and we expect that ddl replay will reconstruct it

                node.executeInternal("DROP TABLE fql_ks.fql_table;");

                // without --replay-ddl-statements, the replay will fail on insert because underlying table is not there
                final ToolResult negativeRunner = ToolRunner.invokeClass("org.apache.cassandra.fqltool.FullQueryLogTool",
                                                                         "replay",
                                                                         "--keyspace", "fql_ks",
                                                                         "--target", "127.0.0.1",
                                                                         "--", temporaryFolder.getRoot().getAbsolutePath());

                assertEquals(0, negativeRunner.getExitCode());

                try
                {
                    node.executeInternalWithResult("SELECT * from fql_ks.fql_table");
                    fail("This query should fail because we do not expect fql_ks.fql_table to be created!");
                }
                catch (final Exception ex)
                {
                    assertTrue(ex.getMessage().contains("table fql_table does not exist"));
                }

                // here we replay with --replay-ddl-statements so table will be created and insert will succeed
                final ToolResult positiveRunner = ToolRunner.invokeClass("org.apache.cassandra.fqltool.FullQueryLogTool",
                                                                         "replay",
                                                                         "--keyspace", "fql_ks",
                                                                         "--target", "127.0.0.1",
                                                                         // important
                                                                         "--replay-ddl-statements",
                                                                         "--", temporaryFolder.getRoot().getAbsolutePath());

                assertEquals(0, positiveRunner.getExitCode());

                assertRows(node.executeInternalWithResult("SELECT * from fql_ks.fql_table"), QueryResults.builder().row(1).build());
            }
        }
    }
}
