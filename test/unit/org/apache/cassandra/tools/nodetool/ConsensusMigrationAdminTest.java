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

package org.apache.cassandra.tools.nodetool;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.service.paxos.Paxos;
import org.apache.cassandra.tools.ToolRunner;
import org.apache.cassandra.tools.ToolRunner.ToolResult;

import static org.assertj.core.api.Assertions.assertThat;

public class ConsensusMigrationAdminTest extends CQLTester
{
    private static Config.PaxosVariant originalPaxosVariant;

    @BeforeClass
    public static void setup() throws Exception
    {
        originalPaxosVariant = Paxos.getPaxosVariant();
        Paxos.setPaxosVariant(Config.PaxosVariant.v2);
        requireNetwork();
        startJMXServer();
    }

    @AfterClass
    public static void restoreConfiguration()
    {
        Paxos.setPaxosVariant(originalPaxosVariant);
    }

    @Test
    public void testBeginMigrationWithTables()
    {
        createMigratingTable();
        ToolResult tool = ToolRunner.invokeNodetool("consensus_admin", "begin-migration", keyspace(), currentTable());
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).contains("Marked requested ranges as migrating");
    }

    @Test
    public void testFinishMigrationWithTables()
    {
        createMigratingTable();
        ToolResult tool = ToolRunner.invokeNodetool("consensus_admin", "finish-migration", keyspace(), currentTable());
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).contains("Finished consensus migration")
                                    .contains("and tables [" + currentTable() + ']');
    }

    private void createMigratingTable()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int) WITH transactional_mode = 'off'");
        alterTable("ALTER TABLE %s WITH transactional_mode = 'full'");
    }
}
