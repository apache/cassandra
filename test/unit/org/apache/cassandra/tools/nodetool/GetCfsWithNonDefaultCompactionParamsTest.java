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

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CompactionStrategyMigrationOptions;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.compaction.CompactionStrategyMigrationManager;
import org.apache.cassandra.tools.ToolRunner;

import static org.assertj.core.api.Assertions.assertThat;

public class GetCfsWithNonDefaultCompactionParamsTest extends CQLTester
{
    @BeforeClass
    public static void setup() throws Exception
    {
        CQLTester.setUpClass();
        CQLTester.requireAuthentication();
        startJMXServer();
    }

    @Before
    public void init()
    {
        CompactionStrategyMigrationManager.instance.setup(DatabaseDescriptor.getCompactionStrategyMigrationOptions());
    }

    @Test
    public void testNoUserTables()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("getcfswithnondefaultcompactionparams");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isEqualTo("User tables with non-default compaction params:\n\n");
    }

    @Test
    public void testWithUserTables()
    {
        createTable(KEYSPACE, "CREATE TABLE %s (a INT PRIMARY KEY, b TEXT);", "default1");
        createTable(KEYSPACE, "CREATE TABLE %s (a INT PRIMARY KEY, b TEXT) WITH compaction = {'class': 'LeveledCompactionStrategy'};", "default2");
        createTable(KEYSPACE, "CREATE TABLE %s (a INT PRIMARY KEY, b TEXT) WITH compaction = {'class': 'TimeWindowCompactionStrategy'};", "default3");
        createTable(KEYSPACE, "CREATE TABLE %s (a INT PRIMARY KEY, b TEXT) WITH compaction = {'class': 'LeveledCompactionStrategy', 'min_threshold': 3};", "nondefault1");
        createTable(KEYSPACE, "CREATE TABLE %s (a INT PRIMARY KEY, b TEXT) WITH compaction = {'class': 'SizeTieredCompactionStrategy', 'min_threshold': 3};", "nondefault2");
        createTable(KEYSPACE, "CREATE TABLE %s (a INT PRIMARY KEY, b TEXT) WITH compaction = {'class': 'DateTieredCompactionStrategy'};", "nondefault3");

        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("getcfswithnondefaultcompactionparams");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isEqualTo("User tables with non-default compaction params:\n" +
                                               "Table: cql_test_keyspace.nondefault1, option: CompactionParams{class=org.apache.cassandra.db.compaction.LeveledCompactionStrategy, options={min_threshold=3, max_threshold=32}}\n" +
                                               "Table: cql_test_keyspace.nondefault2, option: CompactionParams{class=org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy, options={min_threshold=3, max_threshold=32}}\n" +
                                               "Table: cql_test_keyspace.nondefault3, option: CompactionParams{class=org.apache.cassandra.db.compaction.DateTieredCompactionStrategy, options={min_threshold=4, max_threshold=32}}\n\n");
    }
}
