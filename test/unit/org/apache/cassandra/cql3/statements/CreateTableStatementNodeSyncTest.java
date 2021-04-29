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

package org.apache.cassandra.cql3.statements;

import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.datastax.driver.core.ResultSet;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.schema.KeyspaceParams;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class CreateTableStatementNodeSyncTest extends CQLTester
{
    @Parameterized.Parameters(name = "tableOptions = {0}")
    public static Set<String> tableOptions()
    {
        return ImmutableSet.of(
            "WITH nodesync = { 'enabled' : 'true', 'incremental' : 'true' }",
            "WITH nodesync = { 'enabled' : 'true' }",
            "WITH nodesync = { 'enabled' : 'false' }",
            "WITH nodesync = { 'enabled' : 'true', 'deadline_target_sec': 60 }"
        );
    }

    @Parameterized.Parameter()
    public String tableOptions;

    @BeforeClass
    public static void setup() throws Exception
    {
        SchemaLoader.createKeyspace("ks", KeyspaceParams.simple(1));
    }

    @Test
    public void dseNodesyncShouldBeIgnoredWithWarning() throws Throwable
    {
        String tableName = createTableName();

        // should not throw
        ResultSet rows = executeNet(String.format("CREATE TABLE ks.%s (k int PRIMARY KEY, v int) %s", tableName, tableOptions));

        assertTrue(rows.wasApplied());

        String warning = rows.getAllExecutionInfo().get(0).getWarnings().get(0);
        assertThat(warning, containsString("The unsupported 'nodesync' table option was ignored."));

        assertNoNodesyncTableParamater(tableName);
    }

    private void assertNoNodesyncTableParamater(String tableName) throws Throwable
    {
        ResultSet result = executeNet("DESCRIBE TABLE ks." + tableName);

        String createStatement = result.one().getString("create_statement");
        assertThat(createStatement, not(containsString("nodesync")));
    }
}
