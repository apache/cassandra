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
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.KeyspaceParams;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class CreateTableStatementCompactionStrategiesTest extends CQLTester
{
    @Parameterized.Parameters(name = "compactionStrategy = {0}")
    public static Set<String> strategies()
    {
        return ImmutableSet.of(
            "{'class': 'org.apache.cassandra.db.compaction.MemoryOnlyStrategy', 'max_threshold': '32', 'min_threshold': '4'}",
            "{'class': 'MemoryOnlyStrategy', 'max_threshold': '32', 'min_threshold': '4'}",
            "{'class': 'org.apache.cassandra.db.compaction.TieredCompactionStrategy', 'tiering_strategy': 'TimeWindowStorageStrategy', 'config': 'strategy1', 'max_tier_ages': '3600,7200'}",
            "{'class': 'TieredCompactionStrategy', 'tiering_strategy': 'TimeWindowStorageStrategy', 'config': 'strategy1', 'max_tier_ages': '3600,7200'}"
        );
    }

    @Parameterized.Parameter()
    public String compactionStrategy;

    @BeforeClass
    public static void setup() throws Exception
    {
        SchemaLoader.createKeyspace("ks", KeyspaceParams.simple(1));
    }

    @Test
    public void dseCompactionStrategyShouldBeIgnoredWithWarning() throws Throwable
    {
        String tableName = createTableName();

        // should not throw
        ResultSet rows = executeNet(String.format("CREATE TABLE ks.%s (k int PRIMARY KEY, v int) WITH " +
                                                  "compaction = %s;", tableName, compactionStrategy));

        assertTrue(rows.wasApplied());

        String warning = rows.getAllExecutionInfo().get(0).getWarnings().get(0);
        assertThat(warning, containsString("The compaction strategy parameter was overridden with the default"));

        assertDefaultCompactionStrategy(tableName);
    }

    private void assertDefaultCompactionStrategy(String tableName) throws Throwable
    {
        ResultSet result = executeNet("DESCRIBE TABLE ks." + tableName);

        String createStatement = result.one().getString("create_statement");
        assertThat(createStatement, containsString(CompactionParams.DEFAULT.klass().getCanonicalName()));
    }
}
