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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.tools.ToolRunner.ToolResult;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.db.compaction.CompactionHistoryTabularData.COMPACTION_TYPE_PROPERTY;
import static org.apache.cassandra.tools.ToolRunner.invokeNodetool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class CompactionHistoryTest extends CQLTester
{
    @Parameter
    public List<String> cmd;

    @Parameter(1)
    public String compactionType;

    @Parameter(2)
    public int systemTableRecord;

    @Parameters(name = "{index}: cmd={0} compactionType={1} systemTableRecord={2}")
    public static Collection<Object[]> data()
    {
        List<Object[]> result = new ArrayList<>();
        result.add(new Object[]{ Lists.newArrayList("compact"), OperationType.MAJOR_COMPACTION.type, 1 });
        result.add(new Object[]{ Lists.newArrayList("garbagecollect"), OperationType.GARBAGE_COLLECT.type, 10 });
        result.add(new Object[]{ Lists.newArrayList("upgradesstables", "-a"), OperationType.UPGRADE_SSTABLES.type, 10 });
        return result;
    }

    @BeforeClass
    public static void setup() throws Exception
    {
        requireNetwork();
        startJMXServer();
    }

    @Test
    public void testCompactionProperties() throws Throwable
    {
        createTable("CREATE TABLE %s (id text, value text, PRIMARY KEY ((id)))");
        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(currentTable());
        cfs.disableAutoCompaction();
        // write SSTables for the specific key
        for (int i = 0; i < 10; i++)
        {
            for (int j = 0; j < 3; j++) // write more than once to ensure overlap for UCS
                execute("INSERT INTO %s (id, value) VALUES (?, ?)", "key" + i + j, "value" + i + j);
            flush(keyspace());
        }

        int expectedSSTablesCount = 10;
        assertThat(cfs.getTracker().getView().liveSSTables()).hasSize(expectedSSTablesCount);

        ImmutableList.Builder<String> builder = ImmutableList.builder();
        List<String> cmds = builder.addAll(cmd).add(keyspace()).add(currentTable()).build();
        compactionHistoryResultVerify(keyspace(), currentTable(), ImmutableMap.of(COMPACTION_TYPE_PROPERTY, compactionType), cmds);

        String cql = "select keyspace_name,columnfamily_name,compaction_properties  from system." + SystemKeyspace.COMPACTION_HISTORY +
                     " where keyspace_name = '" + keyspace() + "' AND columnfamily_name = '" + currentTable() + "' ALLOW FILTERING";
        Object[][] objects = new Object[systemTableRecord][];
        for (int i = 0; i != systemTableRecord; ++i)
        {
            objects[i] = row(keyspace(), currentTable(), ImmutableMap.of(COMPACTION_TYPE_PROPERTY, compactionType));
        }
        assertRows(execute(cql), objects);
    }

    private void compactionHistoryResultVerify(String keyspace, String table, Map<String, String> properties, List<String> cmds)
    {
        ToolResult toolCompact = invokeNodetool(cmds);
        toolCompact.assertOnCleanExit();

        ToolResult toolHistory = invokeNodetool("compactionhistory");
        toolHistory.assertOnCleanExit();
        assertCompactionHistoryOutPut(toolHistory, keyspace, table, properties);
    }

    public static void assertCompactionHistoryOutPut(ToolResult toolHistory, String keyspace, String table, Map<String, String> properties)
    {
        String stdout = toolHistory.getStdout();
        String[] resultArray = stdout.split(System.lineSeparator());
        assertTrue(Arrays.stream(resultArray)
                         .anyMatch(result -> result.contains('{' + FBUtilities.toString(properties) + '}')
                                             && result.contains(keyspace)
                                             && result.contains(table)));
    }
}
