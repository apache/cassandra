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
package org.apache.cassandra.tools;

import java.util.Set;

import org.hamcrest.CoreMatchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.UpdateBuilder;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tools.ToolRunner.ToolResult;

import static org.apache.cassandra.SchemaLoader.standardCFMD;
import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_UTIL_ALLOW_TOOL_REINIT_FOR_TEST;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Class that tests tables for {@link SSTableOfflineRelevel} by populating schema and SSTables using {@link SchemaLoader}.
 * <p/>
 * Note: the complete coverage for {@link SSTableOfflineRelevel} is composed of:
 * - {@link SSTableOfflineRelevelTest}
 * - {@link SSTableOfflineRelevelOnSSTablesTest}
 */
public class SSTableOfflineRelevelOnSSTablesTest extends OfflineToolUtils
{
    private static WithProperties properties;

    @BeforeClass
    public static void setup() throws Exception
    {
        CassandraRelevantProperties.PARTITIONER.setString("org.apache.cassandra.dht.Murmur3Partitioner");
        properties = new WithProperties().set(TEST_UTIL_ALLOW_TOOL_REINIT_FOR_TEST, true);
        ServerTestUtils.prepareServerNoRegister();
        StorageService.instance.initServer();
    }

    @AfterClass
    public static void teardown() throws Exception
    {
        SchemaLoader.cleanupAndLeaveDirs();
        if (properties != null)
            properties.close();
    }

    @Test
    public void testDryRunRelevelWithSSTables()
    {
        String ks = "SSTableOfflineRelevelTestDryRun";
        String table = "dryrun_table";
        SchemaLoader.createKeyspace(ks, KeyspaceParams.simple(1), standardCFMD(ks, table));
        populateTable(ks, table, 10, 3);

        Set<SSTableReader> sstables = getSSTables(ks, table);
        assertTrue("Expected SSTables to be generated", sstables.size() >= 2);

        ToolResult tool = ToolRunner.invokeClass(SSTableOfflineRelevel.class, "--dry-run", ks, table);
        assertEquals(0, tool.getExitCode());
        assertThat(tool.getStdout(), CoreMatchers.containsStringIgnoringCase("Current leveling:"));
        assertThat(tool.getStdout(), CoreMatchers.containsStringIgnoringCase("Potential leveling:"));
        assertThat(tool.getStdout(), CoreMatchers.containsStringIgnoringCase("L0="));

        for (SSTableReader sstable : getSSTables(ks, table))
        {
            assertEquals(0, sstable.getSSTableLevel());
        }
    }

    @Test
    public void testRelevelWithSSTables()
    {
        String ks = "SSTableOfflineRelevelTestRelevel";
        String table = "relevel_table";
        SchemaLoader.createKeyspace(ks, KeyspaceParams.simple(1), standardCFMD(ks, table));
        populateTable(ks, table, 10, 3);

        Set<SSTableReader> sstables = getSSTables(ks, table);
        assertTrue("Expected SSTables to be generated", sstables.size() >= 2);

        ToolResult tool = ToolRunner.invokeClass(SSTableOfflineRelevel.class, ks, table);
        assertEquals(0, tool.getExitCode());
        assertThat(tool.getStdout(), CoreMatchers.containsStringIgnoringCase("Current leveling:"));
        assertThat(tool.getStdout(), CoreMatchers.containsStringIgnoringCase("New leveling:"));
    }

    @Test
    public void testRelevelMultipleSSTablesOverflowToL0()
    {
        String ks = "SSTableOfflineRelevelTestOverflow";
        String table = "overflow_table";
        SchemaLoader.createKeyspace(ks, KeyspaceParams.simple(1), standardCFMD(ks, table));
        populateTable(ks, table, 50, 10);

        ToolResult tool = ToolRunner.invokeClass(SSTableOfflineRelevel.class, ks, table);
        assertEquals(0, tool.getExitCode());
        assertThat(tool.getStdout(), CoreMatchers.containsStringIgnoringCase("New leveling:"));
        assertThat(tool.getStdout(), CoreMatchers.containsStringIgnoringCase("L0="));
    }

    private static void populateTable(String ksName, String tableName, int partitions, int sstablesCount)
    {
        ColumnFamilyStore cfs = Keyspace.open(ksName).getColumnFamilyStore(tableName);
        cfs.disableAutoCompaction();

        int partitionsPerSSTable = Math.max(1, partitions / sstablesCount);
        int partitionIdx = 0;
        for (int s = 0; s < sstablesCount; s++)
        {
            for (int i = 0; i < partitionsPerSSTable; i++)
            {
                UpdateBuilder.create(cfs.metadata(), "key_" + partitionIdx++)
                             .newRow("c1").add("val", "value_" + partitionIdx)
                             .apply();
            }
            org.apache.cassandra.Util.flush(cfs);
        }
    }

    private static Set<SSTableReader> getSSTables(String ksName, String tableName)
    {
        ColumnFamilyStore cfs = Keyspace.open(ksName).getColumnFamilyStore(tableName);
        return cfs.getLiveSSTables();
    }
}
