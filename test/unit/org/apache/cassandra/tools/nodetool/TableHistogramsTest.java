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

import org.apache.commons.lang3.StringUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.auth.AuthKeyspace;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SchemaKeyspace;
import org.apache.cassandra.schema.SystemDistributedKeyspace;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.tools.ToolRunner;
import org.apache.cassandra.tracing.TraceKeyspace;

import static org.apache.cassandra.tools.ToolRunner.invokeNodetool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNotEquals;

/**
 * @see TableHistograms
 */
public class TableHistogramsTest extends CQLTester
{
    private static final String INFO_ROW = "Percentile      Read Latency     Write Latency          SSTables    Partition Size        Cell Count";
    private final int ALL_TABLE_SIZE = SystemKeyspace.TABLE_NAMES.size() +
                                       SchemaKeyspace.metadata().tables.size() +
                                       TraceKeyspace.TABLE_NAMES.size() +
                                       AuthKeyspace.TABLE_NAMES.size() +
                                       SystemDistributedKeyspace.TABLE_NAMES.size() +
                                       AccordKeyspace.tables().size() +
                                       1; // DistributedMetadataLogKeyspace contains a single table

    @BeforeClass
    public static void setup() throws Exception
    {
        requireNetwork();
        startJMXServer();
    }

    @Test
    public void testWithNoTableSpecified()
    {
        ToolRunner.ToolResult tool = invokeNodetool("tablehistograms");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).contains(SchemaConstants.SYSTEM_KEYSPACE_NAME);
        assertThat(tool.getStdout()).contains(SchemaConstants.SCHEMA_KEYSPACE_NAME);
        assertThat(tool.getStdout()).contains(SchemaConstants.TRACE_KEYSPACE_NAME);
        assertThat(tool.getStdout()).contains(SchemaConstants.AUTH_KEYSPACE_NAME);
        assertThat(tool.getStdout()).contains(SchemaConstants.DISTRIBUTED_KEYSPACE_NAME);
        assertThat(StringUtils.countMatches(tool.getStdout(), INFO_ROW)).isEqualTo(ALL_TABLE_SIZE);
    }

    @Test
    public void testWithOneTableSpecified()
    {
        //format 1 : ks.table
        ToolRunner.ToolResult tool = invokeNodetool("tablehistograms", "system.local");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).contains(SchemaConstants.SYSTEM_KEYSPACE_NAME);
        assertThat(StringUtils.countMatches(tool.getStdout(), INFO_ROW)).isEqualTo(1);

        // format 2 : ks table
        tool = invokeNodetool("tablehistograms", "system", "local");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).contains(SchemaConstants.SYSTEM_KEYSPACE_NAME);
        assertThat(StringUtils.countMatches(tool.getStdout(), INFO_ROW)).isEqualTo(1);
    }

    @Test
    public void testWithMoreThanOneTableSpecified()
    {
        //format 1 : ks1.tb1 ks2.tb2
        ToolRunner.ToolResult tool = invokeNodetool("tablehistograms", "system.local", "system.paxos");
        assertNotEquals(0, tool.getExitCode());
        assertThat(tool.getStdout()).contains("nodetool: tablehistograms requires <keyspace> <table> or <keyspace.table> format argument");

        // format 2 : ks1 tb1 ks2 tb2
        tool = invokeNodetool("tablehistograms", "system", "local", "system", "paxos");
        assertNotEquals(0, tool.getExitCode());
        assertThat(tool.getStdout()).contains("nodetool: Unmatched arguments from index 7: 'system', 'paxos'");

        // format 3 : ks1.tb1 ks2
        tool = invokeNodetool("tablehistograms", "system.local", "system");
        assertNotEquals(0, tool.getExitCode());
        assertThat(tool.getStdout()).contains("nodetool: tablehistograms requires <keyspace> <table> or <keyspace.table> format argument");
    }
}
