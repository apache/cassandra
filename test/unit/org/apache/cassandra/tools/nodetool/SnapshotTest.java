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

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.tools.ToolRunner;

import static org.apache.cassandra.tools.ToolRunner.invokeNodetool;
import static org.assertj.core.api.Assertions.assertThat;

/**o
 * Tests for the {@code nodetool snapshot} command
 */
public class SnapshotTest extends CQLTester
{
    @BeforeClass
    public static void setup() throws Exception
    {
        requireNetwork();
        startJMXServer();
    }

    @Test
    public void testSnapshotAllKeyspaces()
    {
        ToolRunner.ToolResult tool = invokeNodetool("snapshot");
        tool.assertOnCleanExit();

        assertThat(tool.getExitCode()).isEqualTo(0);
        assertThat(tool.getStdout()).contains("Requested creating snapshot(s) for [all keyspaces]");
    }

    @Test
    public void testSnapshotSpecifySingleKeyspace()
    {
        ToolRunner.ToolResult tool = invokeNodetool("snapshot", "system_schema");
        tool.assertOnCleanExit();

        assertThat(tool.getExitCode()).isEqualTo(0);
        assertThat(tool.getStdout()).contains("Requested creating snapshot(s) for [system_schema]");
    }

    @Test
    public void testSnapshotSpecifyMultipleKeyspaces()
    {
        ToolRunner.ToolResult tool = invokeNodetool("snapshot", "system", "system_schema");
        tool.assertOnCleanExit();

        assertThat(tool.getExitCode()).isEqualTo(0);
        assertThat(tool.getStdout()).contains("Requested creating snapshot(s) for [system, system_schema]");
    }

    @Test
    public void testSnapshotWithName()
    {
        ToolRunner.ToolResult tool = invokeNodetool("snapshot", "-t", "custom_snapshot_name");
        tool.assertOnCleanExit();

        assertThat(tool.getExitCode()).isEqualTo(0);
        assertThat(tool.getStdout()).contains("Requested creating snapshot(s) for [all keyspaces] with snapshot name [custom_snapshot_name] and options {skipFlush=false}");
    }

    @Test
    public void testInvalidSnapshotName()
    {
        ToolRunner.ToolResult tool = invokeNodetool("snapshot", "-t", "invalid" + File.pathSeparator() + "name");
        assertThat(tool.getExitCode()).isEqualTo(2);
        assertThat(tool.getStderr()).contains("Snapshot name cannot contain " + File.pathSeparator());
    }

    @Test
    public void testSkipFlushOption()
    {
        ToolRunner.ToolResult tool = invokeNodetool("snapshot", "-t", "skip_flush", "-sf");
        tool.assertOnCleanExit();

        assertThat(tool.getExitCode()).isEqualTo(0);
        assertThat(tool.getStdout()).contains("Requested creating snapshot(s) for [all keyspaces] with snapshot name [skip_flush] and options {skipFlush=true}");
    }

    @Test
    public void testTTLOption()
    {
        ToolRunner.ToolResult tool = invokeNodetool("snapshot", "-t", "ttl", "--ttl", "5h");
        tool.assertOnCleanExit();

        assertThat(tool.getExitCode()).isEqualTo(0);
        assertThat(tool.getStdout()).contains("Requested creating snapshot(s) for [all keyspaces] with snapshot name [ttl] and options {skipFlush=false, ttl=5h}");
    }

    @Test
    public void testInvalidTTLOption()
    {
        ToolRunner.ToolResult tool = invokeNodetool("snapshot", "-t", "ttl", "--ttl", "infinity");
        assertThat(tool.getExitCode()).isEqualTo(1);
        assertThat(tool.getStdout()).contains("Invalid duration: infinity");
    }

    @Test
    public void testTableOption()
    {
        ToolRunner.ToolResult tool = invokeNodetool("snapshot", "-t", "table", "--table", "keyspaces", "system_schema");
        tool.assertOnCleanExit();

        assertThat(tool.getExitCode()).isEqualTo(0);
        assertThat(tool.getStdout()).contains("Requested creating snapshot(s) for [system_schema] with snapshot name [table] and options {skipFlush=false}");
    }

    @Test
    public void testInvalidTableWithMultipleKeyspacesOption()
    {
        ToolRunner.ToolResult tool = invokeNodetool("snapshot", "-t", "table", "--table", "keyspaces", "system", "system_schema");
        assertThat(tool.getExitCode()).isEqualTo(2);
        assertThat(tool.getStderr()).contains("When specifying the table for a snapshot, you must specify one and only one keyspace");
    }

    @Test
    public void testInvalidTableWithKeyspaceTableListOption()
    {
        ToolRunner.ToolResult tool = invokeNodetool("snapshot", "-t", "table", "--table", "keyspaces", "-kt", "ks.table");
        assertThat(tool.getExitCode()).isEqualTo(2);
        assertThat(tool.getStderr()).contains("When specifying the Keyspace table list (using -kt,--kt-list,-kc,--kc.list), you must not also specify keyspaces to snapshot");
    }

    @Test
    public void testInvalidTableWithoutKeyspaceOption()
    {
        ToolRunner.ToolResult tool = invokeNodetool("snapshot", "-t", "table", "--table", "keyspaces");
        assertThat(tool.getExitCode()).isEqualTo(2);
        assertThat(tool.getStderr()).contains("When specifying the table for a snapshot, you must specify one and only one keyspace");
    }

    @Test
    public void testKeyspaceTableListOption()
    {
        ToolRunner.ToolResult tool = invokeNodetool("snapshot", "-t", "kt_option", "-kt", "system_schema.keyspaces,system_schema.aggregates");
        tool.assertOnCleanExit();

        assertThat(tool.getExitCode()).isEqualTo(0);
        assertThat(tool.getStdout()).contains("Requested creating snapshot(s) for [system_schema.keyspaces,system_schema.aggregates] with snapshot name [kt_option] and options {skipFlush=false}");
    }

    @Test
    public void testInvalidKeyspacesWithKeyspaceTableListOptions()
    {
        ToolRunner.ToolResult tool = invokeNodetool("snapshot", "-kt", "ks.table", "ks");
        assertThat(tool.getExitCode()).isEqualTo(2);
        assertThat(tool.getStderr()).contains("When specifying the Keyspace table list (using -kt,--kt-list,-kc,--kc.list), you must not also specify keyspaces to snapshot");
    }
}
