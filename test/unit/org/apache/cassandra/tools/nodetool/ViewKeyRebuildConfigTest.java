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

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.ToolRunner;
import org.apache.cassandra.tools.ToolRunner.ToolResult;

import static org.assertj.core.api.Assertions.assertThat;

public class ViewKeyRebuildConfigTest extends CQLTester
{
    private static NodeProbe probe;

    @BeforeClass
    public static void setup() throws Exception
    {
        startJMXServer();
        probe = new NodeProbe(jmxHost, jmxPort);
    }

    @AfterClass
    public static void teardown() throws IOException
    {
        probe.close();
    }

    @Test
    public void testGetViewKeyRebuildConfig()
    {
        ToolResult tool = ToolRunner.invokeNodetool("getviewkeyrebuildconfig");
        tool.assertOnCleanExit();

        String output = tool.getStdout();
        assertThat(output).contains("ViewKeyRebuildConfig:");
        assertThat(output).contains("rebuild_on_deletion_enabled:");
        assertThat(output).contains("apply_mutations_enabled:");
        assertThat(output).contains("verbose_logging_enabled:");
        assertThat(output).contains("view_read_enabled:");
    }

    @Test
    public void testSetViewKeyRebuildConfig_RebuildOnDeletionEnabled()
    {
        // Set to true
        ToolResult tool = ToolRunner.invokeNodetool("setviewkeyrebuildconfig", "rebuild_on_deletion_enabled", "true");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).contains("View key rebuild_on_deletion_enabled set to: true");

        // Verify with get
        tool = ToolRunner.invokeNodetool("getviewkeyrebuildconfig");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).contains("rebuild_on_deletion_enabled: true");

        // Restore to false
        tool = ToolRunner.invokeNodetool("setviewkeyrebuildconfig", "rebuild_on_deletion_enabled", "false");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).contains("View key rebuild_on_deletion_enabled set to: false");

        // Verify restoration
        tool = ToolRunner.invokeNodetool("getviewkeyrebuildconfig");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).contains("rebuild_on_deletion_enabled: false");
    }

    @Test
    public void testSetViewKeyRebuildConfig_ApplyMutationsEnabled()
    {
        // Set to true
        ToolResult tool = ToolRunner.invokeNodetool("setviewkeyrebuildconfig", "apply_mutations_enabled", "true");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).contains("View key apply_mutations_enabled set to: true");

        // Verify with get
        tool = ToolRunner.invokeNodetool("getviewkeyrebuildconfig");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).contains("apply_mutations_enabled: true");

        // Restore to false
        tool = ToolRunner.invokeNodetool("setviewkeyrebuildconfig", "apply_mutations_enabled", "false");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).contains("View key apply_mutations_enabled set to: false");

        // Verify restoration
        tool = ToolRunner.invokeNodetool("getviewkeyrebuildconfig");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).contains("apply_mutations_enabled: false");
    }

    @Test
    public void testSetViewKeyRebuildConfig_VerboseLoggingEnabled()
    {
        // Set to true
        ToolResult tool = ToolRunner.invokeNodetool("setviewkeyrebuildconfig", "verbose_logging_enabled", "true");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).contains("View key verbose_logging_enabled set to: true");

        // Verify with get
        tool = ToolRunner.invokeNodetool("getviewkeyrebuildconfig");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).contains("verbose_logging_enabled: true");

        // Restore to false
        tool = ToolRunner.invokeNodetool("setviewkeyrebuildconfig", "verbose_logging_enabled", "false");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).contains("View key verbose_logging_enabled set to: false");

        // Verify restoration
        tool = ToolRunner.invokeNodetool("getviewkeyrebuildconfig");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).contains("verbose_logging_enabled: false");
    }

    @Test
    public void testSetViewKeyRebuildConfig_ViewReadEnabled()
    {
        // Set to true
        ToolResult tool = ToolRunner.invokeNodetool("setviewkeyrebuildconfig", "view_read_enabled", "true");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).contains("View key view_read_enabled set to: true");

        // Verify with get
        tool = ToolRunner.invokeNodetool("getviewkeyrebuildconfig");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).contains("view_read_enabled: true");

        // Restore to false
        tool = ToolRunner.invokeNodetool("setviewkeyrebuildconfig", "view_read_enabled", "false");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).contains("View key view_read_enabled set to: false");

        // Verify restoration
        tool = ToolRunner.invokeNodetool("getviewkeyrebuildconfig");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).contains("view_read_enabled: false");
    }

    @Test
    public void testSetViewKeyRebuildConfig_NoArgs()
    {
        ToolResult tool = ToolRunner.invokeNodetool("setviewkeyrebuildconfig");
        assertThat(tool.getExitCode()).isNotEqualTo(0);
    }

    @Test
    public void testSetViewKeyRebuildConfig_MissingValue()
    {
        ToolResult tool = ToolRunner.invokeNodetool("setviewkeyrebuildconfig", "rebuild_on_deletion_enabled");
        assertThat(tool.getExitCode()).isNotEqualTo(0);
    }

    @Test
    public void testSetViewKeyRebuildConfig_InvalidParam()
    {
        ToolResult tool = ToolRunner.invokeNodetool("setviewkeyrebuildconfig", "invalid_param", "true");
        assertThat(tool.getExitCode()).isNotEqualTo(0);
        assertThat(tool.getStdout()).contains("Unknown parameter");
    }
}
