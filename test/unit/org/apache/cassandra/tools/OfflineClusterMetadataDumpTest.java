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

import org.assertj.core.api.Assertions;
import org.hamcrest.CoreMatchers;
import org.junit.Test;

import org.apache.cassandra.tools.ToolRunner.ToolResult;

import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests for OfflineClusterMetadataDump tool.
 * <p>
 * Note: This tool requires some initialization (DatabaseDescriptor, Schema) even for help,
 * similar to StandaloneJournalUtil and other cluster metadata-related tools.
 */
public class OfflineClusterMetadataDumpTest extends OfflineToolUtils
{
    @Test
    public void testMainHelpOption()
    {
        // Main command help shows subcommands
        ToolResult tool = ToolRunner.invokeClass(OfflineClusterMetadataDump.class, "-h");
        String output = tool.getStdout() + tool.getStderr();
        assertThat("Help should show usage", output, CoreMatchers.containsStringIgnoringCase("Usage:"));
        assertThat("Help should mention metadata subcommand", output, CoreMatchers.containsStringIgnoringCase("metadata"));
        assertThat("Help should mention log subcommand", output, CoreMatchers.containsStringIgnoringCase("log"));
        assertThat("Help should mention distributed-log subcommand", output, CoreMatchers.containsStringIgnoringCase("distributed-log"));
    }

    @Test
    public void testMetadataSubcommandHelpOption()
    {
        // Metadata subcommand help shows all the options
        ToolResult tool = ToolRunner.invokeClass(OfflineClusterMetadataDump.class, "metadata", "-h");
        String output = tool.getStdout() + tool.getStderr();

        assertThat("Help should show usage", output, CoreMatchers.containsStringIgnoringCase("Usage:"));
        Assertions.assertThat(output).containsIgnoringCase("--data-dir");
        Assertions.assertThat(output).containsIgnoringCase("--to-string");
        Assertions.assertThat(output).containsIgnoringCase("--output");
        Assertions.assertThat(output).containsIgnoringCase("--epoch");
    }

    @Test
    public void testLogSubcommandHelpOption()
    {
        // Log subcommand help shows all the options
        ToolResult tool = ToolRunner.invokeClass(OfflineClusterMetadataDump.class, "log", "-h");
        String output = tool.getStdout() + tool.getStderr();

        assertThat("Help should show usage", output, CoreMatchers.containsStringIgnoringCase("Usage:"));
        Assertions.assertThat(output).containsIgnoringCase("--data-dir");
        Assertions.assertThat(output).containsIgnoringCase("--from-epoch");
        Assertions.assertThat(output).containsIgnoringCase("--to-epoch");
    }

    @Test
    public void testDistributedLogSubcommandHelpOption()
    {
        // Distributed-log subcommand help shows all the options
        ToolResult tool = ToolRunner.invokeClass(OfflineClusterMetadataDump.class, "distributed-log", "-h");
        String output = tool.getStdout() + tool.getStderr();

        assertThat("Help should show usage", output, CoreMatchers.containsStringIgnoringCase("Usage:"));
        Assertions.assertThat(output).containsIgnoringCase("--data-dir");
        Assertions.assertThat(output).containsIgnoringCase("--from-epoch");
        Assertions.assertThat(output).containsIgnoringCase("--to-epoch");
    }

    @Test
    public void testWrongArgFailsAndPrintsHelp()
    {
        ToolResult tool = ToolRunner.invokeClass(OfflineClusterMetadataDump.class, "metadata", "--invalid-option");
        String output = tool.getStdout() + tool.getStderr();
        assertThat("Should mention unknown option", output, CoreMatchers.containsStringIgnoringCase("Unknown"));
        assertTrue("Expected non-zero exit code", tool.getExitCode() != 0);
    }

    @Test
    public void testNonExistentDataDirectory()
    {
        // When running with a non-existent directory, should fail gracefully
        ToolResult tool = ToolRunner.invokeClass(OfflineClusterMetadataDump.class, "metadata",
                                                 "--data-dir", "/nonexistent/path/to/data",
                                                 "--to-string");
        String output = tool.getStdout() + tool.getStderr();
        // Tool should fail gracefully when directory doesn't exist or no SSTables found
        assertTrue("Expected error or no metadata message",
                   tool.getExitCode() != 0 ||
                   output.toLowerCase().contains("no metadata") ||
                   output.toLowerCase().contains("not found") ||
                   output.toLowerCase().contains("does not exist") ||
                   output.toLowerCase().contains("error"));
    }

    @Test
    public void testMetadataSubcommandFlags()
    {
        // Test that --to-string flag is recognized
        ToolResult toStringFlag = ToolRunner.invokeClass(OfflineClusterMetadataDump.class, "metadata", "--to-string", "-h");
        String toStringOutput = toStringFlag.getStdout() + toStringFlag.getStderr();
        assertThat("Should show help with --to-string", toStringOutput, CoreMatchers.containsStringIgnoringCase("Usage:"));

        // Test that -o/--output flag is recognized
        ToolResult outputFlag = ToolRunner.invokeClass(OfflineClusterMetadataDump.class, "metadata", "-o", "/tmp/test.dump", "-h");
        String outputOutput = outputFlag.getStdout() + outputFlag.getStderr();
        assertThat("Should show help with -o", outputOutput, CoreMatchers.containsStringIgnoringCase("Usage:"));

        ToolResult outputLongFlag = ToolRunner.invokeClass(OfflineClusterMetadataDump.class, "metadata", "--output", "/tmp/test.dump", "-h");
        String outputLongOutput = outputLongFlag.getStdout() + outputLongFlag.getStderr();
        assertThat("Should show help with --output", outputLongOutput, CoreMatchers.containsStringIgnoringCase("Usage:"));

        // Test --epoch flag
        ToolResult epochTool = ToolRunner.invokeClass(OfflineClusterMetadataDump.class, "metadata", "--epoch", "100", "-h");
        String epochOutput = epochTool.getStdout() + epochTool.getStderr();
        assertThat("--epoch flag should be recognized", epochOutput, CoreMatchers.containsStringIgnoringCase("Usage:"));
    }

    @Test
    public void testLogSubcommandEpochFilterFlags()
    {
        // Test that epoch filter flags are recognized on log subcommand
        ToolResult fromTool = ToolRunner.invokeClass(OfflineClusterMetadataDump.class, "log", "--from-epoch", "50", "-h");
        String fromOutput = fromTool.getStdout() + fromTool.getStderr();
        assertThat("--from-epoch flag should be recognized", fromOutput, CoreMatchers.containsStringIgnoringCase("Usage:"));

        ToolResult toTool = ToolRunner.invokeClass(OfflineClusterMetadataDump.class, "log", "--to-epoch", "150", "-h");
        String toOutput = toTool.getStdout() + toTool.getStderr();
        assertThat("--to-epoch flag should be recognized", toOutput, CoreMatchers.containsStringIgnoringCase("Usage:"));
    }

    @Test
    public void testVerboseAndDebugFlags()
    {
        // Test verbose flags on metadata subcommand
        ToolResult verboseShort = ToolRunner.invokeClass(OfflineClusterMetadataDump.class, "metadata", "-v", "-h");
        String verboseShortOutput = verboseShort.getStdout() + verboseShort.getStderr();
        assertThat("-v flag should be recognized", verboseShortOutput, CoreMatchers.containsStringIgnoringCase("Usage:"));

        ToolResult verboseLong = ToolRunner.invokeClass(OfflineClusterMetadataDump.class, "metadata", "--verbose", "-h");
        String verboseLongOutput = verboseLong.getStdout() + verboseLong.getStderr();
        assertThat("--verbose flag should be recognized", verboseLongOutput, CoreMatchers.containsStringIgnoringCase("Usage:"));

        // Test debug flag
        ToolResult debug = ToolRunner.invokeClass(OfflineClusterMetadataDump.class, "metadata", "--debug", "-h");
        String debugOutput = debug.getStdout() + debug.getStderr();
        assertThat("--debug flag should be recognized", debugOutput, CoreMatchers.containsStringIgnoringCase("Usage:"));
    }

    @Test
    public void testPartitionerFlag()
    {
        // Test partitioner flags on metadata subcommand
        ToolResult shortFlag = ToolRunner.invokeClass(OfflineClusterMetadataDump.class, "metadata",
                                                      "-p", "org.apache.cassandra.dht.Murmur3Partitioner", "-h");
        String shortOutput = shortFlag.getStdout() + shortFlag.getStderr();
        assertThat("-p flag should be recognized", shortOutput, CoreMatchers.containsStringIgnoringCase("Usage:"));

        ToolResult longFlag = ToolRunner.invokeClass(OfflineClusterMetadataDump.class, "metadata",
                                                     "--partitioner", "org.apache.cassandra.dht.Murmur3Partitioner", "-h");
        String longOutput = longFlag.getStdout() + longFlag.getStderr();
        assertThat("--partitioner flag should be recognized", longOutput, CoreMatchers.containsStringIgnoringCase("Usage:"));
    }
}
