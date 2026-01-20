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
 * Tests for SSTableMetadataDump tool.
 * <p>
 * Note: This tool requires some initialization (DatabaseDescriptor, Schema) even for help,
 * similar to StandaloneJournalUtil and other TCM-related tools.
 */
public class SSTableMetadataDumpTest extends OfflineToolUtils
{
    @Test
    public void testMainHelpOption()
    {
        // Main command help shows subcommands
        ToolResult tool = ToolRunner.invokeClass(SSTableMetadataDump.class, "-h");
        String output = tool.getStdout() + tool.getStderr();
        assertThat("Help should show usage", output, CoreMatchers.containsStringIgnoringCase("Usage:"));
        assertThat("Help should mention dump subcommand", output, CoreMatchers.containsStringIgnoringCase("dump"));
    }

    @Test
    public void testDumpSubcommandHelpOption()
    {
        // Dump subcommand help shows all the options
        ToolResult tool = ToolRunner.invokeClass(SSTableMetadataDump.class, "dump", "-h");
        String output = tool.getStdout() + tool.getStderr();

        assertThat("Help should show usage", output, CoreMatchers.containsStringIgnoringCase("Usage:"));
        // Check for key options
        Assertions.assertThat(output).containsIgnoringCase("--epochs");
        Assertions.assertThat(output).containsIgnoringCase("--schema");
        Assertions.assertThat(output).containsIgnoringCase("--directory");
        Assertions.assertThat(output).containsIgnoringCase("--tokens");
        Assertions.assertThat(output).containsIgnoringCase("--all");
    }

    @Test
    public void testMaybeChangeDocs()
    {
        // If you added, modified options or help, please update docs if necessary
        ToolResult tool = ToolRunner.invokeClass(SSTableMetadataDump.class, "dump", "-h");
        String output = tool.getStdout() + tool.getStderr();

        // Verify key options are documented
        Assertions.assertThat(output).containsIgnoringCase("--data-dir");
        Assertions.assertThat(output).containsIgnoringCase("--output");
        Assertions.assertThat(output).containsIgnoringCase("--to-string");
        Assertions.assertThat(output).containsIgnoringCase("--text");
        Assertions.assertThat(output).containsIgnoringCase("--epochs");
        Assertions.assertThat(output).containsIgnoringCase("--schema");
        Assertions.assertThat(output).containsIgnoringCase("--directory");
        Assertions.assertThat(output).containsIgnoringCase("--tokens");
        Assertions.assertThat(output).containsIgnoringCase("--snapshots");
        Assertions.assertThat(output).containsIgnoringCase("--transformations");
        Assertions.assertThat(output).containsIgnoringCase("--all");
        Assertions.assertThat(output).containsIgnoringCase("--epoch");
        Assertions.assertThat(output).containsIgnoringCase("--from-epoch");
        Assertions.assertThat(output).containsIgnoringCase("--to-epoch");
        Assertions.assertThat(output).containsIgnoringCase("--verbose");
        Assertions.assertThat(output).containsIgnoringCase("--debug");
    }

    @Test
    public void testWrongArgFailsAndPrintsHelp()
    {
        ToolResult tool = ToolRunner.invokeClass(SSTableMetadataDump.class, "dump", "--invalid-option");
        String output = tool.getStdout() + tool.getStderr();
        assertThat("Should mention unknown option", output, CoreMatchers.containsStringIgnoringCase("Unknown"));
        assertTrue("Expected non-zero exit code", tool.getExitCode() != 0);
    }

    @Test
    public void testNonExistentDataDirectory()
    {
        // When running with a non-existent directory, should fail gracefully
        ToolResult tool = ToolRunner.invokeClass(SSTableMetadataDump.class, "dump",
                                                  "--data-dir", "/nonexistent/path/to/data",
                                                  "--all");
        String output = tool.getStdout() + tool.getStderr();
        // Tool should fail gracefully when directory doesn't exist or no SSTables found
        assertTrue("Expected error or no sstables message",
                   tool.getExitCode() != 0 ||
                   output.toLowerCase().contains("no sstables") ||
                   output.toLowerCase().contains("not found") ||
                   output.toLowerCase().contains("does not exist") ||
                   output.toLowerCase().contains("error"));
    }

    @Test
    public void testOutputModeFlags()
    {
        // Test that --to-string flag is recognized
        ToolResult toStringFlag = ToolRunner.invokeClass(SSTableMetadataDump.class, "dump", "--to-string", "-h");
        String toStringOutput = toStringFlag.getStdout() + toStringFlag.getStderr();
        assertThat("Should show help with --to-string", toStringOutput, CoreMatchers.containsStringIgnoringCase("Usage:"));

        // Test that --text flag is recognized
        ToolResult textFlag = ToolRunner.invokeClass(SSTableMetadataDump.class, "dump", "--text", "-h");
        String textOutput = textFlag.getStdout() + textFlag.getStderr();
        assertThat("Should show help with --text", textOutput, CoreMatchers.containsStringIgnoringCase("Usage:"));

        // Test that -o/--output flag is recognized
        ToolResult outputFlag = ToolRunner.invokeClass(SSTableMetadataDump.class, "dump", "-o", "/tmp/test.dump", "-h");
        String outputOutput = outputFlag.getStdout() + outputFlag.getStderr();
        assertThat("Should show help with -o", outputOutput, CoreMatchers.containsStringIgnoringCase("Usage:"));

        ToolResult outputLongFlag = ToolRunner.invokeClass(SSTableMetadataDump.class, "dump", "--output", "/tmp/test.dump", "-h");
        String outputLongOutput = outputLongFlag.getStdout() + outputLongFlag.getStderr();
        assertThat("Should show help with --output", outputLongOutput, CoreMatchers.containsStringIgnoringCase("Usage:"));
    }

    @Test
    public void testScopeFlagsRecognized()
    {
        // Test that all scope flags are recognized (combined with -h to avoid needing real data)
        String[] scopeFlags = {"--epochs", "--schema", "--directory", "--tokens", "--snapshots", "--transformations", "--all"};

        for (String flag : scopeFlags)
        {
            ToolResult tool = ToolRunner.invokeClass(SSTableMetadataDump.class, "dump", flag, "-h");
            String output = tool.getStdout() + tool.getStderr();
            assertThat("Flag " + flag + " should be recognized", output, CoreMatchers.containsStringIgnoringCase("Usage:"));
        }
    }

    @Test
    public void testEpochFilterFlags()
    {
        // Test that epoch filter flags are recognized
        ToolResult epochTool = ToolRunner.invokeClass(SSTableMetadataDump.class, "dump", "--epoch", "100", "-h");
        String epochOutput = epochTool.getStdout() + epochTool.getStderr();
        assertThat("--epoch flag should be recognized", epochOutput, CoreMatchers.containsStringIgnoringCase("Usage:"));

        ToolResult fromTool = ToolRunner.invokeClass(SSTableMetadataDump.class, "dump", "--from-epoch", "50", "-h");
        String fromOutput = fromTool.getStdout() + fromTool.getStderr();
        assertThat("--from-epoch flag should be recognized", fromOutput, CoreMatchers.containsStringIgnoringCase("Usage:"));

        ToolResult toTool = ToolRunner.invokeClass(SSTableMetadataDump.class, "dump", "--to-epoch", "150", "-h");
        String toOutput = toTool.getStdout() + toTool.getStderr();
        assertThat("--to-epoch flag should be recognized", toOutput, CoreMatchers.containsStringIgnoringCase("Usage:"));
    }

    @Test
    public void testVerboseAndDebugFlags()
    {
        // Test verbose flags
        ToolResult verboseShort = ToolRunner.invokeClass(SSTableMetadataDump.class, "dump", "-v", "-h");
        String verboseShortOutput = verboseShort.getStdout() + verboseShort.getStderr();
        assertThat("-v flag should be recognized", verboseShortOutput, CoreMatchers.containsStringIgnoringCase("Usage:"));

        ToolResult verboseLong = ToolRunner.invokeClass(SSTableMetadataDump.class, "dump", "--verbose", "-h");
        String verboseLongOutput = verboseLong.getStdout() + verboseLong.getStderr();
        assertThat("--verbose flag should be recognized", verboseLongOutput, CoreMatchers.containsStringIgnoringCase("Usage:"));

        // Test debug flag
        ToolResult debug = ToolRunner.invokeClass(SSTableMetadataDump.class, "dump", "--debug", "-h");
        String debugOutput = debug.getStdout() + debug.getStderr();
        assertThat("--debug flag should be recognized", debugOutput, CoreMatchers.containsStringIgnoringCase("Usage:"));
    }

    @Test
    public void testPartitionerFlag()
    {
        // Test partitioner flags
        ToolResult shortFlag = ToolRunner.invokeClass(SSTableMetadataDump.class, "dump",
                                                       "-p", "org.apache.cassandra.dht.Murmur3Partitioner", "-h");
        String shortOutput = shortFlag.getStdout() + shortFlag.getStderr();
        assertThat("-p flag should be recognized", shortOutput, CoreMatchers.containsStringIgnoringCase("Usage:"));

        ToolResult longFlag = ToolRunner.invokeClass(SSTableMetadataDump.class, "dump",
                                                      "--partitioner", "org.apache.cassandra.dht.Murmur3Partitioner", "-h");
        String longOutput = longFlag.getStdout() + longFlag.getStderr();
        assertThat("--partitioner flag should be recognized", longOutput, CoreMatchers.containsStringIgnoringCase("Usage:"));
    }
}
