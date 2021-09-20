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

import java.util.Arrays;

import org.junit.Test;

import org.apache.cassandra.tools.ToolRunner.ToolResult;
import org.assertj.core.api.Assertions;
import org.hamcrest.CoreMatchers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class StandaloneUpgraderTest extends OfflineToolUtils
{
    @Test
    public void testMaybeChangeDocs()
    {
        // If you added, modified options or help, please update docs if necessary
        ToolResult tool = ToolRunner.invokeClass(StandaloneUpgrader.class, "-h");
        String help = "usage: sstableupgrade [options] <keyspace> <cf> [snapshot]\n" + 
                       "--\n" + 
                       "Upgrade the sstables in the given cf (or snapshot) to the current version\n" + 
                       "of Cassandra.This operation will rewrite the sstables in the specified cf\n" + 
                       "to match the currently installed version of Cassandra.\n" + 
                       "The snapshot option will only upgrade the specified snapshot. Upgrading\n" + 
                       "snapshots is required before attempting to restore a snapshot taken in a\n" + 
                       "major version older than the major version Cassandra is currently running.\n" + 
                       "This will replace the files in the given snapshot as well as break any\n" + 
                       "hard links to live sstables.\n" + 
                       "--\n" + 
                       "Options are:\n" + 
                       "    --debug         display stack traces\n" + 
                       " -h,--help          display this help message\n" + 
                       " -k,--keep-source   do not delete the source sstables\n";
        Assertions.assertThat(tool.getStdout()).isEqualTo(help);
    }

    @Test
    public void testWrongArgFailsAndPrintsHelp()
    {
        ToolResult tool = ToolRunner.invokeClass(StandaloneUpgrader.class, "--debugwrong", "system_schema", "tables");
        assertThat(tool.getStdout(), CoreMatchers.containsStringIgnoringCase("usage:"));
        assertThat(tool.getCleanedStderr(), CoreMatchers.containsStringIgnoringCase("Unrecognized option"));
        assertEquals(1, tool.getExitCode());
    }

    @Test
    public void testDefaultCall()
    {
        ToolResult tool = ToolRunner.invokeClass(StandaloneUpgrader.class, "system_schema", "tables");
        Assertions.assertThat(tool.getStdout()).isEqualTo("Found 0 sstables that need upgrading.\n");
        Assertions.assertThat(tool.getCleanedStderr()).isEmpty();
        assertEquals(0, tool.getExitCode());
        assertCorrectEnvPostTest();
    }

    @Test
    public void testFlagArgs()
    {
        Arrays.asList("--debug", "-k", "--keep-source").forEach(arg -> {
            ToolResult tool = ToolRunner.invokeClass(StandaloneUpgrader.class,
                                                       arg,
                                                       "system_schema",
                                                       "tables");
            Assertions.assertThat(tool.getStdout()).as("Arg: [%s]", arg).isEqualTo("Found 0 sstables that need upgrading.\n");
            Assertions.assertThat(tool.getCleanedStderr()).as("Arg: [%s]", arg).isEmpty();
            assertEquals(0, tool.getExitCode());
            assertCorrectEnvPostTest();
        });
    }

    @Test
    public void testHelpArg()
    {
        Arrays.asList("-h", "--help").forEach(arg -> {
            ToolResult tool = ToolRunner.invokeClass(StandaloneUpgrader.class, arg);
            Assertions.assertThat(tool.getStdout()).as("Arg: [%s]", arg).isNotEmpty();
            Assertions.assertThat(tool.getCleanedStderr()).as("Arg: [%s]", arg).isEmpty();
            tool.assertOnExitCode();
            assertCorrectEnvPostTest();
        });
    }
}
