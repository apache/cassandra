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

import org.apache.cassandra.io.sstable.VerifyTest;
import org.apache.cassandra.tools.ToolRunner.ToolResult;
import org.assertj.core.api.Assertions;
import org.hamcrest.CoreMatchers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Note: the complete coverage is composed of:
 * - {@link StandaloneVerifierOnSSTablesTest}
 * - {@link StandaloneVerifierTest}
 * - {@link VerifyTest}
*/
public class StandaloneVerifierTest extends OfflineToolUtils
{
    @Test
    public void testMaybeChangeDocs()
    {
        // If you added, modified options or help, please update docs if necessary
        ToolResult tool = ToolRunner.invokeClass(StandaloneVerifier.class, "-h");
        String help = "usage: sstableverify [options] <keyspace> <column_family> force\n" +
                       "--\n" + 
                       "Verify the sstable for the provided table.\n" + 
                       "--\n" +
                       "NOTE: There are significant risks associated with using this tool; it\n" +
                       "likely doesn't do what you expect and there are known edge cases. You must\n" +
                       "provide a -f or --force argument in order to allow usage of the tool ->\n" +
                       "see CASSANDRA-9947 and CASSANDRA-17017 for known risks.\n" +
                       "https://issues.apache.org/jira/browse/CASSANDRA-9947\n" +
                       "https://issues.apache.org/jira/browse/CASSANDRA-17017\n" +
                       "--\n" +
                       "Options are:\n" +
                       " -c,--check_version          make sure sstables are the latest version\n" + 
                       "    --debug                  display stack traces\n" + 
                       " -e,--extended               extended verification\n" +
                       " -f,--force                  force verify - see CASSANDRA-17017\n" +
                       " -h,--help                   display this help message\n" +
                       " -q,--quick                  do a quick check, don't read all data\n" + 
                       " -r,--mutate_repair_status   don't mutate repair status\n" + 
                       " -t,--token_range <range>    long token range of the format left,right.\n" +
                       "                             This may be provided multiple times to define\n" +
                       "                             multiple different ranges\n" +
                       " -v,--verbose                verbose output\n";

        Assertions.assertThat(tool.getStdout()).isEqualTo(help);
    }

    @Test
    public void testWrongArgFailsAndPrintsHelp()
    {
        ToolResult tool = ToolRunner.invokeClass(StandaloneVerifier.class, "--debugwrong", "system_schema", "tables", "-f");
        assertThat(tool.getStdout(), CoreMatchers.containsStringIgnoringCase("usage:"));
        assertThat(tool.getCleanedStderr(), CoreMatchers.containsStringIgnoringCase("Unrecognized option"));
        assertEquals(1, tool.getExitCode());
    }

    @Test
    public void testDefaultCall()
    {
        Arrays.asList("-f", "--force").forEach(arg -> {
            ToolResult tool = ToolRunner.invokeClass(StandaloneVerifier.class, "system_schema", "tables", arg);
            assertThat(tool.getStdout(), CoreMatchers.containsStringIgnoringCase("using the following options"));
            Assertions.assertThat(tool.getCleanedStderr()).isEmpty();
            assertEquals(0, tool.getExitCode());
            assertCorrectEnvPostTest();
            tool.assertOnCleanExit();
        });
    }

    @Test
    public void testDebugArg()
    {
        ToolResult tool = ToolRunner.invokeClass(StandaloneVerifier.class, "--debug", "system_schema", "tables", "-f");
        assertThat(tool.getStdout(), CoreMatchers.containsStringIgnoringCase("debug=true"));
        Assertions.assertThat(tool.getCleanedStderr()).isEmpty();
        assertCorrectEnvPostTest();
        tool.assertOnCleanExit();
    }

    @Test
    public void testExtendedArg()
    {
        Arrays.asList("-e", "--extended").forEach(arg -> {
            ToolResult tool = ToolRunner.invokeClass(StandaloneVerifier.class,
                                                     arg,
                                                     "system_schema",
                                                     "tables",
                                                     "--force");
            assertThat(tool.getStdout(), CoreMatchers.containsStringIgnoringCase("extended=true"));
            Assertions.assertThat(tool.getCleanedStderr()).isEmpty();
            assertCorrectEnvPostTest();
            tool.assertOnCleanExit();
        });
    }

    @Test
    public void testQuickArg()
    {
        Arrays.asList("-q", "--quick").forEach(arg -> {
            ToolResult tool = ToolRunner.invokeClass(StandaloneVerifier.class,
                                                     arg,
                                                     "system_schema",
                                                     "tables",
                                                     "-f");
            assertThat(tool.getStdout(), CoreMatchers.containsStringIgnoringCase("quick=true"));
            Assertions.assertThat(tool.getCleanedStderr()).isEmpty();
            assertCorrectEnvPostTest();
            tool.assertOnCleanExit();
        });
    }

    @Test
    public void testRepairStatusArg()
    {
        Arrays.asList("-r", "--mutate_repair_status").forEach(arg -> {
            ToolResult tool = ToolRunner.invokeClass(StandaloneVerifier.class,
                                                     arg,
                                                     "system_schema",
                                                     "tables",
                                                     "--force");
            assertThat(tool.getStdout(), CoreMatchers.containsStringIgnoringCase("mutateRepairStatus=true"));
            Assertions.assertThat(tool.getCleanedStderr()).isEmpty();
            assertCorrectEnvPostTest();
            tool.assertOnCleanExit();
        });
    }

    @Test
    public void testHelpArg()
    {
        Arrays.asList("-h", "--help").forEach(arg -> {
            ToolResult tool = ToolRunner.invokeClass(StandaloneVerifier.class, arg);
            assertThat(tool.getStdout(), CoreMatchers.containsStringIgnoringCase("usage:"));
            Assertions.assertThat(tool.getCleanedStderr()).isEmpty();
            assertCorrectEnvPostTest();
            tool.assertOnCleanExit();
        });
    }

    @Test
    public void testVerboseArg()
    {
        Arrays.asList("-v", "--verbose").forEach(arg -> {
            ToolResult tool = ToolRunner.invokeClass(StandaloneVerifier.class,
                                                     arg,
                                                     "system_schema",
                                                     "tables",
                                                     "-f");
            assertThat(tool.getStdout(), CoreMatchers.containsStringIgnoringCase("verbose=true"));
            Assertions.assertThat(tool.getCleanedStderr()).isEmpty();
            assertCorrectEnvPostTest();
            tool.assertOnCleanExit();
        });
    }

    @Test
    public void testTooFewArgs()
    {
        ToolResult tool = ToolRunner.invokeClass(StandaloneVerifier.class, "one_arg");
        assertThat(tool.getStdout(), CoreMatchers.containsStringIgnoringCase("usage:"));
        assertThat(tool.getCleanedStderr(), CoreMatchers.containsStringIgnoringCase("Missing arguments"));
        assertEquals(1, tool.getExitCode());
    }

    @Test
    public void testTooManyArgs()
    {
        ToolResult tool = ToolRunner.invokeClass(StandaloneVerifier.class, "one_arg", "two_arg", "toomany_arg");
        assertThat(tool.getStdout(), CoreMatchers.containsStringIgnoringCase("usage:"));
        assertThat(tool.getCleanedStderr(), CoreMatchers.containsStringIgnoringCase("Too many arguments"));
        assertEquals(1, tool.getExitCode());
    }

    @Test
    public void testFailsWithoutForce()
    {
        Arrays.asList("-r", "--mutate_repair_status").forEach(arg -> {
            ToolResult tool = ToolRunner.invokeClass(StandaloneVerifier.class,
                                                     arg,
                                                     "system_schema",
                                                     "tables",
                                                     "debug");
            assertThat(tool.getStdout(), CoreMatchers.containsStringIgnoringCase("usage:"));
            assertEquals(1, tool.getExitCode());
        });
    }

    @Test
    public void testBadForceArgument()
    {
        Arrays.asList("bf", "badforce", "garbage", "forrce").forEach(arg -> {
            ToolResult tool = ToolRunner.invokeClass(StandaloneVerifier.class,
                                                     "system_schema",
                                                     "tables",
                                                     arg);
            assertThat(tool.getStdout(), CoreMatchers.containsStringIgnoringCase("usage"));
            assertEquals(1, tool.getExitCode());
        });
    }
}