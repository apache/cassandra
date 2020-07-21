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
import org.junit.runner.RunWith;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.assertj.core.api.Assertions;
import org.hamcrest.CoreMatchers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(OrderedJUnit4ClassRunner.class)
public class StandaloneVerifierTest extends OfflineToolUtils
{
    private final ToolRunner.Runners runner = new ToolRunner.Runners();

    @Test
    public void testNoArgsPrintsHelp()
    {
        try (ToolRunner tool = runner.invokeClassAsTool(StandaloneVerifier.class.getName()))
        {
            assertThat(tool.getStdout(), CoreMatchers.containsStringIgnoringCase("usage:"));
            assertThat(tool.getCleanedStderr(), CoreMatchers.containsStringIgnoringCase("Missing arguments"));
            assertEquals(1, tool.getExitCode());
        }
        assertNoUnexpectedThreadsStarted(null, null);
        assertSchemaNotLoaded();
        assertCLSMNotLoaded();
        assertSystemKSNotLoaded();
        assertKeyspaceNotLoaded();
        assertServerNotLoaded();
    }

    @Test
    public void testMaybeChangeDocs()
    {
        // If you added, modified options or help, please update docs if necessary
        try (ToolRunner tool = runner.invokeClassAsTool(StandaloneVerifier.class.getName(), "-h"))
        {
            String help = "usage: sstableverify [options] <keyspace> <column_family>\n" + 
                           "--\n" + 
                           "Verify the sstable for the provided table.\n" + 
                           "--\n" + 
                           "Options are:\n" + 
                           " -c,--check_version          make sure sstables are the latest version\n" + 
                           "    --debug                  display stack traces\n" + 
                           " -e,--extended               extended verification\n" + 
                           " -h,--help                   display this help message\n" + 
                           " -q,--quick                  do a quick check, don't read all data\n" + 
                           " -r,--mutate_repair_status   don't mutate repair status\n" + 
                           " -t,--token_range <range>    long token range of the format left,right.\n" + 
                           "                             This may be provided multiple times to define multiple different ranges\n" + 
                           " -v,--verbose                verbose output\n";
            Assertions.assertThat(tool.getStdout()).isEqualTo(help);
        }
    }

    @Test
    public void testWrongArgFailsAndPrintsHelp()
    {
        try (ToolRunner tool = runner.invokeClassAsTool(StandaloneVerifier.class.getName(), "--debugwrong", "system_schema", "tables"))
        {
            assertThat(tool.getStdout(), CoreMatchers.containsStringIgnoringCase("usage:"));
            assertThat(tool.getCleanedStderr(), CoreMatchers.containsStringIgnoringCase("Unrecognized option"));
            assertEquals(1, tool.getExitCode());
        }
    }

    @Test
    public void testDefaultCall()
    {
        try (ToolRunner tool = runner.invokeClassAsTool(StandaloneVerifier.class.getName(), "system_schema", "tables"))
        {
            assertThat(tool.getStdout(), CoreMatchers.containsStringIgnoringCase("using the following options"));
            Assertions.assertThat(tool.getCleanedStderr()).isEmpty();
            assertEquals(0,tool.getExitCode());
        }
        assertCorrectEnvPostTest();
    }

    @Test
    public void testDebugArg()
    {
        try (ToolRunner tool = runner.invokeClassAsTool(StandaloneVerifier.class.getName(), "--debug", "system_schema", "tables"))
        {
            assertThat(tool.getStdout(), CoreMatchers.containsStringIgnoringCase("debug=true"));
            Assertions.assertThat(tool.getCleanedStderr()).isEmpty();
            tool.assertOnExitCode();
        }
        assertCorrectEnvPostTest();
    }

    @Test
    public void testExtendedArg()
    {
        Arrays.asList("-e", "--extended").forEach(arg -> {
            try (ToolRunner tool = runner.invokeClassAsTool(StandaloneVerifier.class.getName(),
                                                       arg,
                                                       "system_schema",
                                                       "tables"))
            {
                assertThat(tool.getStdout(), CoreMatchers.containsStringIgnoringCase("extended=true"));
                Assertions.assertThat(tool.getCleanedStderr()).isEmpty();
                tool.assertOnExitCode();
            }
            assertCorrectEnvPostTest();
        });
    }

    @Test
    public void testQuickArg()
    {
        Arrays.asList("-q", "--quick").forEach(arg -> {
            try (ToolRunner tool = runner.invokeClassAsTool(StandaloneVerifier.class.getName(),
                                                       arg,
                                                       "system_schema",
                                                       "tables"))
            {
                assertThat(tool.getStdout(), CoreMatchers.containsStringIgnoringCase("quick=true"));
                Assertions.assertThat(tool.getCleanedStderr()).isEmpty();
                tool.assertOnExitCode();
            }
            assertCorrectEnvPostTest();
        });
    }

    @Test
    public void testRepairStatusArg()
    {
        Arrays.asList("-r", "--mutate_repair_status").forEach(arg -> {
            try (ToolRunner tool = runner.invokeClassAsTool(StandaloneVerifier.class.getName(),
                                                       arg,
                                                       "system_schema",
                                                       "tables"))
            {
                assertThat(tool.getStdout(), CoreMatchers.containsStringIgnoringCase("mutateRepairStatus=true"));
                Assertions.assertThat(tool.getCleanedStderr()).isEmpty();
                tool.assertOnExitCode();
            }
            assertCorrectEnvPostTest();
        });
    }

    @Test
    public void testHelpArg()
    {
        Arrays.asList("-h", "--help").forEach(arg -> {
            try (ToolRunner tool = runner.invokeClassAsTool(StandaloneVerifier.class.getName(), arg))
            {
                assertThat(tool.getStdout(), CoreMatchers.containsStringIgnoringCase("usage:"));
                Assertions.assertThat(tool.getCleanedStderr()).isEmpty();
                tool.assertOnExitCode();
            }
            assertCorrectEnvPostTest();
        });
    }

    @Test
    public void testVerboseArg()
    {
        Arrays.asList("-v", "--verbose").forEach(arg -> {
            try (ToolRunner tool = runner.invokeClassAsTool(StandaloneVerifier.class.getName(),
                                                       arg,
                                                       "system_schema",
                                                       "tables"))
            {
                assertThat(tool.getStdout(), CoreMatchers.containsStringIgnoringCase("verbose=true"));
                Assertions.assertThat(tool.getCleanedStderr()).isEmpty();
                tool.assertOnExitCode();
            }
            assertCorrectEnvPostTest();
        });
    }
}
