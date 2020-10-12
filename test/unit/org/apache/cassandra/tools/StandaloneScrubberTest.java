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

import org.apache.commons.lang3.tuple.Pair;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.tools.ToolRunner.ToolResult;
import org.assertj.core.api.Assertions;
import org.hamcrest.CoreMatchers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(OrderedJUnit4ClassRunner.class)
public class StandaloneScrubberTest extends OfflineToolUtils
{
    @Test
    public void testNoArgsPrintsHelp()
    {
        ToolResult tool = ToolRunner.invokeClass(StandaloneScrubber.class);
        assertThat(tool.getStdout(), CoreMatchers.containsStringIgnoringCase("usage:"));
        assertThat(tool.getCleanedStderr(), CoreMatchers.containsStringIgnoringCase("Missing arguments"));
        assertEquals(1, tool.getExitCode());
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
        ToolResult tool = ToolRunner.invokeClass(StandaloneScrubber.class, "-h");
        String help = "usage: sstablescrub [options] <keyspace> <column_family>\n" + 
                       "--\n" + 
                       "Scrub the sstable for the provided table.\n" + 
                       "--\n" + 
                       "Options are:\n" + 
                       "    --debug                     display stack traces\n" + 
                       " -e,--header-fix <arg>          Option whether and how to perform a check of the sstable serialization-headers and fix\n" + 
                       "                                known, fixable issues.\n" + 
                       "                                Possible argument values:\n" + 
                       "                                - validate-only: validate the serialization-headers, but do not fix those. Do not continue with scrub - i.e. only\n" + 
                       "                                validate the header (dry-run of fix-only).\n" + 
                       "                                - validate: (default) validate the serialization-headers, but do not fix those and only continue with scrub if no error\n" + 
                       "                                were detected.\n" + 
                       "                                - fix-only: validate and fix the serialization-headers, don't continue with scrub.\n" + 
                       "                                - fix: validate and fix the serialization-headers, do not fix and do not continue with scrub if the serialization-header\n" + 
                       "                                check encountered errors.\n" + 
                       "                                - off: don't perform the serialization-header checks.\n" + 
                       " -h,--help                      display this help message\n" + 
                       " -m,--manifest-check            only check and repair the leveled manifest, without actually scrubbing the sstables\n" + 
                       " -n,--no-validate               do not validate columns using column validator\n" + 
                       " -r,--reinsert-overflowed-ttl   Rewrites rows with overflowed expiration date affected by CASSANDRA-14092 with the\n" + 
                       "                                maximum supported expiration date of 2038-01-19T03:14:06+00:00. The rows are rewritten with the original timestamp\n" + 
                       "                                incremented by one millisecond to override/supersede any potential tombstone that may have been generated during\n" + 
                       "                                compaction of the affected rows.\n" + 
                       " -s,--skip-corrupted            skip corrupt rows in counter tables\n" + 
                       " -v,--verbose                   verbose output\n";
        Assertions.assertThat(tool.getStdout()).isEqualTo(help);
    }

    @Test
    public void testWrongArgFailsAndPrintsHelp()
    {
        ToolResult tool = ToolRunner.invokeClass(StandaloneScrubber.class, "--debugwrong", "system_schema", "tables");
        assertThat(tool.getStdout(), CoreMatchers.containsStringIgnoringCase("usage:"));
        assertThat(tool.getCleanedStderr(), CoreMatchers.containsStringIgnoringCase("Unrecognized option"));
        assertEquals(1, tool.getExitCode());
    }

    @Test
    public void testDefaultCall()
    {
        ToolResult tool = ToolRunner.invokeClass(StandaloneScrubber.class, "system_schema", "tables");
        assertThat(tool.getStdout(), CoreMatchers.containsStringIgnoringCase("Pre-scrub sstables snapshotted into snapshot"));
        Assertions.assertThat(tool.getCleanedStderr()).isEmpty();
        assertEquals(0, tool.getExitCode());
        assertCorrectEnvPostTest();
    }

    @Test
    public void testFlagArgs()
    {
        Arrays.asList("--debug",
                      "-m",
                      "--manifest-check",
                      "-n",
                      "--no-validate",
                      "-r",
                      "--reinsert-overflowed-ttl",
                      "-s",
                      "--skip-corrupted",
                      "-v",
                      "--verbose")
              .forEach(arg -> {
                  ToolResult tool = ToolRunner.invokeClass(StandaloneScrubber.class,
                                                                  arg,
                                                                  "system_schema",
                                                                  "tables");
                  assertThat("Arg: [" + arg + "]", tool.getStdout(), CoreMatchers.containsStringIgnoringCase("Pre-scrub sstables snapshotted into snapshot"));
                  Assertions.assertThat(tool.getCleanedStderr()).as("Arg: [%s]", arg).isEmpty();
                  tool.assertOnExitCode();
                  assertCorrectEnvPostTest();
              });
    }

    @Test
    public void testHelpArg()
    {
        Arrays.asList("-h", "--help").forEach(arg -> {
            ToolResult tool = ToolRunner.invokeClass(StandaloneScrubber.class, arg);
            assertThat("Arg: [" + arg + "]", tool.getStdout(), CoreMatchers.containsStringIgnoringCase("usage:"));
            Assertions.assertThat(tool.getCleanedStderr()).as("Arg: [%s]", arg).isEmpty();
            tool.assertOnExitCode();
            assertCorrectEnvPostTest();
        });
    }

    @Test
    public void testHeaderFixArg()
    {
        Arrays.asList(Pair.of("-e", ""),
                      Pair.of("-e", "wrong"),
                      Pair.of("--header-fix", ""),
                      Pair.of("--header-fix", "wrong"))
              .forEach(arg -> {
                  ToolResult tool = ToolRunner.invokeClass(StandaloneScrubber.class,
                                                                  arg.getLeft(),
                                                                  arg.getRight(),
                                                                  "system_schema",
                                                                  "tables");
                  assertThat("Arg: [" + arg + "]", tool.getStdout(), CoreMatchers.containsStringIgnoringCase("usage:"));
                  assertTrue("Arg: [" + arg + "]\n" + tool.getCleanedStderr(), tool.getCleanedStderr().contains("Invalid argument value"));
                  assertEquals(1, tool.getExitCode());
              });

        Arrays.asList(Pair.of("-e", "validate-only"),
                      Pair.of("-e", "validate"),
                      Pair.of("-e", "fix-only"),
                      Pair.of("-e", "fix"),
                      Pair.of("-e", "off"),
                      Pair.of("--header-fix", "validate-only"),
                      Pair.of("--header-fix", "validate"),
                      Pair.of("--header-fix", "fix-only"),
                      Pair.of("--header-fix", "fix"),
                      Pair.of("--header-fix", "off"))
              .forEach(arg -> {
                  ToolResult tool = ToolRunner.invokeClass(StandaloneScrubber.class,
                                                                  arg.getLeft(),
                                                                  arg.getRight(),
                                                                  "system_schema",
                                                                  "tables");
                  assertThat("Arg: [" + arg + "]", tool.getStdout(), CoreMatchers.containsStringIgnoringCase("Pre-scrub sstables snapshotted into snapshot"));
                  Assertions.assertThat(tool.getCleanedStderr()).as("Arg: [%s]", arg).isEmpty();
                  tool.assertOnExitCode();
                  assertCorrectEnvPostTest();
              });
    }
}
