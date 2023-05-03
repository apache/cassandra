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

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.tools.ToolRunner.ToolResult;
import org.assertj.core.api.Assertions;
import org.hamcrest.CoreMatchers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class StandaloneSplitterTest extends OfflineToolUtils
{
    // Note: StandaloneSplitter modifies sstables

    @BeforeClass
    public static void before()
    {
        // the legacy tables use a different partitioner :(
        // (Don't use ByteOrderedPartitioner.class.getName() as that would initialize the class and work
        // against the goal of this test to check classes and threads initialized by the tool.)
        CassandraRelevantProperties.PARTITIONER.setString("org.apache.cassandra.dht.ByteOrderedPartitioner");
    }

    @Test
    public void testMaybeChangeDocs()
    {
        // If you added, modified options or help, please update docs if necessary
        ToolResult tool = ToolRunner.invokeClass(StandaloneSplitter.class, "-h");
        String help = "usage: sstablessplit [options] <filename> [<filename>]*\n" + 
                      "--\n" + 
                      "Split the provided sstables files in sstables of maximum provided file\n" + 
                      "size (see option --size).\n" + 
                      "--\n" + 
                      "Options are:\n" + 
                      "    --debug         display stack traces\n" + 
                      " -h,--help          display this help message\n" + 
                      "    --no-snapshot   don't snapshot the sstables before splitting\n" + 
                      " -s,--size <size>   maximum size in MB for the output sstables (default:\n" + 
                      "                    50)\n";
        Assertions.assertThat(tool.getStdout()).isEqualTo(help);
    }

    @Test
    public void testWrongArgFailsAndPrintsHelp()
    {
        ToolResult tool = ToolRunner.invokeClass(StandaloneSplitter.class, "--debugwrong", "mockFile");
        assertThat(tool.getStdout(), CoreMatchers.containsStringIgnoringCase("usage:"));
        assertThat(tool.getCleanedStderr(), CoreMatchers.containsStringIgnoringCase("Unrecognized option"));
        assertEquals(1, tool.getExitCode());
    }

    @Test
    public void testWrongFilename()
    {
        ToolResult tool = ToolRunner.invokeClass(StandaloneSplitter.class, "mockFile");
        assertThat(tool.getStdout(), CoreMatchers.containsStringIgnoringCase("Skipping inexisting file mockFile"));
        assertThat(tool.getCleanedStderr(), CoreMatchers.containsStringIgnoringCase("No valid sstables to split"));
        assertEquals(1, tool.getExitCode());
        assertCorrectEnvPostTest();
    }

    @Test
    public void testFlagArgs()
    {
        Arrays.asList("--debug", "--no-snapshot").forEach(arg -> {
            ToolResult tool = ToolRunner.invokeClass(StandaloneSplitter.class, arg, "mockFile");
            assertThat("Arg: [" + arg + "]", tool.getStdout(), CoreMatchers.containsStringIgnoringCase("Skipping inexisting file mockFile"));
            assertThat("Arg: [" + arg + "]", tool.getCleanedStderr(), CoreMatchers.containsStringIgnoringCase("No valid sstables to split"));
            assertEquals(1, tool.getExitCode());
            assertCorrectEnvPostTest();
        });
    }

    @Test
    public void testSizeArg()
    {
        Arrays.asList(Pair.of("-s", ""), Pair.of("-s", "w"), Pair.of("--size", ""), Pair.of("--size", "w"))
              .forEach(arg -> {
                  ToolResult tool = ToolRunner.invokeClass(StandaloneSplitter.class,
                                                           arg.getLeft(),
                                                           arg.getRight(),
                                                           "mockFile");
                  assertEquals(-1, tool.getExitCode());
                  Assertions.assertThat(tool.getStderr()).contains(NumberFormatException.class.getSimpleName());
              });

        Arrays.asList(Pair.of("-s", "0"),
                      Pair.of("-s", "1000"),
                      Pair.of("-s", "-1"),
                      Pair.of("--size", "0"),
                      Pair.of("--size", "1000"),
                      Pair.of("--size", "-1"))
              .forEach(arg -> {
                  ToolResult tool = ToolRunner.invokeClass(StandaloneSplitter.class,
                                                                  arg.getLeft(),
                                                                  arg.getRight(),
                                                                  "mockFile");
                  assertThat("Arg: [" + arg + "]", tool.getStdout(), CoreMatchers.containsStringIgnoringCase("Skipping inexisting file mockFile"));
                  assertThat("Arg: [" + arg + "]", tool.getCleanedStderr(), CoreMatchers.containsStringIgnoringCase("No valid sstables to split"));
                  assertEquals(1, tool.getExitCode());
                  assertCorrectEnvPostTest();
              });
    }
}
