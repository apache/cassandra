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
import org.junit.runner.RunWith;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.assertj.core.api.Assertions;
import org.hamcrest.CoreMatchers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

@RunWith(OrderedJUnit4ClassRunner.class)
public class StandaloneSplitterTest extends OfflineToolUtils
{
    private final ToolRunner.Runners runner = new ToolRunner.Runners();

    // Note: StandaloneSplitter modifies sstables

    @BeforeClass
    public static void before()
    {
        // the legacy tables use a different partitioner :(
        // (Don't use ByteOrderedPartitioner.class.getName() as that would initialize the class and work
        // against the goal of this test to check classes and threads initialized by the tool.)
        System.setProperty("cassandra.partitioner", "org.apache.cassandra.dht.ByteOrderedPartitioner");
    }

    @Test
    public void testNoArgsPrintsHelp()
    {
        try (ToolRunner tool = runner.invokeClassAsTool(StandaloneSplitter.class.getName()))
        {
            assertThat(tool.getStdout(), CoreMatchers.containsStringIgnoringCase("usage:"));
            assertThat(tool.getCleanedStderr(), CoreMatchers.containsStringIgnoringCase("No sstables to split"));
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
        try (ToolRunner tool = runner.invokeClassAsTool(StandaloneSplitter.class.getName(), "-h"))
        {
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
    }

    @Test
    public void testWrongArgFailsAndPrintsHelp()
    {
        try (ToolRunner tool = runner.invokeClassAsTool(StandaloneSplitter.class.getName(), "--debugwrong", "mockFile"))
        {
            assertThat(tool.getStdout(), CoreMatchers.containsStringIgnoringCase("usage:"));
            assertThat(tool.getCleanedStderr(), CoreMatchers.containsStringIgnoringCase("Unrecognized option"));
            assertEquals(1, tool.getExitCode());
        }
    }

    @Test
    public void testWrongFilename()
    {
        try (ToolRunner tool = runner.invokeClassAsTool(StandaloneSplitter.class.getName(), "mockFile"))
        {
            assertThat(tool.getStdout(), CoreMatchers.containsStringIgnoringCase("Skipping inexisting file mockFile"));
            assertThat(tool.getCleanedStderr(), CoreMatchers.containsStringIgnoringCase("No valid sstables to split"));
            assertEquals(1, tool.getExitCode());
        }
        assertCorrectEnvPostTest();
    }

    @Test
    public void testFlagArgs()
    {
        Arrays.asList("--debug", "--no-snapshot").forEach(arg -> {
            try (ToolRunner tool = runner.invokeClassAsTool(StandaloneSplitter.class.getName(), arg, "mockFile"))
            {
                assertThat("Arg: [" + arg + "]", tool.getStdout(), CoreMatchers.containsStringIgnoringCase("Skipping inexisting file mockFile"));
                assertThat("Arg: [" + arg + "]", tool.getCleanedStderr(), CoreMatchers.containsStringIgnoringCase("No valid sstables to split"));
                assertEquals(1, tool.getExitCode());
            }
            assertCorrectEnvPostTest();
        });
    }

    @Test
    public void testSizeArg()
    {
        Arrays.asList(Pair.of("-s", ""), Pair.of("-s", "w"), Pair.of("--size", ""), Pair.of("--size", "w"))
              .forEach(arg -> {
                  try
                  {
                      runner.invokeClassAsTool(StandaloneSplitter.class.getName(),
                                               arg.getLeft(),
                                               arg.getRight(),
                                               "mockFile");
                      fail("Shouldn't be able to parse wrong input as number");
                  }
                  catch(RuntimeException e)
                  {
                      if (!(e.getCause() instanceof NumberFormatException))
                          fail("Should have failed parsing a non-num.");
                  }
              });

        Arrays.asList(Pair.of("-s", "0"),
                      Pair.of("-s", "1000"),
                      Pair.of("-s", "-1"),
                      Pair.of("--size", "0"),
                      Pair.of("--size", "1000"),
                      Pair.of("--size", "-1"))
              .forEach(arg -> {
                  try (ToolRunner tool = runner.invokeClassAsTool(StandaloneSplitter.class.getName(),
                                                                  arg.getLeft(),
                                                                  arg.getRight(),
                                                                  "mockFile"))
                  {
                      assertThat("Arg: [" + arg + "]", tool.getStdout(), CoreMatchers.containsStringIgnoringCase("Skipping inexisting file mockFile"));
                      assertThat("Arg: [" + arg + "]", tool.getCleanedStderr(), CoreMatchers.containsStringIgnoringCase("No valid sstables to split"));
                      assertEquals(1, tool.getExitCode());
                  }
                  assertCorrectEnvPostTest();
              });
    }
}
