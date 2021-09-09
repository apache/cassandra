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

import org.apache.cassandra.tools.ToolRunner.ToolResult;
import org.assertj.core.api.Assertions;
import org.hamcrest.CoreMatchers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class StandaloneSSTableUtilTest extends OfflineToolUtils
{
    @Test
    public void testWrongArgFailsAndPrintsHelp()
    {
        ToolResult tool = ToolRunner.invokeClass(StandaloneSSTableUtil.class, "--debugwrong", "system_schema", "tables");
        assertThat(tool.getStdout(), CoreMatchers.containsStringIgnoringCase("usage:"));
        assertThat(tool.getCleanedStderr(), CoreMatchers.containsStringIgnoringCase("Unrecognized option"));
        assertEquals(1, tool.getExitCode());
    }

    @Test
    public void testMaybeChangeDocs()
    {
        // If you added, modified options or help, please update docs if necessary
        ToolResult tool = ToolRunner.invokeClass(StandaloneSSTableUtil.class, "-h");
        String help = "usage: sstableutil [options] <keyspace> <column_family>\n" + 
                       "--\n" + 
                       "List sstable files for the provided table.\n" + 
                       "--\n" + 
                       "Options are:\n" + 
                       " -c,--cleanup      clean-up any outstanding transactions\n" + 
                       " -d,--debug        display stack traces\n" + 
                       " -h,--help         display this help message\n" + 
                       " -o,--oplog        include operation logs\n" + 
                       " -t,--type <arg>   all (list all files, final or temporary), tmp (list\n" + 
                       "                   temporary files only), final (list final files only),\n" + 
                       " -v,--verbose      verbose output\n";
        Assertions.assertThat(tool.getStdout()).isEqualTo(help);
    }

    @Test
    public void testDefaultCall()
    {
        ToolResult tool = ToolRunner.invokeClass(StandaloneSSTableUtil.class, "system_schema", "tables");
        assertThat(tool.getStdout(), CoreMatchers.containsStringIgnoringCase("Listing files..."));
        Assertions.assertThat(tool.getCleanedStderr()).isEmpty();
        assertEquals(0, tool.getExitCode());
        assertCorrectEnvPostTest();
    }

    @Test
    public void testListFilesArgs()
    {
        Arrays.asList("-d", "--debug", "-o", "-oplog", "-v", "--verbose").forEach(arg -> {
            ToolResult tool = ToolRunner.invokeClass(StandaloneSSTableUtil.class,
                                                            arg,
                                                            "system_schema",
                                                            "tables");
            Assertions.assertThat(tool.getStdout()).as("Arg: [%s]", arg).isEqualTo("Listing files...\n");
            Assertions.assertThat(tool.getCleanedStderr()).as("Arg: [%s]", arg).isEmpty();
            tool.assertOnExitCode();
            assertCorrectEnvPostTest();
        });
    }

    @Test
    public void testCleanupArg()
    {
        Arrays.asList("-c", "--cleanup").forEach(arg -> {
            ToolResult tool = ToolRunner.invokeClass(StandaloneSSTableUtil.class,
                                                            arg,
                                                            "system_schema",
                                                            "tables");
            assertThat("Arg: [" + arg + "]", tool.getStdout(), CoreMatchers.containsStringIgnoringCase("Cleaning up..."));
            Assertions.assertThat(tool.getCleanedStderr()).as("Arg: [%s]", arg).isEmpty();
            tool.assertOnExitCode();
            assertCorrectEnvPostTest();
        });
    }

    @Test
    public void testHelpArg()
    {
        Arrays.asList("-h", "--help").forEach(arg -> {
            ToolResult tool = ToolRunner.invokeClass(StandaloneSSTableUtil.class, arg);
            assertThat("Arg: [" + arg + "]", tool.getStdout(), CoreMatchers.containsStringIgnoringCase("usage:"));
            Assertions.assertThat(tool.getCleanedStderr()).as("Arg: [%s]", arg).isEmpty();
            tool.assertOnExitCode();
            assertCorrectEnvPostTest();
        });
    }

    @Test
    public void testTypeArg()
    {
        Arrays.asList("-t", "--type").forEach(arg -> {
            ToolResult tool = ToolRunner.invokeClass(StandaloneSSTableUtil.class,
                                                            arg,
                                                            "system_schema",
                                                            "tables");
            assertThat("Arg: [" + arg + "]", tool.getStdout(), CoreMatchers.containsStringIgnoringCase("usage:"));
            assertThat("Arg: [" + arg + "]", tool.getCleanedStderr(), CoreMatchers.containsStringIgnoringCase("Missing arguments"));
            assertEquals("Arg: [" + arg + "]", 1, tool.getExitCode());
            assertCorrectEnvPostTest();
        });
        
        //'-t wrong' renders 'all' file types
        Arrays.asList(Pair.of("-t", "all"),
                      Pair.of("-t", "tmp"),
                      Pair.of("-t", "final"),
                      Pair.of("-t", "wrong"),
                      Pair.of("--type", "all"),
                      Pair.of("--type", "tmp"),
                      Pair.of("--type", "final"),
                      Pair.of("--type", "wrong"))
              .forEach(arg -> {
                  ToolResult tool = ToolRunner.invokeClass(StandaloneSSTableUtil.class,
                                                             arg.getLeft(),
                                                             arg.getRight(),
                                                             "system_schema",
                                                             "tables");
                  Assertions.assertThat(tool.getStdout()).as("Arg: [%s]", arg).isEqualTo("Listing files...\n");
                  Assertions.assertThat(tool.getCleanedStderr()).as("Arg: [%s]", arg).isEmpty();
                  tool.assertOnExitCode();
                  assertCorrectEnvPostTest();
              });
    }
}
