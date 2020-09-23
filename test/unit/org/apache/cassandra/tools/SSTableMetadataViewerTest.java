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

import org.apache.commons.codec.digest.DigestUtils;
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
import static org.junit.Assert.fail;

@RunWith(OrderedJUnit4ClassRunner.class)
public class SSTableMetadataViewerTest extends OfflineToolUtils
{
    @Test
    public void testNoArgsPrintsHelp()
    {
        ToolResult tool = ToolRunner.invokeClass(SSTableMetadataViewer.class);
        {
            assertTrue(tool.getStdout(), tool.getStdout().isEmpty());
            assertThat(tool.getCleanedStderr(), CoreMatchers.containsStringIgnoringCase("Options:"));
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
        ToolResult tool = ToolRunner.invokeClass(SSTableMetadataViewer.class, "-h");
        assertEquals("a30d3c489bcf56ddb2140184c5c62422", DigestUtils.md5Hex(tool.getCleanedStderr()));
    }

    @Test
    public void testWrongArgFailsAndPrintsHelp()
    {
        ToolResult tool = ToolRunner.invokeClass(SSTableMetadataViewer.class, "--debugwrong", "ks", "tab");
        assertTrue(tool.getStdout(), tool.getStdout().isEmpty());
        assertThat(tool.getCleanedStderr(), CoreMatchers.containsStringIgnoringCase("Options:"));
        assertEquals(1, tool.getExitCode());
    }

    @Test
    public void testDefaultCall()
    {
        ToolResult tool = ToolRunner.invokeClass(SSTableMetadataViewer.class, "ks", "tab");
        assertThat(tool.getStdout(), CoreMatchers.containsStringIgnoringCase("No such file"));
        Assertions.assertThat(tool.getCleanedStderr()).isEmpty();
        assertEquals(0,tool.getExitCode());
        assertGoodEnvPostTest();
    }

    @Test
    public void testFlagArgs()
    {
        Arrays.asList("-c",
                      "--colors",
                      "-s",
                      "--scan",
                      "-u",
                      "--unicode")
              .forEach(arg -> {
                  ToolResult tool = ToolRunner.invokeClass(SSTableMetadataViewer.class, arg, "ks", "tab");
                  assertThat("Arg: [" + arg + "]", tool.getStdout(), CoreMatchers.containsStringIgnoringCase("No such file"));
                  Assertions.assertThat(tool.getCleanedStderr()).as("Arg: [%s]", arg).isEmpty();
                  assertEquals(0,tool.getExitCode());
                  assertGoodEnvPostTest();
              });
    }

    @Test
    public void testGCArg()
    {
        Arrays.asList(Pair.of("-g", ""),
                      Pair.of("-g", "w"),
                      Pair.of("--gc_grace_seconds", ""),
                      Pair.of("--gc_grace_seconds", "w"))
              .forEach(arg -> {
                  ToolResult tool = ToolRunner.invokeClass(SSTableMetadataViewer.class,
                                                           arg.getLeft(),
                                                           arg.getRight(),
                                                           "ks",
                                                           "tab");
                  assertEquals(-1, tool.getExitCode());
                  Assertions.assertThat(tool.getStderr()).contains(NumberFormatException.class.getSimpleName());
              });

        Arrays.asList(Pair.of("-g", "5"), Pair.of("--gc_grace_seconds", "5")).forEach(arg -> {
            ToolResult tool = ToolRunner.invokeClass(SSTableMetadataViewer.class,
                                                            arg.getLeft(),
                                                            arg.getRight(),
                                                            "ks",
                                                            "tab");
            assertThat("Arg: [" + arg + "]", tool.getStdout(), CoreMatchers.containsStringIgnoringCase("No such file"));
            Assertions.assertThat(tool.getCleanedStderr()).as("Arg: [%s]", arg).isEmpty();
            tool.assertOnExitCode();
            assertGoodEnvPostTest();
        });
    }

    @Test
    public void testTSUnitArg()
    {
        Arrays.asList(Pair.of("-t", ""),
                      Pair.of("-t", "w"),
                      Pair.of("--timestamp_unit", ""),
                      Pair.of("--timestamp_unit", "w"))
              .forEach(arg -> {
                  ToolResult tool = ToolRunner.invokeClass(SSTableMetadataViewer.class,
                                                           arg.getLeft(),
                                                           arg.getRight(),
                                                           "ks",
                                                           "tab");
                  assertEquals(-1, tool.getExitCode());
                  Assertions.assertThat(tool.getStderr()).contains(IllegalArgumentException.class.getSimpleName());
              });

        Arrays.asList(Pair.of("-t", "SECONDS"), Pair.of("--timestamp_unit", "SECONDS")).forEach(arg -> {
            ToolResult tool = ToolRunner.invokeClass(SSTableMetadataViewer.class,
                                                       arg.getLeft(),
                                                       arg.getRight(),
                                                       "ks",
                                                       "tab");
            assertThat("Arg: [" + arg + "]", tool.getStdout(), CoreMatchers.containsStringIgnoringCase("No such file"));
            Assertions.assertThat(tool.getCleanedStderr()).as("Arg: [%s]", arg).isEmpty();
            tool.assertOnExitCode();
            assertGoodEnvPostTest();
        });
    }

    private void assertGoodEnvPostTest()
    {
        assertNoUnexpectedThreadsStarted(null, OPTIONAL_THREADS_WITH_SCHEMA);
        assertSchemaNotLoaded();
        assertCLSMNotLoaded();
        assertSystemKSNotLoaded();
        assertKeyspaceNotLoaded();
        assertServerNotLoaded();
    }
}
