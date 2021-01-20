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

import java.io.IOException;
import java.nio.file.Files;

import org.apache.cassandra.io.util.File;
import org.junit.Test;

import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.tools.ToolRunner.ToolResult;
import org.assertj.core.api.Assertions;
import org.hamcrest.CoreMatchers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class SSTableRepairedAtSetterTest extends OfflineToolUtils
{
    @Test
    public void testNoArgsPrintsHelp()
    {
        ToolResult tool = ToolRunner.invokeClass(SSTableRepairedAtSetter.class);
        assertThat(tool.getStdout(), CoreMatchers.containsStringIgnoringCase("usage:"));
        Assertions.assertThat(tool.getCleanedStderr()).isEmpty();
        assertEquals(1, tool.getExitCode());
        assertNoUnexpectedThreadsStarted(null, false);
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
        ToolResult tool = ToolRunner.invokeClass(SSTableRepairedAtSetter.class, "-h");
        String help = "This command should be run with Cassandra stopped, otherwise you will get very strange behavior\n" + 
                      "Verify that Cassandra is not running and then execute the command like this:\n" + 
                      "Usage: sstablerepairedset --really-set [--is-repaired | --is-unrepaired] [-f <sstable-list> | <sstables>]\n";
        Assertions.assertThat(tool.getStdout()).isEqualTo(help);
    }

    @Test
    public void testWrongArgFailsAndPrintsHelp() throws IOException
    {
        ToolResult tool = ToolRunner.invokeClass(SSTableRepairedAtSetter.class,
                                                       "--debugwrong",
                                                       findOneSSTable("legacy_sstables", "legacy_ma_simple"));
        assertThat(tool.getStdout(), CoreMatchers.containsStringIgnoringCase("usage:"));
        Assertions.assertThat(tool.getCleanedStderr()).isEmpty();
        assertEquals(1, tool.getExitCode());
    }

    @Test
    public void testIsrepairedArg() throws Exception
    {
        ToolResult tool = ToolRunner.invokeClass(SSTableRepairedAtSetter.class,
                                                       "--really-set",
                                                       "--is-repaired",
                                                       findOneSSTable("legacy_sstables", "legacy_ma_simple"));
        tool.assertOnCleanExit();
        assertNoUnexpectedThreadsStarted(OPTIONAL_THREADS_WITH_SCHEMA, false);
        assertSchemaNotLoaded();
        assertCLSMNotLoaded();
        assertSystemKSNotLoaded();
        assertKeyspaceNotLoaded();
        assertServerNotLoaded();
    }

    @Test
    public void testIsunrepairedArg() throws Exception
    {
        ToolResult tool = ToolRunner.invokeClass(SSTableRepairedAtSetter.class,
                                                 "--really-set",
                                                 "--is-unrepaired",
                                                 findOneSSTable("legacy_sstables", "legacy_ma_simple"));
        tool.assertOnCleanExit();
        assertNoUnexpectedThreadsStarted(OPTIONAL_THREADS_WITH_SCHEMA, false);
        assertSchemaNotLoaded();
        assertCLSMNotLoaded();
        assertSystemKSNotLoaded();
        assertKeyspaceNotLoaded();
        assertServerNotLoaded();
    }

    @Test
    public void testFilesArg() throws Exception
    {
        File tmpFile = FileUtils.createTempFile("sstablelist.txt", "tmp");
        tmpFile.deleteOnExit();
        Files.write(tmpFile.toPath(), findOneSSTable("legacy_sstables", "legacy_ma_simple").getBytes());
        
        String file = tmpFile.absolutePath();
        ToolResult tool = ToolRunner.invokeClass(SSTableRepairedAtSetter.class, "--really-set", "--is-repaired", "-f", file);
        tool.assertOnCleanExit();
        assertNoUnexpectedThreadsStarted(OPTIONAL_THREADS_WITH_SCHEMA, false);
        assertSchemaNotLoaded();
        assertCLSMNotLoaded();
        assertSystemKSNotLoaded();
        assertKeyspaceNotLoaded();
        assertServerNotLoaded();
    }
}
