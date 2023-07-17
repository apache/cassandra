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

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.tools.ToolRunner.ToolResult;
import org.assertj.core.api.Assertions;

import static org.hamcrest.CoreMatchers.containsStringIgnoringCase;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class SSTableExportTest extends OfflineToolUtils
{
    private static String sstable;

    @BeforeClass
    public static void setupTest() throws IOException
    {
        sstable = findOneSSTable("legacy_sstables", "legacy_ma_simple");
    }

    @Test
    public void testNoArgsPrintsHelp()
    {
        ToolResult tool = ToolRunner.invokeClass(SSTableExport.class);
        assertThat(tool.getStdout(), containsStringIgnoringCase("usage:"));
        assertThat(tool.getCleanedStderr(), containsStringIgnoringCase("You must supply exactly one sstable"));
        assertThat(tool.getCleanedStderr(), not(containsStringIgnoringCase("before the -k/-x options")));
        assertEquals(1, tool.getExitCode());
        assertPostTestEnv();
    }

    @Test
    public void testMaybeChangeDocs()
    {
        // If you added, modified options or help, please update docs if necessary
        ToolResult tool = ToolRunner.invokeClass(SSTableExport.class);
        String help = "usage: sstabledump <sstable file path> <options>\n" +
                       "Dump contents of given SSTable to standard output in JSON format.\n" +
                       " -d         CQL row per line internal representation\n" +
                       " -e         enumerate partition keys only\n" +
                       " -k <arg>   List of included partition keys\n" +
                       " -l         Output json lines, by partition\n" +
                       " -t         Print raw timestamps instead of iso8601 date strings\n" +
                       " -x <arg>   List of excluded partition keys\n";
        Assertions.assertThat(tool.getStdout()).isEqualTo(help);
        assertPostTestEnv();
    }

    @Test
    public void testWrongArgFailsAndPrintsHelp()
    {
        ToolResult tool = ToolRunner.invokeClass(SSTableExport.class, "--debugwrong", sstable);
        assertThat(tool.getStdout(), containsStringIgnoringCase("usage:"));
        assertThat(tool.getCleanedStderr(), containsStringIgnoringCase("Unrecognized option"));
        assertEquals(1, tool.getExitCode());
        assertPostTestEnv();
    }

    @Test
    public void testPKArgOutOfOrder()
    {
        ToolResult tool = ToolRunner.invokeClass(SSTableExport.class, "-k", "0", sstable);
        assertThat(tool.getStdout(), containsStringIgnoringCase("usage:"));
        assertThat(tool.getCleanedStderr(), containsStringIgnoringCase("You must supply exactly one sstable"));
        assertThat(tool.getCleanedStderr(), containsStringIgnoringCase("before the -k/-x options"));
        assertEquals(1, tool.getExitCode());
        assertPostTestEnv();
    }

    @Test
    public void testExcludePKArgOutOfOrder()
    {
        ToolResult tool = ToolRunner.invokeClass(SSTableExport.class, "-x", "0", sstable);
        assertThat(tool.getStdout(), containsStringIgnoringCase("usage:"));
        assertThat(tool.getCleanedStderr(), containsStringIgnoringCase("You must supply exactly one sstable"));
        assertThat(tool.getCleanedStderr(), containsStringIgnoringCase("before the -k/-x options"));
        assertEquals(1, tool.getExitCode());
        assertPostTestEnv();
    }


    /**
     * Runs post-test assertions about loaded classed and started threads.
     */
    private void assertPostTestEnv()
    {
        assertNoUnexpectedThreadsStarted(OPTIONAL_THREADS_WITH_SCHEMA, false);
        assertSchemaNotLoaded();
        assertCLSMNotLoaded();
        assertSystemKSNotLoaded();
        assertKeyspaceNotLoaded();
        assertServerNotLoaded();
    }
}
