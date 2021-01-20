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

import org.junit.Test;

import org.apache.cassandra.tools.ToolRunner.ToolResult;
import org.assertj.core.api.Assertions;
import org.hamcrest.CoreMatchers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class SSTableExpiredBlockersTest extends OfflineToolUtils
{
    @Test
    public void testNoArgsPrintsHelp()
    {
        ToolResult tool = ToolRunner.invokeClass(SSTableExpiredBlockers.class);
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
        ToolResult tool = ToolRunner.invokeClass(SSTableExpiredBlockers.class);
        String help = "Usage: sstableexpiredblockers <keyspace> <table>\n";
        Assertions.assertThat(tool.getStdout()).isEqualTo(help);
    }

    @Test
    public void testWrongArgsIgnored()
    {
        ToolResult tool = ToolRunner.invokeClass(SSTableExpiredBlockers.class, "--debugwrong", "system_schema", "tables");
        assertThat(tool.getStdout(), CoreMatchers.containsStringIgnoringCase("No sstables for"));
        Assertions.assertThat(tool.getCleanedStderr()).isEmpty();
        assertEquals(1, tool.getExitCode());
        assertCorrectEnvPostTest();
    }

    @Test
    public void testDefaultCall()
    {
        ToolResult tool = ToolRunner.invokeClass(SSTableExpiredBlockers.class, "system_schema", "tables");
        assertThat(tool.getStdout(), CoreMatchers.containsStringIgnoringCase("No sstables for"));
        Assertions.assertThat(tool.getCleanedStderr()).isEmpty();
        assertEquals(1, tool.getExitCode());
        assertCorrectEnvPostTest();
    }
}
