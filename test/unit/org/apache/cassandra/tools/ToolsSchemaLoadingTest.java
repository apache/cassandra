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

import org.hamcrest.CoreMatchers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class ToolsSchemaLoadingTest extends OfflineToolUtils
{
    @Test
    public void testNoArgsPrintsHelpStandaloneVerifier()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeClass(StandaloneVerifier.class);
        assertThat(tool.getStdout(), CoreMatchers.containsStringIgnoringCase("usage:"));
        assertThat(tool.getCleanedStderr(), CoreMatchers.containsStringIgnoringCase("Missing arguments"));
        assertEquals(1, tool.getExitCode());
        assertNoUnexpectedThreadsStarted(null, false);
        assertSchemaNotLoaded();
        assertCLSMNotLoaded();
        assertSystemKSNotLoaded();
        assertKeyspaceNotLoaded();
        assertServerNotLoaded();
    }

    @Test
    public void testNoArgsPrintsHelpStandaloneScrubber()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeClass(StandaloneScrubber.class);
        assertThat(tool.getStdout(), CoreMatchers.containsStringIgnoringCase("usage:"));
        assertThat(tool.getCleanedStderr(), CoreMatchers.containsStringIgnoringCase("Missing arguments"));
        assertEquals(1, tool.getExitCode());
        assertNoUnexpectedThreadsStarted(null, false);
        assertSchemaNotLoaded();
        assertCLSMNotLoaded();
        assertSystemKSNotLoaded();
        assertKeyspaceNotLoaded();
        assertServerNotLoaded();
    }

    @Test
    public void testNoArgsPrintsHelpStandaloneSplitter()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeClass(StandaloneSplitter.class);
        assertThat(tool.getStdout(), CoreMatchers.containsStringIgnoringCase("usage:"));
        assertThat(tool.getCleanedStderr(), CoreMatchers.containsStringIgnoringCase("No sstables to split"));
        assertEquals(1, tool.getExitCode());
        assertNoUnexpectedThreadsStarted(null, false);
        assertSchemaNotLoaded();
        assertCLSMNotLoaded();
        assertSystemKSNotLoaded();
        assertKeyspaceNotLoaded();
        assertServerNotLoaded();
    }

    @Test
    public void testNoArgsPrintsHelpStandaloneSSTableUtil()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeClass(StandaloneSSTableUtil.class);
        assertThat(tool.getStdout(), CoreMatchers.containsStringIgnoringCase("usage:"));
        assertThat(tool.getCleanedStderr(), CoreMatchers.containsStringIgnoringCase("Missing arguments"));
        assertEquals(1, tool.getExitCode());
        assertNoUnexpectedThreadsStarted(null, false);
        assertSchemaNotLoaded();
        assertCLSMNotLoaded();
        assertSystemKSNotLoaded();
        assertKeyspaceNotLoaded();
        assertServerNotLoaded();
    }

    @Test
    public void testNoArgsPrintsHelpStandaloneUpgrader()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeClass(StandaloneUpgrader.class);
        assertThat(tool.getStdout(), CoreMatchers.containsStringIgnoringCase("usage:"));
        assertThat(tool.getCleanedStderr(), CoreMatchers.containsStringIgnoringCase("Missing arguments"));
        assertEquals(1, tool.getExitCode());
        assertNoUnexpectedThreadsStarted(null, false);
        assertSchemaNotLoaded();
        assertCLSMNotLoaded();
        assertSystemKSNotLoaded();
        assertKeyspaceNotLoaded();
        assertServerNotLoaded();
    }
}
