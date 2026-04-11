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

package org.apache.cassandra.tools.nodetool.mock;

import org.junit.Test;

import org.apache.cassandra.tools.ToolRunner;

import static org.assertj.core.api.Assertions.assertThat;

public class SjkMockTest extends AbstractNodetoolMock
{

    @Test
    public void testSjk()
    {
        ToolRunner.ToolResult result = invokeNodetool("sjk");
        result.assertOnCleanExit();
        assertThat(result.getStdout()).contains("Usage: <main class> [options] [command] [command options]");
        assertThat(result.getStdout()).contains("    gc      [Print GC] Print GC log like information for remote process");
    }

    @Test
    public void testSjkHelp()
    {
        ToolRunner.ToolResult result = invokeNodetool("sjk", "--help");
        result.assertOnCleanExit();
        assertThat(result.getStdout()).contains("Usage: <main class> [options] [command] [command options]");
        assertThat(result.getStdout()).contains("    ttop      [Thread Top] Displays threads from JVM process");
    }

    @Test
    public void testSjkTtop()
    {
        ToolRunner.ToolResult result = invokeNodetool("sjk", "hh", "--top-number", "10000", "--live");
        result.assertOnCleanExit();
        assertThat(result.getStdout()).contains(" #      Instances          Bytes  Type");
        assertThat(result.getStdout()).containsPattern(" +\\d+ +\\d+ +org\\.apache\\.cassandra\\.config\\.DatabaseDescriptor\\$1");
    }

    @Test
    public void testSjkMxdumpQueryFiltersResults()
    {
        ToolRunner.ToolResult allBeans = invokeNodetool("sjk", "mxdump");
        allBeans.assertOnCleanExit();
        assertThat(allBeans.getStdout()).contains(STORAGE_SERVICE_MBEAN);
        assertThat(allBeans.getStdout()).contains(FAILURE_DETECTOR_MBEAN);

        ToolRunner.ToolResult filtered = invokeNodetool("sjk", "mxdump", "-q",
                                                        STORAGE_SERVICE_MBEAN);
        filtered.assertOnCleanExit();
        assertThat(filtered.getStdout()).contains(STORAGE_SERVICE_MBEAN);
        assertThat(filtered.getStdout()).doesNotContain(FAILURE_DETECTOR_MBEAN);
        assertThat(filtered.getStdout()).doesNotContain(GOSSIPER_MBEAN);
    }

    @Test
    public void testSjkMxdumpQueryWildcard()
    {
        ToolRunner.ToolResult filtered = invokeNodetool("sjk", "mxdump", "-q",
                                                        "org.apache.cassandra.net:*");
        filtered.assertOnCleanExit();
        assertThat(filtered.getStdout()).contains(FAILURE_DETECTOR_MBEAN);
        assertThat(filtered.getStdout()).contains(GOSSIPER_MBEAN);
        assertThat(filtered.getStdout()).contains(MESSAGING_SERVICE_MBEAN);
        assertThat(filtered.getStdout()).doesNotContain(STORAGE_SERVICE_MBEAN);
        assertThat(filtered.getStdout()).doesNotContain(CACHE_SERVICE_MBEAN);
    }
}
