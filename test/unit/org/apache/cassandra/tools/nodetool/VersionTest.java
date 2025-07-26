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
package org.apache.cassandra.tools.nodetool;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.tools.ToolRunner;

import static org.assertj.core.api.Assertions.assertThat;

public class VersionTest extends CQLTester
{

    @BeforeClass
    public static void setup() throws Exception
    {
        requireNetwork();
        startJMXServer();
    }

    @Test
    public void testBasic()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("version");
        tool.assertOnExitCode();
        String stdout = tool.getStdout();
        assertThat(stdout).containsPattern("ReleaseVersion:\\s+\\S+");
    }

    @Test
    public void testVOption()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("version", "-v");
        tool.assertOnExitCode();
        String stdout = tool.getStdout();
        assertThat(stdout).containsPattern("ReleaseVersion:\\s+\\S+");
        assertThat(stdout).containsPattern("BuildDate:\\s+\\S+");
        assertThat(stdout).containsPattern("GitSHA:\\s+\\S+");
        assertThat(stdout).containsPattern("JVM vendor/version:\\s+\\S+");
    }
}
