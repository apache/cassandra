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

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.tools.ToolRunner;

import static java.lang.String.format;
import static org.apache.cassandra.auth.AuthKeyspace.CIDR_GROUPS;
import static org.apache.cassandra.schema.SchemaConstants.AUTH_KEYSPACE_NAME;
import static org.assertj.core.api.Assertions.assertThat;

public class UpdateCIDRGroupTest extends CQLTester
{
    @BeforeClass
    public static void setup() throws Exception
    {
        CQLTester.requireAuthentication();

        startJMXServer();
    }

    @Before
    public void cleanCidrGroups() throws Throwable
    {
        execute(format("TRUNCATE %s.%s", AUTH_KEYSPACE_NAME, CIDR_GROUPS));
    }

    @Test
    public void testUpdateCidrGroup()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("listcidrgroups");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isEmpty();

        tool = ToolRunner.invokeNodetool("updatecidrgroup", "test1", "10.11.12.0/24");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isEmpty();

        tool = ToolRunner.invokeNodetool("updatecidrgroup", "test2", "11.11.12.0/24", "12.11.0.0/18");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isEmpty();

        tool = ToolRunner.invokeNodetool("listcidrgroups");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).contains("test1");
        assertThat(tool.getStdout()).contains("test2");

        tool = ToolRunner.invokeNodetool("listcidrgroups", "test1");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).contains("10.11.12.0/24");

        tool = ToolRunner.invokeNodetool("listcidrgroups", "test2");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).contains("11.11.12.0/24");
        assertThat(tool.getStdout()).contains("12.11.0.0/18");
    }

    @Test
    public void testUpdateSameCidrGroup()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("listcidrgroups");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isEmpty();

        tool = ToolRunner.invokeNodetool("updatecidrgroup", "test", "10.11.12.0/24");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isEmpty();

        tool = ToolRunner.invokeNodetool("listcidrgroups", "test");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).contains("10.11.12.0/24");

        tool = ToolRunner.invokeNodetool("updatecidrgroup", "test", "11.11.12.0/24");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isEmpty();

        tool = ToolRunner.invokeNodetool("listcidrgroups", "test");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).contains("11.11.12.0/24");
    }

    @Test
    public void testUpdateInvalidCidrGroup()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("updatecidrgroup", "test", "10.11.12.0/33");
        assertThat(tool.getStderr()).contains("is not a valid CIDR String");

        tool = ToolRunner.invokeNodetool("listcidrgroups", "test");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isEmpty();
    }
}
