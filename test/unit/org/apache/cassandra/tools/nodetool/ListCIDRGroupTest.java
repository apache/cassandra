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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CIDR;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.tools.ToolRunner;

import static org.apache.cassandra.auth.AuthTestUtils.insertCidrsMappings;
import static org.assertj.core.api.Assertions.assertThat;

public class ListCIDRGroupTest extends CQLTester
{
    @BeforeClass
    public static void setup() throws Exception
    {
        CQLTester.requireAuthentication();
        startJMXServer();
    }

    @Before
    public void before()
    {
        Map<String, List<CIDR>> cidrsMapping = new HashMap<>()
        {{
            put("test1", Collections.singletonList(CIDR.getInstance("10.11.12.0/24")));
            put("test2", Arrays.asList(CIDR.getInstance("11.11.12.0/24"), CIDR.getInstance("12.11.12.0/18")));
        }};

        insertCidrsMappings(cidrsMapping);
    }

    @Test
    public void testListCidrGroup()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("listcidrgroups");
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
}
