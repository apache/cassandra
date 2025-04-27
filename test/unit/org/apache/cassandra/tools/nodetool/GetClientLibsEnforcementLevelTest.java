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

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.tools.ToolRunner;

import static org.junit.Assert.assertTrue;

public class GetClientLibsEnforcementLevelTest extends CQLTester
{
    @BeforeClass
    public static void setup() throws Exception
    {
        CQLTester.setUpClass();
        startJMXServer();
        requireNetwork();
    }

    @Test
    public void testGetClientLibsEnforcementLevel()
    {
        // Set initial value to none
        DatabaseDescriptor.setClientLibsEnforcementLevel(Config.ClientLibsEnforcementLevel.none);

        // Test get command for 'none'
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("getclientlibsenforcementlevel");
        tool.assertOnCleanExit();
        String output = tool.getStdout();
        assertTrue(output.contains("Client Libraries Enforcement Level: none"));

        // Set to soft and verify
        DatabaseDescriptor.setClientLibsEnforcementLevel(Config.ClientLibsEnforcementLevel.soft);
        tool = ToolRunner.invokeNodetool("getclientlibsenforcementlevel");
        tool.assertOnCleanExit();
        output = tool.getStdout();
        assertTrue(output.contains("Client Libraries Enforcement Level: soft"));

        // Set to hard and verify
        DatabaseDescriptor.setClientLibsEnforcementLevel(Config.ClientLibsEnforcementLevel.hard);
        tool = ToolRunner.invokeNodetool("getclientlibsenforcementlevel");
        tool.assertOnCleanExit();
        output = tool.getStdout();
        assertTrue(output.contains("Client Libraries Enforcement Level: hard"));

        // Set back to none
        DatabaseDescriptor.setClientLibsEnforcementLevel(Config.ClientLibsEnforcementLevel.none);
    }
}
