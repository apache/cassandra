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

import static org.junit.Assert.assertEquals;

public class SetClientLibsEnforcementLevelTest extends CQLTester
{
    @BeforeClass
    public static void setup() throws Exception
    {
        CQLTester.setUpClass();
        startJMXServer();
        requireNetwork();
    }

    @Test
    public void testSetClientLibsEnforcementLevel()
    {
        // Verify initial value is none
        assertEquals(Config.ClientLibsEnforcementLevel.none, DatabaseDescriptor.getClientLibsEnforcementLevel());

        // Set to soft and verify
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("setclientlibsenforcementlevel", "soft");
        tool.assertOnCleanExit();
        assertEquals(Config.ClientLibsEnforcementLevel.soft, DatabaseDescriptor.getClientLibsEnforcementLevel());

        // Set to hard and verify
        tool = ToolRunner.invokeNodetool("setclientlibsenforcementlevel", "hard");
        tool.assertOnCleanExit();
        assertEquals(Config.ClientLibsEnforcementLevel.hard, DatabaseDescriptor.getClientLibsEnforcementLevel());

        // Set back to none and verify
        tool = ToolRunner.invokeNodetool("setclientlibsenforcementlevel", "none");
        tool.assertOnCleanExit();
        assertEquals(Config.ClientLibsEnforcementLevel.none, DatabaseDescriptor.getClientLibsEnforcementLevel());

        // Test invalid value
        tool = ToolRunner.invokeNodetool("setclientlibsenforcementlevel", "invalid");
        assertEquals(1, tool.getExitCode());
        // Verify value hasn't changed
        assertEquals(Config.ClientLibsEnforcementLevel.none, DatabaseDescriptor.getClientLibsEnforcementLevel());
    }
}
