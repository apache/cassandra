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

import java.util.HashSet;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.tools.ToolRunner;
import org.apache.cassandra.tools.ToolRunner.ToolResult;
import org.apache.cassandra.config.DatabaseDescriptor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SetAllowedClientLibDriversTest extends CQLTester
{
    @BeforeClass
    public static void setup() throws Exception
    {
        CQLTester.setUpClass();
        startJMXServer();
        requireNetwork();
    }

    @Test
    public void testSetAllowedClientLibDrivers()
    {
        // Clear allowed drivers first
        Set<String> emptySet = new HashSet<>();
        DatabaseDescriptor.setAllowedClientLibDrivers(emptySet);

        // Test setting to empty (clear)
        ToolResult tool = ToolRunner.invokeNodetool("setallowedclientlibdrivers", "");
        assertEquals(0, tool.getExitCode());
        assertTrue(tool.getStdout().contains("Cleared allowed client library drivers"));
        assertEquals(0, DatabaseDescriptor.getAllowedClientLibDrivers().size());

        // Test setting single driver
        tool = ToolRunner.invokeNodetool("setallowedclientlibdrivers", "java-driver");
        assertEquals(0, tool.getExitCode());
        assertTrue(tool.getStdout().contains("Set allowed client library drivers to: java-driver"));
        assertEquals(1, DatabaseDescriptor.getAllowedClientLibDrivers().size());
        assertTrue(DatabaseDescriptor.getAllowedClientLibDrivers().contains("java-driver"));

        // Test setting multiple drivers
        tool = ToolRunner.invokeNodetool("setallowedclientlibdrivers", "java-driver,python-driver,nodejs-driver");
        assertEquals(0, tool.getExitCode());
        assertTrue(tool.getStdout().contains("Set allowed client library drivers to: "));
        assertEquals(3, DatabaseDescriptor.getAllowedClientLibDrivers().size());
        assertTrue(DatabaseDescriptor.getAllowedClientLibDrivers().contains("java-driver"));
        assertTrue(DatabaseDescriptor.getAllowedClientLibDrivers().contains("python-driver"));
        assertTrue(DatabaseDescriptor.getAllowedClientLibDrivers().contains("nodejs-driver"));

        // Reset to empty for other tests
        DatabaseDescriptor.setAllowedClientLibDrivers(emptySet);
    }
}
