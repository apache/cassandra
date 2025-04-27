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

public class GetAllowedClientLibDriversTest extends CQLTester
{
    @BeforeClass
    public static void setup() throws Exception
    {
        CQLTester.setUpClass();
        startJMXServer();
        requireNetwork();
    }

    @Test
    public void testGetAllowedClientLibDrivers()
    {
        // Clear allowed drivers first
        Set<String> emptySet = new HashSet<>();
        DatabaseDescriptor.setAllowedClientLibDrivers(emptySet);

        ToolResult tool = ToolRunner.invokeNodetool("getallowedclientlibdrivers");
        assertEquals(0, tool.getExitCode());
        assertTrue(tool.getStdout().contains("No drivers are set. All client library drivers are allowed."));

        // Test with some drivers set
        Set<String> drivers = new HashSet<>();
        drivers.add("java-driver");
        drivers.add("python-driver");
        DatabaseDescriptor.setAllowedClientLibDrivers(drivers);

        tool = ToolRunner.invokeNodetool("getallowedclientlibdrivers");
        assertEquals(0, tool.getExitCode());
        assertTrue(tool.getStdout().contains("Allowed Client Library Drivers: "));
        assertTrue(tool.getStdout().contains("java-driver"));
        assertTrue(tool.getStdout().contains("python-driver"));

        // Reset to empty for other tests
        DatabaseDescriptor.setAllowedClientLibDrivers(emptySet);
    }
}
