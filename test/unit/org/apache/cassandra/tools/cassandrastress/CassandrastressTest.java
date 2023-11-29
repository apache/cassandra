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

package org.apache.cassandra.tools.cassandrastress;

import java.util.Arrays;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.service.GCInspector;
import org.apache.cassandra.tools.ToolRunner;
import org.apache.cassandra.tools.ToolRunner.ToolResult;
import org.hamcrest.CoreMatchers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class CassandrastressTest extends CQLTester
{
    @BeforeClass
    public static void setUp() throws Exception
    {
        requireNetwork();
        startJMXServer();
        GCInspector.register();// required by stress tool metrics
    }

    @Test
    public void testNoArgsPrintsHelp()
    {
        ToolResult tool = ToolRunner.invokeCassandraStress();
        assertThat(tool.getStdout(), CoreMatchers.containsStringIgnoringCase("usage:"));
        assertTrue("Tool stderr: " +  tool.getCleanedStderr(), tool.getCleanedStderr().isEmpty());
        assertEquals(1, tool.getExitCode());
    }

    @Test
    public void testNodesArg()
    {
        String[] baseArgs = new String[] { "write", "n=10", "no-warmup", "-rate", "threads=1", "-port",
                                           String.format("jmx=%d", jmxPort), String.format("native=%d", nativePort)};
        invokeAndAssertCleanExit(baseArgs);

        String ip = "127.0.0.1";
        invokeAndAssertCleanExit(baseArgs, "-node", ip);

        String ipAndPort = String.format("%s:%d", ip, nativePort);
        invokeAndAssertCleanExit(baseArgs, "-node", ipAndPort);

        String ipsAndPort = String.format("%s,%s", ipAndPort, ipAndPort);
        invokeAndAssertCleanExit(baseArgs, "-node", ipsAndPort);

        String hostNameAndPort = String.format("localhost:%s", nativePort);
        invokeAndAssertCleanExit(baseArgs, "-node", hostNameAndPort);

        invokeAndAssertCleanExit(baseArgs, "-mode", "simplenative", "prepared");
        invokeAndAssertCleanExit(baseArgs, "-mode", "simplenative");
        invokeAndAssertCleanExit(baseArgs, "-mode");
        invokeAndAssertCleanExit(baseArgs, "-mode", "unprepared");
    }

    void invokeAndAssertCleanExit(String[] baseArgs, String ... extraArgs)
    {
        String[] args = Arrays.copyOf(baseArgs, baseArgs.length + extraArgs.length);
        System.arraycopy(extraArgs, 0, args, baseArgs.length, extraArgs.length);
        ToolResult tool = ToolRunner.invokeCassandraStress(args);
        tool.assertOnCleanExit();
    }
}
