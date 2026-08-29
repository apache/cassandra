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

package org.apache.cassandra.distributed.test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.tools.ToolRunner;
import org.apache.cassandra.tools.ToolRunner.ToolResult;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test for enabling and disabling binary protocol
 *
 * @see org.apache.cassandra.tools.nodetool.EnableBinary
 * @see org.apache.cassandra.tools.nodetool.DisableBinary
 */
public class NodeToolEnableDisableBinaryTest extends TestBaseImpl
{
    @Test
    public void testEnableDisableBinary() throws Throwable
    {
        try (ICluster<?> nodeCluster = init(builder().withNodes(1)
                                                        .withConfig(config -> config.with(NETWORK, GOSSIP, NATIVE_PROTOCOL))
                                                        .start()))
        {
            nodeCluster.get(1).nodetool("disableautocompaction");

            // We can connect
            assertTrue(canConnect());

            // We can't connect after disabling
            ToolResult tool = ToolRunner.invokeNodetoolJvmDtest(nodeCluster.get(1), "disablebinary");
            Assertions.assertThat(tool.getStdout()).containsIgnoringCase("Stop listening for CQL clients");
            assertTrue(tool.getCleanedStderr().isEmpty());
            assertEquals(0, tool.getExitCode());
            assertFalse(canConnect());

            // We can connect after re-enabling
            tool = ToolRunner.invokeNodetoolJvmDtest(nodeCluster.get(1), "enablebinary");
            Assertions.assertThat(tool.getStdout()).containsIgnoringCase("Starting listening for CQL clients");
            assertTrue(tool.getCleanedStderr().isEmpty());
            assertEquals(0, tool.getExitCode());
            assertTrue(canConnect());
        }
    }

    /**
     * With the binary protocol disabled, the management port must still accept connections and serve
     * queries against system tables. Operators and monitoring tools drive the cluster through that port,
     * so disabling the client transport must not take it down with it.
     */
    @Test
    public void testSystemQueriesViaManagementPortWhenBinaryDisabled() throws Throwable
    {
        int managementPort = 11211;
        int numberOfNodes = 1;
        try (ICluster<?> managementCluster = init(builder().withNodes(numberOfNodes)
                                                        .withConfig(config -> config.with(NETWORK, GOSSIP, NATIVE_PROTOCOL)
                                                                                    .set("start_native_transport_management", true)
                                                                                    .set("native_transport_management_port", managementPort))
                                                        .start()))
        {
            managementCluster.get(1).nodetool("disableautocompaction");
            assertTrue("Regular native CQL port should is accessible", canConnect());
            assertTrue("Management port should is accessible", canConnect(managementPort));

            ToolResult tool = ToolRunner.invokeNodetoolJvmDtest(managementCluster.get(1), "disablebinary");
            Assertions.assertThat(tool.getStdout()).containsIgnoringCase("Stop listening for CQL clients");
            assertEquals(0, tool.getExitCode());
            assertFalse("Regular native CQL port should NOT be accessible", canConnect());
            assertTrue("Management port should is accessible", canConnect(managementPort));

            try (Cluster c = Cluster.builder()
                                    .addContactPoint("127.0.0.1")
                                    .withPort(managementPort)
                                    .build();
                 Session s = c.connect())
            {
                assertFalse("system.local should return data",
                            s.execute("SELECT * FROM system.local").all().isEmpty());
                assertTrue("system.peers is NOT empty, howeverwe only have one node in the cluster",
                           s.execute("SELECT * FROM system.peers").all().isEmpty());
                assertFalse("system_schema.keyspaces should return data",
                            s.execute("SELECT * FROM system_schema.keyspaces").all().isEmpty());
                assertFalse("system_schema.tables should return data",
                            s.execute("SELECT * FROM system_schema.tables").all().isEmpty());
            }

            tool = ToolRunner.invokeNodetoolJvmDtest(managementCluster.get(1), "enablebinary");
            assertEquals(0, tool.getExitCode());
            assertTrue("Should connect to regular binary after re-enabling", canConnect());
        }
    }

    private boolean canConnect()
    {
        return canConnect(9042);
    }

    private boolean canConnect(int port)
    {
        boolean canConnect = false;
        try(com.datastax.driver.core.Cluster c = com.datastax.driver.core.Cluster.builder()
                                                                                 .addContactPoint("127.0.0.1")
                                                                                 .withPort(port)
                                                                                 .build();
            Session s = c.connect("system_schema"))
        {
            s.execute("SELECT * FROM system_schema.aggregates");
            canConnect = true;
        }
        catch(Exception e)
        {
            canConnect = false;
        }

        return canConnect;
    }
}
