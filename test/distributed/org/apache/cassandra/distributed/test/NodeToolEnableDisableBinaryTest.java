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

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.Session;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.tools.ToolRunner;
import org.apache.cassandra.tools.ToolRunner.ToolResult;
import org.assertj.core.api.Assertions;

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
    private static ICluster cluster;

    @Before
    public void setupEnv() throws IOException
    {
        if (cluster == null)
        {
            cluster = init(builder().withNodes(1)
                                    .withConfig(config -> config.with(NETWORK, GOSSIP, NATIVE_PROTOCOL))
                                    .start());
            cluster.get(1).nodetool("disableautocompaction");
        }
    }

    @AfterClass
    public static void teardownEnv() throws Exception
    {
        cluster.close();
    }

    @Test
    public void testEnableDisableBinary() throws Throwable
    {
        // We can connect
        assertTrue(canConnect());

        // We can't connect after disabling
        ToolResult tool = ToolRunner.invokeNodetoolJvmDtest(cluster.get(1), "disablebinary");
        Assertions.assertThat(tool.getStdout()).containsIgnoringCase("Stop listening for CQL clients");
        assertTrue(tool.getCleanedStderr().isEmpty());
        assertEquals(0, tool.getExitCode());
        assertFalse(canConnect());

        // We can connect after re-enabling
        tool = ToolRunner.invokeNodetoolJvmDtest(cluster.get(1), "enablebinary");
        Assertions.assertThat(tool.getStdout()).containsIgnoringCase("Starting listening for CQL clients");
        assertTrue(tool.getCleanedStderr().isEmpty());
        assertEquals(0, tool.getExitCode());
        assertTrue(canConnect());
    }

    private boolean canConnect()
    {
        boolean canConnect = false;
        try(com.datastax.driver.core.Cluster c = com.datastax.driver.core.Cluster.builder()
                                                                                 .addContactPoint("127.0.0.1")
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
