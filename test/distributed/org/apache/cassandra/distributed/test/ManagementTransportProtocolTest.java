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

import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.tools.ToolRunner;
import org.apache.cassandra.tools.ToolRunner.ToolResult;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * End-to-end tests for the management native transport protocol (CEP-38).
 *
 *  @see org.apache.cassandra.service.NativeTransportManagementService
 */
public class ManagementTransportProtocolTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(ManagementTransportProtocolTest.class);

    private static final int MANAGEMENT_PORT = 11211;
    private static final int NATIVE_PORT = 9042;
    private static Cluster CLUSTER;

    @BeforeClass
    public static void setupCluster() throws Exception
    {
        CLUSTER = init(Cluster.build(1)
                              .withConfig(config -> config.with(NETWORK, GOSSIP, NATIVE_PROTOCOL)
                                                          .set("start_native_transport_management", true)
                                                          .set("native_transport_management_port", MANAGEMENT_PORT))
                              .start());
    }

    @AfterClass
    public static void teardownCluster()
    {
        if  (CLUSTER == null)
            return;
        CLUSTER.close();
    }

    @Test
    public void testManagementPortRemainsUpWhenBinaryDisabled() throws Throwable
    {
        assertTrue("Regular port accessible", isAlive(NATIVE_PORT));
        assertTrue("Management port accessible", isAlive(MANAGEMENT_PORT));

        ToolResult tool = ToolRunner.invokeNodetoolJvmDtest(CLUSTER.get(1), "disablebinary");
        assertEquals(0, tool.getExitCode());
        assertFalse("Regular port NOT accessible after disable", isAlive(NATIVE_PORT));
        assertTrue("Management port still accessible after disable", isAlive(MANAGEMENT_PORT));

        tool = ToolRunner.invokeNodetoolJvmDtest(CLUSTER.get(1), "enablebinary");
        assertEquals(0, tool.getExitCode());
        assertTrue("Regular port accessible after re-enable", isAlive(NATIVE_PORT));
    }

    @Test
    public void testSelectOnSystemKeyspacesAllowed()
    {
        try (com.datastax.driver.core.Cluster c = driverOnPort(MANAGEMENT_PORT);
             Session s = c.connect())
        {
            assertFalse(s.execute("SELECT * FROM system.local").all().isEmpty());
            // system.peers is empty on single-node
            assertTrue(s.execute("SELECT * FROM system.peers").all().isEmpty());
            assertFalse(s.execute("SELECT * FROM system_schema.keyspaces").all().isEmpty());
            assertFalse(s.execute("SELECT * FROM system_schema.tables").all().isEmpty());
            assertFalse(s.execute("SELECT * FROM system_schema.columns").all().isEmpty());
        }
    }

    @Test
    public void testSelectOnVirtualKeyspacesAllowed()
    {
        try (com.datastax.driver.core.Cluster c = driverOnPort(MANAGEMENT_PORT);
             Session s = c.connect())
        {
            s.execute("SELECT * FROM system_virtual_schema.keyspaces");
            s.execute("SELECT * FROM system_virtual_schema.tables");
        }
    }

    @Test(expected = InvalidQueryException.class)
    public void testSelectOnUserKeyspaceRejected()
    {
        CLUSTER.schemaChange("CREATE TABLE IF NOT EXISTS " + KEYSPACE + ".mgmt_test (pk int PRIMARY KEY, v int)");
        try (com.datastax.driver.core.Cluster c = driverOnPort(MANAGEMENT_PORT);
             Session s = c.connect())
        {
            s.execute("SELECT * FROM " + KEYSPACE + ".mgmt_test");
        }
    }

    @Test(expected = InvalidQueryException.class)
    public void testInsertRejectedOnManagementPort()
    {
        try (com.datastax.driver.core.Cluster c = driverOnPort(MANAGEMENT_PORT);
             Session s = c.connect())
        {
            s.execute("INSERT INTO system.local (key) VALUES ('test')");
        }
    }

    @Test(expected = InvalidQueryException.class)
    public void testDDLRejectedOnManagementPort()
    {
        try (com.datastax.driver.core.Cluster c = driverOnPort(MANAGEMENT_PORT);
             Session s = c.connect())
        {
            s.execute("CREATE TABLE " + KEYSPACE + ".should_fail (pk int PRIMARY KEY)");
        }
    }

    @Test(expected = InvalidQueryException.class)
    public void testUnqualifiedSelectRejected()
    {
        try (com.datastax.driver.core.Cluster c = driverOnPort(MANAGEMENT_PORT);
             Session s = c.connect())
        {
            s.execute("SELECT * FROM local");
        }
    }

    /**
     * This is also a corner case for the driver's behavior on the management port.
     * When connecting, the driver sends a USE statement for the keyspace provided in connect().
     * If that keyspace is a system keyspace, the USE msut succeeds, and subsequent queries without
     * keyspace qualification are allowed. If that keyspace is a user keyspace, the USE should fail.
     */
    @Test
    public void testConnectWithSystemKeyspaceAllowed()
    {
        try (com.datastax.driver.core.Cluster c = driverOnPort(MANAGEMENT_PORT);
             Session s = c.connect("system_schema"))
        {
            // Driver sends USE system_schema, then we query a table without qualification
            assertFalse(s.execute("SELECT * FROM system_schema.keyspaces").all().isEmpty());
        }
    }

    @Test
    public void testConnectWithUserKeyspaceRejected()
    {
        try (com.datastax.driver.core.Cluster c = driverOnPort(MANAGEMENT_PORT);
             Session s = c.connect(KEYSPACE))
        {
            fail("Should not be able to USE a user keyspace on management port");
        }
        catch (Exception e)
        {
            logger.debug("Expected failure when connecting with user keyspace: {}", e.getMessage());
        }
    }

    private static com.datastax.driver.core.Cluster driverOnPort(int port)
    {
        return com.datastax.driver.core.Cluster.builder()
                                               .addContactPoint("127.0.0.1")
                                               .withPort(port)
                                               .build();
    }

    private static boolean isAlive(int port)
    {
        try (com.datastax.driver.core.Cluster c = driverOnPort(port);
             Session s = c.connect())
        {
            s.execute("SELECT * FROM system.local");
            return true;
        }
        catch (Exception e)
        {
            return false;
        }
    }
}
