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

package org.apache.cassandra.transport;

import java.net.InetAddress;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ProtocolVersion;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;

import static org.apache.cassandra.transport.ProtocolTestHelper.cleanupPeers;
import static org.apache.cassandra.transport.ProtocolTestHelper.setStaticLimitInConfig;
import static org.apache.cassandra.transport.ProtocolTestHelper.setupPeer;
import static org.apache.cassandra.transport.ProtocolTestHelper.updatePeerInfo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ProtocolNegotiationTest extends CQLTester
{
    // to avoid JMX naming clashes between cluster metrics
    private int clusterId = 0;

    @BeforeClass
    public static void setup()
    {
        prepareNetwork();
    }

    @Before
    public void clearConfig()
    {
        setStaticLimitInConfig(null);
    }

    @Test
    public void serverSupportsV3AndV4ByDefault() throws Throwable
    {
        reinitializeNetwork();
        // client can explicitly request either V3 or V4
        testConnection(ProtocolVersion.V3, ProtocolVersion.V3);
        testConnection(ProtocolVersion.V4, ProtocolVersion.V4);

        // if not specified, V4 is the default
        testConnection(null, ProtocolVersion.V4);
    }

    @Test
    public void testStaticLimit() throws Throwable
    {
        try
        {
            reinitializeNetwork();
            // No limit enforced to start
            assertEquals(Integer.MIN_VALUE, DatabaseDescriptor.getNativeProtocolMaxVersionOverride());
            testConnection(null, ProtocolVersion.V4);

            // Update DatabaseDescriptor, then re-initialise the server to force it to read it
            setStaticLimitInConfig(ProtocolVersion.V3.toInt());
            reinitializeNetwork();
            assertEquals(3, DatabaseDescriptor.getNativeProtocolMaxVersionOverride());
            testConnection(ProtocolVersion.V4, ProtocolVersion.V3);
            testConnection(ProtocolVersion.V3, ProtocolVersion.V3);
            testConnection(null, ProtocolVersion.V3);
        } finally {
            setStaticLimitInConfig(null);
        }
    }

    @Test
    public void testDynamicLimit() throws Throwable
    {
        InetAddress peer1 = setupPeer("127.1.0.1", "2.2.0");
        InetAddress peer2 = setupPeer("127.1.0.2", "2.2.0");
        InetAddress peer3 = setupPeer("127.1.0.3", "2.2.0");
        reinitializeNetwork();
        try
        {
            // legacy peers means max negotiable version is V3
            testConnection(ProtocolVersion.V4, ProtocolVersion.V3);
            testConnection(ProtocolVersion.V3, ProtocolVersion.V3);
            testConnection(null, ProtocolVersion.V3);

            // receive notification that 2 peers have upgraded to a version that fully supports V4
            updatePeerInfo(peer1, "3.0.0");
            updatePeerInfo(peer2, "3.0.0");
            updateMaxNegotiableProtocolVersion();
            // version should still be capped
            testConnection(ProtocolVersion.V4, ProtocolVersion.V3);
            testConnection(ProtocolVersion.V3, ProtocolVersion.V3);
            testConnection(null, ProtocolVersion.V3);

            // no legacy peers so V4 is negotiable
            // after the last peer upgrades, cap should be lifted
            updatePeerInfo(peer3, "3.0.0");
            updateMaxNegotiableProtocolVersion();
            testConnection(ProtocolVersion.V4, ProtocolVersion.V4);
            testConnection(ProtocolVersion.V3, ProtocolVersion.V3);
            testConnection(null, ProtocolVersion.V4);
        } finally {
            cleanupPeers(peer1, peer2, peer3);
        }
    }

    private void testConnection(com.datastax.driver.core.ProtocolVersion requestedVersion,
                                com.datastax.driver.core.ProtocolVersion expectedVersion)
    {
        long start = System.nanoTime();
        boolean expectError = requestedVersion != null && requestedVersion != expectedVersion;
        Cluster.Builder builder = Cluster.builder()
                                         .addContactPoints(nativeAddr)
                                         .withClusterName("Test Cluster" + clusterId++)
                                         .withPort(nativePort);

        if (requestedVersion != null)
            builder = builder.withProtocolVersion(requestedVersion) ;

        Cluster cluster = builder.build();
        logger.info("Setting up cluster took {}ms", TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS));
        start = System.nanoTime();
        try {
            cluster.connect();
            if (expectError)
                fail("Expected a protocol exception");
        }
        catch (Exception e)
        {
            if (!expectError)
            {
                e.printStackTrace();
                fail("Did not expect any exception");
            }

            assertTrue(e.getMessage().contains(String.format("Host does not support protocol version %s but %s", requestedVersion, expectedVersion)));
        } finally {
            logger.info("Testing connection took {}ms", TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS));
            start = System.nanoTime();
            cluster.closeAsync();
            logger.info("Tearing down cluster connection took {}ms", TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS));

        }
    }

}
