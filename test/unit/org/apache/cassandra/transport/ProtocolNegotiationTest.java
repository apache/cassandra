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

import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Session;
import org.apache.cassandra.cql3.CQLTester;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ProtocolNegotiationTest extends CQLTester
{
    // to avoid JMX naming clashes between cluster metrics
    private int clusterId = 0;

    @BeforeClass
    public static void setup()
    {
        requireNetwork();
    }

    @Test
    public void serverSupportsV3AndV4ByDefault()
    {
        reinitializeNetwork();
        // client can explicitly request either V3 or V4
        testConnection(ProtocolVersion.V3, ProtocolVersion.V3);
        testConnection(ProtocolVersion.V4, ProtocolVersion.V4);

        // if not specified, V4 is the default
        testConnection(null, ProtocolVersion.V4);
        testConnection(ProtocolVersion.NEWEST_SUPPORTED, ProtocolVersion.V4);
    }

    @Test
    public void supportV5ConnectionWithBetaOption()
    {
        reinitializeNetwork();
        testConnection(ProtocolVersion.V5, ProtocolVersion.V5);
        testConnection(ProtocolVersion.NEWEST_BETA, ProtocolVersion.V5);
    }

    @Test
    public void olderVersionsAreUnsupported()
    {
        reinitializeNetwork();
        testConnection(ProtocolVersion.V1, ProtocolVersion.V4);
        testConnection(ProtocolVersion.V2, ProtocolVersion.V4);
    }

    private void testConnection(com.datastax.driver.core.ProtocolVersion requestedVersion,
                                com.datastax.driver.core.ProtocolVersion expectedVersion)
    {
        boolean expectError = requestedVersion != null && requestedVersion != expectedVersion;
        Cluster.Builder builder = Cluster.builder()
                                         .addContactPoints(nativeAddr)
                                         .withClusterName("Test Cluster" + clusterId++)
                                         .withPort(nativePort);

        if (requestedVersion != null)
        {
            if (requestedVersion.toInt() > org.apache.cassandra.transport.ProtocolVersion.CURRENT.asInt())
                builder = builder.allowBetaProtocolVersion();
            else
                builder = builder.withProtocolVersion(requestedVersion);
        }

        Cluster cluster = builder.build();
        try (Session session = cluster.connect())
        {
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
            e.printStackTrace();
            assertTrue(e.getMessage().contains(String.format("Host does not support protocol version %s", requestedVersion)));
        } finally {
            cluster.closeAsync();
        }
    }

}
