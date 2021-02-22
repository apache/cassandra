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

import java.io.IOException;
import java.util.Random;

import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.exceptions.TransportException;
import org.apache.cassandra.transport.messages.OptionsMessage;
import org.apache.cassandra.transport.messages.QueryMessage;
import org.apache.cassandra.transport.messages.StartupMessage;

import static com.datastax.driver.core.ProtocolVersion.NEWEST_BETA;
import static com.datastax.driver.core.ProtocolVersion.NEWEST_SUPPORTED;
import static com.datastax.driver.core.ProtocolVersion.V1;
import static com.datastax.driver.core.ProtocolVersion.V2;
import static com.datastax.driver.core.ProtocolVersion.V3;
import static com.datastax.driver.core.ProtocolVersion.V4;
import static com.datastax.driver.core.ProtocolVersion.V5;
import static com.datastax.driver.core.ProtocolVersion.V6;
import static org.apache.cassandra.transport.messages.StartupMessage.CQL_VERSION;
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
        requireNetwork();
    }

    @Before
    public void initNetwork()
    {
        reinitializeNetwork();
    }

    @Test
    public void serverSupportsV3AndV4AndV5ByDefault()
    {
        // client can explicitly request either V3, V4 or V5
        testConnection(V3, V3);
        testConnection(V4, V4);
        testConnection(V5, V5);

        // if not specified, V5 is the default
        testConnection(null, V5);
        testConnection(NEWEST_SUPPORTED, V5);
    }

    @Test
    public void supportV6ConnectionWithBetaOption()
    {
        testConnection(V6, V6);
        testConnection(NEWEST_BETA, V6);
    }

    @Test
    public void olderVersionsAreUnsupported()
    {
        testConnection(V1, V4);
        testConnection(V2, V4);
    }

    @Test
    public void preNegotiationResponsesHaveCorrectStreamId()
    {
        ProtocolVersion.SUPPORTED.forEach(this::testStreamIdsAcrossNegotiation);
    }

    @Test
    public void validateReceivedMessageVersionMatchesNegotiated()
    {
        ProtocolVersion.SUPPORTED.forEach(this::validateMessageVersion);
    }

    private void testStreamIdsAcrossNegotiation(ProtocolVersion version)
    {
        long seed = System.currentTimeMillis();
        Random random = new Random(seed);
        SimpleClient.Builder builder = SimpleClient.builder(nativeAddr.getHostAddress(), nativePort);
        if (version.isBeta())
            builder.useBeta();
        else
            builder.protocolVersion(version);

        try (SimpleClient client = builder.build())
        {
            client.establishConnection();
            // Before STARTUP the client hasn't yet negotiated a protocol version.
            // All OPTIONS messages are received by the intial connection handler.
            OptionsMessage options = new OptionsMessage();
            for (int i = 0; i < 100; i++)
            {
                int streamId = random.nextInt(254) + 1;
                options.setStreamId(streamId);
                Message.Response response = client.execute(options);
                assertEquals(String.format("StreamId mismatch; version: %s, seed: %s, iter: %s, expected: %s, actual: %s",
                                           version, seed, i, streamId, response.getStreamId()),
                             streamId, response.getStreamId());
            }

            int streamId = random.nextInt(254) + 1;
            // STARTUP messages are handled by the initial connection handler
            StartupMessage startup = new StartupMessage(ImmutableMap.of(CQL_VERSION, QueryProcessor.CQL_VERSION.toString()));
            startup.setStreamId(streamId);
            Message.Response response = client.execute(startup);
            assertEquals(String.format("StreamId mismatch after negotiation; version: %s, expected: %s, actual %s",
                                       version, streamId, response.getStreamId()),
                         streamId, response.getStreamId());

            // Following STARTUP, the version specific handlers are fully responsible for processing messages
            QueryMessage query = new QueryMessage("SELECT * FROM system.local", QueryOptions.DEFAULT);
            query.setStreamId(streamId);
            response = client.execute(query);
            assertEquals(String.format("StreamId mismatch after negotiation; version: %s, expected: %s, actual %s",
                                       version, streamId, response.getStreamId()),
                         streamId, response.getStreamId());
        }
        catch (IOException e)
        {
            e.printStackTrace();
            fail("Error establishing connection");
        }
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
            session.execute("SELECT * FROM system.local");
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

    private void validateMessageVersion(ProtocolVersion version)
    {
        SimpleClient.Builder builder = SimpleClient.builder(nativeAddr.getHostAddress(), nativePort)
                                                   .protocolVersion(version);
        if (version.isBeta())
            builder.useBeta();

        Random r = new Random();
        ProtocolVersion wrongVersion = version;
        while (wrongVersion.isSmallerThan(ProtocolVersion.MIN_SUPPORTED_VERSION) || wrongVersion == version)
            wrongVersion = ProtocolVersion.values()[r.nextInt(ProtocolVersion.values().length - 1)];

        try (SimpleClient client = builder.build().connect(false))
        {
            // The connection has been negotiated to use $version. Force the next message to be
            // encoded with a different version and it should trigger a ProtocolException
            final ProtocolVersion v = wrongVersion;
            QueryMessage query = new QueryMessage("SELECT * FROM system.local", QueryOptions.DEFAULT)
            {
                @Override
                public Envelope encode(ProtocolVersion originalVersion)
                {
                    return super.encode(v);
                }
            };
            try
            {
                client.execute(query);
                fail("Expected a protocol exception");
            }
            catch (RuntimeException e)
            {
                assertTrue(e.getCause() instanceof TransportException);
                assertTrue(e.getCause().getMessage().startsWith("Invalid message version"));
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
            fail("Error establishing connection");
        }
    }
}
