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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.SimpleClient;
import org.apache.cassandra.transport.messages.ErrorMessage;
import org.apache.cassandra.transport.messages.QueryMessage;
import org.apache.cassandra.transport.messages.ReadyMessage;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.transport.messages.StartupMessage;

import static org.apache.cassandra.config.DatabaseDescriptor.getNativeTransportPort;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;


public class StartupMessageValidationTest extends TestBaseImpl
{
    @BeforeClass
    public static void initClientSide()
    {
        DatabaseDescriptor.clientInitialization();
    }

    @Test
    public void validatesStartupOptionKeysAndValues() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build()
                                           .withNodes(1)
                                           .withConfig(config -> config.with(GOSSIP, NETWORK, NATIVE_PROTOCOL))
                                           .start()))
        {
            InetSocketHost host = hostAndPort(cluster);

            // Newline
            assertRejected(host, "invalid\nkey", "value");
            assertRejected(host, "key", "invalid\nvalue");

            // Carriage return
            assertRejected(host, "invalid\rkey", "value");
            assertRejected(host, "key", "invalid\rvalue");

            // Control characters
            assertRejected(host, "invalid\u0000key", "value");
            assertRejected(host, "key", "invalid\u0000value");

            assertRejected(host, "invalid\u0001key", "value");
            assertRejected(host, "key", "invalid\u0001value");

            // Unicode format characters: LEFT-TO-RIGHT MARK (U+200E)
            assertRejected(host, "invalid\u200Ekey", "value");
            assertRejected(host, "key", "invalid\u200Evalue");

            // Valid Unicode must be accepted.
            Map<String, String> options = startupOptions();
            options.put("custom-λ", "café-雪-🙂");

            try (SimpleClient client = SimpleClient.builder(host.address, host.port)
                                                   .protocolVersion(ProtocolVersion.V5)
                                                   .build())
            {
                client.connect(options);
            }
        }
    }

    @Test
    public void acceptsStartupRetryOnV5AfterRejection() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build()
                                           .withNodes(1)
                                           .withConfig(config -> config.with(GOSSIP, NETWORK, NATIVE_PROTOCOL))
                                           .start()))
        {
            InetSocketHost host = hostAndPort(cluster);

            try (SimpleClient client = SimpleClient.builder(host.address, host.port)
                                                   .protocolVersion(ProtocolVersion.V5)
                                                   .build())
            {
                Map<String, String> missingCqlVersion = new HashMap<>();
                missingCqlVersion.put("FOO", "bar");

                try
                {
                    client.connect(missingCqlVersion);
                    Assert.fail("Expected STARTUP validation failure");
                }
                catch (RuntimeException e)
                {
                    Assert.assertNotNull(e.getCause());
                    Assert.assertTrue("Unexpected error: " + e.getCause(),
                                      e.getCause().getMessage().contains("Missing value CQL_VERSION"));
                }

                // Both sides are still unframed: the server keeps its initial handler after a rejected STARTUP and
                // SimpleClient switches to V5 framing only on READY, so a second STARTUP establishes the connection.
                Message.Response response = client.execute(new StartupMessage(startupOptions()));
                Assert.assertTrue("Expected READY but received: " + response, response instanceof ReadyMessage);

                // The framed pipeline installed by the retry is usable.
                response = client.execute(new QueryMessage("SELECT key FROM system.local", QueryOptions.DEFAULT));
                Assert.assertTrue("Expected rows but received: " + response, response instanceof ResultMessage.Rows);
            }
        }
    }

    @Test
    public void oversizedStartupAfterFailedStartupIsRejectedBeforeDecoding() throws Throwable
    {
        try (WithProperties ignored = new WithProperties()
                                      .set(CassandraRelevantProperties.STARTUP_CLIENT_OPTION_MAX_COUNT, "3")
                                      .set(CassandraRelevantProperties.STARTUP_CLIENT_OPTION_KEY_MAX_LENGTH, "16")
                                      .set(CassandraRelevantProperties.STARTUP_CLIENT_OPTION_VALUE_MAX_LENGTH, "16"))
        {
            try (Cluster cluster = init(Cluster.build()
                                               .withNodes(1)
                                               .withConfig(config -> config.with(GOSSIP, NETWORK, NATIVE_PROTOCOL))
                                               .start()))
            {
                InetSocketHost host = hostAndPort(cluster);

                Map<String, String> invalidOptions = new HashMap<>();
                invalidOptions.put("FOO", "bar");

                try (SimpleClient client = SimpleClient.builder(host.address, host.port)
                                                       .protocolVersion(ProtocolVersion.V4)
                                                       .build())
                {
                    try
                    {
                        client.connect(invalidOptions);
                        Assert.fail("Expected STARTUP validation failure");
                    }
                    catch (RuntimeException e)
                    {
                        Assert.assertNotNull(e.getCause());
                        Assert.assertTrue("Unexpected error: " + e.getCause(),
                                          e.getCause().getMessage().contains("Missing value CQL_VERSION"));
                    }

                    Map<String, String> oversizedOptions = new LinkedHashMap<>();
                    oversizedOptions.put(StartupMessage.CQL_VERSION, "1234567890123456");
                    oversizedOptions.put("a".repeat(17), "b".repeat(17));
                    oversizedOptions.put("c".repeat(18), "d".repeat(18));

                    Message.Response response =
                    client.execute(new StartupMessage(oversizedOptions), false);

                    Assert.assertTrue("Expected ErrorMessage but received: " + response,
                                      response instanceof ErrorMessage);

                    ErrorMessage error = (ErrorMessage) response;

                    Assert.assertTrue("Expected envelope size rejection but received: " + error.error,
                                      error.error.getMessage().contains("Request is too big"));
                }
            }
        }
    }

    private static void assertRejected(InetSocketHost host,
                                       String key,
                                       String value)
    {
        Map<String, String> options = startupOptions();
        options.put(key, value);

        try (SimpleClient client = SimpleClient.builder(host.address, host.port)
                                               .protocolVersion(ProtocolVersion.V5)
                                               .build())
        {
            client.connect(options);
            Assert.fail("Expected STARTUP validation to reject key: " + key
                        + ", value: " + value);
        }
        catch (RuntimeException e)
        {
            Assert.assertNotNull("Expected server error", e.getCause());
            Assert.assertTrue("Expected ProtocolException but got: " + e.getCause(),
                              e.getCause() instanceof ProtocolException);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static Map<String, String> startupOptions()
    {
        Map<String, String> options = new HashMap<>();
        options.put(StartupMessage.CQL_VERSION, "3.0.0");
        return options;
    }

    private static InetSocketHost hostAndPort(Cluster cluster)
    {
        String address = cluster.get(1).config().broadcastAddress()
                                .getAddress().getHostAddress();

        int port = cluster.get(1).callOnInstance(() -> getNativeTransportPort());

        return new InetSocketHost(address, port);
    }

    private static class InetSocketHost
    {
        final String address;
        final int port;

        InetSocketHost(String address, int port)
        {
            this.address = address;
            this.port = port;
        }
    }
}

