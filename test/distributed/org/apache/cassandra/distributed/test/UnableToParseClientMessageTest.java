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
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.buffer.Unpooled;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.transport.WrappedSimpleClient;

/**
 * If a client sends a message that can not be parsed by the server then we need to detect this and update metrics
 * for monitoring.
 *
 * An issue was found between 2.1 to 3.0 upgrades with regards to paging serialization. Since
 * this is a serialization issue we hit similar paths by sending bad bytes to the server, so can simulate the mixed-mode
 * paging issue without needing to send proper messages.
 */
public class UnableToParseClientMessageTest extends TestBaseImpl
{
    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void badMessageCausesProtocolException() throws IOException, InterruptedException
    {
        try (Cluster cluster = init(Cluster.build(1).withConfig(c -> c.with(Feature.values())).start()))
        {
            // write gibberish to the native protocol
            IInvokableInstance node = cluster.get(1);
            // make sure everything is fine at the start
            node.runOnInstance(() -> {
                Assert.assertEquals(0, CassandraMetricsRegistry.Metrics.getMeters()
                                                                       .get("org.apache.cassandra.metrics.Client.ProtocolException")
                                                                       .getCount());
                Assert.assertEquals(0, CassandraMetricsRegistry.Metrics.getMeters()
                                                                       .get("org.apache.cassandra.metrics.Client.UnknownException")
                                                                       .getCount());
            });

            try (WrappedSimpleClient client = new WrappedSimpleClient("127.0.0.1", 9042))
            {
                client.connect(false, true);

                // this should return a failed response
                String response = client.write(Unpooled.wrappedBuffer("This is just a test".getBytes(StandardCharsets.UTF_8)), false).toString();
                Assert.assertTrue("Resposne '" + response + "' expected to contain 'Invalid or unsupported protocol version (84); the lowest supported version is 3 and the greatest is 4'",
                                  response.contains("Invalid or unsupported protocol version (84)"));

                node.runOnInstance(() -> {
                    Util.spinAssertEquals(1L,
                                          () -> CassandraMetricsRegistry.Metrics.getMeters()
                                                                                .get("org.apache.cassandra.metrics.Client.ProtocolException")
                                                                                .getCount(),
                                          10);

                    Assert.assertEquals(0, CassandraMetricsRegistry.Metrics.getMeters()
                                                                           .get("org.apache.cassandra.metrics.Client.UnknownException")
                                                                           .getCount());
                });
                List<String> results = node.logs().grep("Protocol exception with client networking").getResult();
                results.forEach(s -> Assert.assertTrue("Expected logs '" + s + "' to contain: Invalid or unsupported protocol version (84)",
                                                       s.contains("Invalid or unsupported protocol version (84)")));
                Assert.assertEquals(1, results.size()); // this logs less offtan than metrics as the log has a nospamlogger wrapper
            }
        }
    }
}
