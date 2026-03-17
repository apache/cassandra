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
import java.net.InetSocketAddress;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.transport.SimpleClient;

public class GracefulDisconnectIT extends TestBaseImpl
{
    @Test
    public void testGracefulDisconnectIntegrated() throws IOException
    {
        try (Cluster cluster = Cluster.build(1).withConfig(config -> config.with(Feature.NATIVE_PROTOCOL, Feature.GOSSIP)).start())
        {
            cluster.get(1).runOnInstance(() -> {
                org.apache.cassandra.config.DatabaseDescriptor.setGracefulDisconnectMaxDrainMs(10000);
            });
            InetSocketAddress nativeAddr = cluster.get(1).config().broadcastAddress();
            SimpleClient client = SimpleClient.builder(nativeAddr.getHostString(), 9042)
                                              .build();
            client.connect(false);
        }
        catch (Exception e)
        {
            Assert.fail(e.getMessage());
        }
    }
}
