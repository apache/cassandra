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

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.transport.messages.RegisterMessage;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class ClientNotificiationsTest extends CQLTester
{
    private static Server.EventNotifier notifier = new Server.EventNotifier();

    @Before
    public void setup()
    {
        requireNetwork(builder -> builder.withEventNotifier(notifier), builder -> {});
    }

    @Parameterized.Parameter(0)
    public ProtocolVersion version;

    @Parameterized.Parameters(name = "{index}: protocol version={0}")
    public static Iterable<ProtocolVersion> params()
    {
        return ProtocolVersion.SUPPORTED;
    }

    @Test
    public void testNotifications() throws Exception
    {
        SimpleClient.Builder builder = SimpleClient.builder(nativeAddr.getHostAddress(), nativePort)
                                                   .protocolVersion(version);
        if (version.isBeta())
            builder.useBeta();

        try (SimpleClient client = builder.build())
        {
            EventHandler handler = new EventHandler();
            client.setEventHandler(handler);
            client.connect(false);
            client.execute(new RegisterMessage(Collections.singletonList(Event.Type.STATUS_CHANGE)));
            client.execute(new RegisterMessage(Collections.singletonList(Event.Type.TOPOLOGY_CHANGE)));
            client.execute(new RegisterMessage(Collections.singletonList(Event.Type.SCHEMA_CHANGE)));

            InetAddressAndPort broadcastAddress = FBUtilities.getBroadcastAddressAndPort();
            InetAddressAndPort nativeAddress = FBUtilities.getBroadcastNativeAddressAndPort();
            KeyspaceMetadata ks = KeyspaceMetadata.create("ks", KeyspaceParams.simple(1));

            // Necessary or else the NEW_NODE notification is deferred (CASSANDRA-11038)
            // (note: this works because the notifications are for the local address)
            StorageService.instance.setRpcReady(true);

            notifier.onUp(broadcastAddress);
            notifier.onDown(broadcastAddress);
            notifier.onJoinCluster(broadcastAddress);
            notifier.onMove(broadcastAddress);
            notifier.onLeaveCluster(broadcastAddress);
            notifier.onCreateKeyspace(ks);
            notifier.onAlterKeyspace(ks, ks);
            notifier.onDropKeyspace(ks, true);

            handler.assertNextEvent(Event.StatusChange.nodeUp(nativeAddress));
            handler.assertNextEvent(Event.StatusChange.nodeDown(nativeAddress));
            handler.assertNextEvent(Event.TopologyChange.newNode(nativeAddress));
            handler.assertNextEvent(Event.TopologyChange.movedNode(nativeAddress));
            handler.assertNextEvent(Event.TopologyChange.removedNode(nativeAddress));
            handler.assertNextEvent(new Event.SchemaChange(Event.SchemaChange.Change.CREATED, "ks"));
            handler.assertNextEvent(new Event.SchemaChange(Event.SchemaChange.Change.UPDATED, "ks"));
            handler.assertNextEvent(new Event.SchemaChange(Event.SchemaChange.Change.DROPPED, "ks"));
        }
    }

    static class EventHandler extends SimpleClient.SimpleEventHandler
    {
        public void assertNextEvent(Event expected)
        {
            try
            {
                Event actual = queue.poll(100, TimeUnit.MILLISECONDS);
                assertEquals(expected, actual);
            }
            catch (InterruptedException e)
            {
                throw new AssertionError(String.format("Expected event %s, but not received withing timeout", expected));
            }
        }
    }
}
