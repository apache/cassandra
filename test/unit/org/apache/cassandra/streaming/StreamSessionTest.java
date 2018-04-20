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

package org.apache.cassandra.streaming;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingServiceTest;

import static org.junit.Assert.assertEquals;

public class StreamSessionTest
{
    @BeforeClass
    public static void beforeClass() throws UnknownHostException
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setBackPressureStrategy(new MessagingServiceTest.MockBackPressureStrategy(Collections.emptyMap()));
        DatabaseDescriptor.setBroadcastAddress(InetAddress.getByName("127.0.0.3"));
    }

    @Test
    public void testStreamSessionUsesCorrectRemoteIp_Succeeds() throws UnknownHostException
    {
        InetAddressAndPort localAddr = InetAddressAndPort.getByName("127.0.0.1:7000");
        InetAddressAndPort preferredAddr = InetAddressAndPort.getByName("127.0.0.2:7000");
        StreamSession streamSession = new StreamSession(StreamOperation.BOOTSTRAP, localAddr,
                          new DefaultConnectionFactory(), 0, UUID.randomUUID(), PreviewKind.ALL,
                          inetAddressAndPort -> preferredAddr);

        assertEquals(preferredAddr, streamSession.getMessageSender().getConnectionId().connectionAddress());
    }

    @Test
    public void testStreamSessionUsesCorrectRemoteIpNullMapper_Succeeds() throws UnknownHostException
    {
        InetAddressAndPort localAddr = InetAddressAndPort.getByName("127.0.0.1:7000");

        StreamSession streamSession = new StreamSession(StreamOperation.BOOTSTRAP, localAddr,
                          new DefaultConnectionFactory(), 0, UUID.randomUUID(), PreviewKind.ALL, (peer) -> null);

        assertEquals(localAddr, streamSession.getMessageSender().getConnectionId().connectionAddress());
    }
}
