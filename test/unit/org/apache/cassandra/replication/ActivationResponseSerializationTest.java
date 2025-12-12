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
package org.apache.cassandra.replication;

import java.io.IOException;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.Serializers;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.Pair;

import static org.assertj.core.api.Assertions.assertThat;

public class ActivationResponseSerializationTest
{
    private static final int VERSION = MessagingService.current_version;

    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testRoundtripSuccess() throws IOException
    {
        Pair<InetAddressAndPort, InetAddressAndPort> pair = Pair.create(InetAddressAndPort.getLocalHost(), InetAddressAndPort.getLocalHost());
        ActivationResponse response = new ActivationResponse(pair);

        try (DataOutputBuffer output = new DataOutputBuffer())
        {
            Serializers.testSerde(output, ActivationResponse.serializer, response, VERSION);
        }
    }

    @Test
    public void testRoundtripDifferentAddresses() throws IOException
    {
        InetAddressAndPort coordinator = InetAddressAndPort.getByName("127.0.0.1");
        InetAddressAndPort peer = InetAddressAndPort.getByName("127.0.0.2");
        Pair<InetAddressAndPort, InetAddressAndPort> pair = Pair.create(coordinator, peer);
        ActivationResponse response = new ActivationResponse(pair);

        try (DataOutputBuffer output = new DataOutputBuffer())
        {
            Serializers.testSerde(output, ActivationResponse.serializer, response, VERSION);
        }
    }

    @Test
    public void testEqualsAndHashCode() throws Exception
    {
        InetAddressAndPort addr1 = InetAddressAndPort.getByName("127.0.0.1");
        InetAddressAndPort addr2 = InetAddressAndPort.getByName("127.0.0.2");
        Pair<InetAddressAndPort, InetAddressAndPort> pair1 = Pair.create(addr1, addr2);
        Pair<InetAddressAndPort, InetAddressAndPort> pair2 = Pair.create(addr1, addr2);

        ActivationResponse resp1 = new ActivationResponse(pair1);
        ActivationResponse resp2 = new ActivationResponse(pair2);

        assertThat(resp1).isEqualTo(resp2);
        assertThat(resp1.hashCode()).isEqualTo(resp2.hashCode());
    }
}
