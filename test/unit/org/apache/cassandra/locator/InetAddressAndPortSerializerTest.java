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

package org.apache.cassandra.locator;

import java.nio.ByteBuffer;

import org.junit.Test;

import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;

import static org.apache.cassandra.locator.InetAddressAndPort.Serializer.inetAddressAndPortSerializer;
import static org.junit.Assert.assertEquals;

public class InetAddressAndPortSerializerTest
{
    @Test
    public void testRoundtrip() throws Exception
    {
        InetAddressAndPort ipv4 = InetAddressAndPort.getByName("127.0.0.1:42");
        InetAddressAndPort ipv6 = InetAddressAndPort.getByName("[2001:db8:0:0:0:ff00:42:8329]:42");

        for (MessagingService.Version version : MessagingService.Version.supportedVersions())
        {
            testAddress(ipv4, version.value);
            testAddress(ipv6, version.value);
        }
    }

    private void testAddress(InetAddressAndPort address, int version) throws Exception
    {
        ByteBuffer out;
        try (DataOutputBuffer dob = new DataOutputBuffer())
        {
            inetAddressAndPortSerializer.serialize(address, dob, version);
            out = dob.buffer();
        }
        assertEquals(out.remaining(), inetAddressAndPortSerializer.serializedSize(address, version));

        InetAddressAndPort roundtripped;
        try (DataInputBuffer dib = new DataInputBuffer(out, false))
        {
            roundtripped = inetAddressAndPortSerializer.deserialize(dib, version);
        }

        if (version >= MessagingService.VERSION_40)
        {
            assertEquals(address, roundtripped);
        }
        else
        {
            assertEquals(address.getAddress(), roundtripped.getAddress());
            assertEquals(InetAddressAndPort.getDefaultPort(), roundtripped.getPort());
        }
    }
}
