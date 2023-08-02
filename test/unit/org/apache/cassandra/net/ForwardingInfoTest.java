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

package org.apache.cassandra.net;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.InetAddressAndPort;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ForwardingInfoTest
{

    @Test
    public void testSupportedVersions() throws Exception
    {
        for (MessagingService.Version version : MessagingService.Version.supportedVersions())
            testVersion(version.value);
    }

    private void testVersion(int version) throws Exception
    {
        InetAddressAndPort.initializeDefaultPort(65532);
        List<InetAddressAndPort> addresses = ImmutableList.of(InetAddressAndPort.getByNameOverrideDefaults("127.0.0.1", 42),
                                                              InetAddressAndPort.getByName("127.0.0.1"),
                                                              InetAddressAndPort.getByName("127.0.0.1:7000"),
                                                              InetAddressAndPort.getByNameOverrideDefaults("2001:0db8:0000:0000:0000:ff00:0042:8329", 42),
                                                              InetAddressAndPort.getByName("2001:0db8:0000:0000:0000:ff00:0042:8329"),
                                                              InetAddressAndPort.getByName("[2001:0db8:0000:0000:0000:ff00:0042:8329]:7000"));

        ForwardingInfo ftc = new ForwardingInfo(addresses, new long[] { 44, 45, 46, 47, 48, 49 });
        ByteBuffer buffer;
        try (DataOutputBuffer dob = new DataOutputBuffer())
        {
            ForwardingInfo.serializer.serialize(ftc, dob, version);
            buffer = dob.buffer();
        }

        assertEquals(buffer.remaining(), ForwardingInfo.serializer.serializedSize(ftc, version));

        ForwardingInfo deserialized;
        try (DataInputBuffer dib = new DataInputBuffer(buffer, false))
        {
            deserialized = ForwardingInfo.serializer.deserialize(dib, version);
        }

        assertTrue(Arrays.equals(ftc.messageIds, deserialized.messageIds));

        Iterator<InetAddressAndPort> iterator = deserialized.targets.iterator();
        if (version >= MessagingService.VERSION_40)
        {
            for (int ii = 0; ii < addresses.size(); ii++)
            {
                InetAddressAndPort original = addresses.get(ii);
                InetAddressAndPort roundtripped = iterator.next();
                assertEquals(original, roundtripped);
            }
        }
        else
        {
            for (int ii = 0; ii < addresses.size(); ii++)
            {
                InetAddressAndPort original = addresses.get(ii);
                InetAddressAndPort roundtripped = iterator.next();
                assertEquals(original.getAddress(), roundtripped.getAddress());
                //3.0 can't send port numbers so you get the defaults
                assertEquals(65532, roundtripped.getPort());
            }
        }
    }
}
