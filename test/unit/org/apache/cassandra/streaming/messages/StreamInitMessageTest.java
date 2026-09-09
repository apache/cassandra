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

package org.apache.cassandra.streaming.messages;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.replication.ShortMutationId;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamingDataOutputPlusFixed;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class StreamInitMessageTest
{
    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testRoundTripTransferId() throws IOException
    {
        // null means this session is not part of a coordinated transfer, and must survive the round trip as null
        for (ShortMutationId transferId : new ShortMutationId[]{ null, new ShortMutationId(12, 34) })
        {
            StreamInitMessage roundTripped = roundTrip(message(transferId), MessagingService.VERSION_61);
            assertEquals(transferId, roundTripped.transferId);
            assertEquals(transferId != null, roundTripped.transferId != null);
        }
    }

    @Test
    public void testTransferIdNotSentToOlderVersions() throws IOException
    {
        // the field was added along with mutation tracking, so peers before VERSION_61 neither read nor write it
        StreamInitMessage roundTripped = roundTrip(message(new ShortMutationId(12, 34)), MessagingService.VERSION_60);
        assertNull(roundTripped.transferId);
    }

    private static StreamInitMessage message(ShortMutationId transferId)
    {
        TimeUUID planId = nextTimeUUID();
        return new StreamInitMessage(InetAddressAndPort.getByAddress(InetAddress.getLoopbackAddress()),
                                     0, planId, StreamOperation.REPAIR, planId, PreviewKind.NONE, transferId);
    }

    private static StreamInitMessage roundTrip(StreamInitMessage message, int version) throws IOException
    {
        long size = StreamInitMessage.serializer.serializedSize(message, version);
        ByteBuffer buffer = ByteBuffer.allocate(Math.toIntExact(size));

        try (StreamingDataOutputPlusFixed out = new StreamingDataOutputPlusFixed(buffer))
        {
            StreamInitMessage.serializer.serialize(message, out, version, null);
        }

        // an exact-sized buffer verifies that serializedSize agrees with what was actually written
        assertEquals("serializedSize does not match the bytes written", size, buffer.position());
        buffer.flip();

        try (DataInputBuffer in = new DataInputBuffer(buffer, false))
        {
            return StreamInitMessage.serializer.deserialize(in, version);
        }
    }
}
