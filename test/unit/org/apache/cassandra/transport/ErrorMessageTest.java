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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.exceptions.ReadFailureException;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.exceptions.WriteFailureException;
import org.apache.cassandra.transport.messages.ErrorMessage;

import static org.junit.Assert.assertEquals;

public class ErrorMessageTest
{
    private static Map<InetAddress, RequestFailureReason> failureReasonMap1;
    private static Map<InetAddress, RequestFailureReason> failureReasonMap2;

    @BeforeClass
    public static void setUpFixtures() throws UnknownHostException
    {
        failureReasonMap1 = new HashMap<>();
        failureReasonMap1.put(InetAddress.getByName("127.0.0.1"), RequestFailureReason.READ_TOO_MANY_TOMBSTONES);
        failureReasonMap1.put(InetAddress.getByName("127.0.0.2"), RequestFailureReason.READ_TOO_MANY_TOMBSTONES);
        failureReasonMap1.put(InetAddress.getByName("127.0.0.3"), RequestFailureReason.UNKNOWN);

        failureReasonMap2 = new HashMap<>();
        failureReasonMap2.put(InetAddress.getByName("127.0.0.1"), RequestFailureReason.UNKNOWN);
        failureReasonMap2.put(InetAddress.getByName("127.0.0.2"), RequestFailureReason.UNKNOWN);
    }

    @Test
    public void testV5ReadFailureSerDeser()
    {
        int receivedBlockFor = 3;
        ConsistencyLevel consistencyLevel = ConsistencyLevel.ALL;
        boolean dataPresent = false;
        ReadFailureException rfe = new ReadFailureException(consistencyLevel, receivedBlockFor, receivedBlockFor, dataPresent, failureReasonMap1);

        ErrorMessage deserialized = serializeAndGetDeserializedErrorMessage(ErrorMessage.fromException(rfe), ProtocolVersion.V5);
        ReadFailureException deserializedRfe = (ReadFailureException) deserialized.error;

        assertEquals(failureReasonMap1, deserializedRfe.failureReasonByEndpoint);
        assertEquals(receivedBlockFor, deserializedRfe.received);
        assertEquals(receivedBlockFor, deserializedRfe.blockFor);
        assertEquals(consistencyLevel, deserializedRfe.consistency);
        assertEquals(dataPresent, deserializedRfe.dataPresent);
    }

    @Test
    public void testV5WriteFailureSerDeser()
    {
        int receivedBlockFor = 3;
        ConsistencyLevel consistencyLevel = ConsistencyLevel.ALL;
        WriteType writeType = WriteType.SIMPLE;
        WriteFailureException wfe = new WriteFailureException(consistencyLevel, receivedBlockFor, receivedBlockFor, writeType, failureReasonMap2);

        ErrorMessage deserialized = serializeAndGetDeserializedErrorMessage(ErrorMessage.fromException(wfe), ProtocolVersion.V5);
        WriteFailureException deserializedWfe = (WriteFailureException) deserialized.error;

        assertEquals(failureReasonMap2, deserializedWfe.failureReasonByEndpoint);
        assertEquals(receivedBlockFor, deserializedWfe.received);
        assertEquals(receivedBlockFor, deserializedWfe.blockFor);
        assertEquals(consistencyLevel, deserializedWfe.consistency);
        assertEquals(writeType, deserializedWfe.writeType);
    }

    /**
     * Make sure that the map passed in to create a Read/WriteFailureException is copied
     * so later modifications to the map passed in don't affect the map in the exception.
     *
     * This is to prevent potential issues in serialization if the map created in
     * ReadCallback/AbstractWriteResponseHandler is modified due to a delayed failure
     * response after the exception is created.
     */
    @Test
    public void testRequestFailureExceptionMakesCopy() throws UnknownHostException
    {
        Map<InetAddress, RequestFailureReason> modifiableFailureReasons = new HashMap<>(failureReasonMap1);
        ReadFailureException rfe = new ReadFailureException(ConsistencyLevel.ALL, 3, 3, false, modifiableFailureReasons);
        WriteFailureException wfe = new WriteFailureException(ConsistencyLevel.ALL, 3, 3, WriteType.SIMPLE, modifiableFailureReasons);

        modifiableFailureReasons.put(InetAddress.getByName("127.0.0.4"), RequestFailureReason.UNKNOWN);

        assertEquals(failureReasonMap1, rfe.failureReasonByEndpoint);
        assertEquals(failureReasonMap1, wfe.failureReasonByEndpoint);
    }

    private ErrorMessage serializeAndGetDeserializedErrorMessage(ErrorMessage message, ProtocolVersion version)
    {
        ByteBuf buffer = Unpooled.buffer(ErrorMessage.codec.encodedSize(message, version));
        ErrorMessage.codec.encode(message, buffer, version);
        return ErrorMessage.codec.decode(buffer, version);
    }
}
