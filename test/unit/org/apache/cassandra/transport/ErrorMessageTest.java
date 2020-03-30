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

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.exceptions.CasWriteTimeoutException;
import org.apache.cassandra.exceptions.CasWriteUnknownResultException;
import org.apache.cassandra.exceptions.ReadFailureException;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.exceptions.WriteFailureException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.transport.messages.EncodeAndDecodeTestBase;
import org.apache.cassandra.transport.messages.ErrorMessage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ErrorMessageTest extends EncodeAndDecodeTestBase<ErrorMessage>
{
    private static Map<InetAddressAndPort, RequestFailureReason> failureReasonMap1;
    private static Map<InetAddressAndPort, RequestFailureReason> failureReasonMap2;

    @BeforeClass
    public static void setUpFixtures() throws UnknownHostException
    {
        failureReasonMap1 = new HashMap<>();
        failureReasonMap1.put(InetAddressAndPort.getByName("127.0.0.1"), RequestFailureReason.READ_TOO_MANY_TOMBSTONES);
        failureReasonMap1.put(InetAddressAndPort.getByName("127.0.0.2"), RequestFailureReason.READ_TOO_MANY_TOMBSTONES);
        failureReasonMap1.put(InetAddressAndPort.getByName("127.0.0.3"), RequestFailureReason.UNKNOWN);

        failureReasonMap2 = new HashMap<>();
        failureReasonMap2.put(InetAddressAndPort.getByName("127.0.0.1"), RequestFailureReason.UNKNOWN);
        failureReasonMap2.put(InetAddressAndPort.getByName("127.0.0.2"), RequestFailureReason.UNKNOWN);
    }

    @Test
    public void testV5ReadFailureSerDeser()
    {
        int receivedBlockFor = 3;
        ConsistencyLevel consistencyLevel = ConsistencyLevel.ALL;
        boolean dataPresent = false;
        ReadFailureException rfe = new ReadFailureException(consistencyLevel, receivedBlockFor, receivedBlockFor, dataPresent, failureReasonMap1);

        ErrorMessage deserialized = encodeThenDecode(ErrorMessage.fromException(rfe), ProtocolVersion.V5);
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

        ErrorMessage deserialized = encodeThenDecode(ErrorMessage.fromException(wfe), ProtocolVersion.V5);
        WriteFailureException deserializedWfe = (WriteFailureException) deserialized.error;

        assertEquals(failureReasonMap2, deserializedWfe.failureReasonByEndpoint);
        assertEquals(receivedBlockFor, deserializedWfe.received);
        assertEquals(receivedBlockFor, deserializedWfe.blockFor);
        assertEquals(consistencyLevel, deserializedWfe.consistency);
        assertEquals(writeType, deserializedWfe.writeType);
    }

    @Test
    public void testV5CasWriteTimeoutSerDeser()
    {
        int contentions = 1;
        int receivedBlockFor = 3;
        ConsistencyLevel consistencyLevel = ConsistencyLevel.SERIAL;
        CasWriteTimeoutException ex = new CasWriteTimeoutException(WriteType.CAS, consistencyLevel, 0, receivedBlockFor, contentions);

        ErrorMessage deserialized = encodeThenDecode(ErrorMessage.fromException(ex), ProtocolVersion.V5);
        assertTrue(deserialized.error instanceof CasWriteTimeoutException);
        CasWriteTimeoutException deserializedEx = (CasWriteTimeoutException) deserialized.error;

        assertEquals(WriteType.CAS, deserializedEx.writeType);
        assertEquals(contentions, deserializedEx.contentions);
        assertEquals(consistencyLevel, deserializedEx.consistency);
        assertEquals(0, deserializedEx.received);
        assertEquals(receivedBlockFor, deserializedEx.blockFor);
        assertEquals(ex.getMessage(), deserializedEx.getMessage());
        assertTrue(deserializedEx.getMessage().contains("CAS operation timed out: received 0 of 3 required responses after 1 contention retries"));
    }

    @Test
    public void testV4CasWriteTimeoutSerDeser()
    {
        int contentions = 1;
        int receivedBlockFor = 3;
        ConsistencyLevel consistencyLevel = ConsistencyLevel.SERIAL;
        CasWriteTimeoutException ex = new CasWriteTimeoutException(WriteType.CAS, consistencyLevel, receivedBlockFor, receivedBlockFor, contentions);

        ErrorMessage deserialized = encodeThenDecode(ErrorMessage.fromException(ex), ProtocolVersion.V4);
        assertTrue(deserialized.error instanceof WriteTimeoutException);
        assertFalse(deserialized.error instanceof CasWriteTimeoutException);
        WriteTimeoutException deserializedEx = (WriteTimeoutException) deserialized.error;

        assertEquals(WriteType.CAS, deserializedEx.writeType);
        assertEquals(consistencyLevel, deserializedEx.consistency);
        assertEquals(receivedBlockFor, deserializedEx.received);
        assertEquals(receivedBlockFor, deserializedEx.blockFor);
    }

    @Test
    public void testV5CasWriteResultUnknownSerDeser()
    {
        int receivedBlockFor = 3;
        ConsistencyLevel consistencyLevel = ConsistencyLevel.SERIAL;
        CasWriteUnknownResultException ex = new CasWriteUnknownResultException(consistencyLevel, receivedBlockFor, receivedBlockFor);

        ErrorMessage deserialized = encodeThenDecode(ErrorMessage.fromException(ex), ProtocolVersion.V5);
        assertTrue(deserialized.error instanceof CasWriteUnknownResultException);
        CasWriteUnknownResultException deserializedEx = (CasWriteUnknownResultException) deserialized.error;

        assertEquals(consistencyLevel, deserializedEx.consistency);
        assertEquals(receivedBlockFor, deserializedEx.received);
        assertEquals(receivedBlockFor, deserializedEx.blockFor);
        assertEquals(ex.getMessage(), deserializedEx.getMessage());
        assertTrue(deserializedEx.getMessage().contains("CAS operation result is unknown"));
    }

    @Test
    public void testV4CasWriteResultUnknownSerDeser()
    {
        int receivedBlockFor = 3;
        ConsistencyLevel consistencyLevel = ConsistencyLevel.SERIAL;
        CasWriteUnknownResultException ex = new CasWriteUnknownResultException(consistencyLevel, receivedBlockFor, receivedBlockFor);

        ErrorMessage deserialized = encodeThenDecode(ErrorMessage.fromException(ex), ProtocolVersion.V4);
        assertTrue(deserialized.error instanceof WriteTimeoutException);
        assertFalse(deserialized.error instanceof CasWriteUnknownResultException);
        WriteTimeoutException deserializedEx = (WriteTimeoutException) deserialized.error;

        assertEquals(consistencyLevel, deserializedEx.consistency);
        assertEquals(receivedBlockFor, deserializedEx.received);
        assertEquals(receivedBlockFor, deserializedEx.blockFor);
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
        Map<InetAddressAndPort, RequestFailureReason> modifiableFailureReasons = new HashMap<>(failureReasonMap1);
        ReadFailureException rfe = new ReadFailureException(ConsistencyLevel.ALL, 3, 3, false, modifiableFailureReasons);
        WriteFailureException wfe = new WriteFailureException(ConsistencyLevel.ALL, 3, 3, WriteType.SIMPLE, modifiableFailureReasons);

        modifiableFailureReasons.put(InetAddressAndPort.getByName("127.0.0.4"), RequestFailureReason.UNKNOWN);

        assertEquals(failureReasonMap1, rfe.failureReasonByEndpoint);
        assertEquals(failureReasonMap1, wfe.failureReasonByEndpoint);
    }

    protected Message.Codec<ErrorMessage> getCodec()
    {
        return ErrorMessage.codec;
    }
}
