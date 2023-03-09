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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.tracing.Tracing.TraceType;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.FreeRunningClock;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.net.Message.serializer;
import static org.apache.cassandra.net.MessagingService.VERSION_40;
import static org.apache.cassandra.net.NoPayload.noPayload;
import static org.apache.cassandra.net.ParamType.RESPOND_TO;
import static org.apache.cassandra.net.ParamType.TRACE_SESSION;
import static org.apache.cassandra.net.ParamType.TRACE_TYPE;
import static org.apache.cassandra.utils.MonotonicClock.Global.approxTime;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;

import static org.junit.Assert.*;

public class MessageTest
{
    @BeforeClass
    public static void setUpClass() throws Exception
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setCrossNodeTimeout(true);

        Verb._TEST_2.unsafeSetSerializer(() -> new IVersionedSerializer<Integer>()
        {
            public void serialize(Integer value, DataOutputPlus out, int version) throws IOException
            {
                out.writeInt(value);
            }

            public Integer deserialize(DataInputPlus in, int version) throws IOException
            {
                return in.readInt();
            }

            public long serializedSize(Integer value, int version)
            {
                return 4;
            }
        });
    }

    @AfterClass
    public static void tearDownClass() throws Exception
    {
        Verb._TEST_2.unsafeSetSerializer(() -> NoPayload.serializer);
    }

    @Test
    public void testInferMessageSize() throws Exception
    {
        Message<Integer> msg =
            Message.builder(Verb._TEST_2, 37)
                   .withId(1)
                   .from(FBUtilities.getLocalAddressAndPort())
                   .withCreatedAt(approxTime.now())
                   .withExpiresAt(approxTime.now())
                   .withFlag(MessageFlag.CALL_BACK_ON_FAILURE)
                   .withFlag(MessageFlag.TRACK_REPAIRED_DATA)
                   .withParam(TRACE_TYPE, TraceType.QUERY)
                   .withParam(TRACE_SESSION, nextTimeUUID())
                   .build();

        testInferMessageSize(msg, VERSION_40);
    }

    private void testInferMessageSize(Message msg, int version) throws Exception
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            serializer.serialize(msg, out, version);
            assertEquals(msg.serializedSize(version), out.getLength());

            ByteBuffer buffer = out.buffer();

            int payloadSize = (int) msg.verb().serializer().serializedSize(msg.payload, version);
            int serializedSize = msg.serializedSize(version);

            // should return -1 - fail to infer size - for all lengths of buffer until payload length can be read
            for (int limit = 0; limit < serializedSize - payloadSize; limit++)
                assertEquals(-1, serializer.inferMessageSize(buffer, 0, limit));

            // once payload size can be read, should correctly infer message size
            for (int limit = serializedSize - payloadSize; limit < serializedSize; limit++)
                assertEquals(serializedSize, serializer.inferMessageSize(buffer, 0, limit));
        }
    }

    @Test
    public void testBuilder()
    {
        long id = 1;
        InetAddressAndPort from = FBUtilities.getLocalAddressAndPort();
        long createAtNanos = approxTime.now();
        long expiresAtNanos = createAtNanos + TimeUnit.SECONDS.toNanos(1);
        TraceType traceType = TraceType.QUERY;
        TimeUUID traceSession = nextTimeUUID();

        Message<NoPayload> msg =
            Message.builder(Verb._TEST_1, noPayload)
                   .withId(1)
                   .from(from)
                   .withCreatedAt(createAtNanos)
                   .withExpiresAt(expiresAtNanos)
                   .withFlag(MessageFlag.CALL_BACK_ON_FAILURE)
                   .withParam(TRACE_TYPE, TraceType.QUERY)
                   .withParam(TRACE_SESSION, traceSession)
                   .build();

        assertEquals(id, msg.id());
        assertEquals(from, msg.from());
        assertEquals(createAtNanos, msg.createdAtNanos());
        assertEquals(expiresAtNanos, msg.expiresAtNanos());
        assertTrue(msg.callBackOnFailure());
        assertFalse(msg.trackRepairedData());
        assertEquals(traceType, msg.traceType());
        assertEquals(traceSession, msg.traceSession());
        assertNull(msg.forwardTo());
        assertEquals(from, msg.respondTo());
    }

    @Test
    public void testCycleNoPayload() throws IOException
    {
        Message<NoPayload> msg =
            Message.builder(Verb._TEST_1, noPayload)
                   .withId(1)
                   .from(FBUtilities.getLocalAddressAndPort())
                   .withCreatedAt(approxTime.now())
                   .withExpiresAt(approxTime.now() + TimeUnit.SECONDS.toNanos(1))
                   .withFlag(MessageFlag.CALL_BACK_ON_FAILURE)
                   .withParam(TRACE_SESSION, nextTimeUUID())
                   .build();
        testCycle(msg);
    }

    @Test
    public void testCycleWithPayload() throws Exception
    {
        testCycle(Message.out(Verb._TEST_2, 42));
        testCycle(Message.outWithFlag(Verb._TEST_2, 42, MessageFlag.CALL_BACK_ON_FAILURE));
        testCycle(Message.outWithFlags(Verb._TEST_2, 42, MessageFlag.CALL_BACK_ON_FAILURE, MessageFlag.TRACK_REPAIRED_DATA));
        testCycle(Message.outWithParam(1, Verb._TEST_2, 42, RESPOND_TO, FBUtilities.getBroadcastAddressAndPort()));
    }

    @Test
    public void testFailureResponse() throws IOException
    {
        long expiresAt = approxTime.now();
        Message<RequestFailureReason> msg = Message.failureResponse(1, expiresAt, RequestFailureReason.INCOMPATIBLE_SCHEMA);

        assertEquals(1, msg.id());
        assertEquals(Verb.FAILURE_RSP, msg.verb());
        assertEquals(expiresAt, msg.expiresAtNanos());
        assertEquals(RequestFailureReason.INCOMPATIBLE_SCHEMA, msg.payload);
        assertTrue(msg.isFailureResponse());

        testCycle(msg);
    }

    @Test
    public void testBuilderAddTraceHeaderWhenTraceSessionPresent()
    {
        Stream.of(TraceType.values()).forEach(this::testAddTraceHeaderWithType);
    }

    @Test
    public void testBuilderNotAddTraceHeaderWithNoTraceSession()
    {
        Message<NoPayload> msg = Message.builder(Verb._TEST_1, noPayload).withTracingParams().build();
        assertNull(msg.header.traceSession());
    }

    @Test
    public void testCustomParams() throws CharacterCodingException, IOException
    {
        long id = 1;
        InetAddressAndPort from = FBUtilities.getLocalAddressAndPort();

        Message<NoPayload> msg =
            Message.builder(Verb._TEST_1, noPayload)
                   .withId(1)
                   .from(from)
                   .withCustomParam("custom1", "custom1value".getBytes(StandardCharsets.UTF_8))
                   .withCustomParam("custom2", "custom2value".getBytes(StandardCharsets.UTF_8))
                   .build();

        assertEquals(id, msg.id());
        assertEquals(from, msg.from());
        assertEquals(2, msg.header.customParams().size());
        assertEquals("custom1value", new String(msg.header.customParams().get("custom1"), StandardCharsets.UTF_8));
        assertEquals("custom2value", new String(msg.header.customParams().get("custom2"), StandardCharsets.UTF_8));

        DataOutputBuffer out = DataOutputBuffer.scratchBuffer.get();
        Message.serializer.serialize(msg, out, VERSION_40);
        DataInputBuffer in = new DataInputBuffer(out.buffer(), true);
        msg = Message.serializer.deserialize(in, from, VERSION_40);

        assertEquals(id, msg.id());
        assertEquals(from, msg.from());
        assertEquals(2, msg.header.customParams().size());
        assertEquals("custom1value", new String(msg.header.customParams().get("custom1"), StandardCharsets.UTF_8));
        assertEquals("custom2value", new String(msg.header.customParams().get("custom2"), StandardCharsets.UTF_8));
    }

    private void testAddTraceHeaderWithType(TraceType traceType)
    {
        try
        {
            TimeUUID sessionId = Tracing.instance.newSession(traceType);
            Message<NoPayload> msg = Message.builder(Verb._TEST_1, noPayload).withTracingParams().build();
            assertEquals(sessionId, msg.header.traceSession());
            assertEquals(traceType, msg.header.traceType());
        }
        finally
        {
            Tracing.instance.stopSession();
        }
    }

    private void testCycle(Message msg) throws IOException
    {
        testCycle(msg, VERSION_40);
    }

    // serialize (using both variants, all in one or header then rest), verify serialized size, deserialize, compare to the original
    private void testCycle(Message msg, int version) throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            serializer.serialize(msg, out, version);
            assertEquals(msg.serializedSize(version), out.getLength());

            // deserialize the message in one go, compare outcomes
            try (DataInputBuffer in = new DataInputBuffer(out.buffer(), true))
            {
                Message msgOut = serializer.deserialize(in, msg.from(), version);
                assertEquals(0, in.available());
                assertMessagesEqual(msg, msgOut);
            }

            // extract header first, then deserialize the rest of the message and compare outcomes
            ByteBuffer buffer = out.buffer();
            try (DataInputBuffer in = new DataInputBuffer(out.buffer(), false))
            {
                Message.Header headerOut = serializer.extractHeader(buffer, msg.from(), approxTime.now(), version);
                Message msgOut = serializer.deserialize(in, headerOut, version);
                assertEquals(0, in.available());
                assertMessagesEqual(msg, msgOut);
            }
        }
    }

    private static void assertMessagesEqual(Message msg1, Message msg2)
    {
        assertEquals(msg1.id(),                msg2.id());
        assertEquals(msg1.verb(),              msg2.verb());
        assertEquals(msg1.callBackOnFailure(), msg2.callBackOnFailure());
        assertEquals(msg1.trackRepairedData(), msg2.trackRepairedData());
        assertEquals(msg1.traceType(),         msg2.traceType());
        assertEquals(msg1.traceSession(),      msg2.traceSession());
        assertEquals(msg1.respondTo(),         msg2.respondTo());
        assertEquals(msg1.forwardTo(),         msg2.forwardTo());

        Object payload1 = msg1.payload;
        Object payload2 = msg2.payload;

        if (null == payload1)
            assertTrue(payload2 == noPayload || payload2 == null);
        else if (null == payload2)
            assertSame(payload1, noPayload);
        else
            assertEquals(payload1, payload2);
    }

    @Test
    public void testCreationTime()
    {
        long remoteTime = 1632087572480L; // 10111110000000000000000000000000000000000
        long localTime  = 1632087572479L; // 10111101111111111111111111111111111111111
        FreeRunningClock localClock  = new FreeRunningClock(TimeUnit.DAYS.toNanos(1), localTime, 0);

        int remoteCreatedAt = (int) (remoteTime & 0x00000000FFFFFFFFL);

        long localTimeNanos = localClock.now();
        assertTrue( Message.Serializer.calculateCreationTimeNanos(remoteCreatedAt, localClock.translate(), localTimeNanos) > 0);
    }
}
