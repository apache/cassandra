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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.transport.messages.ErrorMessage;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import static org.apache.cassandra.transport.Message.Direction.REQUEST;
import static org.apache.cassandra.transport.Message.Direction.RESPONSE;

public class ProtocolErrorTest {

    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testInvalidProtocolVersion() throws Exception
    {
        // test using a protocol 2 version higher than the current version (1 version higher is current beta)
        testInvalidProtocolVersion(ProtocolVersion.CURRENT.asInt() + 2); //
        // test using a protocol version lower than the lowest version
        for (ProtocolVersion version : ProtocolVersion.UNSUPPORTED)
            testInvalidProtocolVersion(version.asInt());

    }

    public void testInvalidProtocolVersion(int version) throws Exception
    {
        Envelope.Decoder dec = new Envelope.Decoder();

        List<Object> results = new ArrayList<>();
        byte[] bytes = new byte[] {
                (byte) REQUEST.addToVersion(version),  // direction & version
                0x00,  // flags
                0x00, 0x01,  // stream ID
                0x09,  // opcode
                0x00, 0x00, 0x00, 0x21,  // body length
                0x00, 0x00, 0x00, 0x1b, 0x00, 0x1b, 0x53, 0x45,
                0x4c, 0x45, 0x43, 0x54, 0x20, 0x2a, 0x20, 0x46,
                0x52, 0x4f, 0x4d, 0x20, 0x73, 0x79, 0x73, 0x74,
                0x65, 0x6d, 0x2e, 0x6c, 0x6f, 0x63, 0x61, 0x6c,
                0x3b
        };
        ByteBuf buf = Unpooled.wrappedBuffer(bytes);
        try {
            dec.decode(null, buf, results);
            Assert.fail("Expected protocol error");
        } catch (ErrorMessage.WrappedException e) {
            // The stream id is recovered using the attempted version's header layout (CASSANDRA-21508):
            // v1/v2 use a single-byte stream id (header byte 2, which is 0x00 here), whereas v3+ use a
            // two-byte id (0x0001). See decodeRecoversSingleByteStreamIdForOldProtocolVersions.
            int expectedStreamId = version < ProtocolVersion.V3.asInt() ? 0x00 : 0x01;
            Assert.assertEquals(expectedStreamId, e.getStreamId());
            Assert.assertTrue(e.getMessage().contains("Invalid or unsupported protocol version"));
        }
    }

    @Test
    public void testInvalidProtocolVersionShortBody() throws Exception
    {
        // test for CASSANDRA-11464
        Envelope.Decoder dec = new Envelope.Decoder();

        List<Object> results = new ArrayList<>();
        byte[] bytes = new byte[] {
                (byte) REQUEST.addToVersion(1),  // direction & version
                0x00,  // flags
                0x01,  // stream ID
                0x09,  // opcode
                0x00, 0x00, 0x00, 0x21,  // body length
        };
        ByteBuf buf = Unpooled.wrappedBuffer(bytes);
        try {
            dec.decode(null, buf, results);
            Assert.fail("Expected protocol error");
        } catch (ProtocolException e) {
            Assert.assertTrue(e.getMessage().contains("Invalid or unsupported protocol version"));
        }
    }

    @Test
    public void testIncompleteHeaderWithInvalidProtocolVersion() throws Exception
    {
        // CASSANDRA-21508: when fewer than a full header's worth of bytes have arrived AND the protocol
        // version is unsupported, decode cannot trust/recover a stream id. It defers the protocol error and
        // wraps it with Message.UNSET_STREAM_ID, so the channel-level exception handler closes the connection
        // rather than emit an unroutable error frame. Exercises Envelope.Decoder.decode() lines 387-398.
        Envelope.Decoder dec = new Envelope.Decoder();

        List<Object> results = new ArrayList<>();
        // Unsupported version (two above CURRENT) with only part of a header - fewer than Header.LENGTH (9) bytes.
        byte[] bytes = new byte[] {
                (byte) REQUEST.addToVersion(ProtocolVersion.CURRENT.asInt() + 2),  // direction & unsupported version
                0x00,        // flags
                0x00, 0x2a,  // partial header, truncated before the length field
        };
        ByteBuf buf = Unpooled.wrappedBuffer(bytes);
        try {
            dec.decode(null, buf, results);
            Assert.fail("Expected protocol error");
        } catch (ProtocolException e) {
            Assert.assertTrue(e.getMessage().contains("Invalid or unsupported protocol version"));
        }
    }

    @Test
    public void extractHeaderReturnsRecoverableErrorOnBetaFlagViolation() throws Exception
    {
        // CASSANDRA-21508: on the framed (V5+) path, a V6 (beta) envelope whose USE_BETA flag is
        // unset is an invalid but *recoverable* protocol error. Envelope.Decoder.extractHeader documents that
        // it never throws, and instead returns a HeaderExtractionResult carrying the frame's stream id, so the
        // caller (CQLMessageHandler.processOneContainedMessage) can route an ERROR back on that stream and keep
        // processing subsequent envelopes.
        Envelope.Decoder dec = new Envelope.Decoder();

        int streamId = 42;
        // A complete 9-byte header advertising V6 (beta) as a REQUEST, with USE_BETA deliberately unset.
        byte[] header = new byte[] {
                (byte) REQUEST.addToVersion(ProtocolVersion.V6.asInt()),  // direction & beta version
                0x00,        // flags - USE_BETA deliberately unset
                0x00, 0x2a,  // stream id = 42
                0x05,        // opcode = OPTIONS
                0x00, 0x00, 0x00, 0x00,  // body length = 0
        };
        ByteBuffer buf = ByteBuffer.wrap(header);

        Envelope.Decoder.HeaderExtractionResult result;
        try
        {
            result = dec.extractHeader(buf);
        }
        catch (ErrorMessage.WrappedException e)
        {
            throw new AssertionError("extractHeader must not throw on a USE_BETA violation; a recoverable " +
                                     "protocol error escaped as a WrappedException and would tear down the " +
                                     "connection instead of returning a routable error", e);
        }

        Assert.assertFalse("A USE_BETA violation should be reported as a (recoverable) extraction error",
                           result.isSuccess());
        Assert.assertEquals("the frame's stream id must be preserved so the error can be routed back",
                            streamId, result.streamId());
        Assert.assertTrue("expected a USE_BETA protocol error, got: " + result.error().getMessage(),
                          result.error().getMessage().contains("USE_BETA flag is unset"));
    }

    @Test
    public void decodeRecoversSingleByteStreamIdForOldProtocolVersions() throws Exception
    {
        // CASSANDRA-21508: protocol versions 1 and 2 use a shorter header whose stream id is a single
        // byte. When such a version is rejected, decode must recover the stream id using that layout.
        // Reading a 16-bit value would splice the stream id byte together with the following opcode byte and
        // recover a bogus id (e.g. stream 0 + STARTUP opcode 1 -> stream 1), routing the error to a stream the
        // client never used. A v1/v2 client (which downgrades on such an error) would then never see it and
        // its connection would time out - as observed in ProtocolNegotiationTest#olderVersionsAreUnsupported.
        Envelope.Decoder dec = new Envelope.Decoder();

        int streamId = 0x07;
        List<Object> results = new ArrayList<>();
        // A full (>= Header.LENGTH bytes) v1 frame so decode reaches the stream-id recovery, not the
        // incomplete-header path: single-byte stream id, STARTUP opcode, and one body byte.
        byte[] bytes = new byte[] {
                (byte) REQUEST.addToVersion(1),  // unsupported v1
                0x00,                            // flags
                (byte) streamId,                 // v1/v2 single-byte stream id
                0x01,                            // opcode = STARTUP (would be spliced in as the low byte)
                0x00, 0x00, 0x00, 0x01,          // body length = 1
                0x00                             // 1 body byte, so readableBytes == Header.LENGTH
        };
        ByteBuf buf = Unpooled.wrappedBuffer(bytes);
        try {
            dec.decode(null, buf, results);
            Assert.fail("Expected protocol error");
        } catch (ErrorMessage.WrappedException e) {
            Assert.assertEquals("a v1/v2 stream id must be read as a single byte, not spliced with the opcode",
                                streamId, e.getStreamId());
            Assert.assertTrue("expected a ProtocolException cause, got: " + e.getCause(),
                              e.getCause() instanceof ProtocolException);
            Assert.assertTrue(e.getMessage().contains("Invalid or unsupported protocol version"));
        }
    }

    @Test
    public void testInvalidDirection() throws Exception
    {
        Envelope.Decoder dec = new Envelope.Decoder();

        List<Object> results = new ArrayList<>();
        // should generate a protocol exception for using a response with
        // a prepare op, ensure that it comes back with stream ID 1
        byte[] bytes = new byte[] {
                (byte) RESPONSE.addToVersion(ProtocolVersion.CURRENT.asInt()),  // direction & version
                0x00,  // flags
                0x00, 0x01,  // stream ID
                0x09,  // opcode
                0x00, 0x00, 0x00, 0x21,  // body length
                0x00, 0x00, 0x00, 0x1b, 0x00, 0x1b, 0x53, 0x45,
                0x4c, 0x45, 0x43, 0x54, 0x20, 0x2a, 0x20, 0x46,
                0x52, 0x4f, 0x4d, 0x20, 0x73, 0x79, 0x73, 0x74,
                0x65, 0x6d, 0x2e, 0x6c, 0x6f, 0x63, 0x61, 0x6c,
                0x3b
        };
        ByteBuf buf = Unpooled.wrappedBuffer(bytes);
        try {
            dec.decode(null, buf, results);
            Assert.fail("Expected protocol error");
        } catch (ErrorMessage.WrappedException e) {
            // make sure the exception has the correct stream ID
            Assert.assertEquals(1, e.getStreamId());
            Assert.assertTrue(e.getMessage().contains("Wrong protocol direction"));
        }
    }

    @Test
    public void testBodyLengthOverLimit() throws Exception
    {
        Envelope.Decoder dec = new Envelope.Decoder();

        List<Object> results = new ArrayList<>();
        byte[] bytes = new byte[] {
                (byte) REQUEST.addToVersion(ProtocolVersion.CURRENT.asInt()),  // direction & version
                0x00,  // flags
                0x00, 0x01,  // stream ID
                0x09,  // opcode
                0x10, (byte) 0x00, (byte) 0x00, (byte) 0x00,  // body length
        };
        byte[] body = new byte[0x10000000];
        ByteBuf buf = Unpooled.wrappedBuffer(bytes, body);
        try {
            dec.decode(null, buf, results);
            Assert.fail("Expected protocol error");
        } catch (ErrorMessage.WrappedException e) {
            // make sure the exception has the correct stream ID
            Assert.assertEquals(1, e.getStreamId());
            Assert.assertTrue(e.getMessage().contains("Request is too big"));
        }
    }

    @Test
    public void testErrorMessageWithNullString()
    {
        // test for CASSANDRA-11167
        ErrorMessage msg = ErrorMessage.fromTransportException(new ServerError((String) null));
        assert msg.toString().endsWith("null") : msg.toString();
        int size = ErrorMessage.codec.encodedSize(msg, ProtocolVersion.CURRENT);
        ByteBuf buf = Unpooled.buffer(size);
        ErrorMessage.codec.encode(msg, buf, ProtocolVersion.CURRENT);

        ByteBuf expected = Unpooled.wrappedBuffer(new byte[]{
                0x00, 0x00, 0x00, 0x00,  // int error code
                0x00, 0x00               // short message length
        });

        Assert.assertEquals(expected, buf);
    }

    @Test
    public void testUnsupportedMessage() throws Exception
    {
        byte[] bytes = new byte[] {
            (byte) REQUEST.addToVersion(ProtocolVersion.CURRENT.asInt()),  // direction & version
            0x00,  // flags
            0x00, 0x01,  // stream ID
            0x04,  // opcode for obsoleted CREDENTIALS message
            0x00, (byte) 0x00, (byte) 0x00, (byte) 0x10,  // body length
        };
        byte[] body = new byte[0x10];
        ByteBuf buf = Unpooled.wrappedBuffer(bytes, body);
        Envelope decoded = new Envelope.Decoder().decode(buf);
        try {
            decoded.header.type.codec.decode(decoded.body, decoded.header.version);
            Assert.fail("Expected protocol error");
        } catch (ProtocolException e) {
            Assert.assertTrue(e.getMessage().contains("Unsupported message"));
        }
    }
}
