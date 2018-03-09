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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.transport.messages.ErrorMessage;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.cassandra.transport.Message.Direction.*;

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
        Frame.Decoder dec = new Frame.Decoder(null);

        List<Object> results = new ArrayList<>();
        byte[] frame = new byte[] {
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
        ByteBuf buf = Unpooled.wrappedBuffer(frame);
        try {
            dec.decode(null, buf, results);
            Assert.fail("Expected protocol error");
        } catch (ProtocolException e) {
            Assert.assertTrue(e.getMessage().contains("Invalid or unsupported protocol version"));
        }
    }

    @Test
    public void testInvalidProtocolVersionShortFrame() throws Exception
    {
        // test for CASSANDRA-11464
        Frame.Decoder dec = new Frame.Decoder(null);

        List<Object> results = new ArrayList<>();
        byte[] frame = new byte[] {
                (byte) REQUEST.addToVersion(1),  // direction & version
                0x00,  // flags
                0x01,  // stream ID
                0x09,  // opcode
                0x00, 0x00, 0x00, 0x21,  // body length
        };
        ByteBuf buf = Unpooled.wrappedBuffer(frame);
        try {
            dec.decode(null, buf, results);
            Assert.fail("Expected protocol error");
        } catch (ProtocolException e) {
            Assert.assertTrue(e.getMessage().contains("Invalid or unsupported protocol version"));
        }
    }

    @Test
    public void testInvalidDirection() throws Exception
    {
        Frame.Decoder dec = new Frame.Decoder(null);

        List<Object> results = new ArrayList<>();
        // should generate a protocol exception for using a response frame with
        // a prepare op, ensure that it comes back with stream ID 1
        byte[] frame = new byte[] {
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
        ByteBuf buf = Unpooled.wrappedBuffer(frame);
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
        Frame.Decoder dec = new Frame.Decoder(null);

        List<Object> results = new ArrayList<>();
        byte[] frame = new byte[] {
                (byte) REQUEST.addToVersion(ProtocolVersion.CURRENT.asInt()),  // direction & version
                0x00,  // flags
                0x00, 0x01,  // stream ID
                0x09,  // opcode
                0x10, (byte) 0x00, (byte) 0x00, (byte) 0x00,  // body length
        };
        byte[] body = new byte[0x10000000];
        ByteBuf buf = Unpooled.wrappedBuffer(frame, body);
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
    public void testErrorMessageWithNullString() throws Exception
    {
        // test for CASSANDRA-11167
        ErrorMessage msg = ErrorMessage.fromException(new ServerError((String) null));
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
        byte[] incomingFrame = new byte[] {
        (byte) REQUEST.addToVersion(ProtocolVersion.CURRENT.asInt()),  // direction & version
        0x00,  // flags
        0x00, 0x01,  // stream ID
        0x04,  // opcode for obsoleted CREDENTIALS message
        0x00, (byte) 0x00, (byte) 0x00, (byte) 0x10,  // body length
        };
        byte[] body = new byte[0x10];
        ByteBuf buf = Unpooled.wrappedBuffer(incomingFrame, body);
        Frame decodedFrame = new Frame.Decoder(null).decodeFrame(buf);
        try {
            decodedFrame.header.type.codec.decode(decodedFrame.body, decodedFrame.header.version);
            Assert.fail("Expected protocol error");
        } catch (ProtocolException e) {
            Assert.assertTrue(e.getMessage().contains("Unsupported message"));
        }
    }
}
