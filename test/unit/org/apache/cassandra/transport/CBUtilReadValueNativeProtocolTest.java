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

import java.io.IOException;
import java.util.Objects;

import org.assertj.core.api.Assertions;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ErrorMessage;
import org.apache.cassandra.transport.messages.OptionsMessage;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * If a client sends an AUTH_RESPONSE whose declared SASL-token length is {@link Integer#MAX_VALUE},
 * {@link CBUtil#readValue(ByteBuf)} must reject it against the readable byte count rather than
 * attempting the allocation.
 * <p>
 * This reproduces a pre-authentication unbounded-allocation DoS: on unpatched code the read
 * allocates {@code new byte[Integer.MAX_VALUE]} and the JVM throws OutOfMemoryError; on fixed
 * code the client receives an ERROR containing "Cannot read value of length 2147483647" and the
 * server keeps running. The V4 (pre-V5) decoder path is the one the original report exercised.
 */
public class CBUtilReadValueNativeProtocolTest extends CQLTester
{
    @BeforeClass
    public static void setUp()
    {
        requireNetwork();
    }

    // V4 / pre-V5 path is the one the report and the reproducer netcat PoC exercise.
    private static SimpleClient client()
    {
        try
        {
            return SimpleClient.builder(nativeAddr.getHostAddress(), nativePort)
                               .protocolVersion(ProtocolVersion.V4)
                               .build()
                               .connect(false);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Error initializing client", e);
        }
    }

    @Test
    public void authResponseWithUnboundedTokenLengthIsRejected()
    {
        ByteBuf maliciousBody = Unpooled.buffer(4);
        maliciousBody.writeInt(Integer.MAX_VALUE); // 0x7f ff ff ff

        try (SimpleClient client = client())
        {
            Message.Response response = client.execute(new CustomBodyMessage(Message.Type.AUTH_RESPONSE,
                                                                             maliciousBody),
                                                       false);

            Assertions.assertThat(response.type)
                      .as("Server must respond with an ERROR for the malformed AUTH_RESPONSE")
                      .isEqualTo(Message.Type.ERROR);
            Assertions.assertThat(((ErrorMessage) response).error.getMessage())
                      .as("ERROR payload must reference the CBUtil bound check, not OutOfMemoryError")
                      .contains("Cannot read value of length 2147483647");

            // Sanity-check the server is still alive after the malicious frame: a fresh
            // OPTIONS roundtrip on a new connection should succeed.
            try (SimpleClient probe = client())
            {
                Message.Response options = probe.execute(new OptionsMessage(), true);
                Assertions.assertThat(options.type).isEqualTo(Message.Type.SUPPORTED);
            }
        }
    }

    /**
     * Replaces AUTH_RESPONSE's encoder with one that writes a caller-supplied raw body, so
     * SimpleClient transmits exactly the bytes we want for the test. The server still uses
     * the original AuthResponse.Codec.decode on receive, which is what we want — that's the
     * code path under test.
     */
    private static final class CustomBodyMessage extends Message.Request
    {
        private final ByteBuf body;

        CustomBodyMessage(Message.Type type, ByteBuf body)
        {
            super(type);
            this.body = Objects.requireNonNull(body);
        }

        @Override
        public Envelope encode(ProtocolVersion version, int streamId)
        {
            Message.Codec<?> originalCodec = type.codec;
            try
            {
                setCodec(type, new Message.Codec<Message>()
                {
                    @Override
                    public Message decode(ByteBuf in, ProtocolVersion v)
                    {
                        return originalCodec.decode(in, v);
                    }

                    @Override
                    public void encode(Message message, ByteBuf dest, ProtocolVersion v)
                    {
                        dest.writeBytes(body, body.readerIndex(), body.readableBytes());
                    }

                    @Override
                    public int encodedSize(Message message, ProtocolVersion v)
                    {
                        return body.readableBytes();
                    }
                });
                return super.encode(version, streamId);
            }
            finally
            {
                setCodec(type, originalCodec);
            }
        }

        @Override
        protected Message.Response execute(QueryState queryState,
                                           Dispatcher.RequestTime requestTime,
                                           boolean traceRequest)
        {
            throw new AssertionError("execute not expected for a malformed AUTH_RESPONSE");
        }
    }

    private static void setCodec(Message.Type type, Message.Codec<?> codec)
    {
        try
        {
            type.unsafeSetCodec(codec);
        }
        catch (NoSuchFieldException | IllegalAccessException e)
        {
            throw new AssertionError(e);
        }
    }
}
