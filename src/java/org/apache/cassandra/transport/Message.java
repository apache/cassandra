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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;

import io.netty.buffer.ByteBuf;
import io.netty.channel.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.messages.*;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.utils.ReflectionUtils;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;

/**
 * A message from the CQL binary protocol.
 */
public abstract class Message
{
    protected static final Logger logger = LoggerFactory.getLogger(Message.class);

    public interface Codec<M extends Message> extends CBCodec<M> {}

    public enum Direction
    {
        REQUEST, RESPONSE;

        public static Direction extractFromVersion(int versionWithDirection)
        {
            return (versionWithDirection & 0x80) == 0 ? REQUEST : RESPONSE;
        }

        public int addToVersion(int rawVersion)
        {
            return this == REQUEST ? (rawVersion & 0x7F) : (rawVersion | 0x80);
        }
    }

    public enum Type
    {
        ERROR          (0,  Direction.RESPONSE, ErrorMessage.codec),
        STARTUP        (1,  Direction.REQUEST,  StartupMessage.codec),
        READY          (2,  Direction.RESPONSE, ReadyMessage.codec),
        AUTHENTICATE   (3,  Direction.RESPONSE, AuthenticateMessage.codec),
        CREDENTIALS    (4,  Direction.REQUEST,  UnsupportedMessageCodec.instance),
        OPTIONS        (5,  Direction.REQUEST,  OptionsMessage.codec),
        SUPPORTED      (6,  Direction.RESPONSE, SupportedMessage.codec),
        QUERY          (7,  Direction.REQUEST,  QueryMessage.codec),
        RESULT         (8,  Direction.RESPONSE, ResultMessage.codec),
        PREPARE        (9,  Direction.REQUEST,  PrepareMessage.codec),
        EXECUTE        (10, Direction.REQUEST,  ExecuteMessage.codec),
        REGISTER       (11, Direction.REQUEST,  RegisterMessage.codec),
        EVENT          (12, Direction.RESPONSE, EventMessage.codec),
        BATCH          (13, Direction.REQUEST,  BatchMessage.codec),
        AUTH_CHALLENGE (14, Direction.RESPONSE, AuthChallenge.codec),
        AUTH_RESPONSE  (15, Direction.REQUEST,  AuthResponse.codec),
        AUTH_SUCCESS   (16, Direction.RESPONSE, AuthSuccess.codec);

        public final int opcode;
        public final Direction direction;
        public final Codec<?> codec;

        private static final Type[] opcodeIdx;
        static
        {
            int maxOpcode = -1;
            for (Type type : Type.values())
                maxOpcode = Math.max(maxOpcode, type.opcode);
            opcodeIdx = new Type[maxOpcode + 1];
            for (Type type : Type.values())
            {
                if (opcodeIdx[type.opcode] != null)
                    throw new IllegalStateException("Duplicate opcode");
                opcodeIdx[type.opcode] = type;
            }
        }

        Type(int opcode, Direction direction, Codec<?> codec)
        {
            this.opcode = opcode;
            this.direction = direction;
            this.codec = codec;
        }

        public static Type fromOpcode(int opcode, Direction direction)
        {
            if (opcode >= opcodeIdx.length)
                throw new ProtocolException(String.format("Unknown opcode %d", opcode));
            Type t = opcodeIdx[opcode];
            if (t == null)
                throw new ProtocolException(String.format("Unknown opcode %d", opcode));
            if (t.direction != direction)
                throw new ProtocolException(String.format("Wrong protocol direction (expected %s, got %s) for opcode %d (%s)",
                                                          t.direction,
                                                          direction,
                                                          opcode,
                                                          t));
            return t;
        }

        @VisibleForTesting
        public Codec<?> unsafeSetCodec(Codec<?> codec) throws NoSuchFieldException, IllegalAccessException
        {
            Codec<?> original = this.codec;
            Field field = Type.class.getDeclaredField("codec");
            field.setAccessible(true);
            Field modifiers = ReflectionUtils.getModifiersField();
            modifiers.setAccessible(true);
            modifiers.setInt(field, field.getModifiers() & ~Modifier.FINAL);
            field.set(this, codec);
            return original;
        }
    }

    public final Type type;
    protected Connection connection;
    private int streamId;
    private Envelope source;
    private Map<String, ByteBuffer> customPayload;
    protected ProtocolVersion forcedProtocolVersion = null;

    protected Message(Type type)
    {
        this.type = type;
    }

    public void attach(Connection connection)
    {
        this.connection = connection;
    }

    public Connection connection()
    {
        return connection;
    }

    public Message setStreamId(int streamId)
    {
        this.streamId = streamId;
        return this;
    }

    public int getStreamId()
    {
        return streamId;
    }

    public void setSource(Envelope source)
    {
        this.source = source;
    }

    public Envelope getSource()
    {
        return source;
    }

    public Map<String, ByteBuffer> getCustomPayload()
    {
        return customPayload;
    }

    public void setCustomPayload(Map<String, ByteBuffer> customPayload)
    {
        this.customPayload = customPayload;
    }

    @Override
    public String toString()
    {
        return String.format("(%s:%s:%s)", type, streamId, connection == null ? "null" :  connection.getVersion().asInt());
    }

    public static abstract class Request extends Message
    {
        private boolean tracingRequested;

        protected Request(Type type)
        {
            super(type);

            if (type.direction != Direction.REQUEST)
                throw new IllegalArgumentException();
        }

        /**
         * @return true if the execution of this {@link Request} should be recorded in a tracing session
         */
        protected boolean isTraceable()
        {
            return false;
        }

        /**
         * @return true if warnings should be tracked and aborts enforced for resource limits on this {@link Request}
         */
        protected boolean isTrackable()
        {
            return false;
        }

        protected abstract Response execute(QueryState queryState, long queryStartNanoTime, boolean traceRequest);

        public final Response execute(QueryState queryState, long queryStartNanoTime)
        {
            boolean shouldTrace = false;
            TimeUUID tracingSessionId = null;

            if (isTraceable())
            {
                if (isTracingRequested())
                {
                    shouldTrace = true;
                    tracingSessionId = nextTimeUUID();
                    Tracing.instance.newSession(tracingSessionId, getCustomPayload());
                }
                else if (StorageService.instance.shouldTraceProbablistically())
                {
                    shouldTrace = true;
                    Tracing.instance.newSession(getCustomPayload());
                }
            }

            Response response;
            try
            {
                response = execute(queryState, queryStartNanoTime, shouldTrace);
            }
            finally
            {
                if (shouldTrace)
                    Tracing.instance.stopSession();
            }

            if (isTraceable() && isTracingRequested())
                response.setTracingId(tracingSessionId);

            return response;
        }

        void setTracingRequested()
        {
            tracingRequested = true;
        }

        boolean isTracingRequested()
        {
            return tracingRequested;
        }
    }

    public static abstract class Response extends Message
    {
        protected TimeUUID tracingId;
        protected List<String> warnings;

        protected Response(Type type)
        {
            super(type);

            if (type.direction != Direction.RESPONSE)
                throw new IllegalArgumentException();
        }

        Message setTracingId(TimeUUID tracingId)
        {
            this.tracingId = tracingId;
            return this;
        }

        TimeUUID getTracingId()
        {
            return tracingId;
        }

        public Message setWarnings(List<String> warnings)
        {
            this.warnings = warnings;
            return this;
        }

        public List<String> getWarnings()
        {
            return warnings;
        }
    }

    public Envelope encode(ProtocolVersion version)
    {
        EnumSet<Envelope.Header.Flag> flags = EnumSet.noneOf(Envelope.Header.Flag.class);
        @SuppressWarnings("unchecked")
        Codec<Message> codec = (Codec<Message>)this.type.codec;
        try
        {
            int messageSize = codec.encodedSize(this, version);
            ByteBuf body;
            if (this instanceof Response)
            {
                Response message = (Response)this;
                TimeUUID tracingId = message.getTracingId();
                Map<String, ByteBuffer> customPayload = message.getCustomPayload();
                if (tracingId != null)
                    messageSize += TimeUUID.sizeInBytes();
                List<String> warnings = message.getWarnings();
                if (warnings != null)
                {
                    // if cassandra populates warnings for <= v3 protocol, this is a bug
                    if (version.isSmallerThan(ProtocolVersion.V4))
                    {
                        logger.warn("Warnings present in message with version less than v4 (it is {}); warnings={}", version, warnings);
                        warnings = null;
                    }
                    else
                    {
                        messageSize += CBUtil.sizeOfStringList(warnings);
                    }
                }
                if (customPayload != null)
                {
                    if (version.isSmallerThan(ProtocolVersion.V4))
                        throw new ProtocolException("Must not send frame with CUSTOM_PAYLOAD flag for native protocol version < 4");
                    messageSize += CBUtil.sizeOfBytesMap(customPayload);
                }
                body = CBUtil.allocator.buffer(messageSize);
                if (tracingId != null)
                {
                    CBUtil.writeUUID(tracingId, body);
                    flags.add(Envelope.Header.Flag.TRACING);
                }
                if (warnings != null)
                {
                    CBUtil.writeStringList(warnings, body);
                    flags.add(Envelope.Header.Flag.WARNING);
                }
                if (customPayload != null)
                {
                    CBUtil.writeBytesMap(customPayload, body);
                    flags.add(Envelope.Header.Flag.CUSTOM_PAYLOAD);
                }
            }
            else
            {
                assert this instanceof Request;
                if (((Request)this).isTracingRequested())
                    flags.add(Envelope.Header.Flag.TRACING);
                Map<String, ByteBuffer> payload = getCustomPayload();
                if (payload != null)
                    messageSize += CBUtil.sizeOfBytesMap(payload);
                body = CBUtil.allocator.buffer(messageSize);
                if (payload != null)
                {
                    CBUtil.writeBytesMap(payload, body);
                    flags.add(Envelope.Header.Flag.CUSTOM_PAYLOAD);
                }
            }

            try
            {
                codec.encode(this, body, version);
            }
            catch (Throwable e)
            {
                body.release();
                throw e;
            }

            // if the driver attempted to connect with a protocol version lower than the minimum supported
            // version, respond with a protocol error message with the correct message header for that version
            ProtocolVersion responseVersion = forcedProtocolVersion == null
                                              ? version
                                              : forcedProtocolVersion;

            if (responseVersion.isBeta())
                flags.add(Envelope.Header.Flag.USE_BETA);

            return Envelope.create(type, getStreamId(), responseVersion, flags, body);
        }
        catch (Throwable e)
        {
            throw ErrorMessage.wrap(e, getStreamId());
        }
    }

    abstract static class Decoder<M extends Message>
    {
        static Message decodeMessage(Channel channel, Envelope inbound)
        {
            boolean isRequest = inbound.header.type.direction == Direction.REQUEST;
            boolean isTracing = inbound.header.flags.contains(Envelope.Header.Flag.TRACING);
            boolean isCustomPayload = inbound.header.flags.contains(Envelope.Header.Flag.CUSTOM_PAYLOAD);
            boolean hasWarning = inbound.header.flags.contains(Envelope.Header.Flag.WARNING);

            TimeUUID tracingId = isRequest || !isTracing ? null : CBUtil.readTimeUUID(inbound.body);
            List<String> warnings = isRequest || !hasWarning ? null : CBUtil.readStringList(inbound.body);
            Map<String, ByteBuffer> customPayload = !isCustomPayload ? null : CBUtil.readBytesMap(inbound.body);

            if (isCustomPayload && inbound.header.version.isSmallerThan(ProtocolVersion.V4))
                throw new ProtocolException("Received frame with CUSTOM_PAYLOAD flag for native protocol version < 4");

            Message message = inbound.header.type.codec.decode(inbound.body, inbound.header.version);
            message.setStreamId(inbound.header.streamId);
            message.setSource(inbound);
            message.setCustomPayload(customPayload);

            if (isRequest)
            {
                assert message instanceof Request;
                Request req = (Request) message;
                Connection connection = channel.attr(Connection.attributeKey).get();
                req.attach(connection);
                if (isTracing)
                    req.setTracingRequested();
            }
            else
            {
                assert message instanceof Response;
                if (isTracing)
                    ((Response) message).setTracingId(tracingId);
                if (hasWarning)
                    ((Response) message).setWarnings(warnings);
            }
            return message;
        }

        abstract M decode(Channel channel, Envelope inbound);

        private static class RequestDecoder extends Decoder<Request>
        {
            Request decode(Channel channel, Envelope request)
            {
                if (request.header.type.direction != Direction.REQUEST)
                    throw new ProtocolException(String.format("Unexpected RESPONSE message %s, expecting REQUEST",
                                                              request.header.type));

                return (Request) decodeMessage(channel, request);
            }
        }

        private static class ResponseDecoder extends Decoder<Response>
        {
            Response decode(Channel channel, Envelope response)
            {
                if (response.header.type.direction != Direction.RESPONSE)
                    throw new ProtocolException(String.format("Unexpected REQUEST message %s, expecting RESPONSE",
                                                              response.header.type));

                return (Response) decodeMessage(channel, response);
            }
        }
    }

    private static final Decoder.RequestDecoder REQUEST_DECODER = new Decoder.RequestDecoder();
    private static final Decoder.ResponseDecoder RESPONSE_DECODER = new Decoder.ResponseDecoder();

    static Decoder<Message.Request> requestDecoder()
    {
        return REQUEST_DECODER;
    }

    static Decoder<Message.Response> responseDecoder()
    {
        return RESPONSE_DECODER;
    }
}
