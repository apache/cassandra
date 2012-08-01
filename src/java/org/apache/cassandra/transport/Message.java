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

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.oneone.OneToOneDecoder;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.transport.messages.*;

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
        ERROR        (0,  Direction.RESPONSE, ErrorMessage.codec),
        STARTUP      (1,  Direction.REQUEST,  StartupMessage.codec),
        READY        (2,  Direction.RESPONSE, ReadyMessage.codec),
        AUTHENTICATE (3,  Direction.RESPONSE, AuthenticateMessage.codec),
        CREDENTIALS  (4,  Direction.REQUEST,  CredentialsMessage.codec),
        OPTIONS      (5,  Direction.REQUEST,  OptionsMessage.codec),
        SUPPORTED    (6,  Direction.RESPONSE, SupportedMessage.codec),
        QUERY        (7,  Direction.REQUEST,  QueryMessage.codec),
        RESULT       (8,  Direction.RESPONSE, ResultMessage.codec),
        PREPARE      (9,  Direction.REQUEST,  PrepareMessage.codec),
        EXECUTE      (10, Direction.REQUEST,  ExecuteMessage.codec);

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

        private Type(int opcode, Direction direction, Codec<?> codec)
        {
            this.opcode = opcode;
            this.direction = direction;
            this.codec = codec;
        }

        public static Type fromOpcode(int opcode, Direction direction)
        {
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
    }

    public final Type type;
    protected Connection connection;
    protected int streamId;

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

    public void setStreamId(int streamId)
    {
        this.streamId = streamId;
    }

    public int getStreamId()
    {
        return streamId;
    }

    public abstract ChannelBuffer encode();

    public static abstract class Request extends Message
    {
        protected Request(Type type)
        {
            super(type);

            if (type.direction != Direction.REQUEST)
                throw new IllegalArgumentException();
        }

        public abstract Response execute();
    }

    public static abstract class Response extends Message
    {
        protected Response(Type type)
        {
            super(type);

            if (type.direction != Direction.RESPONSE)
                throw new IllegalArgumentException();
        }
    }

    public static class ProtocolDecoder extends OneToOneDecoder
    {
        public Object decode(ChannelHandlerContext ctx, Channel channel, Object msg)
        {
            assert msg instanceof Frame : "Expecting frame, got " + msg;

            Frame frame = (Frame)msg;
            Message message = frame.header.type.codec.decode(frame.body);
            message.setStreamId(frame.header.streamId);
            if (message instanceof Request)
                ((Request)message).attach(frame.connection);
            return message;
        }
    }

    public static class ProtocolEncoder extends OneToOneEncoder
    {
        public Object encode(ChannelHandlerContext ctx, Channel channel, Object msg)
        {
            assert msg instanceof Message : "Expecting message, got " + msg;

            Message message = (Message)msg;
            return Frame.create(message.type, message.getStreamId(), message.encode(), message.connection());
        }
    }

    public static class Dispatcher extends SimpleChannelUpstreamHandler
    {
        @Override
        public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
        {
            assert e.getMessage() instanceof Message : "Expecting message, got " + e.getMessage();

            if (e.getMessage() instanceof Response)
                throw new ProtocolException("Invalid response message received, expecting requests");

            Request request = (Request)e.getMessage();
            Connection connection = request.connection();
            connection.validateNewMessage(request.type);

            logger.debug("Received: " + request);

            Response response = request.execute();
            response.setStreamId(request.getStreamId());
            response.attach(connection);
            response.connection().applyStateTransition(request.type, response.type);

            logger.debug("Responding: " + response);

            e.getChannel().write(response);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
        throws Exception
        {
            ctx.getChannel().write(ErrorMessage.fromException(e.getCause()));
        }
    }
}
