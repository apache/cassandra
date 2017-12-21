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

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.streaming.StreamSession;

/**
 * StreamMessage is an abstract base class that every messages in streaming protocol inherit.
 *
 * Every message carries message type({@link Type}) and streaming protocol version byte.
 */
public abstract class StreamMessage
{
    /** Streaming protocol version */
    public static final int VERSION_40 = 5;
    public static final int CURRENT_VERSION = VERSION_40;

    public static void serialize(StreamMessage message, DataOutputStreamPlus out, int version, StreamSession session) throws IOException
    {
        // message type
        out.writeByte(message.type.type);
        message.type.outSerializer.serialize(message, out, version, session);
    }

    public static long serializedSize(StreamMessage message, int version) throws IOException
    {
        return 1 + message.type.outSerializer.serializedSize(message, version);
    }

    public static StreamMessage deserialize(DataInputPlus in, int version, StreamSession session) throws IOException
    {
        byte b = in.readByte();
        if (b == 0)
            b = -1;
        Type type = Type.get(b);
        return type.inSerializer.deserialize(in, version, session);
    }

    /** StreamMessage serializer */
    public static interface Serializer<V extends StreamMessage>
    {
        V deserialize(DataInputPlus in, int version, StreamSession session) throws IOException;
        void serialize(V message, DataOutputStreamPlus out, int version, StreamSession session) throws IOException;
        long serializedSize(V message, int version) throws IOException;
    }

    /** StreamMessage types */
    public enum Type
    {
        PREPARE_SYN(1, 5, PrepareSynMessage.serializer),
        FILE(2, 0, IncomingFileMessage.serializer, OutgoingFileMessage.serializer),
        RECEIVED(3, 4, ReceivedMessage.serializer),
        COMPLETE(5, 1, CompleteMessage.serializer),
        SESSION_FAILED(6, 5, SessionFailedMessage.serializer),
        KEEP_ALIVE(7, 5, KeepAliveMessage.serializer),
        PREPARE_SYNACK(8, 5, PrepareSynAckMessage.serializer),
        PREPARE_ACK(9, 5, PrepareAckMessage.serializer),
        STREAM_INIT(10, 5, StreamInitMessage.serializer);

        public static Type get(byte type)
        {
            for (Type t : Type.values())
            {
                if (t.type == type)
                    return t;
            }
            throw new IllegalArgumentException("Unknown type " + type);
        }

        private final byte type;
        public final int priority;
        public final Serializer<StreamMessage> inSerializer;
        public final Serializer<StreamMessage> outSerializer;

        @SuppressWarnings("unchecked")
        private Type(int type, int priority, Serializer serializer)
        {
            this(type, priority, serializer, serializer);
        }

        @SuppressWarnings("unchecked")
        private Type(int type, int priority, Serializer inSerializer, Serializer outSerializer)
        {
            this.type = (byte) type;
            this.priority = priority;
            this.inSerializer = inSerializer;
            this.outSerializer = outSerializer;
        }
    }

    public final Type type;

    protected StreamMessage(Type type)
    {
        this.type = type;
    }

    /**
     * @return priority of this message. higher value, higher priority.
     */
    public int getPriority()
    {
        return type.priority;
    }
}
