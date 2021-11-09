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
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.streaming.StreamingChannel;
import org.apache.cassandra.streaming.StreamingDataOutputPlus;
import org.apache.cassandra.streaming.StreamSession;

/**
 * StreamMessage is an abstract base class that every messages in streaming protocol inherit.
 *
 * Every message carries message type({@link Type}) and streaming protocol version byte.
 */
public abstract class StreamMessage
{
    public static void serialize(StreamMessage message, StreamingDataOutputPlus out, int version, StreamSession session) throws IOException
    {
        out.writeByte(message.type.id);
        message.type.outSerializer.serialize(message, out, version, session);
    }

    public static long serializedSize(StreamMessage message, int version) throws IOException
    {
        return 1 + message.type.outSerializer.serializedSize(message, version);
    }

    public static StreamMessage deserialize(DataInputPlus in, int version) throws IOException
    {
        Type type = Type.lookupById(in.readByte());
        return type.inSerializer.deserialize(in, version);
    }

    /** StreamMessage serializer */
    public static interface Serializer<V extends StreamMessage>
    {
        V deserialize(DataInputPlus in, int version) throws IOException;
        void serialize(V message, StreamingDataOutputPlus out, int version, StreamSession session) throws IOException;
        long serializedSize(V message, int version) throws IOException;
    }

    /** StreamMessage types */
    public enum Type
    {
        PREPARE_SYN    (1,  5, PrepareSynMessage.serializer   ),
        STREAM         (2,  0, IncomingStreamMessage.serializer, OutgoingStreamMessage.serializer),
        RECEIVED       (3,  4, ReceivedMessage.serializer     ),
        COMPLETE       (5,  1, CompleteMessage.serializer     ),
        SESSION_FAILED (6,  5, SessionFailedMessage.serializer),
        KEEP_ALIVE     (7,  5, KeepAliveMessage.serializer    ),
        PREPARE_SYNACK (8,  5, PrepareSynAckMessage.serializer),
        PREPARE_ACK    (9,  5, PrepareAckMessage.serializer   ),
        STREAM_INIT    (10, 5, StreamInitMessage.serializer   );

        private static final Map<Integer, Type> idToTypeMap;

        static
        {
            idToTypeMap = new HashMap<>();
            for (Type t : values())
            {
                if (idToTypeMap.put(t.id, t) != null)
                    throw new RuntimeException("Two StreamMessage Types map to the same id: " + t.id);
            }
        }

        public static Type lookupById(int id)
        {
            Type t = idToTypeMap.get(id);
            if (t == null)
                throw new IllegalArgumentException("Invalid type id: " + id);

            return t;
        }

        public final int id;
        public final int priority;

        public final Serializer<StreamMessage> inSerializer;
        public final Serializer<StreamMessage> outSerializer;

        Type(int id, int priority, Serializer serializer)
        {
            this(id, priority, serializer, serializer);
        }

        @SuppressWarnings("unchecked")
        Type(int id, int priority, Serializer inSerializer, Serializer outSerializer)
        {
            if (id < 0 || id > Byte.MAX_VALUE)
                throw new IllegalArgumentException("StreamMessage Type id must be non-negative and less than " + Byte.MAX_VALUE);

            this.id = id;
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

    /**
     * Get or create a {@link StreamSession} based on this stream message data: not all stream messages support this,
     * so the default implementation just throws an exception.
     */
    public StreamSession getOrCreateAndAttachInboundSession(StreamingChannel channel, int messagingVersion)
    {
        throw new UnsupportedOperationException("Not supported by streaming messages of type: " + this.getClass());
    }
}
