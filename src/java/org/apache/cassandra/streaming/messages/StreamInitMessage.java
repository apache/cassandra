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

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.streaming.StreamingChannel;
import org.apache.cassandra.streaming.StreamingDataOutputPlus;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.streaming.StreamResultFuture;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.locator.InetAddressAndPort.Serializer.inetAddressAndPortSerializer;

/**
 * StreamInitMessage is first sent from the node where {@link org.apache.cassandra.streaming.StreamSession} is started,
 * to initiate corresponding {@link org.apache.cassandra.streaming.StreamSession} on the other side.
 */
public class StreamInitMessage extends StreamMessage
{
    public static Serializer<StreamInitMessage> serializer = new StreamInitMessageSerializer();

    public final InetAddressAndPort from;
    public final int sessionIndex;
    public final TimeUUID planId;
    public final StreamOperation streamOperation;

    public final TimeUUID pendingRepair;
    public final PreviewKind previewKind;

    public StreamInitMessage(InetAddressAndPort from, int sessionIndex, TimeUUID planId, StreamOperation streamOperation,
                             TimeUUID pendingRepair, PreviewKind previewKind)
    {
        super(Type.STREAM_INIT);
        this.from = from;
        this.sessionIndex = sessionIndex;
        this.planId = planId;
        this.streamOperation = streamOperation;
        this.pendingRepair = pendingRepair;
        this.previewKind = previewKind;
    }

    @Override
    public StreamSession getOrCreateAndAttachInboundSession(StreamingChannel channel, int messagingVersion)
    {
        StreamSession session = StreamResultFuture.createFollower(sessionIndex, planId, streamOperation, from, channel, messagingVersion, pendingRepair, previewKind)
                                 .getSession(from, sessionIndex);
        session.attachInbound(channel);
        return session;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder(128);
        sb.append("StreamInitMessage: from = ").append(from);
        sb.append(", planId = ").append(planId).append(", session index = ").append(sessionIndex);
        return sb.toString();
    }

    private static class StreamInitMessageSerializer implements Serializer<StreamInitMessage>
    {
        public void serialize(StreamInitMessage message, StreamingDataOutputPlus out, int version, StreamSession session) throws IOException
        {
            inetAddressAndPortSerializer.serialize(message.from, out, version);
            out.writeInt(message.sessionIndex);
            message.planId.serialize(out);
            out.writeUTF(message.streamOperation.getDescription());

            out.writeBoolean(message.pendingRepair != null);
            if (message.pendingRepair != null)
                message.pendingRepair.serialize(out);
            out.writeInt(message.previewKind.getSerializationVal());
        }

        public StreamInitMessage deserialize(DataInputPlus in, int version) throws IOException
        {
            InetAddressAndPort from = inetAddressAndPortSerializer.deserialize(in, version);
            int sessionIndex = in.readInt();
            TimeUUID planId = TimeUUID.deserialize(in);
            String description = in.readUTF();

            TimeUUID pendingRepair = in.readBoolean() ? TimeUUID.deserialize(in) : null;
            PreviewKind previewKind = PreviewKind.deserialize(in.readInt());
            return new StreamInitMessage(from, sessionIndex, planId, StreamOperation.fromString(description),
                                         pendingRepair, previewKind);
        }

        public long serializedSize(StreamInitMessage message, int version)
        {
            long size = inetAddressAndPortSerializer.serializedSize(message.from, version);
            size += TypeSizes.sizeof(message.sessionIndex);
            size += TimeUUID.sizeInBytes();
            size += TypeSizes.sizeof(message.streamOperation.getDescription());
            size += TypeSizes.sizeof(message.pendingRepair != null);
            if (message.pendingRepair != null)
                size += TimeUUID.sizeInBytes();
            size += TypeSizes.sizeof(message.previewKind.getSerializationVal());

            return size;
        }
    }
}
