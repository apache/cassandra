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
package org.apache.cassandra.streaming;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.UUID;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.UUIDSerializer;

public class StreamReply
{
    static enum Status
    {
        FILE_FINISHED,
        FILE_RETRY,
        SESSION_FINISHED,
        SESSION_FAILURE,
    }

    public static final IVersionedSerializer<StreamReply> serializer = new FileStatusSerializer();

    public final UUID sessionId;
    public final String file;
    public final Status action;

    public StreamReply(String file, UUID sessionId, Status action)
    {
        this.file = file;
        this.action = action;
        this.sessionId = sessionId;
    }

    public MessageOut<StreamReply> createMessage()
    {
        return new MessageOut<StreamReply>(MessagingService.Verb.STREAM_REPLY, this, serializer);
    }

    @Override
    public String toString()
    {
        return "StreamReply(" +
               "sessionId=" + sessionId +
               ", file='" + file + '\'' +
               ", action=" + action +
               ')';
    }

    private static class FileStatusSerializer implements IVersionedSerializer<StreamReply>
    {
        public void serialize(StreamReply reply, DataOutput dos, int version) throws IOException
        {
            UUIDSerializer.serializer.serialize(reply.sessionId, dos, MessagingService.current_version);
            dos.writeUTF(reply.file);
            dos.writeInt(reply.action.ordinal());
        }

        public StreamReply deserialize(DataInput dis, int version) throws IOException
        {
            UUID sessionId = UUIDSerializer.serializer.deserialize(dis, MessagingService.current_version);
            String targetFile = dis.readUTF();
            Status action = Status.values()[dis.readInt()];
            return new StreamReply(targetFile, sessionId, action);
        }

        public long serializedSize(StreamReply reply, int version)
        {
            return TypeSizes.NATIVE.sizeof(reply.sessionId) + TypeSizes.NATIVE.sizeof(reply.file) + TypeSizes.NATIVE.sizeof(reply.action.ordinal());
        }
    }
}
