package org.apache.cassandra.streaming;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.io.*;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.FastByteArrayOutputStream;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageProducer;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

class StreamReply implements MessageProducer
{
    static enum Status
    {
        FILE_FINISHED,
        FILE_RETRY,
        SESSION_FINISHED,
    }

    public static final IVersionedSerializer<StreamReply> serializer = new FileStatusSerializer();

    public final long sessionId;
    public final String file;
    public final Status action;

    public StreamReply(String file, long sessionId, Status action)
    {
        this.file = file;
        this.action = action;
        this.sessionId = sessionId;
    }

    public Message getMessage(Integer version) throws IOException
    {
    	FastByteArrayOutputStream bos = new FastByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream( bos );
        serializer.serialize(this, dos, version);
        return new Message(FBUtilities.getBroadcastAddress(), StorageService.Verb.STREAM_REPLY, bos.toByteArray(), version);
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
            dos.writeLong(reply.sessionId);
            dos.writeUTF(reply.file);
            dos.writeInt(reply.action.ordinal());
        }

        public StreamReply deserialize(DataInput dis, int version) throws IOException
        {
            long sessionId = dis.readLong();
            String targetFile = dis.readUTF();
            Status action = Status.values()[dis.readInt()];
            return new StreamReply(targetFile, sessionId, action);
        }

        public long serializedSize(StreamReply streamReply, int version)
        {
            throw new UnsupportedOperationException();
        }
    }
}
