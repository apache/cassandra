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


import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

class FileStatus
{
    private static ICompactSerializer<FileStatus> serializer;

    static enum Action
    {
        // was received successfully, and can be deleted from the source node
        FINISHED,
        // needs to be streamed (or restreamed)
        RETRY,
    }

    static
    {
        serializer = new FileStatusSerializer();
    }

    public static ICompactSerializer<FileStatus> serializer()
    {
        return serializer;
    }

    public final long sessionId;
    public final String file;
    public final Action action;

    public FileStatus(String file, long sessionId, Action action)
    {
        this.file = file;
        this.action = action;
        this.sessionId = sessionId;
    }

    public Message makeStreamStatusMessage() throws IOException
    {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream( bos );
        FileStatus.serializer().serialize(this, dos);
        return new Message(FBUtilities.getLocalAddress(), StorageService.Verb.STREAM_STATUS, bos.toByteArray());
    }

    private static class FileStatusSerializer implements ICompactSerializer<FileStatus>
    {
        public void serialize(FileStatus streamStatus, DataOutputStream dos) throws IOException
        {
            dos.writeLong(streamStatus.sessionId);
            dos.writeUTF(streamStatus.file);
            dos.writeInt(streamStatus.action.ordinal());
        }

        public FileStatus deserialize(DataInputStream dis) throws IOException
        {
            long sessionId = dis.readLong();
            String targetFile = dis.readUTF();
            Action action = Action.values()[dis.readInt()];
            return new FileStatus(targetFile, sessionId, action);
        }
    }
}
