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
    private static ICompactSerializer<FileStatus> serializer_;

    static enum Action
    {
        // was received successfully, and can be deleted from the source node
        DELETE,
        // needs to be streamed (or restreamed)
        STREAM
    }

    static
    {
        serializer_ = new FileStatusSerializer();
    }

    public static ICompactSerializer<FileStatus> serializer()
    {
        return serializer_;
    }

    private final String file_;
    private Action action_;

    /**
     * Create a FileStatus with the default Action: STREAM.
     */
    public FileStatus(String file)
    {
        file_ = file;
        action_ = Action.STREAM;
    }

    public String getFile()
    {
        return file_;
    }

    public void setAction(Action action)
    {
        action_ = action;
    }

    public Action getAction()
    {
        return action_;
    }

    public Message makeStreamStatusMessage() throws IOException
    {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream( bos );
        FileStatus.serializer().serialize(this, dos);
        return new Message(FBUtilities.getLocalAddress(), "", StorageService.Verb.STREAM_FINISHED, bos.toByteArray());
    }

    private static class FileStatusSerializer implements ICompactSerializer<FileStatus>
    {
        public void serialize(FileStatus streamStatus, DataOutputStream dos) throws IOException
        {
            dos.writeUTF(streamStatus.getFile());
            dos.writeInt(streamStatus.getAction().ordinal());
        }

        public FileStatus deserialize(DataInputStream dis) throws IOException
        {
            String targetFile = dis.readUTF();
            FileStatus streamStatus = new FileStatus(targetFile);

            int ordinal = dis.readInt();
            if (ordinal == Action.DELETE.ordinal())
                streamStatus.setAction(Action.DELETE);
            else if (ordinal == Action.STREAM.ordinal())
                streamStatus.setAction(Action.STREAM);
            else
                throw new IOException("Bad FileStatus.Action: " + ordinal);

            return streamStatus;
        }
    }
}
