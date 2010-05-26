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

    public static enum StreamCompletionAction
    {
        DELETE,
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

    private String file_;
    private long expectedBytes_;
    private StreamCompletionAction action_;

    public FileStatus(String file, long expectedBytes)
    {
        file_ = file;
        expectedBytes_ = expectedBytes;
        action_ = StreamCompletionAction.DELETE;
    }

    public String getFile()
    {
        return file_;
    }

    public long getExpectedBytes()
    {
        return expectedBytes_;
    }

    public void setAction(StreamCompletionAction action)
    {
        action_ = action;
    }

    public StreamCompletionAction getAction()
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
            dos.writeLong(streamStatus.getExpectedBytes());
            dos.writeInt(streamStatus.getAction().ordinal());
        }

        public FileStatus deserialize(DataInputStream dis) throws IOException
        {
            String targetFile = dis.readUTF();
            long expectedBytes = dis.readLong();
            FileStatus streamStatus = new FileStatus(targetFile, expectedBytes);

            int ordinal = dis.readInt();
            if ( ordinal == StreamCompletionAction.DELETE.ordinal() )
            {
                streamStatus.setAction(StreamCompletionAction.DELETE);
            }
            else if ( ordinal == StreamCompletionAction.STREAM.ordinal() )
            {
                streamStatus.setAction(StreamCompletionAction.STREAM);
            }

            return streamStatus;
        }
    }
}
