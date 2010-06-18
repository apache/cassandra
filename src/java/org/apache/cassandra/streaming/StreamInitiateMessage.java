/**
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

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.streaming.PendingFile;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

class StreamInitiateMessage
{
    private static ICompactSerializer<StreamInitiateMessage> serializer_;

    static
    {
        serializer_ = new StreamInitiateMessageSerializer();
    }
    
    public static ICompactSerializer<StreamInitiateMessage> serializer()
    {
        return serializer_;
    }
    
    public static Message makeStreamInitiateMessage(StreamInitiateMessage biMessage) throws IOException
    {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream( bos );
        StreamInitiateMessage.serializer().serialize(biMessage, dos);
        return new Message(FBUtilities.getLocalAddress(), "", StorageService.Verb.STREAM_INITIATE, bos.toByteArray() );
    }
    
    protected PendingFile[] streamContexts_ = new PendingFile[0];
   
    public StreamInitiateMessage(PendingFile[] pendingFiles)
    {
        streamContexts_ = pendingFiles;
    }
    
    public PendingFile[] getStreamContext()
    {
        return streamContexts_;
    }

    private static class StreamInitiateMessageSerializer implements ICompactSerializer<StreamInitiateMessage>
    {
        public void serialize(StreamInitiateMessage bim, DataOutputStream dos) throws IOException
        {
            dos.writeInt(bim.streamContexts_.length);
            for ( PendingFile pendingFile : bim.streamContexts_ )
            {
                PendingFile.serializer().serialize(pendingFile, dos);
            }
        }

        public StreamInitiateMessage deserialize(DataInputStream dis) throws IOException
        {
            int size = dis.readInt();
            PendingFile[] pendingFiles = new PendingFile[size];
            for (int i = 0; i < size; i++)
                pendingFiles[i] = PendingFile.serializer().deserialize(dis);
            return new StreamInitiateMessage(pendingFiles);
        }
    }
}
