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

import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

/**
* This class encapsulates the message that needs to be sent to nodes
* that handoff data. The message contains information about ranges
* that need to be transferred and the target node.
*/
class StreamRequestMessage
{
   private static ICompactSerializer<StreamRequestMessage> serializer_;
   static
   {
       serializer_ = new StreamRequestMessageSerializer();
   }

   protected static ICompactSerializer<StreamRequestMessage> serializer()
   {
       return serializer_;
   }

   protected static Message makeStreamRequestMessage(StreamRequestMessage streamRequestMessage)
   {
       ByteArrayOutputStream bos = new ByteArrayOutputStream();
       DataOutputStream dos = new DataOutputStream(bos);
       try
       {
           StreamRequestMessage.serializer().serialize(streamRequestMessage, dos);
       }
       catch (IOException e)
       {
           throw new IOError(e);
       }
       return new Message(FBUtilities.getLocalAddress(), StageManager.STREAM_STAGE, StorageService.Verb.STREAM_REQUEST, bos.toByteArray() );
   }

   protected StreamRequestMetadata[] streamRequestMetadata_ = new StreamRequestMetadata[0];

   // TODO only actually ever need one BM, not an array
   StreamRequestMessage(StreamRequestMetadata... streamRequestMetadata)
   {
       assert streamRequestMetadata != null;
       streamRequestMetadata_ = streamRequestMetadata;
   }

    private static class StreamRequestMessageSerializer implements ICompactSerializer<StreamRequestMessage>
    {
        public void serialize(StreamRequestMessage streamRequestMessage, DataOutputStream dos) throws IOException
        {
            StreamRequestMetadata[] streamRequestMetadata = streamRequestMessage.streamRequestMetadata_;
            dos.writeInt(streamRequestMetadata.length);
            for (StreamRequestMetadata bsmd : streamRequestMetadata)
            {
                StreamRequestMetadata.serializer().serialize(bsmd, dos);
            }
        }

        public StreamRequestMessage deserialize(DataInputStream dis) throws IOException
        {
            int size = dis.readInt();
            StreamRequestMetadata[] streamRequestMetadata = new StreamRequestMetadata[size];
            for (int i = 0; i < size; ++i)
            {
                streamRequestMetadata[i] = StreamRequestMetadata.serializer().deserialize(dis);
            }
            return new StreamRequestMessage(streamRequestMetadata);
        }
    }
}
