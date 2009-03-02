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

package org.apache.cassandra.dht;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.io.StreamContextManager;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.net.io.*;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class BootstrapInitiateMessage implements Serializable
{
    private static ICompactSerializer<BootstrapInitiateMessage> serializer_;
    
    static
    {
        serializer_ = new BootstrapInitiateMessageSerializer();
    }
    
    public static ICompactSerializer<BootstrapInitiateMessage> serializer()
    {
        return serializer_;
    }
    
    public static Message makeBootstrapInitiateMessage(BootstrapInitiateMessage biMessage) throws IOException
    {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream( bos );
        BootstrapInitiateMessage.serializer().serialize(biMessage, dos);
        return new Message( StorageService.getLocalStorageEndPoint(), "", StorageService.bootStrapInitiateVerbHandler_, new Object[]{bos.toByteArray()} );
    }
    
    protected StreamContextManager.StreamContext[] streamContexts_ = new StreamContextManager.StreamContext[0];
   
    public BootstrapInitiateMessage(StreamContextManager.StreamContext[] streamContexts)
    {
        streamContexts_ = streamContexts;
    }
    
    public StreamContextManager.StreamContext[] getStreamContext()
    {
        return streamContexts_;
    }
}

class BootstrapInitiateMessageSerializer implements ICompactSerializer<BootstrapInitiateMessage>
{
    public void serialize(BootstrapInitiateMessage bim, DataOutputStream dos) throws IOException
    {
        dos.writeInt(bim.streamContexts_.length);
        for ( StreamContextManager.StreamContext streamContext : bim.streamContexts_ )
        {
            StreamContextManager.StreamContext.serializer().serialize(streamContext, dos);
        }
    }
    
    public BootstrapInitiateMessage deserialize(DataInputStream dis) throws IOException
    {
        int size = dis.readInt();
        StreamContextManager.StreamContext[] streamContexts = new StreamContextManager.StreamContext[0];
        if ( size > 0 )
        {
            streamContexts = new StreamContextManager.StreamContext[size];
            for ( int i = 0; i < size; ++i )
            {
                streamContexts[i] = StreamContextManager.StreamContext.serializer().deserialize(dis);
            }
        }
        return new BootstrapInitiateMessage(streamContexts);
    }
}

