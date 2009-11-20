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

import java.io.*;

import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.concurrent.StageManager;


 /**
 * This class encapsulates the message that needs to be sent
 * to nodes that handoff data. The message contains information
 * about the node to be bootstrapped and the ranges with which
 * it needs to be bootstrapped.
*/
class BootstrapMetadataMessage
{
    private static ICompactSerializer<BootstrapMetadataMessage> serializer_;
    static
    {
        serializer_ = new BootstrapMetadataMessageSerializer();
    }
    
    protected static ICompactSerializer<BootstrapMetadataMessage> serializer()
    {
        return serializer_;
    }
    
    protected static Message makeBootstrapMetadataMessage(BootstrapMetadataMessage bsMetadataMessage)
    {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream( bos );
        try
        {
            BootstrapMetadataMessage.serializer().serialize(bsMetadataMessage, dos);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
        return new Message(FBUtilities.getLocalAddress(), StageManager.streamStage_, StorageService.bootstrapMetadataVerbHandler_, bos.toByteArray() );
    }        
    
    protected BootstrapMetadata[] bsMetadata_ = new BootstrapMetadata[0];

    // TODO only actually ever need one BM, not an array
    BootstrapMetadataMessage(BootstrapMetadata... bsMetadata)
    {
        assert bsMetadata != null;
        bsMetadata_ = bsMetadata;
    }
}

class BootstrapMetadataMessageSerializer implements ICompactSerializer<BootstrapMetadataMessage>
{
    public void serialize(BootstrapMetadataMessage bsMetadataMessage, DataOutputStream dos) throws IOException
    {
        BootstrapMetadata[] bsMetadata = bsMetadataMessage.bsMetadata_;
        dos.writeInt(bsMetadata.length);
        for (BootstrapMetadata bsmd : bsMetadata)
        {
            BootstrapMetadata.serializer().serialize(bsmd, dos);
        }
    }

    public BootstrapMetadataMessage deserialize(DataInputStream dis) throws IOException
    {            
        int size = dis.readInt();
        BootstrapMetadata[] bsMetadata = new BootstrapMetadata[size];
        for ( int i = 0; i < size; ++i )
        {
            bsMetadata[i] = BootstrapMetadata.serializer().deserialize(dis);
        }
        return new BootstrapMetadataMessage(bsMetadata);
    }
}
