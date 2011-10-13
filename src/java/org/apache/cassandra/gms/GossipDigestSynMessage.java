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

package org.apache.cassandra.gms;

import java.io.*;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;


/**
 * This is the first message that gets sent out as a start of the Gossip protocol in a 
 * round. 
 */

class GossipDigestSynMessage
{
    private static IVersionedSerializer<GossipDigestSynMessage> serializer_;
    static
    {
        serializer_ = new GossipDigestSynMessageSerializer();
    }
    
    String clusterId_;
    List<GossipDigest> gDigests_ = new ArrayList<GossipDigest>();

    public static IVersionedSerializer<GossipDigestSynMessage> serializer()
    {
        return serializer_;
    }
 
    public GossipDigestSynMessage(String clusterId, List<GossipDigest> gDigests)
    {      
        clusterId_ = clusterId;
        gDigests_ = gDigests;
    }
    
    List<GossipDigest> getGossipDigests()
    {
        return gDigests_;
    }
}

class GossipDigestSerializationHelper
{
    private static Logger logger_ = LoggerFactory.getLogger(GossipDigestSerializationHelper.class);
    
    static void serialize(List<GossipDigest> gDigestList, DataOutput dos, int version) throws IOException
    {
        dos.writeInt(gDigestList.size());
        for ( GossipDigest gDigest : gDigestList )
        {
            GossipDigest.serializer().serialize( gDigest, dos, version);
        }
    }

    static List<GossipDigest> deserialize(DataInput dis, int version) throws IOException
    {
        int size = dis.readInt();            
        List<GossipDigest> gDigests = new ArrayList<GossipDigest>(size);
        
        for ( int i = 0; i < size; ++i )
        {
            gDigests.add(GossipDigest.serializer().deserialize(dis, version));
        }        
        return gDigests;
    }
}

class EndpointStatesSerializationHelper
{
    private static final Logger logger_ = LoggerFactory.getLogger(EndpointStatesSerializationHelper.class);

    static void serialize(Map<InetAddress, EndpointState> epStateMap, DataOutput dos, int version) throws IOException
    {
        dos.writeInt(epStateMap.size());
        for (Entry<InetAddress, EndpointState> entry : epStateMap.entrySet())
        {
            InetAddress ep = entry.getKey();
            CompactEndpointSerializationHelper.serialize(ep, dos);
            EndpointState.serializer().serialize(entry.getValue(), dos, version);
        }
    }

    static Map<InetAddress, EndpointState> deserialize(DataInput dis, int version) throws IOException
    {
        int size = dis.readInt();            
        Map<InetAddress, EndpointState> epStateMap = new HashMap<InetAddress, EndpointState>(size);

        for ( int i = 0; i < size; ++i )
        {
            InetAddress ep = CompactEndpointSerializationHelper.deserialize(dis);
            EndpointState epState = EndpointState.serializer().deserialize(dis, version);
            epStateMap.put(ep, epState);
        }
        return epStateMap;
    }
}

class GossipDigestSynMessageSerializer implements IVersionedSerializer<GossipDigestSynMessage>
{   
    public void serialize(GossipDigestSynMessage gDigestSynMessage, DataOutput dos, int version) throws IOException
    {    
        dos.writeUTF(gDigestSynMessage.clusterId_);
        GossipDigestSerializationHelper.serialize(gDigestSynMessage.gDigests_, dos, version);
    }

    public GossipDigestSynMessage deserialize(DataInput dis, int version) throws IOException
    {
        String clusterId = dis.readUTF();
        List<GossipDigest> gDigests = GossipDigestSerializationHelper.deserialize(dis, version);
        return new GossipDigestSynMessage(clusterId, gDigests);
    }

    public long serializedSize(GossipDigestSynMessage gossipDigestSynMessage, int version)
    {
        throw new UnsupportedOperationException();
    }
}

