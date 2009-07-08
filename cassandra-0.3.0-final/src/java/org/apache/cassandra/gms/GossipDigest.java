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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.net.CompactEndPointSerializationHelper;
import org.apache.cassandra.net.EndPoint;
import org.apache.cassandra.net.*;

/**
 * Contains information about a specified list of EndPoints and the largest version 
 * of the state they have generated as known by the local endpoint.
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class GossipDigest implements Comparable<GossipDigest>
{
    private static ICompactSerializer<GossipDigest> serializer_;
    static
    {
        serializer_ = new GossipDigestSerializer();
    }
    
    EndPoint endPoint_;
    int generation_;
    int maxVersion_;

    public static ICompactSerializer<GossipDigest> serializer()
    {
        return serializer_;
    }
    
    GossipDigest(EndPoint endPoint, int generation, int maxVersion)
    {
        endPoint_ = endPoint;
        generation_ = generation; 
        maxVersion_ = maxVersion;
    }
    
    EndPoint getEndPoint()
    {
        return endPoint_;
    }
    
    int getGeneration()
    {
        return generation_;
    }
    
    int getMaxVersion()
    {
        return maxVersion_;
    }
    
    public int compareTo(GossipDigest gDigest)
    {
        if ( generation_ != gDigest.generation_ )
            return ( generation_ - gDigest.generation_ );
        return (maxVersion_ - gDigest.maxVersion_);
    }
    
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(endPoint_);
        sb.append(":");
        sb.append(generation_);
        sb.append(":");
        sb.append(maxVersion_);
        return sb.toString();
    }
}

class GossipDigestSerializer implements ICompactSerializer<GossipDigest>
{       
    public void serialize(GossipDigest gDigest, DataOutputStream dos) throws IOException
    {        
        CompactEndPointSerializationHelper.serialize(gDigest.endPoint_, dos);
        dos.writeInt(gDigest.generation_);
        dos.writeInt(gDigest.maxVersion_);
    }

    public GossipDigest deserialize(DataInputStream dis) throws IOException
    {
        EndPoint endPoint = CompactEndPointSerializationHelper.deserialize(dis);
        int generation = dis.readInt();
        int version = dis.readInt();
        return new GossipDigest(endPoint, generation, version);
    }
}
