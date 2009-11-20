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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Collection;

import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.net.CompactEndPointSerializationHelper;
import java.net.InetAddress;



/**
 * This encapsulates information of the list of 
 * ranges that a target node requires in order to 
 * be bootstrapped. This will be bundled in a 
 * BootstrapMetadataMessage and sent to nodes that
 * are going to handoff the data.
*/
class BootstrapMetadata
{
    private static ICompactSerializer<BootstrapMetadata> serializer_;
    static
    {
        serializer_ = new BootstrapMetadataSerializer();
    }
    
    protected static ICompactSerializer<BootstrapMetadata> serializer()
    {
        return serializer_;
    }
    
    protected InetAddress target_;
    protected Collection<Range> ranges_;
    
    BootstrapMetadata(InetAddress target, Collection<Range> ranges)
    {
        target_ = target;
        ranges_ = ranges;
    }
    
    public String toString()
    {
        StringBuilder sb = new StringBuilder("");
        sb.append(target_);
        sb.append("------->");
        for ( Range range : ranges_ )
        {
            sb.append(range);
            sb.append(" ");
        }
        return sb.toString();
    }
}

class BootstrapMetadataSerializer implements ICompactSerializer<BootstrapMetadata>
{
    public void serialize(BootstrapMetadata bsMetadata, DataOutputStream dos) throws IOException
    {
        CompactEndPointSerializationHelper.serialize(bsMetadata.target_, dos);
        dos.writeInt(bsMetadata.ranges_.size());
        for (Range range : bsMetadata.ranges_)
        {
            Range.serializer().serialize(range, dos);
        }
    }

    public BootstrapMetadata deserialize(DataInputStream dis) throws IOException
    {            
        InetAddress target = CompactEndPointSerializationHelper.deserialize(dis);
        int size = dis.readInt();
        List<Range> ranges = (size == 0) ? null : new ArrayList<Range>();
        for( int i = 0; i < size; ++i )
        {
            ranges.add(Range.serializer().deserialize(dis));
        }            
        return new BootstrapMetadata( target, ranges );
    }
}

