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


import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;

/**
 * This encapsulates information of the list of ranges that a target
 * node requires to be transferred. This will be bundled in a
 * StreamRequestsMessage and sent to nodes that are going to handoff
 * the data.
*/
class StreamRequestMetadata
{
    private static ICompactSerializer<StreamRequestMetadata> serializer_;
    static
    {
        serializer_ = new StreamRequestMetadataSerializer();
    }

    protected static ICompactSerializer<StreamRequestMetadata> serializer()
    {
        return serializer_;
    }

    protected InetAddress target_;
    protected Collection<Range> ranges_;
    protected String table_;

    StreamRequestMetadata(InetAddress target, Collection<Range> ranges, String table)
    {
        target_ = target;
        ranges_ = ranges;
        table_ = table;
    }

    public String toString()
    {
        StringBuilder sb = new StringBuilder("");
        sb.append(table_);
        sb.append("@");
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

class StreamRequestMetadataSerializer implements ICompactSerializer<StreamRequestMetadata>
{
    public void serialize(StreamRequestMetadata srMetadata, DataOutputStream dos) throws IOException
    {
        CompactEndpointSerializationHelper.serialize(srMetadata.target_, dos);
        dos.writeUTF(srMetadata.table_);
        dos.writeInt(srMetadata.ranges_.size());
        for (Range range : srMetadata.ranges_)
        {
            AbstractBounds.serializer().serialize(range, dos);
        }
    }

    public StreamRequestMetadata deserialize(DataInputStream dis) throws IOException
    {
        InetAddress target = CompactEndpointSerializationHelper.deserialize(dis);
        String table = dis.readUTF();
        int size = dis.readInt();
        List<Range> ranges = (size == 0) ? null : new ArrayList<Range>();
        for( int i = 0; i < size; ++i )
        {
            ranges.add((Range) AbstractBounds.serializer().deserialize(dis));
        }
        return new StreamRequestMetadata(target, ranges, table);
    }
}
