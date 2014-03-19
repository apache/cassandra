/*
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
package org.apache.cassandra.repair.messages;

import java.io.DataInput;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;
import org.apache.cassandra.repair.RepairJobDesc;

/**
 * Body part of SYNC_REQUEST repair message.
 * Request {@code src} node to sync data with {@code dst} node for range {@code ranges}.
 *
 * @since 2.0
 */
public class SyncRequest extends RepairMessage
{
    public static MessageSerializer serializer = new SyncRequestSerializer();

    public final InetAddress initiator;
    public final InetAddress src;
    public final InetAddress dst;
    public final Collection<Range<Token>> ranges;

    public SyncRequest(RepairJobDesc desc, InetAddress initiator, InetAddress src, InetAddress dst, Collection<Range<Token>> ranges)
    {
        super(Type.SYNC_REQUEST, desc);
        this.initiator = initiator;
        this.src = src;
        this.dst = dst;
        this.ranges = ranges;
    }

    public static class SyncRequestSerializer implements MessageSerializer<SyncRequest>
    {
        public void serialize(SyncRequest message, DataOutputPlus out, int version) throws IOException
        {
            RepairJobDesc.serializer.serialize(message.desc, out, version);
            CompactEndpointSerializationHelper.serialize(message.initiator, out);
            CompactEndpointSerializationHelper.serialize(message.src, out);
            CompactEndpointSerializationHelper.serialize(message.dst, out);
            out.writeInt(message.ranges.size());
            for (Range<Token> range : message.ranges)
                AbstractBounds.serializer.serialize(range, out, version);
        }

        public SyncRequest deserialize(DataInput in, int version) throws IOException
        {
            RepairJobDesc desc = RepairJobDesc.serializer.deserialize(in, version);
            InetAddress owner = CompactEndpointSerializationHelper.deserialize(in);
            InetAddress src = CompactEndpointSerializationHelper.deserialize(in);
            InetAddress dst = CompactEndpointSerializationHelper.deserialize(in);
            int rangesCount = in.readInt();
            List<Range<Token>> ranges = new ArrayList<>(rangesCount);
            for (int i = 0; i < rangesCount; ++i)
                ranges.add((Range<Token>) AbstractBounds.serializer.deserialize(in, version).toTokenBounds());
            return new SyncRequest(desc, owner, src, dst, ranges);
        }

        public long serializedSize(SyncRequest message, int version)
        {
            long size = RepairJobDesc.serializer.serializedSize(message.desc, version);
            size += 3 * CompactEndpointSerializationHelper.serializedSize(message.initiator);
            size += TypeSizes.NATIVE.sizeof(message.ranges.size());
            for (Range<Token> range : message.ranges)
                size += AbstractBounds.serializer.serializedSize(range, version);
            return size;
        }
    }
}
