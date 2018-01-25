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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.streaming.PreviewKind;

/**
 * Body part of SYNC_REQUEST repair message.
 * Request {@code src} node to sync data with {@code dst} node for range {@code ranges}.
 *
 * @since 2.0
 */
public class SyncRequest extends RepairMessage
{
    public static MessageSerializer serializer = new SyncRequestSerializer();

    public final InetAddressAndPort initiator;
    public final InetAddressAndPort src;
    public final InetAddressAndPort dst;
    public final Collection<Range<Token>> ranges;
    public final PreviewKind previewKind;

   public SyncRequest(RepairJobDesc desc, InetAddressAndPort initiator, InetAddressAndPort src, InetAddressAndPort dst, Collection<Range<Token>> ranges, PreviewKind previewKind)
   {
        super(Type.SYNC_REQUEST, desc);
        this.initiator = initiator;
        this.src = src;
        this.dst = dst;
        this.ranges = ranges;
        this.previewKind = previewKind;
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof SyncRequest))
            return false;
        SyncRequest req = (SyncRequest)o;
        return messageType == req.messageType &&
               desc.equals(req.desc) &&
               initiator.equals(req.initiator) &&
               src.equals(req.src) &&
               dst.equals(req.dst) &&
               ranges.equals(req.ranges) &&
               previewKind == req.previewKind;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(messageType, desc, initiator, src, dst, ranges, previewKind);
    }

    public static class SyncRequestSerializer implements MessageSerializer<SyncRequest>
    {
        public void serialize(SyncRequest message, DataOutputPlus out, int version) throws IOException
        {
            RepairJobDesc.serializer.serialize(message.desc, out, version);
            CompactEndpointSerializationHelper.instance.serialize(message.initiator, out, version);
            CompactEndpointSerializationHelper.instance.serialize(message.src, out, version);
            CompactEndpointSerializationHelper.instance.serialize(message.dst, out, version);
            out.writeInt(message.ranges.size());
            for (Range<Token> range : message.ranges)
            {
                MessagingService.validatePartitioner(range);
                AbstractBounds.tokenSerializer.serialize(range, out, version);
            }
            out.writeInt(message.previewKind.getSerializationVal());
        }

        public SyncRequest deserialize(DataInputPlus in, int version) throws IOException
        {
            RepairJobDesc desc = RepairJobDesc.serializer.deserialize(in, version);
            InetAddressAndPort owner = CompactEndpointSerializationHelper.instance.deserialize(in, version);
            InetAddressAndPort src = CompactEndpointSerializationHelper.instance.deserialize(in, version);
            InetAddressAndPort dst = CompactEndpointSerializationHelper.instance.deserialize(in, version);
            int rangesCount = in.readInt();
            List<Range<Token>> ranges = new ArrayList<>(rangesCount);
            for (int i = 0; i < rangesCount; ++i)
                ranges.add((Range<Token>) AbstractBounds.tokenSerializer.deserialize(in, MessagingService.globalPartitioner(), version));
            PreviewKind previewKind = PreviewKind.deserialize(in.readInt());
            return new SyncRequest(desc, owner, src, dst, ranges, previewKind);
        }

        public long serializedSize(SyncRequest message, int version)
        {
            long size = RepairJobDesc.serializer.serializedSize(message.desc, version);
            size += 3 * CompactEndpointSerializationHelper.instance.serializedSize(message.initiator, version);
            size += TypeSizes.sizeof(message.ranges.size());
            for (Range<Token> range : message.ranges)
                size += AbstractBounds.tokenSerializer.serializedSize(range, version);
            size += TypeSizes.sizeof(message.previewKind.getSerializationVal());
            return size;
        }
    }

    @Override
    public String toString()
    {
        return "SyncRequest{" +
                "initiator=" + initiator +
                ", src=" + src +
                ", dst=" + dst +
                ", ranges=" + ranges +
                ", previewKind=" + previewKind +
                "} " + super.toString();
    }
}
