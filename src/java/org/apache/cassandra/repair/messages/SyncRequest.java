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
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.streaming.PreviewKind;

import static org.apache.cassandra.locator.InetAddressAndPort.Serializer.inetAddressAndPortSerializer;

/**
 * Body part of SYNC_REQUEST repair message.
 * Request {@code src} node to sync data with {@code dst} node for range {@code ranges}.
 *
 * @since 2.0
 */
public class SyncRequest extends RepairMessage
{
    public final InetAddressAndPort initiator;
    public final InetAddressAndPort src;
    public final InetAddressAndPort dst;
    public final Collection<Range<Token>> ranges;
    public final PreviewKind previewKind;
    public final boolean asymmetric;

   public SyncRequest(RepairJobDesc desc,
                      InetAddressAndPort initiator,
                      InetAddressAndPort src,
                      InetAddressAndPort dst,
                      Collection<Range<Token>> ranges,
                      PreviewKind previewKind,
                      boolean asymmetric)
   {
        super(desc);
        this.initiator = initiator;
        this.src = src;
        this.dst = dst;
        this.ranges = ranges;
        this.previewKind = previewKind;
        this.asymmetric = asymmetric;
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof SyncRequest))
            return false;
        SyncRequest req = (SyncRequest)o;
        return desc.equals(req.desc) &&
               initiator.equals(req.initiator) &&
               src.equals(req.src) &&
               dst.equals(req.dst) &&
               ranges.equals(req.ranges) &&
               previewKind == req.previewKind &&
               asymmetric == req.asymmetric;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(desc, initiator, src, dst, ranges, previewKind);
    }

    public static final IVersionedSerializer<SyncRequest> serializer = new IVersionedSerializer<SyncRequest>()
    {
        public void serialize(SyncRequest message, DataOutputPlus out, int version) throws IOException
        {
            RepairJobDesc.serializer.serialize(message.desc, out, version);
            inetAddressAndPortSerializer.serialize(message.initiator, out, version);
            inetAddressAndPortSerializer.serialize(message.src, out, version);
            inetAddressAndPortSerializer.serialize(message.dst, out, version);
            out.writeInt(message.ranges.size());
            for (Range<Token> range : message.ranges)
            {
                IPartitioner.validate(range);
                AbstractBounds.tokenSerializer.serialize(range, out, version);
            }
            out.writeInt(message.previewKind.getSerializationVal());
            out.writeBoolean(message.asymmetric);
        }

        public SyncRequest deserialize(DataInputPlus in, int version) throws IOException
        {
            RepairJobDesc desc = RepairJobDesc.serializer.deserialize(in, version);
            InetAddressAndPort initiator = inetAddressAndPortSerializer.deserialize(in, version);
            InetAddressAndPort src = inetAddressAndPortSerializer.deserialize(in, version);
            InetAddressAndPort dst = inetAddressAndPortSerializer.deserialize(in, version);
            int rangesCount = in.readInt();
            List<Range<Token>> ranges = new ArrayList<>(rangesCount);
            for (int i = 0; i < rangesCount; ++i)
                ranges.add((Range<Token>) AbstractBounds.tokenSerializer.deserialize(in, IPartitioner.global(), version));
            PreviewKind previewKind = PreviewKind.deserialize(in.readInt());
            boolean asymmetric = in.readBoolean();
            return new SyncRequest(desc, initiator, src, dst, ranges, previewKind, asymmetric);
        }

        public long serializedSize(SyncRequest message, int version)
        {
            long size = RepairJobDesc.serializer.serializedSize(message.desc, version);
            size += inetAddressAndPortSerializer.serializedSize(message.initiator, version);
            size += inetAddressAndPortSerializer.serializedSize(message.src, version);
            size += inetAddressAndPortSerializer.serializedSize(message.dst, version);
            size += TypeSizes.sizeof(message.ranges.size());
            for (Range<Token> range : message.ranges)
                size += AbstractBounds.tokenSerializer.serializedSize(range, version);
            size += TypeSizes.sizeof(message.previewKind.getSerializationVal());
            size += TypeSizes.sizeof(message.asymmetric);
            return size;
        }
    };

    @Override
    public String toString()
    {
        return "SyncRequest{" +
                "initiator=" + initiator +
                ", src=" + src +
                ", dst=" + dst +
                ", ranges=" + ranges +
                ", previewKind=" + previewKind +
                ", asymmetric=" + asymmetric +
                "} " + super.toString();
    }
}
