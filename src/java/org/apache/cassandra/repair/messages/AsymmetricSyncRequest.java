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

public class AsymmetricSyncRequest extends RepairMessage
{
    public final InetAddressAndPort initiator;
    public final InetAddressAndPort fetchingNode;
    public final InetAddressAndPort fetchFrom;
    public final Collection<Range<Token>> ranges;
    public final PreviewKind previewKind;

    public AsymmetricSyncRequest(RepairJobDesc desc, InetAddressAndPort initiator, InetAddressAndPort fetchingNode, InetAddressAndPort fetchFrom, Collection<Range<Token>> ranges, PreviewKind previewKind)
    {
        super(desc);
        this.initiator = initiator;
        this.fetchingNode = fetchingNode;
        this.fetchFrom = fetchFrom;
        this.ranges = ranges;
        this.previewKind = previewKind;
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof AsymmetricSyncRequest))
            return false;
        AsymmetricSyncRequest req = (AsymmetricSyncRequest)o;
        return desc.equals(req.desc) &&
               initiator.equals(req.initiator) &&
               fetchingNode.equals(req.fetchingNode) &&
               fetchFrom.equals(req.fetchFrom) &&
               ranges.equals(req.ranges);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(desc, initiator, fetchingNode, fetchFrom, ranges);
    }

    public static final IVersionedSerializer<AsymmetricSyncRequest> serializer = new IVersionedSerializer<AsymmetricSyncRequest>()
    {
        public void serialize(AsymmetricSyncRequest message, DataOutputPlus out, int version) throws IOException
        {
            RepairJobDesc.serializer.serialize(message.desc, out, version);
            inetAddressAndPortSerializer.serialize(message.initiator, out, version);
            inetAddressAndPortSerializer.serialize(message.fetchingNode, out, version);
            inetAddressAndPortSerializer.serialize(message.fetchFrom, out, version);
            out.writeInt(message.ranges.size());
            for (Range<Token> range : message.ranges)
            {
                IPartitioner.validate(range);
                AbstractBounds.tokenSerializer.serialize(range, out, version);
            }
            out.writeInt(message.previewKind.getSerializationVal());
        }

        public AsymmetricSyncRequest deserialize(DataInputPlus in, int version) throws IOException
        {
            RepairJobDesc desc = RepairJobDesc.serializer.deserialize(in, version);
            InetAddressAndPort owner = inetAddressAndPortSerializer.deserialize(in, version);
            InetAddressAndPort src = inetAddressAndPortSerializer.deserialize(in, version);
            InetAddressAndPort dst = inetAddressAndPortSerializer.deserialize(in, version);
            int rangesCount = in.readInt();
            List<Range<Token>> ranges = new ArrayList<>(rangesCount);
            for (int i = 0; i < rangesCount; ++i)
                ranges.add((Range<Token>) AbstractBounds.tokenSerializer.deserialize(in, IPartitioner.global(), version));
            PreviewKind previewKind = PreviewKind.deserialize(in.readInt());
            return new AsymmetricSyncRequest(desc, owner, src, dst, ranges, previewKind);
        }

        public long serializedSize(AsymmetricSyncRequest message, int version)
        {
            long size = RepairJobDesc.serializer.serializedSize(message.desc, version);
            size += inetAddressAndPortSerializer.serializedSize(message.initiator, version);
            size += inetAddressAndPortSerializer.serializedSize(message.fetchingNode, version);
            size += inetAddressAndPortSerializer.serializedSize(message.fetchFrom, version);
            size += TypeSizes.sizeof(message.ranges.size());
            for (Range<Token> range : message.ranges)
                size += AbstractBounds.tokenSerializer.serializedSize(range, version);
            size += TypeSizes.sizeof(message.previewKind.getSerializationVal());
            return size;
        }
    };

    public String toString()
    {
        return "AsymmetricSyncRequest{" +
               "initiator=" + initiator +
               ", fetchingNode=" + fetchingNode +
               ", fetchFrom=" + fetchFrom +
               ", ranges=" + ranges +
               ", previewKind=" + previewKind +
               ", desc="+desc+
               '}';
    }
}
