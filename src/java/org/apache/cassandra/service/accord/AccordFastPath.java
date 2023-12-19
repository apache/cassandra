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

package org.apache.cassandra.service.accord;

import java.io.IOException;
import java.util.Map;

import accord.local.Node;
import com.google.common.collect.ImmutableMap;

import com.google.common.collect.ImmutableSet;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.serializers.TopologySerializers;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.MetadataValue;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

/**
 * Cluster availability info for services that need a consistent view of availability for a given epoch, such
 * as accord topology calculation
 */
public class AccordFastPath implements MetadataValue<AccordFastPath>
{
    public static final AccordFastPath EMPTY = new AccordFastPath(ImmutableMap.of(), Epoch.EMPTY);

    public enum Status
    {
        NORMAL, SHUTDOWN, UNAVAILABLE;

        public boolean isUnavailable()
        {
            switch (this)
            {
                case UNAVAILABLE:
                case SHUTDOWN:
                    return true;
                default:
                    return false;
            }
        }

        public static final MetadataSerializer<Status> serializer = new MetadataSerializer<Status>()
        {
            @Override
            public void serialize(Status status, DataOutputPlus out, Version version) throws IOException
            {
                switch (status)
                {
                    case NORMAL: out.write(0); break;
                    case SHUTDOWN: out.write(1); break;
                    case UNAVAILABLE: out.write(2); break;
                    default: throw new IllegalStateException("Unhandled status: " + this);
                }
            }

            @Override
            public Status deserialize(DataInputPlus in, Version version) throws IOException
            {
                byte b = in.readByte();
                switch (b)
                {
                    case 0: return NORMAL;
                    case 1: return SHUTDOWN;
                    case 2: return UNAVAILABLE;
                    default: throw new IllegalArgumentException("Unhandled status byte: " + b);
                }
            }

            @Override
            public long serializedSize(Status status, Version version)
            {
                return TypeSizes.BYTE_SIZE;
            }
        };
    };

    public static class NodeInfo
    {
        public final Status status;
        public final long updated;

        public NodeInfo(Status status, long updated)
        {
            this.status = status;
            this.updated = updated;
        }

        private static final MetadataSerializer<NodeInfo> serializer = new MetadataSerializer<NodeInfo>()
        {
            @Override
            public void serialize(NodeInfo info, DataOutputPlus out, Version version) throws IOException
            {
                Status.serializer.serialize(info.status, out, version);
                out.writeUnsignedVInt(info.updated);
            }

            @Override
            public NodeInfo deserialize(DataInputPlus in, Version version) throws IOException
            {
                return new NodeInfo(Status.serializer.deserialize(in, version), in.readUnsignedVInt());
            }

            @Override
            public long serializedSize(NodeInfo info, Version version)
            {
                return Status.serializer.serializedSize(info.status, version) + TypeSizes.sizeofUnsignedVInt(info.updated);
            }
        };
    }

    public final ImmutableMap<Node.Id, NodeInfo> info;

    private final Epoch lastModified;

    AccordFastPath(ImmutableMap<Node.Id, NodeInfo> info, Epoch lastModified)
    {
        this.info = info;
        this.lastModified = lastModified;
    }

    public AccordFastPath withoutNode(NodeId tcmId)
    {
        Node.Id node = AccordTopology.tcmIdToAccord(tcmId);
        if (!info.containsKey(node))
            return this;

        ImmutableMap.Builder<Node.Id, NodeInfo> builder = ImmutableMap.builder();
        info.forEach((n, info) -> {
            if (!n.equals(node))
                builder.put(n, info);
        });
        return new AccordFastPath(builder.build(), lastModified);
    }

    public AccordFastPath withNodeStatusSince(Node.Id node, Status status, long updateTimeMillis, long updateDelayMillis)
    {
        NodeInfo current = info.get(node);
        if (status == Status.SHUTDOWN && current != null)
        {
            // nodes report when they're being shutdown and aren't superseded
            updateTimeMillis = Math.max(updateTimeMillis, current.updated + 1);
        }

        if (!canUpdateNodeTo(current, status, updateTimeMillis, updateDelayMillis))
            throw new InvalidRequestException(String.format("cannot transition %s to %s at %s", node, status, updateTimeMillis));

        ImmutableMap.Builder<Node.Id, NodeInfo> builder = ImmutableMap.builder();
        builder.put(node, new NodeInfo(status, updateTimeMillis));
        info.forEach((n, info) -> {
            if (!n.equals(node))
                builder.put(n, info);
        });
        return new AccordFastPath(builder.build(), lastModified);
    }

    public boolean canUpdateNodeTo(NodeInfo current, Status status, long updateTimeMillis, long updateDelayMillis)
    {
        if (current == null)
            return status != Status.NORMAL;

        if (current.status == status)
            return false;

        return updateTimeMillis > current.updated + (status == Status.SHUTDOWN ? 0 : updateDelayMillis);
    }

    public AccordFastPath withLastModified(Epoch epoch)
    {
        return new AccordFastPath(info, epoch);
    }

    public Epoch lastModified()
    {
        return lastModified;
    }

    public ImmutableSet<Node.Id> unavailableIds()
    {
        ImmutableSet.Builder<Node.Id> builder = ImmutableSet.builder();
        info.entrySet().stream()
                .filter(entry -> entry.getValue().status.isUnavailable())
                .map(Map.Entry::getKey)
                .forEach(builder::add);
        return builder.build();
    }

    public static final MetadataSerializer<AccordFastPath> serializer = new MetadataSerializer<AccordFastPath>()
    {
        private void serializeMap(Map<Node.Id, NodeInfo> map, DataOutputPlus out, Version version) throws IOException
        {
            out.writeInt(map.size());
            for (Map.Entry<Node.Id, NodeInfo> entry : map.entrySet())
            {
                TopologySerializers.nodeId.serialize(entry.getKey(), out, version);
                NodeInfo.serializer.serialize(entry.getValue(), out, version);
            }
        }

        public void serialize(AccordFastPath accordFastPath, DataOutputPlus out, Version version) throws IOException
        {
            serializeMap(accordFastPath.info, out, version);
            Epoch.serializer.serialize(accordFastPath.lastModified, out, version);
        }

        private ImmutableMap<Node.Id, NodeInfo> deserializeMap(DataInputPlus in, Version version) throws IOException
        {
            int size = in.readInt();
            if (size == 0)
                return ImmutableMap.of();

            ImmutableMap.Builder<Node.Id, NodeInfo> builder = ImmutableMap.builder();
            for (int i=0; i<size; i++)
                builder.put(TopologySerializers.nodeId.deserialize(in, version),
                        NodeInfo.serializer.deserialize(in, version));
            return builder.build();
        }

        public AccordFastPath deserialize(DataInputPlus in, Version version) throws IOException
        {
            return new AccordFastPath(deserializeMap(in, version),
                                    Epoch.serializer.deserialize(in, version));
        }

        private long serializedMapSize(Map<Node.Id, NodeInfo> map, Version version)
        {
            long size = TypeSizes.INT_SIZE;
            for (Map.Entry<Node.Id, NodeInfo> entry : map.entrySet())
            {
                size += TopologySerializers.nodeId.serializedSize(entry.getKey(), version);
                size += NodeInfo.serializer.serializedSize(entry.getValue(), version);
            }
            return size;
        }

        public long serializedSize(AccordFastPath accordFastPath, Version version)
        {
            return serializedMapSize(accordFastPath.info, version)
                 + Epoch.serializer.serializedSize(accordFastPath.lastModified, version);
        }
    };
}
