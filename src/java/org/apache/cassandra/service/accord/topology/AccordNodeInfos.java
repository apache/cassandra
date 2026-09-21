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

package org.apache.cassandra.service.accord.topology;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

import javax.annotation.Nullable;

import org.agrona.collections.Int2ObjectHashMap;

import accord.local.Node;
import accord.utils.Invariants;
import accord.utils.SortedArrays.SortedArrayList;
import accord.utils.UnhandledEnum;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.MetadataValue;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.utils.CassandraVersion;

/**
 * Cluster availability info for services that need a consistent view of availability for a given epoch, such
 * as accord topology calculation
 */
public class AccordNodeInfos implements MetadataValue<AccordNodeInfos>
{
    /** Introduces {@link AccordNodeInfo.Delta}, and {@link Status} &gt MAYBE_DOWN */
    public static final CassandraVersion MIN_VERSION = new CassandraVersion("6.0-alpha3-SNAPSHOT");

    public static boolean supportsExtendedNodeInfo(ClusterMetadata metadata)
    {
        return supportsExtendedNodeInfo(metadata.directory);
    }

    public static boolean supportsExtendedNodeInfo(Directory directory)
    {
        return directory.clusterMinVersion.cassandraVersion.compareTo(MIN_VERSION) >= 0;
    }

    private static final SortedArrayList<Node.Id> NO_IDS = new SortedArrayList<>(new Node.Id[0]);
    public static final AccordNodeInfos EMPTY = new AccordNodeInfos(new Int2ObjectHashMap<>(1, 0.65f, false), Epoch.EMPTY);

    public enum Status
    {
        NORMAL,
        SHUTDOWN,
        MAYBE_DOWN,
        UNKNOWN_STATUS,
        REMOVED,
        HARD_REMOVED,
        ;

        static final Status[] VALUES = values();

        static Status status(int ordinal)
        {
            if (ordinal < Status.VALUES.length)
                return Status.VALUES[ordinal];
            return Status.UNKNOWN_STATUS;
        }

        public boolean isRemoved()
        {
            return this == REMOVED || this == HARD_REMOVED;
        }

        /**
         * The ordinal is the serialized form (it is also what {@link AccordNodeInfo} packs), so the declaration order above
         * must not change; an ordinal we do not know deserializes as {@link #UNKNOWN_STATUS}.
         */
        public static final MetadataSerializer<Status> serializer = new MetadataSerializer<>()
        {
            @Override
            public void serialize(Status status, DataOutputPlus out, Version version) throws IOException
            {
                out.writeByte(status.ordinal());
            }

            @Override
            public Status deserialize(DataInputPlus in, Version version) throws IOException
            {
                return status(in.readByte());
            }

            @Override
            public long serializedSize(Status status, Version version)
            {
                return TypeSizes.BYTE_SIZE;
            }
        };
    }

    public static class AccordNodeInfo
    {
        static final long STATUS_MASK = 0xf;
        static final long STALE_BIT = 0x10;
        static final long UNREADABLE_BIT = 0x20;

        final long encodedStatus;

        public AccordNodeInfo(long encodedStatus)
        {
            this.encodedStatus = encodedStatus;
        }

        public AccordNodeInfo(AccordNodeInfo copy)
        {
            this.encodedStatus = copy.encodedStatus;
        }

        public AccordNodeInfo(Status status)
        {
            this.encodedStatus = status.ordinal();
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            AccordNodeInfo nodeInfo = (AccordNodeInfo) o;
            return encodedStatus == nodeInfo.encodedStatus;
        }

        public Status status()
        {
            int ordinal = (int) (encodedStatus & STATUS_MASK);
            return Status.status(ordinal);
        }

        public boolean is(Status status)
        {
            int ordinal = (int) (encodedStatus & STATUS_MASK);
            return ordinal == status.ordinal();
        }

        public boolean isStale()
        {
            return 0 != (encodedStatus & STALE_BIT);
        }

        public boolean isUnreadable()
        {
            return 0 != (encodedStatus & UNREADABLE_BIT);
        }

        static long withStatus(long encodedStatus, Status newStatus)
        {
            return (encodedStatus & ~STATUS_MASK) | newStatus.ordinal();
        }

        public static final class Delta
        {
            final long mask;
            final long value;

            private Delta(long mask, long value)
            {
                this.mask = mask;
                this.value = value & mask;
            }

            public static Delta status(Status status)
            {
                return new Delta(STATUS_MASK, status.ordinal());
            }

            public static Delta stale(boolean isStale)
            {
                return new Delta(STALE_BIT, isStale ? STALE_BIT : 0);
            }

            public static Delta unreadable(boolean isUnreadable)
            {
                return new Delta(UNREADABLE_BIT, isUnreadable ? UNREADABLE_BIT : 0);
            }

            public @Nullable Status status()
            {
                return 0 == (mask & STATUS_MASK) ? null : Status.status((int) (value & STATUS_MASK));
            }

            public @Nullable Boolean unreadable()
            {
                return 0 == (mask & UNREADABLE_BIT) ? null : 0 != (value & UNREADABLE_BIT);
            }

            public boolean isEmpty()
            {
                return mask == 0;
            }

            public Delta combine(Delta that)
            {
                Invariants.require(0 == (mask & that.mask), "Overlapping deltas: %s and %s", this, that);
                return new Delta(mask | that.mask, value | that.value);
            }

            long apply(long encodedStatus)
            {
                return (encodedStatus & ~mask) | value;
            }

            public boolean hasNoEffectUpon(AccordNodeInfo info)
            {
                return (info.encodedStatus & mask) == value;
            }

            @Override
            public boolean equals(Object o)
            {
                if (this == o) return true;
                if (!(o instanceof Delta)) return false;
                Delta that = (Delta) o;
                return mask == that.mask && value == that.value;
            }

            @Override
            public int hashCode()
            {
                return Long.hashCode(mask * 31 + value);
            }

            @Override
            public String toString()
            {
                StringBuilder sb = new StringBuilder("{");
                if (0 != (mask & STATUS_MASK))
                    sb.append("status=").append(Status.status((int) (value & STATUS_MASK)));
                if (0 != (mask & STALE_BIT))
                    sb.append(sb.length() > 1 ? ", " : "").append("stale=").append(0 != (value & STALE_BIT));
                if (0 != (mask & UNREADABLE_BIT))
                    sb.append(sb.length() > 1 ? ", " : "").append("unreadable=").append(0 != (value & UNREADABLE_BIT));
                long unknown = mask & ~(STATUS_MASK | STALE_BIT | UNREADABLE_BIT);
                if (unknown != 0)
                    sb.append(sb.length() > 1 ? ", " : "").append("unknownBits=").append(Long.toBinaryString(value & unknown));
                return sb.append('}').toString();
            }

            /**
             * Mask and value, so that a bit this version does not know is still applied faithfully by it (and reported
             * by {@link #toString()} as an unknown bit) rather than silently dropped.
             */
            public static final MetadataSerializer<Delta> serializer = new MetadataSerializer<>()
            {
                @Override
                public void serialize(Delta delta, DataOutputPlus out, Version version) throws IOException
                {
                    out.writeUnsignedVInt(delta.mask);
                    out.writeUnsignedVInt(delta.value);
                }

                @Override
                public Delta deserialize(DataInputPlus in, Version version) throws IOException
                {
                    long mask = in.readUnsignedVInt();
                    return new Delta(mask, in.readUnsignedVInt());
                }

                @Override
                public long serializedSize(Delta delta, Version version)
                {
                    return TypeSizes.sizeofUnsignedVInt(delta.mask) + TypeSizes.sizeofUnsignedVInt(delta.value);
                }
            };
        }

        public boolean includeInFastQuorums()
        {
            return is(Status.NORMAL);
        }

        @Override
        public int hashCode()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString()
        {
            return "NodeInfo{" +
                   "status=" + status() +
                   (isStale() ? ", STALE" : "") +
                   (isUnreadable() ? ", UNREADABLE" : "") +
                   '}';
        }
    }

    public final static class StampedNodeInfo extends AccordNodeInfo
    {
        public static final StampedNodeInfo DEFAULT = new StampedNodeInfo(withStatus(0, Status.NORMAL), 0);

        public final long updated;

        StampedNodeInfo(long encodedStatus, long updated)
        {
            super(encodedStatus);
            this.updated = updated;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            StampedNodeInfo nodeInfo = (StampedNodeInfo) o;
            return updated == nodeInfo.updated && encodedStatus == nodeInfo.encodedStatus;
        }

        StampedNodeInfo withEncodedStatus(long newEncodedStatus)
        {
            return new StampedNodeInfo(newEncodedStatus, updated);
        }

        StampedNodeInfo with(Status newStatus)
        {
            return withEncodedStatus(withStatus(encodedStatus, newStatus));
        }

        StampedNodeInfo with(Status status, long updatedMillis)
        {
            return new StampedNodeInfo(withStatus(encodedStatus, status), updatedMillis);
        }

        StampedNodeInfo withStale(boolean isStale)
        {
            return withEncodedStatus((encodedStatus & ~STALE_BIT) | (isStale ? STALE_BIT : 0));
        }

        StampedNodeInfo withUnreadable(boolean isUnreadable)
        {
            return withEncodedStatus((encodedStatus & ~UNREADABLE_BIT) | (isUnreadable ? UNREADABLE_BIT : 0));
        }

        @Override
        public String toString()
        {
            return "NodeInfo{" +
                   "status=" + status() +
                   (isStale() ? ", STALE" : "") +
                   (isUnreadable() ? ", UNREADABLE" : "") +
                   ", updated=" + updated +
                   '}';
        }

        public static final MetadataSerializer<StampedNodeInfo> serializer = new MetadataSerializer<StampedNodeInfo>()
        {
            @Override
            public void serialize(StampedNodeInfo info, DataOutputPlus out, Version version) throws IOException
            {
                out.writeUnsignedVInt(info.encodedStatus);
                out.writeUnsignedVInt(info.updated);
            }

            @Override
            public StampedNodeInfo deserialize(DataInputPlus in, Version version) throws IOException
            {
                long encodedStatus = in.readUnsignedVInt();
                long updated = in.readUnsignedVInt();
                return new StampedNodeInfo(encodedStatus, updated);
            }

            @Override
            public long serializedSize(StampedNodeInfo info, Version version)
            {
                return TypeSizes.sizeofUnsignedVInt(info.encodedStatus) + TypeSizes.sizeofUnsignedVInt(info.updated);
            }
        };
    }

    private final Int2ObjectHashMap<StampedNodeInfo> info;
    private final SortedArrayList<Node.Id> excludeFromFastQuorum;
    private final SortedArrayList<Node.Id> hardRemoved;
    private final SortedArrayList<Node.Id> stale;
    private final Epoch lastModified;

    AccordNodeInfos(Int2ObjectHashMap<StampedNodeInfo> info, Epoch lastModified)
    {
        this.info = info;
        // these are pure functions of info, and are consumed on every topology calculation, so compute them once here
        this.excludeFromFastQuorum = select(info, i -> !i.includeInFastQuorums());
        this.hardRemoved = select(info, i -> i.is(Status.HARD_REMOVED));
        this.stale = select(info, AccordNodeInfo::isStale);
        this.lastModified = lastModified;
    }

    private static SortedArrayList<Node.Id> select(Int2ObjectHashMap<StampedNodeInfo> info, Predicate<AccordNodeInfo> test)
    {
        if (info.isEmpty())
            return NO_IDS;

        List<Node.Id> ids = null;
        Int2ObjectHashMap<StampedNodeInfo>.EntryIterator iterator = info.entrySet().iterator();
        while (iterator.hasNext())
        {
            iterator.next();
            if (test.test(iterator.getValue()))
            {
                if (ids == null)
                    ids = new ArrayList<>();
                ids.add(new Node.Id(iterator.getIntKey()));
            }
        }
        return ids == null ? NO_IDS : SortedArrayList.ofUnsorted(ids.toArray(new Node.Id[0]));
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AccordNodeInfos that = (AccordNodeInfos) o;
        return info.equals(that.info) && lastModified.equals(that.lastModified);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(info, lastModified);
    }

    public AccordNodeInfo get(Node.Id id)
    {
        return info.get(id.id);
    }

    public AccordNodeInfo get(NodeId id)
    {
        return info.get(id.id());
    }

    public StampedNodeInfo getOrDefault(Node.Id id)
    {
        StampedNodeInfo current = info.get(id.id);
        return current == null ? StampedNodeInfo.DEFAULT : current;
    }

    public Status status(Node.Id id)
    {
        return getOrDefault(id).status();
    }

    @Override
    public String toString()
    {
        return "AccordNodeInfos{info=" + info + ", lastModified=" + lastModified + '}';
    }

    public AccordNodeInfos withNodes(Iterable<NodeId> members)
    {
        Int2ObjectHashMap<StampedNodeInfo> copy = null;
        for (NodeId member : members)
        {
            if (info.containsKey(member.id()))
                continue;

            if (copy == null)
                copy = new Int2ObjectHashMap<>(info);
            copy.put(member.id(), StampedNodeInfo.DEFAULT);
        }
        return copy == null ? this : new AccordNodeInfos(copy, lastModified);
    }

    /**
     * Remove no longer needed entries - currently not wired in,
     * as we need to remove only once the epoch has been expired by Accord
     */
    public AccordNodeInfos withoutNodes(Iterable<NodeId> members)
    {
        Int2ObjectHashMap<StampedNodeInfo> copy = null;
        for (NodeId member : members)
        {
            if (!info.containsKey(member.id()))
                continue;

            if (copy == null)
                copy = new Int2ObjectHashMap<>(info);
            copy.remove(member.id());
        }
        return copy == null ? this : new AccordNodeInfos(copy, lastModified);
    }

    /**
     * Members that have been removed from the Directory are marked REMOVED
     */
    public AccordNodeInfos withRemoved(Iterable<NodeId> members)
    {
        Int2ObjectHashMap<StampedNodeInfo> copy = null;
        for (NodeId member : members)
        {
            StampedNodeInfo current = info.get(member.id());
            if (current != null && current.status().isRemoved())
                continue;

            if (copy == null)
                copy = new Int2ObjectHashMap<>(info);
            copy.put(member.id(), current == null ? new StampedNodeInfo(Status.REMOVED.ordinal(), 0)
                                                  : current.with(Status.REMOVED));
        }
        return copy == null ? this : new AccordNodeInfos(copy, lastModified);
    }

    private static boolean shouldUpdateDownStatus(StampedNodeInfo current, Status newStatus, long nowMillis, long updateDelayMillis)
    {
        Invariants.require(newStatus.compareTo(Status.MAYBE_DOWN) <= 0);
        if (current == null)
            return true; // will effectively populate it

        if (current.is(newStatus))
            return false;

        Status currentStatus = current.status();
        switch (currentStatus)
        {
            default: throw new UnhandledEnum(currentStatus);
            case UNKNOWN_STATUS:
            case REMOVED:
            case HARD_REMOVED:
                return false;

            case SHUTDOWN:
                if (newStatus == Status.MAYBE_DOWN)
                    return false; // avoid race condition between peer-declared MAYBE_DOWN and self-declared SHUTDOWN
            case NORMAL:
            case MAYBE_DOWN:
                if (newStatus == Status.SHUTDOWN) // TODO (expected): remove SHUTDOWN branches once alpha versions are no longer extant to upgrade from, as we no longer declare transitions from SHUTDOWN
                    return true; // self-declared, only when cluster min_version < MIN_VERSION; ignores updateDelayMillis

                return nowMillis - current.updated >= updateDelayMillis;
        }
    }

    public boolean shouldUpdateDownStatus(Node.Id node, Status newStatus, long nowMillis, long updateDelayMillis)
    {
        return shouldUpdateDownStatus(info.get(node.id), newStatus, nowMillis, updateDelayMillis);
    }

    /**
     * Note that &lt MIN_VERSION nodes also send SHUTDOWN statuses for themselves via this message type.
     */
    public AccordNodeInfos maybeChangeDownStatus(Node.Id node, Status newStatus, long nowMillis, long updateDelayMillis)
    {
        Invariants.require(newStatus == Status.MAYBE_DOWN || newStatus == Status.NORMAL || newStatus == Status.SHUTDOWN,
                           "%s is not a status that should be updated by AccordChangeDownStatus", newStatus);

        StampedNodeInfo current = info.get(node.id);
        if (!shouldUpdateDownStatus(current, newStatus, nowMillis, updateDelayMillis))
            return this;

        Int2ObjectHashMap<StampedNodeInfo> copy = new Int2ObjectHashMap<>(info);
        copy.put(node.id, current == null ? new StampedNodeInfo(newStatus.ordinal(), nowMillis) : current.with(newStatus, nowMillis));
        return new AccordNodeInfos(copy, lastModified);
    }

    /**
     * Apply a change to some of a node's fields, leaving the rest alone. The node must still be a member: a removed node
     * has nothing left to say about it, except that it is now hard removed.
     */
    public AccordNodeInfos withNodeInfo(Node.Id node, AccordNodeInfo.Delta delta, long updatedMillis)
    {
        StampedNodeInfo current = getOrDefault(node);

        Status newStatus = Status.status((int) (delta.apply(current.encodedStatus) & AccordNodeInfo.STATUS_MASK));
        switch (current.status())
        {
            default: throw new UnhandledEnum(current.status());
            case HARD_REMOVED:
                throw new InvalidRequestException(String.format("Cannot update %s: it is hard removed", node));
            case REMOVED:
                if (newStatus != Status.HARD_REMOVED)
                    throw new InvalidRequestException(String.format("Cannot update %s to %s: it is removed", node, newStatus));
            case UNKNOWN_STATUS:
            case NORMAL:
            case MAYBE_DOWN:
            case SHUTDOWN:
                break;
        }

        StampedNodeInfo newInfo = new StampedNodeInfo(delta.apply(current.encodedStatus), updatedMillis);
        if (newInfo.equals(current))
            return this;

        Int2ObjectHashMap<StampedNodeInfo> copy = new Int2ObjectHashMap<>(info);
        copy.put(node.id, newInfo);
        return new AccordNodeInfos(copy, lastModified);
    }

    public AccordNodeInfos withStale(SortedArrayList<Node.Id> markStale)
    {
        return withStale(markStale, true);
    }

    public AccordNodeInfos withoutStale(SortedArrayList<Node.Id> unmarkStale)
    {
        return withStale(unmarkStale, false);
    }

    private AccordNodeInfos withStale(SortedArrayList<Node.Id> update, boolean isStale)
    {
        Int2ObjectHashMap<StampedNodeInfo> copy = null;
        for (Node.Id node : update)
        {
            StampedNodeInfo current = info.get(node.id);
            if (current == null)
            {
                if (!isStale)
                    continue;
                current = StampedNodeInfo.DEFAULT;
            }
            else if (current.isStale() == isStale)
            {
                continue;
            }

            if (copy == null)
                copy = new Int2ObjectHashMap<>(info);
            copy.put(node.id, current.withStale(isStale));
        }
        return copy == null ? this : new AccordNodeInfos(copy, lastModified);
    }

    public AccordNodeInfos withHardRemoved(SortedArrayList<Node.Id> markHardRemoved)
    {
        Int2ObjectHashMap<StampedNodeInfo> copy = null;
        for (Node.Id node : markHardRemoved)
        {
            StampedNodeInfo current = info.get(node.id);
            if (current != null && current.is(Status.HARD_REMOVED))
                continue;

            if (copy == null)
                copy = new Int2ObjectHashMap<>(info);
            copy.put(node.id, current == null ? new StampedNodeInfo(Status.HARD_REMOVED.ordinal(), 0)
                                              : current.with(Status.HARD_REMOVED));
        }
        return copy == null ? this : new AccordNodeInfos(copy, lastModified);
    }

    public AccordNodeInfos withLastModified(Epoch epoch)
    {
        return new AccordNodeInfos(info, epoch);
    }

    public Epoch lastModified()
    {
        return lastModified;
    }

    public SortedArrayList<Node.Id> excludedFromFastQuorum()
    {
        return excludeFromFastQuorum;
    }

    public SortedArrayList<Node.Id> hardRemoved()
    {
        return hardRemoved;
    }

    public SortedArrayList<Node.Id> stale()
    {
        return stale;
    }

    public static final MetadataSerializer<AccordNodeInfos> serializer = new MetadataSerializer<>()
    {
        private void serializeMap(Int2ObjectHashMap<StampedNodeInfo> map, DataOutputPlus out, Version version) throws IOException
        {
            out.writeUnsignedVInt32(map.size());
            Int2ObjectHashMap<StampedNodeInfo>.EntryIterator iterator = map.entrySet().iterator();
            while (iterator.hasNext())
            {
                iterator.next();
                out.writeInt(iterator.getIntKey()); // inherited from TopologySerializers
                StampedNodeInfo.serializer.serialize(iterator.getValue(), out, version);
            }
        }

        public void serialize(AccordNodeInfos nodeInfos, DataOutputPlus out, Version version) throws IOException
        {
            serializeMap(nodeInfos.info, out, version);
            Epoch.serializer.serialize(nodeInfos.lastModified, out, version);
        }

        private Int2ObjectHashMap<StampedNodeInfo> deserializeMap(DataInputPlus in, Version version) throws IOException
        {
            int size = in.readUnsignedVInt32();
            if (size == 0)
                return EMPTY.info;

            Int2ObjectHashMap<StampedNodeInfo> map = new Int2ObjectHashMap<>((int)((size + 1)/0.65f), 0.65f, false);
            for (int i = 0; i < size; i++)
                map.put(in.readInt(), StampedNodeInfo.serializer.deserialize(in, version));
            return map;
        }

        public AccordNodeInfos deserialize(DataInputPlus in, Version version) throws IOException
        {
            return new AccordNodeInfos(deserializeMap(in, version),
                                       Epoch.serializer.deserialize(in, version));
        }

        private long serializedMapSize(Int2ObjectHashMap<StampedNodeInfo> map, Version version)
        {
            long size = TypeSizes.sizeofUnsignedVInt(map.size());
            size += (long) map.size() * TypeSizes.INT_SIZE;
            Int2ObjectHashMap<StampedNodeInfo>.EntryIterator iterator = map.entrySet().iterator();
            while (iterator.hasNext())
            {
                iterator.next();
                size += StampedNodeInfo.serializer.serializedSize(iterator.getValue(), version);
            }
            return size;
        }

        public long serializedSize(AccordNodeInfos nodeInfos, Version version)
        {
            return serializedMapSize(nodeInfos.info, version)
                   + Epoch.serializer.serializedSize(nodeInfos.lastModified, version);
        }
    };
}
