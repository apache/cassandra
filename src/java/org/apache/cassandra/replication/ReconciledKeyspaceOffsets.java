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

package org.apache.cassandra.replication;

import java.io.IOException;
import java.util.Collection;
import java.util.Objects;
import java.util.function.BiConsumer;

import org.agrona.collections.Long2ObjectHashMap;

import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.CollectionSerializers;
import org.apache.cassandra.utils.Int64Serializer;

public class ReconciledKeyspaceOffsets
{
    /**
     * Simple data holder for offsets and their associated range
     */
    static class Entry
    {
        public final Offsets.Immutable offsets;
        public final Range<Token> range;

        public Entry(Offsets.Immutable offsets, Range<Token> range)
        {
            this.offsets = offsets;
            this.range = range;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Entry entry = (Entry) o;
            return Objects.equals(offsets, entry.offsets) && Objects.equals(range, entry.range);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(offsets, range);
        }

        @Override
        public String toString()
        {
            return "LogEntry{offsets=" + offsets + ", range=" + range + '}';
        }
    }

    private final Long2ObjectHashMap<Entry> logEntries;

    private ReconciledKeyspaceOffsets(Long2ObjectHashMap<Entry> logEntries)
    {
        this.logEntries = logEntries;
    }

    public boolean isFullyReconciled(ShortMutationId mutationId)
    {
        Entry entry = logEntries.get(mutationId.logId());
        return entry != null && entry.offsets.contains(mutationId.offset());
    }

    public boolean isFullyReconciled(long logId, Offsets.Immutable offsets)
    {
        Entry entry = logEntries.get(logId);
        if (entry == null)
            return false;

        Offsets.RangeIterator diff = Offsets.difference(offsets.rangeIterator(), entry.offsets.rangeIterator());
        return !diff.tryAdvance();
    }

    public Offsets.Immutable get(CoordinatorLogId logId)
    {
        Entry entry = logEntries.get(logId.asLong());
        return entry != null ? entry.offsets : null;
    }

    public Range<Token> getRange(CoordinatorLogId logId)
    {
        Entry entry = logEntries.get(logId.asLong());
        return entry != null ? entry.range : null;
    }

    public Entry getLogEntry(CoordinatorLogId logId)
    {
        return logEntries.get(logId.asLong());
    }

    public Long2ObjectHashMap<Offsets.Immutable> getAllOffsets()
    {
        Long2ObjectHashMap<Offsets.Immutable> result = new Long2ObjectHashMap<>();
        logEntries.forEachLong((logId, entry) -> result.put(logId, entry.offsets));
        return result;
    }

    public Long2ObjectHashMap<Range<Token>> getAllRanges()
    {
        Long2ObjectHashMap<Range<Token>> result = new Long2ObjectHashMap<>();
        logEntries.forEachLong((logId, entry) -> result.put(logId, entry.range));
        return result;
    }

    void forEach(BiConsumer<CoordinatorLogId, Entry> consumer)
    {
        logEntries.forEachLong((logId, entry) -> consumer.accept(new CoordinatorLogId(logId), entry));
    }

    public boolean isEmpty()
    {
        return logEntries.isEmpty();
    }

    public int size()
    {
        return logEntries.size();
    }

    public boolean contains(CoordinatorLogId logId)
    {
        return logEntries.containsKey(logId.asLong());
    }

    /**
     * Selects log entries whose ranges intersect with any of the target ranges
     * and adds them to the provided builder.
     *
     * @param targetRanges ranges to intersect with
     * @param builder      builder to add intersecting entries to
     */
    void selectIntersecting(Collection<Range<Token>> targetRanges, Builder builder)
    {
        logEntries.forEachLong((logId, entry) -> {
            Range<Token> logRange = entry.range;
            for (Range<Token> targetRange : targetRanges)
            {
                if (logRange.intersects(targetRange))
                {
                    builder.put(new CoordinatorLogId(logId), entry.offsets, entry.range);
                    break;
                }
            }
        });
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReconciledKeyspaceOffsets that = (ReconciledKeyspaceOffsets) o;
        return Objects.equals(logEntries, that.logEntries);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(logEntries);
    }

    @Override
    public String toString()
    {
        return "ReconciledKeyspaceOffsets{" +
               "logEntries=" + logEntries +
               '}';
    }

    public static class Builder
    {
        private final Long2ObjectHashMap<Entry> logEntries = new Long2ObjectHashMap<>();

        public Builder put(CoordinatorLogId logId, Offsets.Immutable reconciled, Range<Token> range)
        {
            logEntries.put(logId.asLong(), new Entry(reconciled, range));
            return this;
        }

        public ReconciledKeyspaceOffsets build()
        {
            return new ReconciledKeyspaceOffsets(logEntries);
        }
    }

    public static Builder builder()
    {
        return new Builder();
    }

    private static final VersionedSerializer<Entry> entrySerializer = new VersionedSerializer<>()
    {
        @Override
        public void serialize(Entry e, DataOutputPlus out, Version version) throws IOException
        {
            Offsets.serializer.serialize(e.offsets, out);
            AbstractBounds.tokenSerializer.serialize(e.range, out, version.messagingVersion());
        }

        @Override
        public Entry deserialize(DataInputPlus in, Version version) throws IOException
        {
            Offsets.Immutable offsets = Offsets.serializer.deserialize(in);
            Range<Token> range = (Range<Token>) AbstractBounds.tokenSerializer.deserialize(in, IPartitioner.global(), version.messagingVersion());
            return new Entry(offsets, range);
        }

        @Override
        public long serializedSize(Entry e, Version version)
        {
            return Offsets.serializer.serializedSize(e.offsets) + AbstractBounds.tokenSerializer.serializedSize(e.range, version.messagingVersion());
        }
    };

    public static final VersionedSerializer<ReconciledKeyspaceOffsets> serializer = new VersionedSerializer<>()
    {
        @Override
        public void serialize(ReconciledKeyspaceOffsets keyspaceOffsets, DataOutputPlus out, Version version) throws IOException
        {
            CollectionSerializers.serializeMap(
                keyspaceOffsets.logEntries, out, version, Int64Serializer.serializer, entrySerializer
            );
        }

        @Override
        public ReconciledKeyspaceOffsets deserialize(DataInputPlus in, Version version) throws IOException
        {
            Long2ObjectHashMap<Entry> logEntries =
                CollectionSerializers.deserializeMap(in, version, Int64Serializer.serializer, entrySerializer, i -> new Long2ObjectHashMap<>());
            return new ReconciledKeyspaceOffsets(logEntries);
        }

        @Override
        public long serializedSize(ReconciledKeyspaceOffsets keyspaceOffsets, Version version)
        {
            return CollectionSerializers.serializedMapSize(keyspaceOffsets.logEntries, version, Int64Serializer.serializer, entrySerializer);
        }
    };
}
