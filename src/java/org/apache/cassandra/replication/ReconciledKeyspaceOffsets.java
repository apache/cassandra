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
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class ReconciledKeyspaceOffsets
{
    public static final IVersionedSerializer<ReconciledKeyspaceOffsets> serializer = new Serializer();

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

    public static class Serializer implements IVersionedSerializer<ReconciledKeyspaceOffsets>
    {
        @Override
        public void serialize(ReconciledKeyspaceOffsets keyspaceOffsets, DataOutputPlus out, int version) throws IOException
        {
            out.writeInt(keyspaceOffsets.logEntries.size());

            keyspaceOffsets.logEntries.forEachLong((logId, entry) -> {
                try
                {
                    out.writeLong(logId);
                    Offsets.serializer.serialize(entry.offsets, out, version);
                    AbstractBounds.tokenSerializer.serialize(entry.range, out, version);
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
            });
        }

        @Override
        public ReconciledKeyspaceOffsets deserialize(DataInputPlus in, int version) throws IOException
        {
            int logCount = in.readInt();
            Long2ObjectHashMap<Entry> logEntries = new Long2ObjectHashMap<>();

            for (int j = 0; j < logCount; j++)
            {
                long logId = in.readLong();
                Offsets.Immutable offsets = Offsets.serializer.deserialize(in, version);
                Range<Token> range = (Range<Token>) AbstractBounds.tokenSerializer.deserialize(in, IPartitioner.global(), version);
                logEntries.put(logId, new Entry(offsets, range));
            }

            return new ReconciledKeyspaceOffsets(logEntries);
        }

        @Override
        public long serializedSize(ReconciledKeyspaceOffsets keyspaceOffsets, int version)
        {
            long size = TypeSizes.sizeof(keyspaceOffsets.logEntries.size());

            final long[] totalSize = { size };
            keyspaceOffsets.logEntries.forEachLong((logId, entry) -> {
                totalSize[0] += TypeSizes.sizeof(logId);
                totalSize[0] += Offsets.serializer.serializedSize(entry.offsets, version);
                totalSize[0] += AbstractBounds.tokenSerializer.serializedSize(entry.range, version);
            });

            return totalSize[0];
        }
    }
}
