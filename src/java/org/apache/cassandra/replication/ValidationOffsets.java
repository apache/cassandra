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
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.agrona.collections.Long2ObjectHashMap;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

/**
 * Per-log offset frontier produced by mutation-tracking incremental repair, used by the
 * tracked keyspace reconciled data validation to filter SSTables
 * ({@link #covers(ImmutableCoordinatorLogOffsets, Collection)}) and journal mutations
 * ({@link #containsMutation(ShortMutationId)}) down to what's safe to compare.
 */
public final class ValidationOffsets
{
    public static final ValidationOffsets EMPTY = new ValidationOffsets(ImmutableMap.of());

    private final Long2ObjectHashMap<Offsets.Immutable> perLog;

    public ValidationOffsets(Map<CoordinatorLogId, Offsets.Immutable> perLog)
    {
        this.perLog = new Long2ObjectHashMap<>(perLog.size(), 0.9f, false);
        for (Map.Entry<CoordinatorLogId, Offsets.Immutable> entry : perLog.entrySet())
            this.perLog.put(entry.getKey().asLong(), entry.getValue());
    }

    /**
     * @return true iff {@code sstableOffsets} is non-empty and every offset in it is ≤ these
     * offsets, or belongs to a log whose shard doesn't intersect {@code ranges}. An SSTable with no offsets at
     * all (bulk import, or written before mutation tracking existed) is NOT considered covered.
     */
    public boolean covers(ImmutableCoordinatorLogOffsets sstableOffsets, Collection<Range<Token>> ranges)
    {
        if (sstableOffsets == null || sstableOffsets.isEmpty())
            return false;
        boolean sawEntry = false;
        for (Map.Entry<Long, Offsets.Immutable> entry : sstableOffsets.entries())
        {
            sawEntry = true;
            long logId = entry.getKey();
            Offsets.Immutable offsetsForLog = perLog.get(logId);
            if (offsetsForLog == null)
            {
                if (logShardIntersects(CoordinatorLogId.fromLong(logId), ranges))
                    return false;
                continue;
            }
            if (!Offsets.Immutable.difference(entry.getValue(), offsetsForLog).isEmpty())
                return false;
        }
        return sawEntry;
    }

    /**
     * @return true if the log's shard intersects {@code ranges}, or the shard is unknown
     */
    private static boolean logShardIntersects(CoordinatorLogId logId, Collection<Range<Token>> ranges)
    {
        Range<Token> shardRange = MutationTrackingService.instance().rangeForLog(logId);
        if (shardRange == null)
            return true;
        for (Range<Token> range : ranges)
        {
            if (range.intersects(shardRange))
                return true;
        }
        return false;
    }

    /**
     * @return true iff the mutation's offset is ≤ these offsets for the mutation's log.
     */
    public boolean containsMutation(ShortMutationId id)
    {
        Offsets.Immutable offsetsForLog = perLog.get(id.logId());
        return offsetsForLog != null && offsetsForLog.contains(id.offset());
    }

    @Override
    public String toString()
    {
        return "ValidationOffsets{logs=" + perLog.size() + '}';
    }

    public static final UnversionedSerializer<ValidationOffsets> serializer = new UnversionedSerializer<>()
    {
        public void serialize(ValidationOffsets offsets, DataOutputPlus out) throws IOException
        {
            out.writeUnsignedVInt32(offsets.perLog.size());
            for (Offsets.Immutable offsetsForLog : offsets.perLog.values())
                Offsets.serializer.serialize(offsetsForLog, out);
        }

        public ValidationOffsets deserialize(DataInputPlus in) throws IOException
        {
            int size = in.readUnsignedVInt32();
            Map<CoordinatorLogId, Offsets.Immutable> perLog = new HashMap<>();
            for (int i = 0; i < size; i++)
            {
                Offsets.Immutable offsetsForLog = Offsets.serializer.deserialize(in);
                perLog.put(offsetsForLog.logId(), offsetsForLog);
            }
            return new ValidationOffsets(perLog);
        }

        public long serializedSize(ValidationOffsets offsets)
        {
            long size = TypeSizes.sizeofUnsignedVInt(offsets.perLog.size());
            for (Offsets.Immutable offsetsForLog : offsets.perLog.values())
                size += Offsets.serializer.serializedSize(offsetsForLog);
            return size;
        }
    };

    /**
     * Flattens log-to-offset maps, taking the union when multiple offsets are reported for the same log.
     */
    public static ValidationOffsets flatten(Iterable<Map<CoordinatorLogId, Offsets.Immutable>> logToOffsetMaps)
    {
        Map<CoordinatorLogId, Offsets.Immutable> flattened = new HashMap<>();
        for (Map<CoordinatorLogId, Offsets.Immutable> logToOffsetMap : logToOffsetMaps)
            for (Map.Entry<CoordinatorLogId, Offsets.Immutable> entry : logToOffsetMap.entrySet())
                flattened.merge(entry.getKey(), entry.getValue(), Offsets.Immutable::union);
        return new ValidationOffsets(flattened);
    }
}
