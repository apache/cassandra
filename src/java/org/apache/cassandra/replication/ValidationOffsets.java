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
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.CollectionSerializers;

/**
 * Per-log offset frontier produced by mutation-tracking incremental repair, used by the
 * tracked keyspace reconciled data validation to filter SSTables
 * ({@link #containsAll(ImmutableCoordinatorLogOffsets)}) and journal mutations
 * ({@link #containsMutation(ShortMutationId)}) down to what's safe to compare.
 */
public final class ValidationOffsets
{
    public static final ValidationOffsets EMPTY = new ValidationOffsets(ImmutableMap.of());

    private final ImmutableMap<CoordinatorLogId, Offsets.Immutable> perLog;

    public ValidationOffsets(Map<CoordinatorLogId, Offsets.Immutable> perLog)
    {
        this.perLog = ImmutableMap.copyOf(perLog);
    }

    /**
     * @return true iff {@code sstableOffsets} is non-empty and every offset in it is ≤ these
     * offsets. An SSTable with no offsets at all (bulk import, or written before mutation
     * tracking existed) or one that references a log these offsets don't know about is NOT
     * considered contained.
     */
    public boolean containsAll(ImmutableCoordinatorLogOffsets sstableOffsets)
    {
        if (sstableOffsets == null || sstableOffsets.isEmpty())
            return false;
        boolean sawEntry = false;
        for (Map.Entry<Long, Offsets.Immutable> entry : sstableOffsets.entries())
        {
            sawEntry = true;
            CoordinatorLogId logId = CoordinatorLogId.fromLong(entry.getKey());
            Offsets.Immutable offsetsForLog = perLog.get(logId);
            if (offsetsForLog == null)
                return false;
            if (!offsetsContainAll(offsetsForLog, entry.getValue()))
                return false;
        }
        return sawEntry;
    }

    /**
     * @return true iff the mutation's offset is ≤ these offsets for the mutation's log.
     */
    public boolean containsMutation(ShortMutationId id)
    {
        Offsets.Immutable offsetsForLog = perLog.get(id.asLogId());
        return offsetsForLog != null && offsetsForLog.contains(id.offset());
    }

    private static boolean offsetsContainAll(Offsets.Immutable superset, Offsets.Immutable subset)
    {
        for (ShortMutationId id : subset)
        {
            if (!superset.contains(id.offset()))
                return false;
        }
        return true;
    }

    @Override
    public String toString()
    {
        return "ValidationOffsets{logs=" + perLog.size() + '}';
    }

    public static final UnversionedSerializer<ValidationOffsets> serializer = new UnversionedSerializer<>()
    {
        private final UnversionedSerializer<Map<CoordinatorLogId, Offsets.Immutable>> perLogSerializer =
        CollectionSerializers.newMapSerializer(CoordinatorLogId.serializer, Offsets.serializer);

        public void serialize(ValidationOffsets offsets, DataOutputPlus out) throws IOException
        {
            perLogSerializer.serialize(offsets.perLog, out);
        }

        public ValidationOffsets deserialize(DataInputPlus in) throws IOException
        {
            Map<CoordinatorLogId, Offsets.Immutable> perLog = perLogSerializer.deserialize(in);
            return new ValidationOffsets(perLog);
        }

        public long serializedSize(ValidationOffsets offsets)
        {
            return perLogSerializer.serializedSize(offsets.perLog);
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
