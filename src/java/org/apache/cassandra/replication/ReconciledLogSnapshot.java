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
import java.util.Objects;
import java.util.function.BiConsumer;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

/**
 * Container for reconciled offsets organized by keyspace and shard, including range information for each log.
 * This is similar to LogReconciledOffsets but adds range tracking for coordinator logs.
 */
public class ReconciledLogSnapshot
{
    public static final IVersionedSerializer<ReconciledLogSnapshot> serializer = new Serializer();

    private final ImmutableMap<String, ReconciledKeyspaceOffsets> reconciled;

    private ReconciledLogSnapshot(ImmutableMap<String, ReconciledKeyspaceOffsets> reconciled)
    {
        this.reconciled = reconciled;
    }

    public boolean isFullyReconciled(String keyspace, ShortMutationId mutationId)
    {
        ReconciledKeyspaceOffsets keyspaceOffsets = reconciled.get(keyspace);
        if (keyspaceOffsets == null)
            return true;
        return keyspaceOffsets.isFullyReconciled(mutationId);
    }

    public void forEach(BiConsumer<String, ReconciledKeyspaceOffsets> consumer)
    {
        reconciled.forEach(consumer);
    }

    public Offsets.Immutable get(String keyspace, CoordinatorLogId logId)
    {
        ReconciledKeyspaceOffsets keyspaceOffsets = reconciled.get(keyspace);
        return keyspaceOffsets != null ? keyspaceOffsets.get(logId) : null;
    }

    public Range<Token> getRange(String keyspace, CoordinatorLogId logId)
    {
        ReconciledKeyspaceOffsets keyspaceOffsets = reconciled.get(keyspace);
        return keyspaceOffsets != null ? keyspaceOffsets.getRange(logId) : null;
    }

    public ReconciledKeyspaceOffsets getKeyspace(String keyspace)
    {
        return reconciled.get(keyspace);
    }

    public ImmutableMap<String, ReconciledKeyspaceOffsets> getAll()
    {
        return reconciled;
    }

    public boolean isEmpty()
    {
        return size() == 0;
    }

    public int size()
    {
        return reconciled.values().stream().mapToInt(ReconciledKeyspaceOffsets::size).sum();
    }

    /**
     * Creates a filtered subset of this snapshot containing only log entries whose ranges
     * intersect with the specified keyspace ranges.
     *
     * @param keyspaceRanges map of keyspace name to ranges to filter by
     * @return new ReconciledLogSnapshot containing only intersecting entries
     */
    public ReconciledLogSnapshot select(Map<String, ? extends Collection<Range<Token>>> keyspaceRanges)
    {
        ReconciledLogSnapshot.Builder builder = ReconciledLogSnapshot.builder();

        for (Map.Entry<String, ? extends Collection<Range<Token>>> entry : keyspaceRanges.entrySet())
        {
            String keyspace = entry.getKey();
            Collection<Range<Token>> targetRanges = entry.getValue();

            ReconciledKeyspaceOffsets keyspaceOffsets = reconciled.get(keyspace);
            if (keyspaceOffsets == null)
                continue;

            ReconciledKeyspaceOffsets.Builder keyspaceBuilder = builder.getKeyspaceBuilder(keyspace);
            keyspaceOffsets.selectIntersecting(targetRanges, keyspaceBuilder);
        }

        return builder.build();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReconciledLogSnapshot that = (ReconciledLogSnapshot) o;
        return Objects.equals(reconciled, that.reconciled);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(reconciled);
    }

    @Override
    public String toString()
    {
        return "ShardReconciledOffsets{" +
               "reconciled=" + reconciled +
               '}';
    }

    public static class Builder
    {
        private final Map<String, ReconciledKeyspaceOffsets.Builder> keyspaceBuilders = new HashMap<>();

        public Builder put(String keyspace, CoordinatorLogId logId, Offsets.Immutable offsets, Range<Token> range)
        {
            keyspaceBuilders.computeIfAbsent(keyspace, k -> ReconciledKeyspaceOffsets.builder())
                           .put(logId, offsets, range);
            return this;
        }

        ReconciledKeyspaceOffsets.Builder getKeyspaceBuilder(String keyspace)
        {
            return keyspaceBuilders.computeIfAbsent(keyspace, k -> ReconciledKeyspaceOffsets.builder());
        }

        public ReconciledLogSnapshot build()
        {
            ImmutableMap.Builder<String, ReconciledKeyspaceOffsets> builder = ImmutableMap.builder();
            for (Map.Entry<String, ReconciledKeyspaceOffsets.Builder> entry : keyspaceBuilders.entrySet())
            {
                ReconciledKeyspaceOffsets ks = entry.getValue().build();
                if (!ks.isEmpty())
                    builder.put(entry.getKey(), ks);
            }
            return new ReconciledLogSnapshot(builder.build());
        }
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Serializer implements IVersionedSerializer<ReconciledLogSnapshot>
    {
        @Override
        public void serialize(ReconciledLogSnapshot offsets, DataOutputPlus out, int version) throws IOException
        {
            out.writeInt(offsets.reconciled.size());

            for (Map.Entry<String, ReconciledKeyspaceOffsets> keyspaceEntry : offsets.reconciled.entrySet())
            {
                out.writeUTF(keyspaceEntry.getKey());
                ReconciledKeyspaceOffsets.serializer.serialize(keyspaceEntry.getValue(), out, version);
            }
        }

        @Override
        public ReconciledLogSnapshot deserialize(DataInputPlus in, int version) throws IOException
        {
            int keyspaceCount = in.readInt();
            ImmutableMap.Builder<String, ReconciledKeyspaceOffsets> builder = ImmutableMap.builder();

            for (int i = 0; i < keyspaceCount; i++)
            {
                String keyspace = in.readUTF();
                ReconciledKeyspaceOffsets keyspaceOffsets = ReconciledKeyspaceOffsets.serializer.deserialize(in, version);
                builder.put(keyspace, keyspaceOffsets);
            }

            return new ReconciledLogSnapshot(builder.build());
        }

        @Override
        public long serializedSize(ReconciledLogSnapshot offsets, int version)
        {
            long size = TypeSizes.sizeof(offsets.reconciled.size());

            for (Map.Entry<String, ReconciledKeyspaceOffsets> keyspaceEntry : offsets.reconciled.entrySet())
            {
                size += TypeSizes.sizeof(keyspaceEntry.getKey());
                size += ReconciledKeyspaceOffsets.serializer.serializedSize(keyspaceEntry.getValue(), version);
            }

            return size;
        }
    }
}