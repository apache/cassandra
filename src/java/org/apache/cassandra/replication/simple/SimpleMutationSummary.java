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

package org.apache.cassandra.replication.simple;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.MutationId;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.replication.MutationSummary;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

public class SimpleMutationSummary implements MutationSummary
{
    public final TableId tableId;
    public final ImmutableSortedMap<DecoratedKey, ImmutableSortedSet<MutationId>> ids;

    private SimpleMutationSummary(TableId tableId, ImmutableSortedMap<DecoratedKey, ImmutableSortedSet<MutationId>> ids)
    {
        this.tableId = tableId;
        this.ids = ids;
    }

    public static SimpleMutationSummary empty(TableId tableId)
    {
        return new SimpleMutationSummary(tableId, ImmutableSortedMap.of());
    }

    public SimpleMutationSummary merge(SimpleMutationSummary that)
    {
        Preconditions.checkArgument(this.tableId.equals(that.tableId));
        Map<DecoratedKey, ImmutableSortedSet<MutationId>> merged = new HashMap<>(ids);
        for (Map.Entry<DecoratedKey, ImmutableSortedSet<MutationId>> entry : that.ids.entrySet())
        {
            if (merged.containsKey(entry.getKey()))
            {
                Set<MutationId> keyIds = new HashSet<>(merged.get(entry.getKey()));
                keyIds.addAll(entry.getValue());
                merged.put(entry.getKey(), ImmutableSortedSet.copyOf(keyIds));
            }
            else
            {
                merged.put(entry.getKey(), entry.getValue());
            }
        }

        return new SimpleMutationSummary(tableId, ImmutableSortedMap.copyOf(merged));
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || getClass() != o.getClass()) return false;
        SimpleMutationSummary that = (SimpleMutationSummary) o;
        return Objects.equals(ids, that.ids);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(ids);
    }

    @Override
    public TableId tableId()
    {
        return tableId;
    }

    @Override
    public long digest()
    {
        return hashCode();
    }

    public static SimpleMutationSummary of(TableId tableId, DecoratedKey key, SortedSet<MutationId> ids)
    {
        return new SimpleMutationSummary(tableId, ImmutableSortedMap.of(key, ImmutableSortedSet.copyOf(ids)));
    }

    public static SimpleMutationSummary of(TableId tableId, DecoratedKey key, MutationId... ids)
    {
        return new SimpleMutationSummary(tableId, ImmutableSortedMap.of(key, ImmutableSortedSet.copyOf(ids)));
    }

    public static final IVersionedSerializer<SimpleMutationSummary> serializer = new IVersionedSerializer<SimpleMutationSummary>()
    {
        @Override
        public void serialize(SimpleMutationSummary summary, DataOutputPlus out, int version) throws IOException
        {
            summary.tableId.serialize(out);
            out.writeInt(summary.ids.size());
            for (Map.Entry<DecoratedKey, ImmutableSortedSet<MutationId>> entry : summary.ids.entrySet())
            {
                ByteBufferUtil.writeWithVIntLength(entry.getKey().getKey(), out);
                out.writeInt(entry.getValue().size());
                for (MutationId id : entry.getValue())
                    MutationId.serializer.serialize(id, out, version);
            }
        }

        @Override
        public SimpleMutationSummary deserialize(DataInputPlus in, int version) throws IOException
        {
            TableId tableId = TableId.deserialize(in);
            TableMetadata metadata = Schema.instance.getTableMetadata(tableId);
            IPartitioner partitioner = metadata.partitioner;

            int numKeys = in.readInt();
            if (numKeys == 0)
                return empty(tableId);

            Map<DecoratedKey, ImmutableSortedSet<MutationId>> ids = Maps.newHashMapWithExpectedSize(numKeys);
            for (int i = 0; i < numKeys; i++)
            {
                DecoratedKey key = partitioner.decorateKey(ByteBufferUtil.readWithVIntLength(in));
                int numIds = in.readInt();
                Set<MutationId> keyIds = Sets.newHashSetWithExpectedSize(numIds);
                for (int j = 0; j < numIds; j++)
                    keyIds.add(MutationId.serializer.deserialize(in, version));

                ids.put(key, ImmutableSortedSet.copyOf(keyIds));
            }

            return new SimpleMutationSummary(tableId, ImmutableSortedMap.copyOf(ids));
        }

        @Override
        public long serializedSize(SimpleMutationSummary summary, int version)
        {

            long size = summary.tableId.serializedSize();
            size += TypeSizes.sizeof(summary.ids.size());
            for (Map.Entry<DecoratedKey, ImmutableSortedSet<MutationId>> entry : summary.ids.entrySet())
            {
                size += ByteBufferUtil.serializedSizeWithVIntLength(entry.getKey().getKey());
                size += TypeSizes.sizeof(entry.getValue().size());
                for (MutationId id : entry.getValue())
                    size += MutationId.serializer.serializedSize(id, version);
            }
            return size;
        }
    };
}
