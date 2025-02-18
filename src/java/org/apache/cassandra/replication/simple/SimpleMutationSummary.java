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
import java.util.Map;
import java.util.Objects;
import java.util.SortedSet;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.MutationId;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.replication.MutationSummary;
import org.apache.cassandra.utils.ByteBufferUtil;

public class SimpleMutationSummary implements MutationSummary
{
    private static final SimpleMutationSummary EMPTY = new SimpleMutationSummary(ImmutableSortedMap.of());

    @VisibleForTesting
    public final ImmutableSortedMap<DecoratedKey, ImmutableSortedSet<MutationId>> ids;

    private SimpleMutationSummary(ImmutableSortedMap<DecoratedKey, ImmutableSortedSet<MutationId>> ids)
    {
        this.ids = ids;
    }

    public SimpleMutationSummary merge(SimpleMutationSummary that)
    {
        ImmutableSortedMap.Builder<DecoratedKey, ImmutableSortedSet<MutationId>> builder = ImmutableSortedMap.builder();
        builder.putAll(this.ids);
        builder.putAll(that.ids);
        return new SimpleMutationSummary(builder.build());
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
    public long digest()
    {
        return hashCode();
    }

    public static SimpleMutationSummary empty()
    {
        return EMPTY;
    }

    public static SimpleMutationSummary of(DecoratedKey key, SortedSet<MutationId> ids)
    {
        return new SimpleMutationSummary(ImmutableSortedMap.of(key, ImmutableSortedSet.copyOf(ids)));
    }

    public static SimpleMutationSummary of(DecoratedKey key, MutationId... ids)
    {
        return new SimpleMutationSummary(ImmutableSortedMap.of(key, ImmutableSortedSet.copyOf(ids)));
    }

    public static MutationSummary.Serializer<SimpleMutationSummary> serializer = new Serializer<SimpleMutationSummary>()
    {
        @Override
        public void serialize(SimpleMutationSummary summary, DataOutputPlus out, int version) throws IOException
        {
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
        public SimpleMutationSummary deserialize(IPartitioner partitioner, DataInputPlus in, int version) throws IOException
        {
            int numKeys = in.readInt();
            if (numKeys == 0)
                return empty();

            ImmutableSortedMap.Builder<DecoratedKey, ImmutableSortedSet<MutationId>> builder = ImmutableSortedMap.builder();
            for (int i = 0; i < numKeys; i++)
            {
                DecoratedKey key = partitioner.decorateKey(ByteBufferUtil.readWithVIntLength(in));
                int numIds = in.readInt();
                ImmutableSortedSet.Builder<MutationId> ids = ImmutableSortedSet.builder();
                for (int j = 0; j < numIds; j++)
                    ids.add(MutationId.serializer.deserialize(in, version));

                builder.put(key, ids.build());
            }

            return new SimpleMutationSummary(builder.build());
        }

        @Override
        public long serializedSize(SimpleMutationSummary summary, int version)
        {
            long size = TypeSizes.sizeof(summary.ids.size());
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
