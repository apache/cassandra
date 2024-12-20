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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.MutationId;
import org.apache.cassandra.replication.MutationSummary;

public class SimpleMutationSummary implements MutationSummary
{
    private static final SimpleMutationSummary EMPTY = new SimpleMutationSummary(ImmutableSortedMap.of());

    @VisibleForTesting
    public final ImmutableSortedMap<DecoratedKey, ImmutableSortedSet<MutationId>> ids;

    private SimpleMutationSummary(ImmutableSortedMap<DecoratedKey, ImmutableSortedSet<MutationId>> ids)
    {
        this.ids = ids;
    }

    public static class Builder
    {
        private final Map<DecoratedKey, Set<MutationId>> ids = new HashMap<>();

        public Builder add(DecoratedKey key, MutationId id)
        {
            ids.computeIfAbsent(key, k -> new TreeSet<>()).add(id);
            return this;
        }

        public Builder add(DecoratedKey key, Collection<MutationId> id)
        {
            ids.computeIfAbsent(key, k -> new TreeSet<>()).addAll(id);
            return this;
        }

        public SimpleMutationSummary build()
        {
            ImmutableSortedMap.Builder<DecoratedKey, ImmutableSortedSet<MutationId>> builder = ImmutableSortedMap.builder();
            for (Map.Entry<DecoratedKey, Set<MutationId>> entry : ids.entrySet())
                builder.put(entry.getKey(), ImmutableSortedSet.copyOf(entry.getValue()));

            return new SimpleMutationSummary(builder.build());
        }
    }

    public static Builder builder()
    {
        return new Builder();
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
}
