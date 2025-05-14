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

package org.apache.cassandra.db;

import java.util.Iterator;

import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.collect.Iterators;

import org.agrona.collections.Long2ObjectHashMap;
import org.apache.cassandra.replication.MutationId;
import org.apache.cassandra.replication.ShortMutationId;

@NotThreadSafe
public class CoordinatorLogBoundariesBuilder
{
    private static final MutationId NONE = MutationId.none();
    private static final int NONE_OFFSET = NONE.offset();
    private final Long2ObjectHashMap<MutationId> ids;

    public CoordinatorLogBoundariesBuilder()
    {
        this.ids = new Long2ObjectHashMap<>();
    }

    public CoordinatorLogBoundariesBuilder add(MutationId mutationId)
    {
        if (mutationId.isNone())
            return this;
        long logId = mutationId.logId();

        MutationId existing = ids.get(logId);
        if (existing == null || ShortMutationId.comparator.compare(existing, mutationId) < 0)
            ids.put(mutationId.logId(), mutationId);
        return this;
    }

    public CoordinatorLogBoundariesBuilder addAll(CoordinatorLogBoundaries boundaries)
    {
        for (long logId : boundaries)
            add(boundaries.max(logId));
        return this;
    }

    public CoordinatorLogBoundaries build()
    {
        return new CoordinatorLogBoundaries()
        {
            @Override
            public int maxOffset(long logId)
            {
                MutationId id = ids.get(logId);
                return id == null ? NONE_OFFSET : id.offset();
            }

            @Override
            public MutationId max(long logId)
            {
                return ids.getOrDefault(logId, CoordinatorLogBoundariesBuilder.NONE);
            }

            @Override
            public int size()
            {
                return ids.size();
            }

            @Override
            public Iterator<Long> iterator()
            {
                return Iterators.unmodifiableIterator(ids.keySet().iterator());
            }
        };
    }
}
