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

import javax.annotation.concurrent.ThreadSafe;

import com.google.common.collect.Iterators;

import org.apache.cassandra.replication.MutationId;
import org.apache.cassandra.replication.ShortMutationId;
import org.jctools.maps.NonBlockingHashMapLong;

/**
 * A replica can only receive writes from another replica it shares ranges with, and tracked writes are executed by
 * coordinators, so this should contain up to (2*RF - 1) keys.
 * Consider wrapping value in AtomicReference to avoid false sharing, see: https://trishagee.com/2011/07/22/dissecting_the_disruptor_why_its_so_fast_part_two__magic_cache_line_padding/
 */
@ThreadSafe
class CoordinatorLogBoundariesMap extends NonBlockingHashMapLong<MutationId> implements MutableCoordinatorLogBoundaries
{
    private static final MutationId NONE = MutationId.none();
    private static final int NONE_OFFSET = NONE.offset();

    protected CoordinatorLogBoundariesMap(int size)
    {
       super(size);
    }

    protected CoordinatorLogBoundariesMap()
    {
        super();
    }

    public void add(MutationId mutationId)
    {
        long logId = mutationId.logId();
        merge(logId, mutationId, (existing, updating) -> {
            if (ShortMutationId.comparator.compare(existing, updating) < 0)
                return updating;
            return existing;
        });
    }

    @Override
    public int maxOffset(long logId)
    {
        MutationId id = get(logId);
        return id == null ? NONE_OFFSET : id.offset();
    }

    @Override
    public MutationId max(long logId)
    {
        return getOrDefault(logId, NONE);
    }

    @Override
    public Iterator<Long> iterator()
    {
        return Iterators.unmodifiableIterator(keySet().iterator());
    }
}
