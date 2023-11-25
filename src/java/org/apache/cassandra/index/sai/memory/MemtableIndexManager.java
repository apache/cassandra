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

package org.apache.cassandra.index.sai.memory;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.iterators.KeyRangeUnionIterator;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.FBUtilities;

public class MemtableIndexManager
{
    private final StorageAttachedIndex index;
    private final ConcurrentMap<Memtable, MemtableIndex> liveMemtableIndexMap;

    public MemtableIndexManager(StorageAttachedIndex index)
    {
        this.index = index;
        this.liveMemtableIndexMap = new ConcurrentHashMap<>();
    }

    public long index(DecoratedKey key, Row row, Memtable mt)
    {
        MemtableIndex current = liveMemtableIndexMap.get(mt);

        // We expect the relevant IndexMemtable to be present most of the time, so only make the
        // call to computeIfAbsent() if it's not. (see https://bugs.openjdk.java.net/browse/JDK-8161372)
        MemtableIndex target = (current != null)
                               ? current
                               : liveMemtableIndexMap.computeIfAbsent(mt, memtable -> new MemtableIndex(index));

        long start = Clock.Global.nanoTime();

        long bytes = 0;

        if (index.termType().isNonFrozenCollection())
        {
            Iterator<ByteBuffer> bufferIterator = index.termType().valuesOf(row, FBUtilities.nowInSeconds());
            if (bufferIterator != null)
            {
                while (bufferIterator.hasNext())
                {
                    ByteBuffer value = bufferIterator.next();
                    bytes += target.index(key, row.clustering(), value);
                }
            }
        }
        else
        {
            ByteBuffer value = index.termType().valueOf(key, row, FBUtilities.nowInSeconds());
            bytes += target.index(key, row.clustering(), value);
        }
        index.indexMetrics().memtableIndexWriteLatency.update(Clock.Global.nanoTime() - start, TimeUnit.NANOSECONDS);
        return bytes;
    }

    public long update(DecoratedKey key, Row oldRow, Row newRow, Memtable memtable)
    {
        if (!index.termType().isVector())
        {
            return index(key, newRow, memtable);
        }

        MemtableIndex target = liveMemtableIndexMap.get(memtable);
        if (target == null)
            return 0;

        ByteBuffer oldValue = index.termType().valueOf(key, oldRow, FBUtilities.nowInSeconds());
        ByteBuffer newValue = index.termType().valueOf(key, newRow, FBUtilities.nowInSeconds());
        return target.update(key, oldRow.clustering(), oldValue, newValue);
    }

    public void renewMemtable(Memtable renewed)
    {
        for (Memtable memtable : liveMemtableIndexMap.keySet())
        {
            // remove every index but the one that corresponds to the post-truncate Memtable
            if (renewed != memtable)
            {
                liveMemtableIndexMap.remove(memtable);
            }
        }
    }

    public void discardMemtable(Memtable discarded)
    {
        liveMemtableIndexMap.remove(discarded);
    }

    @Nullable
    public MemtableIndex getPendingMemtableIndex(LifecycleNewTracker tracker)
    {
        return liveMemtableIndexMap.keySet().stream()
                                   .filter(m -> tracker.equals(m.getFlushTransaction()))
                                   .findFirst()
                                   .map(liveMemtableIndexMap::get)
                                   .orElse(null);
    }

    public KeyRangeIterator searchMemtableIndexes(QueryContext queryContext, Expression e, AbstractBounds<PartitionPosition> keyRange)
    {
        Collection<MemtableIndex> memtableIndexes = liveMemtableIndexMap.values();

        if (memtableIndexes.isEmpty())
        {
            return KeyRangeIterator.empty();
        }

        KeyRangeIterator.Builder builder = KeyRangeUnionIterator.builder(memtableIndexes.size());

        for (MemtableIndex memtableIndex : memtableIndexes)
        {
            builder.add(memtableIndex.search(queryContext, e, keyRange));
        }

        return builder.build();
    }

    public KeyRangeIterator limitToTopResults(QueryContext context, List<PrimaryKey> source, Expression e)
    {
        Collection<MemtableIndex> memtables = liveMemtableIndexMap.values();

        if (memtables.isEmpty())
        {
            return KeyRangeIterator.empty();
        }

        KeyRangeUnionIterator.Builder builder = KeyRangeUnionIterator.builder(memtables.size());

        for (MemtableIndex index : memtables)
        {
            builder.add(index.limitToTopResults(source, e, context.vectorContext().limit()));
        }

        return builder.build();
    }

    public long liveMemtableWriteCount()
    {
        return liveMemtableIndexMap.values().stream().mapToLong(MemtableIndex::writeCount).sum();
    }

    public long estimatedMemIndexMemoryUsed()
    {
        return liveMemtableIndexMap.values().stream().mapToLong(MemtableIndex::estimatedMemoryUsed).sum();
    }

    @VisibleForTesting
    public int size()
    {
        return liveMemtableIndexMap.size();
    }

    public void invalidate()
    {
        liveMemtableIndexMap.clear();
    }
}
