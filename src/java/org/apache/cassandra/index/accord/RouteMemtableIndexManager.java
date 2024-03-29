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

package org.apache.cassandra.index.accord;

import java.nio.ByteBuffer;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.schema.TableId;

import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.apache.cassandra.utils.Clock.Global.nowInSeconds;

public class RouteMemtableIndexManager implements MemtableIndexManager
{
    private final ConcurrentMap<Memtable, MemtableIndex> liveMemtableIndexMap = new ConcurrentHashMap<>();
    private final RouteIndex index;

    public RouteMemtableIndexManager(RouteIndex index)
    {
        this.index = index;
    }

    @Override
    public long index(DecoratedKey key, Row row, Memtable mt)
    {
        if (row.isStatic())
            return 0;
        //TODO (performance): we dropped jdk8 and this was fixed in jdk8... so do we need to do this still?
        MemtableIndex current = liveMemtableIndexMap.get(mt);

        // We expect the relevant IndexMemtable to be present most of the time, so only make the
        // call to computeIfAbsent() if it's not. (see https://bugs.openjdk.java.net/browse/JDK-8161372)
        MemtableIndex target = (current != null)
                               ? current
                               : liveMemtableIndexMap.computeIfAbsent(mt, memtable -> new MemtableIndex());

        long start = nanoTime();

        long bytes = 0;

        // simplified version of org.apache.cassandra.index.sai.utils.IndexTermType.valueOf
        Cell<?> cell = row.getCell(index.column());
        ByteBuffer value = cell == null || !cell.isLive(nowInSeconds()) ? null : cell.buffer();

        bytes += target.index(key, row.clustering(), value);
        index.indexMetrics().memtableIndexWriteLatency.update(nanoTime() - start, TimeUnit.NANOSECONDS);
        return bytes;
    }

    @Override
    public MemtableIndex getPendingMemtableIndex(LifecycleNewTracker tracker)
    {
        return liveMemtableIndexMap.keySet().stream()
                                   .filter(m -> tracker.equals(m.getFlushTransaction()))
                                   .findFirst()
                                   .map(liveMemtableIndexMap::get)
                                   .orElse(null);
    }

    @Override
    public void discardMemtable(Memtable memtable)
    {
        liveMemtableIndexMap.remove(memtable);
    }

    @Override
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

    @Override
    public NavigableSet<ByteBuffer> search(int storeId, TableId tableId, byte[] start, boolean startInclusive, byte[] end, boolean endInclusive)
    {
        TreeSet<ByteBuffer> matches = new TreeSet<>();
        liveMemtableIndexMap.values().forEach(m -> matches.addAll(m.search(storeId, tableId, start, startInclusive, end, endInclusive)));
        return matches;
    }
}
