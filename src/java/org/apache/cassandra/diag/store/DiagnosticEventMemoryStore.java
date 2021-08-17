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

package org.apache.cassandra.diag.store;

import java.util.Comparator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.diag.DiagnosticEvent;

/**
 * Simple on-heap memory store that allows to buffer and retrieve a fixed number of events.
 */
public final class DiagnosticEventMemoryStore implements DiagnosticEventStore<Long>
{
    private final AtomicLong lastKey = new AtomicLong(0);

    private int maxSize = 200;

    // event access will mostly happen based on a recent event offset, so we add new events to the head of the list
    // for optimized search times
    private final ConcurrentSkipListMap<Long, DiagnosticEvent> events = new ConcurrentSkipListMap<>(Comparator.reverseOrder());

    public void load()
    {
        // no-op
    }

    public void store(DiagnosticEvent event)
    {
        long keyHead = lastKey.incrementAndGet();
        events.put(keyHead, event);

        // remove elements starting exceeding max size
        if (keyHead > maxSize) events.tailMap(keyHead - maxSize).clear();
    }

    public NavigableMap<Long, DiagnosticEvent> scan(Long id, int limit)
    {
        assert id != null && id >= 0;
        assert limit >= 0;

        // [10..1].headMap(2, false): [10..3]
        ConcurrentNavigableMap<Long, DiagnosticEvent> newerEvents = events.headMap(id, true);
        // [3..10]
        ConcurrentNavigableMap<Long, DiagnosticEvent> ret = newerEvents.descendingMap();
        if (limit == 0)
        {
            return ret;
        }
        else
        {
            Map.Entry<Long, DiagnosticEvent> first = ret.firstEntry();
            if (first == null) return ret;
            else return ret.headMap(first.getKey() + limit);
        }
    }

    public Long getLastEventId()
    {
        return lastKey.get();
    }

    @VisibleForTesting
    int size()
    {
        return events.size();
    }

    @VisibleForTesting
    void setMaxSize(int maxSize)
    {
        this.maxSize = maxSize;
    }

}
