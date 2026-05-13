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

package org.apache.cassandra.tcm;

import java.util.List;

import org.junit.Test;

import org.apache.cassandra.tcm.log.Entry;
import org.apache.cassandra.tcm.log.LogState;

import static org.junit.Assert.assertEquals;

public class AtomicLongBackedProcessorTest
{
    private static Entry entry(long epoch)
    {
        return new Entry(new Entry.Id(epoch), Epoch.create(epoch), null);
    }

    @Test
    public void outOfOrderAppendsAreSorted()
    {
        AtomicLongBackedProcessor.InMemoryStorage storage = new AtomicLongBackedProcessor.InMemoryStorage();
        storage.append(entry(1));
        storage.append(entry(3));
        storage.append(entry(2));

        List<Entry> entries = storage.getLogState(Epoch.EMPTY).entries;
        assertEquals(3, entries.size());
        assertEquals(Epoch.create(1), entries.get(0).epoch);
        assertEquals(Epoch.create(2), entries.get(1).epoch);
        assertEquals(Epoch.create(3), entries.get(2).epoch);
    }

    @Test
    public void inOrderAppendsRemainSorted()
    {
        AtomicLongBackedProcessor.InMemoryStorage storage = new AtomicLongBackedProcessor.InMemoryStorage();
        storage.append(entry(1));
        storage.append(entry(2));
        storage.append(entry(3));

        List<Entry> entries = storage.getLogState(Epoch.EMPTY).entries;
        assertEquals(3, entries.size());
        assertEquals(Epoch.create(1), entries.get(0).epoch);
        assertEquals(Epoch.create(2), entries.get(1).epoch);
        assertEquals(Epoch.create(3), entries.get(2).epoch);
    }

    @Test
    public void getLogStateReturnsSortedView()
    {
        AtomicLongBackedProcessor.InMemoryStorage storage = new AtomicLongBackedProcessor.InMemoryStorage();
        storage.append(entry(5));
        storage.append(entry(1));
        storage.append(entry(4));
        storage.append(entry(2));
        storage.append(entry(3));

        LogState state = storage.getLogState(Epoch.EMPTY);
        long previous = Long.MIN_VALUE;
        for (Entry e : state.entries)
        {
            long current = e.epoch.getEpoch();
            assertEquals("entries must be monotonically increasing, got " + state.entries,
                         true, current > previous);
            previous = current;
        }
        assertEquals(5, state.entries.size());
    }
}
