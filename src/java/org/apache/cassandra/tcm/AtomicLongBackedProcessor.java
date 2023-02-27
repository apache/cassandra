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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.tcm.log.Entry;
import org.apache.cassandra.tcm.log.LocalLog;
import org.apache.cassandra.tcm.log.LogState;
import org.apache.cassandra.tcm.log.LogStorage;
import org.apache.cassandra.tcm.log.Replication;

/**
 * This Processor should only be used for tests and for offline tools where we need to create tables without
 * persisting any transformations.
 */
public class AtomicLongBackedProcessor extends AbstractLocalProcessor
{
    private static final Logger logger = LoggerFactory.getLogger(AtomicLongBackedProcessor.class);

    private final AtomicLong epochHolder;

    public AtomicLongBackedProcessor(LocalLog log)
    {
        super(log);
        Epoch epoch = log.metadata().epoch;
        assert epoch.is(Epoch.EMPTY) : epoch + " != " + Epoch.EMPTY;
        this.epochHolder = new AtomicLong(epoch.getEpoch());
    }

    @Override
    protected boolean tryCommitOne(Entry.Id entryId, Transformation transform,
                                   Epoch previousEpoch, Epoch nextEpoch,
                                   long previousPeriod, long nextPeriod, boolean sealPeriod)
    {
        if (epochHolder.get() == 0)
        {
            assert previousEpoch.is(Epoch.FIRST) : previousEpoch + " != " + Epoch.FIRST;
            if (!epochHolder.compareAndSet(Epoch.EMPTY.getEpoch(), Epoch.FIRST.getEpoch()))
                return false;
        }
        return epochHolder.compareAndSet(previousEpoch.getEpoch(), nextEpoch.getEpoch());
    }

    @Override
    public ClusterMetadata replayAndWait()
    {
        return log.waitForHighestConsecutive();
    }

    public static class InMemoryStorage implements LogStorage
    {
        private final List<Entry> entries;
        public final MetadataSnapshots metadataSnapshots;

        public InMemoryStorage()
        {
            this.entries = new ArrayList<>();
            this.metadataSnapshots = new InMemoryMetadataSnapshots();
        }

        @Override
        public synchronized void append(long period, Entry entry)
        {
            boolean needsSorting = entries.isEmpty() ? false : entry.epoch.isDirectlyAfter(entries.get(entries.size() - 1).epoch);
            entries.add(entry);
            if (needsSorting)
                Collections.sort(entries);
        }

        @Override
        public synchronized LogState getLogState(Epoch since)
        {
            ClusterMetadata latestSnapshot = metadataSnapshots.getLatestSnapshotAfter(since);
            LogState logState;
            if (latestSnapshot == null)
                logState = new LogState(null, getReplication(since));
            else
                logState = new LogState(latestSnapshot, getReplication(latestSnapshot.epoch));

            return logState;
        }

        @Override
        public synchronized Replication getReplication(Epoch since)
        {
            assert entries != null : "Preserve history is off; can't query log state";
            int idx = indexedBinarySearch(entries, since, e -> e.epoch);
            if (idx < 0)
                idx = 0;
            return Replication.of(entries.subList(idx, entries.size()));
        }

        @Override
        public synchronized Replication getReplication(long startPeriod, Epoch since)
        {
            return getReplication(since);
        }

        private static <T1, T2 extends Comparable<T2>> int indexedBinarySearch(List<T1> list, T2 key, Function<T1, T2> fn) {
            int low = 0;
            int high = list.size()-1;

            while (low <= high) {
                int mid = (low + high) >>> 1;
                T1 midVal = list.get(mid);
                int cmp = fn.apply(midVal).compareTo(key);

                if (cmp < 0)
                    low = mid + 1;
                else if (cmp > 0)
                    high = mid - 1;
                else
                    return mid; // key found
            }
            return -(low + 1);  // key not found
        }
    }

    public static class InMemoryMetadataSnapshots implements MetadataSnapshots
    {
        private SortedMap<Epoch, ClusterMetadata> snapshots;

        public InMemoryMetadataSnapshots()
        {
            this.snapshots = new ConcurrentSkipListMap<>();
        }

        @Override
        public ClusterMetadata getLatestSnapshotAfter(Epoch epoch)
        {
            if (snapshots.isEmpty())
                return null;
            Epoch latest = snapshots.lastKey();
            if (latest.isAfter(epoch))
                return snapshots.get(latest);
            return null;
        }

        @Override
        public ClusterMetadata getSnapshot(Epoch epoch)
        {
            return snapshots.get(epoch);
        }

        @Override
        public void storeSnapshot(ClusterMetadata metadata)
        {
            this.snapshots.put(metadata.epoch, metadata);
        }
    }
}
