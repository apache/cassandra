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
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.tcm.log.Entry;
import org.apache.cassandra.tcm.log.LocalLog;
import org.apache.cassandra.tcm.log.LogState;
import org.apache.cassandra.tcm.log.LogStorage;

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
        this(log, false);
    }

    public AtomicLongBackedProcessor(LocalLog log, boolean isReset)
    {
        super(log);
        Epoch epoch = log.metadata().epoch;
        assert epoch.is(Epoch.EMPTY) || isReset : epoch + " != " + Epoch.EMPTY;
        this.epochHolder = new AtomicLong(epoch.getEpoch());
    }

    @Override
    protected boolean tryCommitOne(Entry.Id entryId, Transformation transform, Epoch previousEpoch, Epoch nextEpoch)
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
    public ClusterMetadata fetchLogAndWait(Epoch waitFor, Retry.Deadline retry)
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
        public synchronized void append(Entry entry)
        {
            boolean needsSorting = entries.isEmpty() ? false : entry.epoch.isDirectlyAfter(entries.get(entries.size() - 1).epoch);
            entries.add(entry);
            if (needsSorting)
                Collections.sort(entries);
        }

        @Override
        public synchronized LogState getLogState(Epoch startEpoch)
        {
            ImmutableList.Builder<Entry> builder = ImmutableList.builder();
            ClusterMetadata latest = metadataSnapshots.getLatestSnapshot();
            Epoch actualSince = latest != null && latest.epoch.isAfter(startEpoch) ? latest.epoch : startEpoch;
            entries.stream().filter(e -> e.epoch.isAfter(actualSince)).forEach(builder::add);
            return new LogState(latest, builder.build());
        }

        @Override
        public synchronized LogState getPersistedLogState()
        {
            return getLogState(Epoch.EMPTY);
        }

        public synchronized LogState getLogStateBetween(ClusterMetadata base, Epoch end)
        {
            ImmutableList.Builder<Entry> builder = ImmutableList.builder();
            entries.stream().filter(e -> e.epoch.isAfter(base.epoch) && e.epoch.isEqualOrBefore(end)).forEach(builder::add);
            return new LogState(base, builder.build());
        }

        @Override
        public synchronized MetadataSnapshots snapshots()
        {
            return metadataSnapshots;
        }

        @Override
        public synchronized EntryHolder getEntries(Epoch since)
        {
            throw new IllegalStateException("We have overridden all callers of this method, it should never be called");
        }
    }

    public static class InMemoryMetadataSnapshots implements MetadataSnapshots
    {
        private final NavigableMap<Epoch, ClusterMetadata> snapshots;

        public InMemoryMetadataSnapshots()
        {
            this.snapshots = new ConcurrentSkipListMap<>();
        }

        @Override
        public ClusterMetadata getSnapshot(Epoch epoch)
        {
            return snapshots.get(epoch);
        }

        @Override
        public ClusterMetadata getSnapshotBefore(Epoch epoch)
        {
            Map.Entry<Epoch, ClusterMetadata> entry = snapshots.floorEntry(epoch);
            return entry == null ? null : entry.getValue();
        }

        @Override
        public ClusterMetadata getLatestSnapshot()
        {
            if (snapshots.isEmpty())
                return null;
            return snapshots.lastEntry().getValue();
        }

        @Override
        public List<Epoch> listSnapshotsSince(Epoch epoch)
        {
            List<Epoch> epochs = new ArrayList<>();
            snapshots.tailMap(epoch).forEach((e, s) -> epochs.add(e));
            return epochs;
        }

        @Override
        public void storeSnapshot(ClusterMetadata metadata)
        {
            this.snapshots.put(metadata.epoch, metadata);
        }
    }
}
