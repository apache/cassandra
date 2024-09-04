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

package org.apache.cassandra.tcm.log;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;

import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.MetadataSnapshots;

public interface LogReader
{
    /**
     * Gets all entries where epoch >= since - could be empty if since is a later epoch than the current highest seen
     */
    EntryHolder getEntries(Epoch since) throws IOException;
    MetadataSnapshots snapshots();

    /**
     * Assuming we have something like:
     *
     * epoch | transformation
     *    10 | TRIGGER_SNAPSHOT
     *    11 | SOMETHING
     *    12 | SOMETHING
     *    13 | TRIGGER_SNAPSHOT
     *    14 | SOMETHING
     *    15 | SOMETHING
     *    16 | TRIGGER_SNAPSHOT
     *    17 | SOMETHING
     *
     * and `startEpoch` is 14, we want to return epoch 15, 16, 17 - not the snapshot at 16 + entry at 17 since it is
     * assumed that the full snapshot is much larger than the transformations.
     * But, if `startEpoch` is 11, we want to return the most recent snapshot (at epoch 16) + entry at 17.
     *
     * If a snapshot is missing we keep reading backwards until we find one, or we end up at `startEpoch` and in that
     * case we return all subsequent entries in the log.
     */
    default LogState getLogState(Epoch startEpoch)
    {
        return getLogState(startEpoch, true);
    }

    default LogState getLogState(Epoch startEpoch, boolean allowSnapshots)
    {
        try
        {
            EntryHolder entries = null;
            // List of snapshots with an epoch > startEpoch
            List<Epoch> snapshotEpochs = snapshots().listSnapshotsSince(startEpoch);

            // If there is at most 1 snapshot with an epoch > startEpoch, we prefer to skip that snapshot and just build a
            // list of consecutive entries
            if (snapshotEpochs.size() <= 1 || !allowSnapshots)
            {
                entries = getEntries(startEpoch);
                if (entries.isContinuous())
                    return new LogState(null, entries.immutable());
                else if (!allowSnapshots)
                    throw new IllegalStateException("Can't construct a continuous log since " + startEpoch + " and inclusion of snapshots is disallowed");
                // Gaps in a persisted log are never expected, but we have not been able to construct a continuous
                // sequence of all entries between startEpoch and the current epoch, so fall back to the general case.
            }

            assert Ordering.<Epoch>from(Comparator.reverseOrder()).isOrdered(snapshotEpochs) : "Epochs from snapshots().listSnapshotsSince(...) should be ordered by most recent epoch first";
            // From the list of snapshots which come after startEpoch, read the latest one available and create a
            // LogState with that as the base plus any subsequent entries. If we have already read a list of entries,
            // this must necessarily be a superset of entries startEpoch the available snapshot.
            // This may include a non-continuous list of entries, which the caller will buffer in its LocalLog and
            // attempt to fill any gaps by requesting additional LogStates from other peers.
            for (Epoch snapshotAt : snapshotEpochs)
            {
                ClusterMetadata snapshot = snapshots().getSnapshot(snapshotAt);
                if (null != snapshot)
                {
                    ImmutableList<Entry> sublist = entries != null
                                                   ? entries.immutable(snapshotAt)
                                                   : getEntries(snapshotAt).immutable();
                    return new LogState(snapshot, sublist);
                }
            }

            // We have been unable to find any suitable snapshot, so the best thing we can do is to include all the
            // entries we have after startEpoch, even if that's a non-continuous list. If we have already read a list of
            // entries subsequent to startEpoch, we can reuse that.
            if (entries == null)
                entries = getEntries(startEpoch);
            return new LogState(null, entries.immutable());

        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    class EntryHolder
    {
        SortedSet<Entry> entries;
        Epoch since;

        public EntryHolder(Epoch since)
        {
            this.entries = new TreeSet<>();
            this.since = since;
        }

        public void add(Entry entry)
        {
            if (entry.epoch.isAfter(since))
                entries.add(entry);
        }

        private boolean isContinuous()
        {
            Epoch prev = since;
            for (Entry e : entries)
            {
                if (!e.epoch.isDirectlyAfter(prev))
                    return false;
                prev = e.epoch;
            }
            return true;
        }

        private ImmutableList<Entry> immutable()
        {
            return ImmutableList.copyOf(entries);
        }

        private ImmutableList<Entry> immutable(Epoch startExclusive)
        {
            ImmutableList.Builder<Entry> list = ImmutableList.builder();
            for (Entry e : entries)
                if (e.epoch.isAfter(startExclusive))
                    list.add(e);
            return list.build();
        }
    }

}
