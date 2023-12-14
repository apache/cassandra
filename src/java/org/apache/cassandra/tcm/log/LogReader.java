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
import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.MetadataSnapshots;
import org.apache.cassandra.tcm.Period;

public interface LogReader
{
    /**
     * Gets all entries where epoch >= since in the given period - could be empty if since is in a later epoch
     */
    EntryHolder getEntries(long period, Epoch since) throws IOException;
    MetadataSnapshots snapshots();

    /**
     * Idea is to fill LogState "backwards" - we start querying the partition at currentPeriod, and if that doesn't
     * include `since` we read currentPeriod - 1. Assuming we have something like

     * epoch | period | transformation
     *    10 |      2 | SEAL_PERIOD
     *    11 |      3 | SOMETHING
     *    12 |      3 | SOMETHING
     *    13 |      3 | SEAL_PERIOD
     *    14 |      4 | SOMETHING
     *    15 |      4 | SOMETHING
     *    16 |      4 | SEAL_PERIOD
     *    17 |      5 | SOMETHING
     *
     * and `since` is 14, we want to return epoch 15, 16, 17 - not the snapshot at 16 + entry at 17 since it is assumed that the full
     * snapshot is much larger than the transformations.
     * But, if `since` is 11, we want to return the most recent snapshot (at epoch 16) + entry at 17.
     *
     * If a snapshot is missing we keep reading backwards until we find one, or we end up at period 0 and in that
     * case we return all transformations in the log.
     */
    default LogState getLogState(long currentPeriod, Epoch since)
    {
        try
        {
            EntryHolder current = getEntries(currentPeriod, since);
            if (current.done)
                return new LogState(null, ImmutableList.sortedCopyOf(current.entries));
            List<Entry> allEntries = new ArrayList<>(current.entries);
            int i = 0;
            while (true)
            {
                i++;
                EntryHolder previous = getEntries(currentPeriod - i, since);
                allEntries.addAll(previous.entries);
                if (isContinuous(since, allEntries) && (previous.done || currentPeriod - i == Period.FIRST))
                {
                    return new LogState(null, ImmutableList.sortedCopyOf(allEntries));
                }
                else
                {
                    if (i == 1)
                    {
                        // we end up here if `since` is not in currentPeriod or currentPeriod - 1
                        // so we should return a snapshot - we prefer returning the most recent snapshot + entries in `current`
                        // but if that doesn't exist we check previous.
                        if (current.min == null && previous.min == null) // we found no entries >= since in the tables -> since > current
                            return LogState.EMPTY;
                        ClusterMetadata snapshot = snapshots().getSnapshot(Epoch.create(current.min.getEpoch() - 1));
                        if (snapshot != null)
                            return new LogState(snapshot, ImmutableList.sortedCopyOf(current.entries));
                    }
                    ClusterMetadata snapshot = snapshots().getSnapshot(Epoch.create(previous.min.getEpoch() - 1));
                    if (snapshot != null)
                        return new LogState(snapshot, ImmutableList.sortedCopyOf(allEntries));
                }
            }

        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static boolean isContinuous(Epoch since, List<Entry> entries)
    {
        entries.sort(Entry::compareTo);
        Epoch prev = since;
        for (Entry e : entries)
        {
            if (!e.epoch.isDirectlyAfter(prev))
                return false;
            prev = e.epoch;
        }
        return true;
    }

    class EntryHolder
    {
        List<Entry> entries;
        Epoch min = null;
        Epoch since;
        boolean done = false;

        public EntryHolder(Epoch since)
        {
            this.entries = new ArrayList<>();
            this.since = since;
        }

        public void add(Entry entry)
        {
            if (entry.epoch.isAfter(since))
            {
                if (entry.epoch.isDirectlyAfter(since))
                    done = true;
                if (min == null || min.isAfter(entry.epoch))
                    min = entry.epoch;
                entries.add(entry);
            }
            else if (entry.epoch.is(since))
            {
                // if `since` is the most recent epoch committed, this avoids an extra query
                done = true;
            }
        }
    }

}
