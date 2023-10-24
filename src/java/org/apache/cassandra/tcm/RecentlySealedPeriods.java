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
import java.util.Map;
import java.util.NavigableMap;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSortedMap;

import org.apache.cassandra.config.CassandraRelevantProperties;

/**
 * In-memory lookup of the most recent sealed periods, indexed by their max epoch.
 * Rebuilt at startup from system.metadata_sealed_periods and updated whenever a
 * SealPeriod is enacted. Used in LogReader::getReplication to identify the period
 * to start from when constructing a list of log entries since a given epoch. If
 * the target is outside the range of this index, we eventually fall back to a read
 * from the system.metadata_sealed_periods table.
 */
public final class RecentlySealedPeriods
{
    public static final RecentlySealedPeriods EMPTY = new RecentlySealedPeriods(ImmutableSortedMap.of());

    /**
     * The maximum number of sealed periods stored in memory.
     */
    private final int maxSize = CassandraRelevantProperties.TCM_RECENTLY_SEALED_PERIOD_INDEX_SIZE.getInt();
    private final NavigableMap<Epoch, Sealed> recent;

    private RecentlySealedPeriods(ImmutableSortedMap<Epoch, Sealed> recent)
    {
        this.recent = recent;
    }

    public static RecentlySealedPeriods init(List<Sealed> recent)
    {
        ImmutableSortedMap.Builder<Epoch, Sealed> builder = ImmutableSortedMap.naturalOrder();
        for (Sealed sealed: recent)
        {
            builder.put(sealed.epoch, sealed);
        }
        return new RecentlySealedPeriods(builder.build());
    }

    public RecentlySealedPeriods with(Epoch epoch, long period)
    {
        NavigableMap<Epoch, Sealed> toKeep = recent.size() < maxSize ? recent
                                                                     : recent.tailMap(recent.firstKey(), false);

        return new RecentlySealedPeriods(ImmutableSortedMap.<Epoch, Sealed>naturalOrder()
                                                           .putAll(toKeep)
                                                           .put(epoch, new Sealed(period, epoch))
                                                           .build());
    }

    /**
     * Find the highest sealed period *after* the target epoch. When catching
     * up or replaying the log, we want the most recent snapshot available,
     * as long as its epoch is greater than the target.
     * If the target epoch is greater than the max epoch in the latest sealed
     * period, then assume there is no suitable snapshot.
     * @param epoch the target epoch
     * @return
     */
    public Sealed lookupEpochForSnapshot(Epoch epoch)
    {
        if (recent.isEmpty())
            return Sealed.EMPTY;

        // if the target is > the highest indexed value there's no need to
        // scan the index. Instead, just signal to the caller that no suitable
        // sealed period was found.
        Map.Entry<Epoch, Sealed> latest = recent.lastEntry();
        return latest.getKey().isAfter(epoch) ? latest.getValue() : Sealed.EMPTY;
    }

    // TODO add a lookupEpochForSnapshotBetween(start, end) so we can walk
    //      through the index if the latest snapshot happens to be missing

    /**
     * Find the sealed period stricly greater than the target epoch.
     * <p>This method is used to identify the period to start from if building a list of all log entries with an
     * epoch greater than the one supplied. If the target epoch happens to be
     * the max in a sealed period, we would start with the period following
     * that. If the target epoch is equal to or after the max in the latest
     * sealed period, assume that the period following that has yet to be sealed.
     * @param epoch the target epoch
     * @return
     */
    public Sealed lookupPeriodForReplication(Epoch epoch)
    {
        // if the target is not within the index we need to signal to the
        // caller that a slower/more expensive/more comprehensive lookup
        // is required
        if (recent.isEmpty() || epoch.isBefore(recent.firstKey()) || epoch.isEqualOrAfter(recent.lastKey()))
            return Sealed.EMPTY;

        return recent.higherEntry(epoch).getValue();
    }

    @VisibleForTesting
    public Sealed[] toArray()
    {
        return recent.values().toArray(new Sealed[recent.size()]);
    }
}
