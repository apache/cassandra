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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.CassandraRelevantProperties;

/**
 * In-memory lookup of the most recent sealed periods, indexed by their max epoch.
 * Rebuilt at startup from system.metadata_sealed_periods and updated whenever a
 * SealPeriod is enacted. Used in LogReader::getReplication to identify the period
 * to start from when constructing a list of log entries since a given epoch. If
 * the target is outside the range of this index, we eventually fall back to a read
 * from the system.metadata_sealed_periods table.
 */
public class RecentlySealedPeriods
{
    private static final Sealed[] EMPTY_ARRAY = new Sealed[0];
    public static final RecentlySealedPeriods EMPTY = new RecentlySealedPeriods(EMPTY_ARRAY);
    private int maxSize = CassandraRelevantProperties.TCM_RECENTLY_SEALED_PERIOD_INDEX_SIZE.getInt();
    private Sealed[] recent;

    private RecentlySealedPeriods(Sealed first)
    {
        this.recent = new Sealed[]{first};
    }

    private RecentlySealedPeriods(Sealed[] recent)
    {
        this.recent = recent;
    }

    public static RecentlySealedPeriods init(List<Sealed> recent)
    {
        Collections.sort(recent);
        return new RecentlySealedPeriods(recent.toArray(new Sealed[recent.size()]));
    }

    public RecentlySealedPeriods with(Epoch epoch, long period)
    {
        if (recent == null)
        {
            return new RecentlySealedPeriods(new Sealed(period, epoch));
        }
        else
        {
            int toCopy = Math.min(recent.length, maxSize - 1);
            int newSize = Math.min(recent.length + 1, maxSize);
            Sealed[] newList = new Sealed[newSize];
            System.arraycopy(recent, recent.length - toCopy, newList, 0, toCopy);
            newList[newSize - 1] = new Sealed(period, epoch);
            // shouldn't be necessary, but is cheap
            Arrays.sort(newList, Sealed::compareTo);
            return new RecentlySealedPeriods(newList);
        }
    }

    /**
     * Find the highest sealed period *after* the target epoch. When catching
     * up or replaying the log, we want the most recent snapshot available,
     * as long as its epoch is greater than the target.
     * If the target epoch is greater than the max epoch in the latest sealed
     * period, then assume there is no suitable snapshot.
     * @param epoch
     * @return
     */
    public Sealed lookupEpochForSnapshot(Epoch epoch)
    {
        // if the target is > the highest indexed value there's no need to
        // scan the index. Instead, just signal to the caller that no suitable
        // sealed period was found.
        if (recent.length > 0)
        {
            Sealed latest = recent[recent.length - 1];
            return latest.epoch.isAfter(epoch) ? latest : Sealed.EMPTY;
        }
        return Sealed.EMPTY;
    }

    // TODO add a lookupEpochForSnapshotBetween(start, end) so we can walk
    //      through the index if the latest snapshot happens to be missing

    /**
     * Find the *closest* sealed period to a target epoch. Used to identify
     * the period to start at if building a list of all log entries with an
     * epoch greater than the one supplied. If the target epoch happens to be
     * the max in a sealed period, we would start with the period following
     * that. If the target epoch is equal to or after the max in the latest
     * sealed period, assume that the period following that has yet to be sealed.
     * @param epoch
     * @return
     */
    public Sealed lookupPeriodForReplication(Epoch epoch)
    {
        // if the target is > the highest indexed value there's no need to
        // scan the index. Instead, just signal to the caller that no suitable
        // sealed period was found.
        if (recent.length > 0 && epoch.isEqualOrAfter(recent[recent.length - 1].epoch))
            return Sealed.EMPTY;

        for (int i = recent.length - 1; i >= 0; i--)
        {
            Sealed e = recent[i];
            if (e.epoch.isEqualOrBefore(epoch))
                return recent[Math.min(i + 1, recent.length - 1)];
        }

        // the target epoch couldn't be found in the index, signal to the
        // caller that a slower/more expensive/more comprehensive lookup
        // is required
        return Sealed.EMPTY;
    }

    @VisibleForTesting
    public Sealed[] array()
    {
        return recent;
    }
}
