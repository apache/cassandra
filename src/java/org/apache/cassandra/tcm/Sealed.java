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
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.SystemKeyspace;

public class Sealed implements Comparable<Sealed>
{
    private static final Logger logger = LoggerFactory.getLogger(Sealed.class);

    public static final Sealed EMPTY = new Sealed(Period.EMPTY, Epoch.EMPTY);
    public final long period;
    public final Epoch epoch;

    private static final AtomicReference<RecentlySealedPeriods> index = new AtomicReference<>(RecentlySealedPeriods.EMPTY);

    public static Sealed lookupForReplication(Epoch since)
    {
        Sealed sealed = index.get().lookupPeriodForReplication(since);
        if (sealed.equals(EMPTY))
        {
            logger.info("No sealed period found in index for epoch {}, querying system table", since);
            sealed = SystemKeyspace.findSealedPeriodForEpochScan(since);
        }
        return sealed;
    }

    public static Sealed lookupForSnapshot(Epoch since)
    {
        Sealed sealed = index.get().lookupEpochForSnapshot(since);
        if (sealed.equals(EMPTY))
        {
            logger.info("No sealed period for snapshot found in index for epoch {}, querying system table", since);
            sealed = SystemKeyspace.getLastSealedPeriod();
        }
        return sealed;
    }

    public static void recordSealedPeriod(long period, Epoch epoch)
    {
        SystemKeyspace.sealPeriod(period, epoch);
        index.updateAndGet(i -> i.with(epoch, period));
    }

    public static void initIndexFromSystemTables()
    {
        int maxSize = CassandraRelevantProperties.TCM_RECENTLY_SEALED_PERIOD_INDEX_SIZE.getInt();
        List<Sealed> recentlySealed = new ArrayList<>(maxSize);
        Sealed last = SystemKeyspace.getLastSealedPeriod();
        recentlySealed.add(last);
        long previous = last.period - 1;
        recentlySealed.addAll(Period.scanLogForRecentlySealed(SystemKeyspace.LocalMetadataLog, previous, maxSize - 1));
        index.set(RecentlySealedPeriods.init(recentlySealed));
    }

    @VisibleForTesting
    public static void unsafeClearLookup()
    {
        index.set(RecentlySealedPeriods.EMPTY);
    }

    public Sealed(long period, long epoch)
    {
        this(period, Epoch.create(epoch));
    }

    public Sealed(long period, Epoch epoch)
    {
        this.period = period;
        this.epoch = epoch;
    }

    @Override
    public String toString()
    {
        return "Sealed{" +
               "period=" + period +
               ", epoch=" + epoch +
               '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (!(o instanceof Sealed)) return false;
        Sealed sealed = (Sealed) o;
        return period == sealed.period && epoch.equals(sealed.epoch);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(period, epoch);
    }

    @Override
    public int compareTo(Sealed o)
    {
        return Long.compare(this.epoch.getEpoch(), o.epoch.getEpoch());
    }
}
