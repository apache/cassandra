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

/**
 * A period that has been sealed.
 * <p>
 * A period is an implementation detail of having the logs stored in C* tables. It is the partition key of
 * the log tables (system.local_metadata_log and cluster_metadata.distributed_metadata_log) and allows to keep partitions
 * to a manageable size. Periods are "sealed" when the period number is bumped and a new partition is used to store transformations in the log table.
 * </p>
 * <p>
 * When a new created epoch is a multiple of the {@code metadata_snapshot_frequency} the {@code LocalLog} snapshot listener
 * will attempt to commit a {@code SealPeriod} transformation. The sealing of the period in the local log will trigger the {@code MetadataSnapshotListener}
 * that will create a snaphot of the cluster metadata.
 * The size of a sealed period is therefore controlled by the {@code metadata_snapshot_frequency} yaml property that control when
 * {@code SealPeriod} transformations are created.
 *
 * </p>
 * @see org.apache.cassandra.tcm.log.LocalLog
 * @see ClusterMetadataService#sealPeriod()
 * @see org.apache.cassandra.tcm.listeners.MetadataSnapshotListener
 */
public class Sealed implements Comparable<Sealed>
{
    private static final Logger logger = LoggerFactory.getLogger(Sealed.class);

    public static final Sealed EMPTY = new Sealed(Period.EMPTY, Epoch.EMPTY);

    /**
     * The period number
     */
    public final long period;

    /**
     * The latest epoch of the period
     */
    public final Epoch epoch;

    private static final AtomicReference<RecentlySealedPeriods> index = new AtomicReference<>(RecentlySealedPeriods.EMPTY);

    public static Sealed lookupForReplication(Epoch since)
    {
        Sealed sealed = index.get().lookupPeriodForReplication(since);
        if (sealed.equals(EMPTY))
        {
            logger.trace("No sealed period found in index for epoch {}, querying system table", since);
            sealed = SystemKeyspace.findSealedPeriodForEpochScan(since);
        }
        return sealed;
    }

    public static Sealed lookupForSnapshot(Epoch since)
    {
        Sealed sealed = index.get().lookupEpochForSnapshot(since);
        if (sealed.equals(EMPTY))
        {
            logger.trace("No sealed period for snapshot found in index for epoch {}, querying system table", since);
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
        int maxSize = CassandraRelevantProperties.CMS_RECENTLY_SEALED_PERIOD_INDEX_SIZE.getInt();
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
        return this.epoch.compareTo(o.epoch);
    }
}
