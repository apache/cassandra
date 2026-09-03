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

package org.apache.cassandra.replication;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.replication.migration.KeyspaceMigrationInfo;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.Clock;

/**
 * Promotes unrepaired sstables whose mutations have become durably reconciled to repaired, in place.
 *
 * Reconciled status is otherwise only ever evaluated at write time, in
 * {@link org.apache.cassandra.io.sstable.format.SSTableWriter#finalizeMetadata}. An sstable written before its
 * mutations reconciled therefore stays unrepaired until something happens to rewrite it, and because offsets union
 * across compaction inputs while reconciliation is a conjunction over all of them, compacting more aggressively makes
 * promotion harder rather than easier. A large sstable at the top of a level may never be rewritten at all.
 *
 * The consequence that matters most is not the domain mixing constraint but tombstone purging, which mutation tracking
 * gates on {@code repairedAt}: without this sweep such an sstable's tombstones are never purgeable. Operator
 * unrepaired-bytes metrics draining correctly is the second reason.
 *
 * Promotion sets {@code repairedAt} and clears the sstable's coordinator log offsets in a single metadata mutation and
 * rewrites no data, following the shape {@link org.apache.cassandra.db.compaction.PendingRepairManager} already uses.
 */
public class ReconciledSSTablePromoter
{
    private static final Logger logger = LoggerFactory.getLogger(ReconciledSSTablePromoter.class);

    /**
     * Promote every eligible sstable of every tracked table.
     */
    public static void sweep()
    {
        for (Keyspace keyspace : Keyspace.all())
        {
            for (ColumnFamilyStore cfs : keyspace.getColumnFamilyStores())
            {
                try
                {
                    promoteEligible(cfs);
                }
                catch (Throwable t)
                {
                    // One table's failure must not stop the sweep; it will be retried on the next pass.
                    logger.warn("Failed promoting reconciled sstables for {}.{}", cfs.getKeyspaceName(), cfs.name, t);
                }
            }
        }
    }

    /**
     * Promote every sstable of {@code cfs} that is unrepaired and whose mutations have all reconciled.
     *
     * @return the sstables promoted
     */
    @VisibleForTesting
    public static Set<SSTableReader> promoteEligible(ColumnFamilyStore cfs) throws IOException
    {
        if (!cfs.metadata().replicationType().isTracked())
            return Collections.emptySet();

        List<SSTableReader> eligible = new ArrayList<>();
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            if (isEligible(cfs, sstable))
                eligible.add(sstable);
        }

        if (eligible.isEmpty())
            return Collections.emptySet();

        // repairedAt is the promotion time, not the moment reconciliation completed. The honest value would be the
        // minimum reconciliation moment across the sstable's mutations, but isDurablyReconciled is present-tense and
        // no such timestamp is recorded anywhere. Promotion time understates how long the data has been consistent,
        // which is the conservative direction.
        long repairedAt = Clock.Global.currentTimeMillis();
        Set<SSTableReader> promoted = cfs.getCompactionStrategyManager().promoteReconciled(eligible, repairedAt);
        if (!promoted.isEmpty())
            logger.info("Promoted {} reconciled sstables of {}.{} to repaired at {}",
                        promoted.size(), cfs.getKeyspaceName(), cfs.name, repairedAt);
        return promoted;
    }

    private static boolean isEligible(ColumnFamilyStore cfs, SSTableReader sstable)
    {
        if (sstable.isRepaired() || sstable.isPendingRepair())
            return false;

        // An sstable with no offsets makes no claim this sweep can act on. It is either commit-log-derived or was
        // already promoted, and in neither case does reconciliation have anything to say about it.
        if (sstable.getSSTableMetadata().coordinatorLogOffsets.isEmpty())
            return false;

        // Same guard as write-time promotion: while a range is still pending migration, incremental repair owns its
        // repair status, so promoting underneath it would fight with anticompaction.
        KeyspaceMigrationInfo migrationInfo = ClusterMetadata.current()
                                                            .mutationTrackingMigrationState
                                                            .getKeyspaceInfo(cfs.getKeyspaceName());
        if (migrationInfo != null && migrationInfo.isRangeInPendingMigration(cfs.metadata().id,
                                                                            sstable.getFirst().getToken(),
                                                                            sstable.getLast().getToken()))
            return false;

        return MutationTrackingService.instance().isDurablyReconciled(sstable.getSSTableMetadata().coordinatorLogOffsets);
    }

    /**
     * Promote the pre-migration sstables covering {@code ranges} when those ranges finish migrating.
     *
     * Same mechanism as the sweep on a different trigger. Pre-migration data carries no offsets, so the sweep itself
     * will never pick it up; completion of the migration is what establishes that it is consistent with peers.
     */
    public static void promoteForCompletedMigration(ColumnFamilyStore cfs, Collection<Range<Token>> ranges) throws IOException
    {
        if (!cfs.metadata().replicationType().isTracked() || ranges.isEmpty())
            return;

        List<SSTableReader> eligible = new ArrayList<>();
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            if (sstable.isRepaired() || sstable.isPendingRepair())
                continue;
            if (!sstable.getSSTableMetadata().coordinatorLogOffsets.isEmpty())
                continue; // journal-derived; the sweep promotes these once they reconcile

            Range<Token> span = new Range<>(sstable.getFirst().getToken(), sstable.getLast().getToken());
            for (Range<Token> range : ranges)
            {
                if (range.contains(span) || range.intersects(span))
                {
                    eligible.add(sstable);
                    break;
                }
            }
        }

        if (eligible.isEmpty())
            return;

        long repairedAt = Clock.Global.currentTimeMillis();
        cfs.getCompactionStrategyManager().mutateRepaired(eligible, repairedAt, ActiveRepairService.NO_PENDING_REPAIR);
        logger.info("Promoted {} pre-migration sstables of {}.{} to repaired at {} after migration completed",
                    eligible.size(), cfs.getKeyspaceName(), cfs.name, repairedAt);
    }
}
