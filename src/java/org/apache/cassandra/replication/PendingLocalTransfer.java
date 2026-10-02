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
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ownership.ReplicaGroups;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

/**
 * Represents a bulk data transfer received on a replica, from completion of streaming into the pending location,
 * through activation when it's made visible to reads. Pending transfers are identified by their streaming plan ID,
 * and made live by {@link ActivationRequest} which associates the streaming plan with a transfer ID that can be
 * represented in mutation summaries.
 */
public class PendingLocalTransfer
{
    private static final Logger logger = LoggerFactory.getLogger(PendingLocalTransfer.class);

    private String logPrefix()
    {
        return String.format("[PendingLocalTransfer #%s]", planId);
    }

    final TimeUUID planId;
    final TableId tableId;
    final Collection<SSTableReader> sstables;
    final long createdAt = currentTimeMillis();
    transient String keyspace;
    transient Range<Token> range;

    volatile boolean activated = false;

    public PendingLocalTransfer(TableId tableId, TimeUUID planId, Collection<SSTableReader> sstables)
    {
        Preconditions.checkState(!sstables.isEmpty());
        this.tableId = tableId;
        this.planId = planId;
        this.sstables = sstables;
        this.keyspace = Objects.requireNonNull(ColumnFamilyStore.getIfExists(tableId)).keyspace.getName();
        this.range = shardRange(keyspace, sstables);
    }

    @VisibleForTesting
    PendingLocalTransfer(TimeUUID planId, Collection<SSTableReader> sstables)
    {
        Preconditions.checkState(!sstables.isEmpty());
        this.planId = planId;
        this.tableId = null;
        this.sstables = sstables;
        this.keyspace = null;
        this.range = null;
    }

    /**
     * Pending transfers should be within a single shard, which are aligned to natural ranges.
     * See ({@link MutationTrackingService.KeyspaceShards#make}).
     */
    private static Range<Token> shardRange(String keyspace, Collection<SSTableReader> sstables)
    {
        ClusterMetadata cm = ClusterMetadata.current();
        ReplicaGroups writes = cm.placements.get(Keyspace.open(keyspace).getMetadata().params.replication).writes;
        Range<Token> range = null;
        for (SSTableReader sstable : sstables)
        {
            if (range == null)
            {
                Token first = sstable.getFirst().getToken();
                range = writes.forRange(first).range();
            }
            else
            {
                AbstractBounds<Token> bounds = sstable.getBounds();
                Preconditions.checkState(!range.isTrulyWrapAround());
                Preconditions.checkState(range.contains(bounds.left));
                Preconditions.checkState(range.contains(bounds.right));
            }
        }

        Preconditions.checkNotNull(range);
        return range;
    }

    private boolean isFullReplica()
    {
        ClusterMetadata cm = ClusterMetadata.current();
        Keyspace ks = Keyspace.open(keyspace);
        ReplicaGroups writes = cm.placements.get(ks.getMetadata().params.replication).writes;
        EndpointsForRange replicas = writes.forRange(range.right).get();
        return replicas.containsSelf() && replicas.selfIfPresent().isFull();
    }

    /**
     * Safely move a transfer into the live set. This must be crash-safe, and the primary invariant we need to
     * preserve is a transfer is only added to the live set iff the transfer ID is present in its mutation summaries.
     * <p>
     * We don't validate checksums here, mostly because a transfer can be activated during a read, if one replica
     * missed the {@link ActivationRequest}. Transfers should not be pending for very long, and should be protected by
     * internode integrity checks provided by TLS.
     * <p>
     * Synchronized to prevent a single activation from running multiple times if requested during read reconciliation
     * and in the background via {@link ActiveLogReconciler}.
     */
    public synchronized boolean activate(ActivationRequest request, Bounds<Token> bounds)
    {
        if (activated)
            return false;

        Preconditions.checkState(isFullReplica());

        long startedActivation = currentTimeMillis();
        logger.info("{} Activating transfer {}, {} ms since pending", logPrefix(), this, startedActivation - createdAt);
        ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(tableId);
        Preconditions.checkNotNull(cfs);
        Preconditions.checkState(!sstables.isEmpty());

        if (request.isPrepare())
        {
            logger.info("{} Not adding SSTables to live set for dryRun {}", logPrefix(), request);
            return false;
        }

        // Modify SSTables metadata to durably set transfer ID before importing
        ImmutableCoordinatorLogOffsets logOffsets =
            new ImmutableCoordinatorLogOffsets.Builder().addTransfer(request.transferId, bounds).build();

        // Ensure no lingering mutation IDs, only activation IDs
        for (SSTableReader sstable : sstables)
        {
            try
            {
                sstable.mutateCoordinatorLogOffsetsAndReload(logOffsets);
            }
            catch (IOException e)
            {
                throw new UncheckedIOException(e);
            }

            Preconditions.checkState(sstable.getCoordinatorLogOffsets().mutations().isEmpty());
            ActivatedTransfers transfers = sstable.getCoordinatorLogOffsets().transfers();
            Preconditions.checkState(!transfers.isEmpty());
        }

        File dst = cfs.getDirectories().getDirectoryForNewSSTables();

        // Retain the original SSTables in pending/ dir on the coordinator, so future streams can get the originals, and
        // we don't need to isolate activated SSTables during compaction
        boolean isCoordinator = request.transferId.hostId == ClusterMetadata.current().myNodeId().id();
        logger.debug("{} {} pending SSTables for activation to {}", isCoordinator ? "Copying" : "Moving", logPrefix(), dst);

        dst.createFileIfNotExists();
        Collection<SSTableReader> moved = new ArrayList<>(sstables.size());
        for (SSTableReader sstable : sstables)
            moved.add(SSTableReader.moveAndOpenSSTable(cfs, sstable.descriptor, cfs.getUniqueDescriptorFor(sstable.descriptor, dst), sstable.getComponents(), isCoordinator));

        // Add all SSTables atomically
        cfs.getTracker().addSSTablesFromTrackedTransfer(moved);
        activated = true;

        Consumer<Integer> onRowCacheInvalidation = invalidatedKeys -> {
            logger.debug("{} Invalidated {} row cache entries on table {}.{} after activating transfer",
                    logPrefix(), invalidatedKeys, cfs.getKeyspaceName(), cfs.getTableName());
        };
        Consumer<Integer> onCounterCacheInvalidation = invalidatedKeys -> {
            logger.debug("{} Invalidated {} counter cache entries on table {}.{} after activating transfer",
                    logPrefix(), invalidatedKeys, cfs.getKeyspaceName(), cfs.getTableName());
        };
        cfs.invalidateRowAndCounterCache(moved, onRowCacheInvalidation, onCounterCacheInvalidation);

        long finishedActivation = currentTimeMillis();
        logger.info("{} Finished activating transfer {} in {} ms", logPrefix(), this, finishedActivation - startedActivation);

        TransferTrackingService.instance().scheduleCleanup();
        return true;
    }

    @Override
    public String toString()
    {
        return "PendingLocalTransfer{" +
               "activated=" + activated +
               ", range=" + range +
               ", keyspace='" + keyspace + '\'' +
               ", createdAt=" + createdAt +
               ", sstables=" + sstables +
               ", tableId=" + tableId +
               ", planId=" + planId +
               '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || getClass() != o.getClass()) return false;
        PendingLocalTransfer transfer = (PendingLocalTransfer) o;
        return Objects.equals(planId, transfer.planId) && Objects.equals(tableId, transfer.tableId) && Objects.equals(sstables, transfer.sstables);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(planId, tableId, sstables);
    }
}
