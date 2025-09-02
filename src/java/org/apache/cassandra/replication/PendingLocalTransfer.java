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
import java.util.Collection;
import java.util.Objects;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.streaming.CassandraStreamReceiver;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ownership.ReplicaGroups;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

/**
 * A transfer on a replica, once present on disk.
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

    public PendingLocalTransfer(TableId tableId, TimeUUID planId, Collection<SSTableReader> sstables)
    {
        Preconditions.checkState(!sstables.isEmpty());
        this.tableId = tableId;
        this.planId = planId;
        this.sstables = sstables;
        this.keyspace = Objects.requireNonNull(ColumnFamilyStore.getIfExists(tableId)).keyspace.getName();
        this.range = shardRange(keyspace, sstables);
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

    /**
     * Safely move a transfer into the live set. This must be crash-safe, and the primary invariant we need to
     * preserve is a transfer is only added to the live set iff the transfer ID is present in its mutation summaries.
     *
     * We don't validate checksums here, mostly because a transfer can be activated during a read, if one replica
     * missed the TransferActivation. Transfers should not be pending for very long, and should be protected by
     * internode integrity checks provided by TLS.
     *
     * TODO: Clear out the row cache and counter cache, like {@link CassandraStreamReceiver#finished}.
     * TODO: Don't add to the live set if coordinator and not an owner for the range
     */
    public void activate(TransferActivation activation)
    {
        logger.info("{} Activating transfer {}, {} ms since pending", logPrefix(), this, currentTimeMillis() - createdAt);
        ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(tableId);
        Preconditions.checkNotNull(cfs);
        Preconditions.checkState(!sstables.isEmpty());

        // Ensure no lingering mutation IDs, only activation IDs
        for (SSTableReader sstable : sstables)
        {
            Preconditions.checkState(sstable.getCoordinatorLogOffsets().isEmpty());

            // Modify SSTables metadata to durably set transfer ID before importing
            ImmutableCoordinatorLogOffsets logOffsets = new ImmutableCoordinatorLogOffsets.Builder()
                                                  .addTransfer(activation.activationId)
                                                  .build();
            try
            {
                sstable.mutateCoordinatorLogOffsetsAndReload(logOffsets);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }
        if (activation.dryRun)
        {
            logger.info("{} Not adding SSTables to live set for dryRun {}", logPrefix(), activation);
            return;
        }
        cfs.getTracker().addSSTablesTracked(sstables);
    }

    @Override
    public String toString()
    {
        return "PendingLocalTransfer{" +
               "planId=" + planId +
               ", tableId=" + tableId +
               ", sstables=" + sstables +
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
