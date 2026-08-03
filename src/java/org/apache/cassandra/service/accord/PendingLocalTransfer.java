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

package org.apache.cassandra.service.accord;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.utils.Invariants;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SSTableImporter;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.txn.TxnRead;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.db.SSTableImporter.getTargetDirectory;
import static org.apache.cassandra.db.SSTableImporter.moveSSTablesBack;
import static org.apache.cassandra.db.SSTableImporter.removeCopiedSSTables;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

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

    volatile boolean activated = false;

    public PendingLocalTransfer(TableId tableId, TimeUUID planId, Collection<SSTableReader> sstables)
    {
        Preconditions.checkState(!sstables.isEmpty());
        this.tableId = tableId;
        this.planId = planId;
        this.sstables = sstables;
        this.keyspace = Objects.requireNonNull(ColumnFamilyStore.getIfExists(tableId)).keyspace.getName();
    }

    /**
     * Safely moves SSTables into the live set. This method is idempotent, as it can be called concurrently
     * by multiple CommandStore executors.
     */
    public synchronized void activate(TxnRead.ImportMetadata importMetadata, long executeAtEpoch)
    {
        if (activated)
            return;

        Invariants.require(importMetadata.getStreamingEpoch() == executeAtEpoch);
        long startedActivation = currentTimeMillis();
        logger.info("{} Activating transfer {}, {} ms since pending", logPrefix(), this, startedActivation - createdAt);
        ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(tableId);
        Preconditions.checkNotNull(cfs);
        Preconditions.checkState(!sstables.isEmpty());

        Set<SSTableImporter.MovedSSTable> movedSSTables = new HashSet<>();
        Collection<SSTableReader> moved = new ArrayList<>(sstables.size());

        for (SSTableReader sstable : sstables)
        {
            try
            {
                File targetDir = getTargetDirectory(cfs, sstable.descriptor, sstable.getComponents());
                Descriptor newDescriptor = cfs.getUniqueDescriptorFor(sstable.descriptor, targetDir);
                movedSSTables.add(new SSTableImporter.MovedSSTable(newDescriptor, sstable.descriptor, sstable.getComponents()));
                SSTableReader movedSSTable = SSTableReader.moveAndOpenSSTable(cfs, sstable.descriptor, newDescriptor, sstable.getComponents(), importMetadata.getCopyData());
                moved.add(movedSSTable);
            }
            catch (Throwable t)
            {
                moved.forEach(s -> s.selfRef().release());
                if (importMetadata.getCopyData())
                    removeCopiedSSTables(movedSSTables);
                else
                    moveSSTablesBack(movedSSTables);

                throw new RuntimeException("Failed importing SSTables", t);
            }
        }

        // Add all SSTables atomically
        cfs.getTracker().addSSTables(moved);
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

        LocalTransfers.instance().schedulePendingLocalTransferCleanup(planId);
    }

    @Override
    public String toString()
    {
        return "PendingLocalTransfer{" +
               "activated=" + activated +
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
