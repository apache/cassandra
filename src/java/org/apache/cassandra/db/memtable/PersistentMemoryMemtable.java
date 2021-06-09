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

package org.apache.cassandra.db.memtable;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.concurrent.OpOrder;

/**
 * Skeleton for persistent memory memtable.
 */
public class PersistentMemoryMemtable
//extends AbstractMemtable
extends SkipListMemtable        // to test framework
{
    public PersistentMemoryMemtable(TableMetadataRef metadaRef, Owner owner)
    {
        super(null, metadaRef, owner);
        // We should possibly link the persistent data of this memtable
    }

    public long put(PartitionUpdate update, UpdateTransaction indexer, OpOrder.Group opGroup)
    {
        // TODO: implement
        return super.put(update, indexer, opGroup);
    }

    public MemtableUnfilteredPartitionIterator makePartitionIterator(ColumnFilter columnFilter, DataRange dataRange)
    {
        // TODO: implement
        return super.makePartitionIterator(columnFilter, dataRange);
    }

    public Partition getPartition(DecoratedKey key)
    {
        // TODO: implement
        return super.getPartition(key);
    }

    public long partitionCount()
    {
        // TODO: implement
        return super.partitionCount();
    }

    public FlushCollection<?> getFlushSet(PartitionPosition from, PartitionPosition to)
    {
        // TODO: implement
        // FIXME: If the memtable can still be written to, this uses a view of the metadata that may not be up-to-date
        // with the content. This may cause streaming to fail e.g. if a new column appears and is added to some row in
        // the memtable between the time that this is constructed and the relevant row is written. Such failures should
        // be recoverable by redoing the stream.
        // If an implementation can produce a view/snapshot of the data at a point before the features were collected,
        // this problem will not occur.
        return super.getFlushSet(from, to);
    }

    public boolean shouldSwitch(ColumnFamilyStore.FlushReason reason)
    {
        // We want to avoid all flushing.
        switch (reason)
        {
        case STARTUP: // Called after reading and replaying the commit log.
        case SHUTDOWN: // Called to flush data before shutdown.
        case INTERNALLY_FORCED: // Called to ensure ordering and persistence of system table events.
        case MEMTABLE_PERIOD_EXPIRED: // The specified memtable expiration time elapsed.
        case INDEX_TABLE_FLUSH: // Flush requested on index table because main table is flushing.
        case STREAMS_RECEIVED: // Flush to save streamed data that was written to memtable.
            return false;   // do not do anything

        case INDEX_BUILD_COMPLETED:
        case INDEX_REMOVED:
            // Both of these are needed as safepoints for index management. Nothing to do.
            return false;

        case VIEW_BUILD_STARTED:
        case INDEX_BUILD_STARTED:
            // TODO: Figure out secondary indexes and views.
            return false;

        case SCHEMA_CHANGE:
            if (!(metadata().params.memtable.factory instanceof Factory))
                return true;    // User has switched to a different memtable class. Flush and release all held data.
            // Otherwise, assuming we can handle the change, don't switch.
            // TODO: Handle
            return false;

        case STREAMING: // Called to flush data so it can be streamed. TODO: How dow we stream?
        case REPAIR: // Called to flush data for repair. TODO: How do we repair?
            // ColumnFamilyStore will create sstables of the affected ranges which will not be consulted on reads and
            // will be deleted after streaming.
            return false;

        case SNAPSHOT:
            // We don't flush for this. Returning false will trigger a performSnapshot call.
            return false;

        case DROP: // Called when a table is dropped. This memtable is no longer necessary.
        case TRUNCATE: // The data is being deleted, but the table remains.
            // Returning true asks the ColumnFamilyStore to replace this memtable object without flushing.
            // This will call discard() below to delete all held data.
            return true;

        case MEMTABLE_LIMIT: // The memtable size limit is reached, and this table was selected for flushing.
                             // Also passed if we call owner.signalLimitReached()
        case COMMITLOG_DIRTY: // Commitlog thinks it needs to keep data from this table.
            // Neither of the above should happen as we specify writesAreDurable and don't use an allocator/cleaner.
            throw new AssertionError();

        case USER_FORCED:
        case UNIT_TESTS:
            return false;
        default:
            throw new AssertionError();
        }
    }

    public void metadataUpdated()
    {
        // TODO: handle
    }

    public void performSnapshot(String snapshotName)
    {
        // TODO: implement. Figure out how to restore snapshot (with external tools).
    }

    public void switchOut(OpOrder.Barrier writeBarrier, AtomicReference<CommitLogPosition> commitLogUpperBound)
    {
        super.switchOut(writeBarrier, commitLogUpperBound);
        // This can prepare the memtable data for deletion; it will still be used while the flush is proceeding.
        // A discard call will follow.
    }

    public void discard()
    {
        // This will be called to release/delete all held data because the memtable is switched, due to having
        // its data flushed, due to a truncate/drop, or due to a schema change to a different memtable class.

        // TODO: Implement. This should delete all memtable data from pmem.
        super.discard();
    }

    public CommitLogPosition getApproximateCommitLogLowerBound()
    {
        // We don't maintain commit log positions
        return CommitLogPosition.NONE;
    }

    public CommitLogPosition getCommitLogLowerBound()
    {
        // We don't maintain commit log positions
        return CommitLogPosition.NONE;
    }

    public CommitLogPosition getCommitLogUpperBound()
    {
        // We don't maintain commit log positions
        return CommitLogPosition.NONE;
    }

    public boolean isClean()
    {
        return partitionCount() == 0;
    }

    public boolean mayContainDataBefore(CommitLogPosition position)
    {
        // We don't track commit log positions, so if we are dirty, we may.
        return !isClean();
    }

    public void addMemoryUsageTo(MemoryUsage stats)
    {
        // our memory usage is not counted
    }

    public void markExtraOnHeapUsed(long additionalSpace, OpOrder.Group opGroup)
    {
        // we don't track this
    }

    public void markExtraOffHeapUsed(long additionalSpace, OpOrder.Group opGroup)
    {
        // we don't track this
    }

    public static Factory factory(Map<String, String> furtherOptions)
    {
        Boolean skipOption = Boolean.parseBoolean(furtherOptions.remove("skipCommitLog"));
        return skipOption ? commitLogSkippingFactory : commitLogWritingFactory;
    }

    private static final Factory commitLogSkippingFactory = new Factory(true);
    private static final Factory commitLogWritingFactory = new Factory(false);

    static class Factory implements Memtable.Factory
    {
        private final boolean skipCommitLog;

        public Factory(boolean skipCommitLog)
        {
            this.skipCommitLog = skipCommitLog;
        }

        public Memtable create(AtomicReference<CommitLogPosition> commitLogLowerBound,
                               TableMetadataRef metadaRef,
                               Owner owner)
        {
            return new PersistentMemoryMemtable(metadaRef, owner);
        }

        public boolean writesShouldSkipCommitLog()
        {
            return skipCommitLog;
        }

        public boolean writesAreDurable()
        {
            return true;
        }

        public boolean streamToMemtable()
        {
            return true;
        }

        public boolean streamFromMemtable()
        {
            return true;
        }
    }

}
