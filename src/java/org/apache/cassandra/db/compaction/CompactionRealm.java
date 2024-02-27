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

package org.apache.cassandra.db.compaction;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;

import com.google.common.base.Function;
import com.google.common.base.Predicate;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.DiskBoundaries;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;

/**
 * An interface for supplying the CFS data relevant to compaction. This is implemented by {@link ColumnFamilyStore} and
 * works together with the {@link CompactionSSTable} interface as an abstraction of the space where compaction
 * strategies operate.
 *
 * ColumnFamilyStore uses its SSTableReaders (which are already open to serve reads) as the CompactionSSTable instances,
 * but alternate implementations can choose to maintain lighter representations (e.g. metadata-only local versions of
 * remote sstables) which need only be switched to readers when the compaction is selected for execution and locks the
 * sstable copies using {@link #tryModify}.
 */
public interface CompactionRealm
{
    /**
     * @return the schema metadata of this table.
     */
    default TableMetadata metadata()
    {
        return metadataRef().get();
    }

    /**
     * @return the schema metadata of this table as a reference, used for long-living objects to keep up-to-date with
     *         changes.
     */
    TableMetadataRef metadataRef();

    default String getTableName()
    {
        return metadata().name;
    }

    default String getKeyspaceName()
    {
        return metadata().keyspace;
    }

    AbstractReplicationStrategy getKeyspaceReplicationStrategy();

    /**
     * @return the partitioner used by this table.
     */
    default IPartitioner getPartitioner()
    {
        return metadata().partitioner;
    }

    /**
     * @return the {@link Directories} backing this table.
     */
    Directories getDirectories();

    /**
     * @return the {@DiskBoundaries} that are currently applied to the directories backing table.
     */
    DiskBoundaries getDiskBoundaries();

    /**
     * @return metrics object for the realm. This can be null during the initial construction of a compaction strategy,
     * but should be set when the strategy is asked to select or run compactions.
     */
    TableMetrics metrics();

    /**
     * @return the secondary index manager, which is responsible for all secondary indexes.
     */
    SecondaryIndexManager getIndexManager();

    /**
     * @return true if tombstones should be purged only from repaired sstables.
     */
    boolean onlyPurgeRepairedTombstones();

    /**
     * @param sstables
     * @return sstables whose key range overlaps with that of the given sstables, not including itself.
     * (The given sstables may or may not overlap with each other.)
     */
    Set<? extends CompactionSSTable> getOverlappingLiveSSTables(Iterable<? extends CompactionSSTable> sstables);

    /**
     * @return true if compaction is operating and false if it has been stopped.
     */
    boolean isCompactionActive();

    /**
     * @return the compaction parameters associated with this table.
     */
    CompactionParams getCompactionParams();

    /**
     * @return true if the table is operating in a mode where no tombstones are allowed to be deleted.
     */
    boolean getNeverPurgeTombstones();

    /**
     * @return the minimum compaction threshold for size-tiered compaction (also when used as helper in leveled and
     * time-window compaction strategies).
     */
    int getMinimumCompactionThreshold();
    /**
     * @return the maximum compaction threshold for size-tiered compaction (also when used as helper in leveled and
     * time-window compaction strategies).
     */
    int getMaximumCompactionThreshold();

    /**
     * @return the write amplification (bytes flushed + bytes compacted / bytes flushed).
     */
    default double getWA()
    {
        TableMetrics metric = metrics();
        if (metric == null)
            return 0;

        double bytesCompacted = metric.compactionBytesWritten.getCount();
        double bytesFlushed = metric.bytesFlushed.getCount();
        return bytesFlushed <= 0 ? 0 : (bytesFlushed + bytesCompacted) / bytesFlushed;
    }

    /**
     * @return the level fanout factor for leveled compaction.
     */
    int getLevelFanoutSize();
    /**
     * @return true if the table and its compaction strategy support opening of incomplete compaction results early.
     */
    boolean supportsEarlyOpen();
    /**
     * @return the expected total size of the result of compacting the given sstables, taking into account ranges in
     * the sstables that would be thrown away because they are no longer processed by this node.
     */
    long getExpectedCompactedFileSize(Iterable<SSTableReader> sstables, OperationType operationType);
    /**
     * @return true if compaction should check if the result of an operation fits in the disk space and reduce its scope
     * when it does not.
     */
    boolean isCompactionDiskSpaceCheckEnabled();

    /**
     * @return all live memtables, or empty if no memtables are available.
     */
    Iterable<Memtable> getAllMemtables();

    /**
     * @return the set of all live sstables.
     */
    Set<? extends CompactionSSTable> getLiveSSTables();
    /**
     * @return the set of sstables which are currently compacting.
     */
    Set<? extends CompactionSSTable> getCompactingSSTables();

    /**
     * Return the subset of the given sstable set which is not currently compacting.
     */
    <S extends CompactionSSTable> Iterable<S> getNoncompactingSSTables(Iterable<S> sstables);

    /**
     * Return the given subset of sstables, i.e. LIVE, NONCOMPACTING or CANONICAL.
     */
    Iterable<? extends CompactionSSTable> getSSTables(SSTableSet set);

    /**
     * Invalidate the given key from local caches.
     */
    void invalidateCachedPartition(DecoratedKey key);

    /**
     * Construct a descriptor for a new sstable in the given location.
     */
    Descriptor newSSTableDescriptor(File locationForDisk);

    /**
     * Initiate a transaction to modify the given sstables and operation type, most often a compaction.
     * The transaction will convert the given CompactionSSTable handles into open SSTableReaders.
     */
    LifecycleTransaction tryModify(Iterable<? extends CompactionSSTable> sstables,
                                   OperationType operationType,
                                   UUID id);

    /**
     * Initiate a transaction to modify the given sstables and operation type, most often a compaction.
     * The transaction will convert the given CompactionSSTable handles into open SSTableReaders.
     */
    default LifecycleTransaction tryModify(Iterable<? extends CompactionSSTable> sstables,
                                           OperationType operationType)
    {
        return tryModify(sstables, operationType, LifecycleTransaction.newId());
    }

    /**
     * Create an overlap tracker for the given set of source sstables. The tracker is used to identify all sstables
     * that overlap with the given sources, which is used to decide if tombstones or other data can be purged.
     */
    OverlapTracker getOverlapTracker(Iterable<SSTableReader> sources);

    interface OverlapTracker extends AutoCloseable
    {
        /**
         * @return all sstables that overlap with the given source set.
         */
        Collection<? extends CompactionSSTable> overlaps();

        /**
         * @return the sstables whose span covers the given key.
         */
        Collection<? extends CompactionSSTable> overlaps(DecoratedKey key);

        /**
         * Get all the sstables whose span covers the given key, open (i.e. convert to SSTableReader) the ones selected
         * by the given filter, and collect the non-null results of applying the given transformation to the resulting
         * SSTableReaders.
         * Used to select shadow sources (i.e. sources of tombstones or data) for garbage-collecting compactions.
         */
        <V> Iterable<V> openSelectedOverlappingSSTables(DecoratedKey key,
                                                        Predicate<CompactionSSTable> filter,
                                                        Function<SSTableReader, V> transformation);

        /**
         * Refresh the overlapping sstables to reflect compactions applied to any of them.
         * Done to avoid holding on to references of obsolete sstables, which will prevent them from being deleted.
         */
        boolean maybeRefresh();
        
        void refreshOverlaps();
    }

    /**
     * Create a CFS snapshot with the given name.
     */
    void snapshotWithoutMemtable(String snapshotId);

    /**
     * Change the repaired status of a set of sstables, usually to reflect a completed repair operation.
     */
    int mutateRepairedWithLock(Collection<SSTableReader> originals, long repairedAt, UUID pendingRepair, boolean isTransient) throws IOException;

    /**
     * Signal that a repair session has completed.
     */
    void repairSessionCompleted(UUID sessionID);

    /**
     * Run an operation with concurrent compactions being stopped.
     */
    <V> V runWithCompactionsDisabled(Callable<V> callable, boolean interruptValidation, boolean interruptViews, TableOperation.StopTrigger trigger);
}
