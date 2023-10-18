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

import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.UnfilteredSource;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.OpOrder;

/**
 * Memtable interface. This defines the operations the ColumnFamilyStore can perform with memtables.
 * They are of several types:
 * - construction factory interface
 * - write and read operations: put, rowIterator and partitionIterator
 * - statistics and features, including partition counts, data size, encoding stats, written columns
 * - memory usage tracking, including methods of retrieval and of adding extra allocated space (used non-CFS secondary
 *   indexes)
 * - flush functionality, preparing the set of partitions to flush for given ranges
 * - lifecycle management, i.e. operations that prepare and execute switch to a different memtable, together
 *   with ways of tracking the affected commit log spans
 *
 * See Memtable_API.md for details on implementing and using alternative memtable implementations.
 */
public interface Memtable extends Comparable<Memtable>, UnfilteredSource
{
    public static final long NO_MIN_TIMESTAMP = -1;

    // Construction

    /**
     * Factory interface for constructing memtables, and querying write durability features.
     *
     * The factory is chosen using the MemtableParams class (passed as argument to
     * {@code CREATE TABLE ... WITH memtable = '<configuration_name>'} where the configuration definition is a map given
     * under {@code memtable_configurations} in cassandra.yaml). To make that possible, implementations must provide
     * either a static {@code FACTORY} field (if they accept no further option) or a static
     * {@code factory(Map<String, String>)} method. In the latter case, the method should avoid creating
     * multiple instances of the factory for the same parameters, or factories should at least implement hashCode and
     * equals.
     */
    interface Factory
    {
        /**
         * Create a memtable.
         *
         * @param commitLogLowerBound A commit log lower bound for the new memtable. This will be equal to the previous
         *                            memtable's upper bound and defines the span of positions that any flushed sstable
         *                            will cover.
         * @param metadaRef Pointer to the up-to-date table metadata.
         * @param owner Owning objects that will receive flush requests triggered by the memtable (e.g. on expiration).
         */
        Memtable create(AtomicReference<CommitLogPosition> commitLogLowerBound, TableMetadataRef metadaRef, Owner owner);

        /**
         * If the memtable can achieve write durability directly (i.e. using some feature other than the commitlog, e.g.
         * persistent memory), it can return true here, in which case the commit log will not store mutations in this
         * table.
         * Note that doing so will prevent point-in-time restores and changed data capture, thus a durable memtable must
         * allow the option of turning commit log writing on even if it does not need it.
         */
        default boolean writesShouldSkipCommitLog()
        {
            return false;
        }

        /**
         * This should be true if the memtable can achieve write durability for crash recovery directly (i.e. using some
         * feature other than the commitlog, e.g. persistent memory).
         * Setting this flag to true means that the commitlog should not replay mutations for this table on restart,
         * and that it should not try to preserve segments that contain relevant data.
         * Unless writesShouldSkipCommitLog() is also true, writes will be recorded in the commit log as they may be
         * needed for changed data capture or point-in-time restore.
         */
        default boolean writesAreDurable()
        {
            return false;
        }

        /**
         * Normally we can receive streamed sstables directly, skipping the memtable stage (zero-copy-streaming). When
         * the memtable is the primary data store (e.g. persistent memtables), it will usually prefer to receive the
         * data instead.
         *
         * If this returns true, all streamed sstables's content will be read and replayed as mutations, disabling
         * zero-copy streaming.
         */
        default boolean streamToMemtable()
        {
            return false;
        }

        /**
         * When we need to stream data, we usually flush and stream the resulting sstables. This will not work correctly
         * if the memtable does not want to flush for streaming (e.g. persistent memtables acting as primary data
         * store), because data (not just recent) will be missing from the streamed view. Such memtables must present
         * their data separately for streaming.
         * In other words if the memtable returns false on shouldSwitch(STREAMING/REPAIR), its factory must return true
         * here.
         *
         * If this flag returns true, streaming will write the relevant content that resides in the memtable to
         * temporary sstables, stream these sstables and then delete them.
         */
        default boolean streamFromMemtable()
        {
            return false;
        }

        /**
         * Override this method to include implementation-specific memtable metrics in the table metrics.
         *
         * Memtable metrics lifecycle matches table lifecycle. It is the table that owns the metrics and
         * decides when to release them.
         */
        default TableMetrics.ReleasableMetric createMemtableMetrics(TableMetadataRef metadataRef)
        {
            return null;
        }
    }

    /**
     * Interface for providing signals back and requesting information from the owner, i.e. the object that controls the
     * memtable. This is usually the ColumnFamilyStore; the interface is used to limit the dependency of memtables on
     * the details of its implementation.
     */
    interface Owner
    {
        /** Signal to the owner that a flush is required (e.g. in response to hitting space limits) */
        Future<CommitLogPosition> signalFlushRequired(Memtable memtable, ColumnFamilyStore.FlushReason reason);

        /** Get the current memtable for this owner. Used to avoid capturing memtable in scheduled flush tasks. */
        Memtable getCurrentMemtable();

        /**
         * Collect the index memtables flushed together with this. Used to accurately calculate memory that would be
         * freed by a flush.
         */
        Iterable<Memtable> getIndexMemtables();

        /**
         * Construct a list of boundaries that split the locally-owned ranges into the given number of shards,
         * splitting the owned space evenly. It is up to the memtable to use this information.
         * Any changes in the ring structure (e.g. added or removed nodes) will invalidate the splits; in such a case
         * the memtable will be sent a {@link #shouldSwitch}(OWNED_RANGES_CHANGE) and, should that return false, a
         * {@link #localRangesUpdated()} call.
         */
        ShardBoundaries localRangeSplits(int shardCount);
    }

    // Main write and read operations

    /**
     * Put new data in the memtable. This operation may block until enough memory is available in the memory pool.
     *
     * @param update the partition update, may be a new partition or an update to an existing one
     * @param indexer receives information about the update's effect
     * @param opGroup write operation group, used to permit the operation to complete if it is needed to complete a
     *                flush to free space.
     *
     * @return the smallest timestamp delta between corresponding rows from existing and update. A
     * timestamp delta being computed as the difference between the cells and DeletionTimes from any existing partition
     * and those in {@code update}. See CASSANDRA-7979.
     */
    long put(PartitionUpdate update, UpdateTransaction indexer, OpOrder.Group opGroup);

    // Read operations are provided by the UnfilteredSource interface.

    // Statistics

    /** Number of partitions stored in the memtable */
    long partitionCount();

    /** Size of the data not accounting for any metadata / mapping overheads */
    long getLiveDataSize();

    /**
     * Number of "operations" (in the sense defined in {@link PartitionUpdate#operationCount()}) the memtable has
     * executed.
     */
    long operationCount();

    /**
     * The table's definition metadata.
     *
     * Note that this tracks the current state of the table and is not necessarily the same as what was used to create
     * the memtable.
     */
    TableMetadata metadata();


    // Memory usage tracking

    /**
     * Add this memtable's used memory to the given usage object. This can be used to retrieve a single memtable's usage
     * as well as to combine the ones of related sstables (e.g. a table and its table-based secondary indexes).
     */
    void addMemoryUsageTo(MemoryUsage usage);


    /**
     * Creates a holder for memory usage collection.
     *
     * This is used to track on- and off-heap memory, as well as the ratio to the total permitted memtable memory.
     */
    static MemoryUsage newMemoryUsage()
    {
        return new MemoryUsage();
    }

    /**
     * Shorthand for the getting a given table's memory usage.
     * Implemented as a static to prevent implementations altering expectations by e.g. returning a cached object.
     */
    static MemoryUsage getMemoryUsage(Memtable memtable)
    {
        MemoryUsage usage = newMemoryUsage();
        memtable.addMemoryUsageTo(usage);
        return usage;
    }

    @NotThreadSafe
    class MemoryUsage
    {
        /** On-heap memory used in bytes */
        public long ownsOnHeap = 0;
        /** Off-heap memory used in bytes */
        public long ownsOffHeap = 0;
        /** On-heap memory as ratio to permitted memtable space */
        public float ownershipRatioOnHeap = 0.0f;
        /** Off-heap memory as ratio to permitted memtable space */
        public float ownershipRatioOffHeap = 0.0f;

        @Override
        public String toString()
        {
            return String.format("%s (%.0f%%) on-heap, %s (%.0f%%) off-heap",
                                 FBUtilities.prettyPrintMemory(ownsOnHeap),
                                 ownershipRatioOnHeap * 100,
                                 FBUtilities.prettyPrintMemory(ownsOffHeap),
                                 ownershipRatioOffHeap * 100);
        }
    }

    /**
     * Adjust the used on-heap space by the given size (e.g. to reflect memory used by a non-table-based index).
     * This operation may block until enough memory is available in the memory pool.
     *
     * @param additionalSpace the number of allocated bytes
     * @param opGroup write operation group, used to permit the operation to complete if it is needed to complete a
     *                flush to free space.
     */
    void markExtraOnHeapUsed(long additionalSpace, OpOrder.Group opGroup);

    /**
     * Adjust the used off-heap space by the given size (e.g. to reflect memory used by a non-table-based index).
     * This operation may block until enough memory is available in the memory pool.
     *
     * @param additionalSpace the number of allocated bytes
     * @param opGroup write operation group, used to permit the operation to complete if it is needed to complete a
     *                flush to free space.
     */
    void markExtraOffHeapUsed(long additionalSpace, OpOrder.Group opGroup);


    // Flushing

    /**
     * Get the collection of data between the given partition boundaries in a form suitable for flushing.
     */
    FlushablePartitionSet<?> getFlushSet(PartitionPosition from, PartitionPosition to);

    /**
     * A collection of partitions for flushing plus some information required for writing an sstable.
     *
     * Note that the listed entries must conform with the specified metadata. In particular, if the memtable is still
     * being written to, care must be taken to not list newer items as they may violate the bounds collected by the
     * encoding stats or refer to columns that don't exist in the collected columns set.
     */
    interface FlushablePartitionSet<P extends Partition> extends Iterable<P>, SSTableWriter.SSTableSizeParameters
    {
        Memtable memtable();

        PartitionPosition from();
        PartitionPosition to();

        /** The commit log position at the time that this memtable was created */
        CommitLogPosition commitLogLowerBound();
        /** The commit log position at the time that this memtable was switched out */
        CommitLogPosition commitLogUpperBound();

        /** The set of all columns that have been written */
        RegularAndStaticColumns columns();
        /** Statistics required for writing an sstable efficiently */
        EncodingStats encodingStats();

        default TableMetadata metadata()
        {
            return memtable().metadata();
        }

        default boolean isEmpty()
        {
            return partitionCount() > 0;
        }
    }


    // Lifecycle management

    /**
     * Called to tell the memtable that it is being switched out and will be flushed (or dropped) and discarded.
     * Will be followed by a {@link #getFlushSet} call (if the table is not truncated or dropped), and a
     * {@link #discard}.
     *
     * @param writeBarrier The barrier that will signal that all writes to this memtable have completed. That is, the
     *                     point after which writes cannot be accepted by this memtable (it is permitted for writes
     *                     before this barrier to go into the next; see {@link #accepts}).
     * @param commitLogUpperBound The upper commit log position for this memtable. The value may be modified after this
     *                            call and will match the next memtable's lower commit log bound.
     */
    void switchOut(OpOrder.Barrier writeBarrier, AtomicReference<CommitLogPosition> commitLogUpperBound);

    /**
     * This memtable is no longer in use or required for outstanding flushes or operations.
     * All held memory must be released.
     */
    void discard();

    /**
     * Decide if this memtable should take a write with the given parameters, or if the write should go to the next
     * memtable. This enforces that no writes after the barrier set by {@link #switchOut} can be accepted, and
     * is also used to define a shared commit log bound as the upper for this memtable and lower for the next.
     */
    boolean accepts(OpOrder.Group opGroup, CommitLogPosition commitLogPosition);

    /** Approximate commit log lower bound, <= getCommitLogLowerBound, used as a time stamp for ordering */
    CommitLogPosition getApproximateCommitLogLowerBound();

    /** The commit log position at the time that this memtable was created */
    CommitLogPosition getCommitLogLowerBound();

    /** The commit log position at the time that this memtable was switched out */
    LastCommitLogPosition getFinalCommitLogUpperBound();

    /** True if the memtable can contain any data that was written before the given commit log position */
    boolean mayContainDataBefore(CommitLogPosition position);

    /** True if the memtable contains no data */
    boolean isClean();

    // The following two methods provide a way of tracking ongoing flushes
    LifecycleTransaction setFlushTransaction(LifecycleTransaction transaction);
    LifecycleTransaction getFlushTransaction();

    /** Order memtables by time as reflected in the commit log position at time of construction */
    default int compareTo(Memtable that)
    {
        return this.getApproximateCommitLogLowerBound().compareTo(that.getApproximateCommitLogLowerBound());
    }

    /**
     * Decides whether the memtable should be switched/flushed for the passed reason.
     * Normally this will return true, but e.g. persistent memtables may choose not to flush. Returning false will
     * trigger further action for some reasons:
     * - SCHEMA_CHANGE will be followed by metadataUpdated().
     * - OWNED_RANGES_CHANGE will be followed by localRangesUpdated().
     * - SNAPSHOT will be followed by performSnapshot().
     * - STREAMING/REPAIR will be followed by creating a FlushSet for the streamed/repaired ranges. This data will be
     *   used to create sstables, which will be streamed and then deleted.
     * This will not be called to perform truncation or drop (in that case the memtable is unconditionally dropped),
     * but a flush may nevertheless be requested in that case to prepare a snapshot.
     */
    boolean shouldSwitch(ColumnFamilyStore.FlushReason reason);

    /**
     * Called when the table's metadata is updated. The memtable's metadata reference now points to the new version.
     * This will not be called if {@link #shouldSwitch } (SCHEMA_CHANGE) returns true, as the memtable will be swapped out
     * instead.
     */
    void metadataUpdated();

    /**
     * Called when the known ranges have been updated and owner.localRangeSplits() may return different values.
     * This will not be called if {@link #shouldSwitch } (OWNED_RANGES_CHANGE) returns true, as the memtable will be
     * swapped out instead.
     */
    void localRangesUpdated();

    /**
     * If the memtable needs to do some special action for snapshots (e.g. because it is persistent and does not want
     * to flush), it should return false on the above with reason SNAPSHOT and implement this method.
     */
    void performSnapshot(String snapshotName);

    /**
     * Special commit log position marker used in the upper bound marker setting process
     * (see {@link org.apache.cassandra.db.ColumnFamilyStore#setCommitLogUpperBound} and {@link AbstractMemtable#accepts})
     */
    public static final class LastCommitLogPosition extends CommitLogPosition
    {
        public LastCommitLogPosition(CommitLogPosition copy)
        {
            super(copy.segmentId, copy.position);
        }
    }
}
