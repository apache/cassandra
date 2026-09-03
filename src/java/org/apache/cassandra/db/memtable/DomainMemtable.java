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

import org.apache.cassandra.db.LogDomain;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.lifecycle.ILifecycleTransaction;
import org.apache.cassandra.schema.TableMetadataRef;

/**
 * A memtable holding one {@link LogDomain}'s writes. These are the units a flush turns into sstables, so state that a
 * consumer keys per flushed unit belongs against one of these.
 */
public interface DomainMemtable extends Memtable
{

    /**
     * Get the collection of data between the given partition boundaries in a form suitable for flushing.
     */
    FlushablePartitionSet<?> getFlushSet(PartitionPosition from, PartitionPosition to);
    /** The commit log position at the time that this memtable was created */
    CommitLogPosition getCommitLogLowerBound();

    /** The commit log position at the time that this memtable was switched out */
    LastCommitLogPosition getFinalCommitLogUpperBound();

    /**
     * True if the memtable can contain any data that was written before the given position in this memtable's log domain.
     * Callers must only pass a position from this memtable's backing log domain (e.g. obtained via {@link Memtable#flushSourceFor}).
     */
    boolean mayContainDataBefore(CommitLogPosition position);
    // The following two methods provide a way of tracking ongoing flushes
    ILifecycleTransaction setFlushTransaction(ILifecycleTransaction transaction);
    ILifecycleTransaction getFlushTransaction();

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
         * @param commitLogLowerBound This memtable's lower bound in the log named by {@code domain}. It is the previous
         *                            generation's upper bound in that log, and defines the span of positions any
         *                            flushed sstable will cover.
         * @param metadaRef Pointer to the up-to-date table metadata.
         * @param owner Owning objects that will receive flush requests triggered by the memtable (e.g. on expiration).
         * @param domain Which backing log this memtable's bounds reference.
         */
        DomainMemtable create(AtomicReference<CommitLogPosition> commitLogLowerBound, TableMetadataRef metadaRef, Owner owner, LogDomain domain);

        /**
         * Create a release action for the memtable's metrics. This is used to release any resources that are not needed.
         * @param metadataRef Pointer to the up-to-date table metadata.
         * @return Runnable that releases the metrics resources.
         */
        default Runnable createMemtableMetricsReleaser(TableMetadataRef metadataRef)
        {
            return () -> {};
        }

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
    }

}
