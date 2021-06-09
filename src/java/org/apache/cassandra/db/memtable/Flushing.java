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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.DiskBoundaries;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.commitlog.IntervalSet;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.partitions.AtomicBTreePartition;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.utils.Throwables.maybeFail;

public class Flushing
{
    private static final Logger logger = LoggerFactory.getLogger(Flushing.class);

    public static List<FlushRunnable> flushRunnables(ColumnFamilyStore cfs,
                                                     Memtable memtable,
                                                     LifecycleTransaction txn)
    {
        LifecycleTransaction ongoingFlushTransaction = memtable.setFlushTransaction(txn);
        Preconditions.checkState(ongoingFlushTransaction == null,
                                 "Attempted to flush Memtable more than once on %s.%s",
                                 cfs.keyspace.getName(),
                                 cfs.name);

        DiskBoundaries diskBoundaries = cfs.getDiskBoundaries();
        List<PartitionPosition> boundaries = diskBoundaries.positions;
        List<Directories.DataDirectory> locations = diskBoundaries.directories;
        return flushRunnables(cfs, memtable, boundaries, locations, txn);
    }

    @VisibleForTesting
    static List<FlushRunnable> flushRunnables(ColumnFamilyStore cfs,
                                              Memtable memtable,
                                              List<PartitionPosition> boundaries,
                                              List<Directories.DataDirectory> locations,
                                              LifecycleTransaction txn)
    {
        if (boundaries == null)
        {
            FlushRunnable runnable = flushRunnable(cfs, memtable, null, null, txn, null);
            return Collections.singletonList(runnable);
        }

        List<FlushRunnable> runnables = new ArrayList<>(boundaries.size());
        PartitionPosition rangeStart = boundaries.get(0).getPartitioner().getMinimumToken().minKeyBound();
        try
        {
            for (int i = 0; i < boundaries.size(); i++)
            {
                PartitionPosition t = boundaries.get(i);
                FlushRunnable runnable = flushRunnable(cfs, memtable, rangeStart, t, txn, locations.get(i));

                runnables.add(runnable);
                rangeStart = t;
            }
            return runnables;
        }
        catch (Throwable e)
        {
            throw Throwables.propagate(abortRunnables(runnables, e));
        }
    }

    @SuppressWarnings("resource")   // writer owned by runnable, to be closed or aborted by its caller
    static FlushRunnable flushRunnable(ColumnFamilyStore cfs,
                                       Memtable memtable,
                                       PartitionPosition from,
                                       PartitionPosition to,
                                       LifecycleTransaction txn,
                                       Directories.DataDirectory flushLocation)
    {
        Memtable.FlushCollection<?> flushSet = memtable.getFlushSet(from, to);
        SSTableFormat.Type formatType = SSTableFormat.Type.current();
        long estimatedSize = formatType.info.getWriterFactory().estimateSize(flushSet);

        Descriptor descriptor = flushLocation == null
                                ? cfs.newSSTableDescriptor(cfs.getDirectories().getWriteableLocationAsFile(estimatedSize), formatType)
                                : cfs.newSSTableDescriptor(cfs.getDirectories().getLocationForDisk(flushLocation), formatType);

        SSTableMultiWriter writer = createFlushWriter(cfs,
                                                      flushSet,
                                                      txn,
                                                      descriptor,
                                                      flushSet.partitionCount());

        return new FlushRunnable(flushSet, writer, cfs.metric, true);
    }

    public static Throwable abortRunnables(List<FlushRunnable> runnables, Throwable t)
    {
        if (runnables != null)
            for (FlushRunnable runnable : runnables)
                t = runnable.abort(t);
        return t;
    }

    /**
     * The valid states for {@link FlushRunnable} writers. The thread writing the contents
     * will transition from IDLE -> RUNNING and back to IDLE when finished using the writer
     * or from ABORTING -> ABORTED if another thread has transitioned from RUNNING -> ABORTING.
     * We can also transition directly from IDLE -> ABORTED. Whichever threads transitions
     * to ABORTED is responsible to abort the writer.
     */
    @VisibleForTesting
    enum FlushRunnableWriterState
    {
        IDLE, // the runnable is idle, either not yet started or completed but with the writer waiting to be committed
        RUNNING, // the runnable is executing, therefore the writer cannot be aborted or else a SEGV may ensue
        ABORTING, // an abort request has been issued, this only happens if abort() is called whilst RUNNING
        ABORTED  // the writer has been aborted, no resources will be leaked
    }

    public static class FlushRunnable implements Callable<SSTableMultiWriter>
    {
        private final Memtable.FlushCollection<?> toFlush;

        private final SSTableMultiWriter writer;
        private final TableMetrics metrics;
        private final boolean isBatchLogTable;
        private final boolean logCompletion;
        private final AtomicReference<FlushRunnableWriterState> state;

        public FlushRunnable(Memtable.FlushCollection<?> flushSet,
                             SSTableMultiWriter writer,
                             TableMetrics metrics,
                             boolean logCompletion)
        {
            this.toFlush = flushSet;
            this.writer = writer;
            this.metrics = metrics;
            this.isBatchLogTable = toFlush.metadata() == SystemKeyspace.Batches;
            this.logCompletion = logCompletion;
            this.state = new AtomicReference<>(FlushRunnableWriterState.IDLE);
        }

        private void writeSortedContents()
        {
            if (!state.compareAndSet(FlushRunnableWriterState.IDLE, FlushRunnableWriterState.RUNNING))
            {
                logger.debug("Failed to write {}, flushed range = ({}, {}], state: {}",
                             toFlush.memtable().toString(), toFlush.from(), toFlush.to(), state);
                return;
            }

            logger.debug("Writing {}, flushed range = ({}, {}], state: {}",
                         toFlush.memtable().toString(), toFlush.from(), toFlush.to(), state);

            try
            {
                // (we can't clear out the map as-we-go to free up memory,
                //  since the memtable is being used for queries in the "pending flush" category)
                for (Partition partition : toFlush)
                {
                    if (state.get() == FlushRunnableWriterState.ABORTING)
                        break;

                    // Each batchlog partition is a separate entry in the log. And for an entry, we only do 2
                    // operations: 1) we insert the entry and 2) we delete it. Further, BL data is strictly local,
                    // we don't need to preserve tombstones for repair. So if both operation are in this
                    // memtable (which will almost always be the case if there is no ongoing failure), we can
                    // just skip the entry (CASSANDRA-4667).
                    if (isBatchLogTable && !partition.partitionLevelDeletion().isLive() && partition.hasRows())
                        continue;

                    if (!partition.isEmpty())
                    {
                        try (UnfilteredRowIterator iter = partition.unfilteredIterator())
                        {
                            writer.append(iter);
                        }
                    }
                }
            }
            finally
            {
                while (true)
                {
                    if (state.compareAndSet(FlushRunnableWriterState.RUNNING, FlushRunnableWriterState.IDLE))
                    {
                        if (logCompletion)
                        {
                            long bytesFlushed = writer.getFilePointer();
                            logger.info("Completed flushing {} ({}) for commitlog position {}",
                                        writer.getFilename(),
                                        FBUtilities.prettyPrintMemory(bytesFlushed),
                                        toFlush.memtable().getCommitLogUpperBound());
                            // Update the metrics
                            metrics.bytesFlushed.inc(bytesFlushed);
                        }

                        break;
                    }
                    else if (state.compareAndSet(FlushRunnableWriterState.ABORTING, FlushRunnableWriterState.ABORTED))
                    {
                        logger.debug("Flushing of {} aborted", writer.getFilename());
                        maybeFail(writer.abort(null));
                        break;
                    }
                }
            }
        }

        @Override
        public SSTableMultiWriter call()
        {
            writeSortedContents();
            return writer;
            // We don't close the writer on error as the caller aborts all runnables if one happens.
        }

        public Throwable abort(Throwable throwable)
        {
            while (true)
            {
                if (state.compareAndSet(FlushRunnableWriterState.IDLE, FlushRunnableWriterState.ABORTED))
                {
                    logger.debug("Flushing of {} aborted", writer.getFilename());
                    return writer.abort(throwable);
                }
                else if (state.compareAndSet(FlushRunnableWriterState.RUNNING, FlushRunnableWriterState.ABORTING))
                {
                    // thread currently executing writeSortedContents() will take care of aborting and throw any exceptions
                    return throwable;
                }
            }
        }

        @VisibleForTesting
        FlushRunnableWriterState state()
        {
            return state.get();
        }
    }

    public static SSTableMultiWriter createFlushWriter(ColumnFamilyStore cfs,
                                                       Memtable.FlushCollection<?> flushSet,
                                                       LifecycleTransaction txn,
                                                       Descriptor descriptor,
                                                       long partitionCount)
    {
        MetadataCollector sstableMetadataCollector = new MetadataCollector(flushSet.metadata().comparator)
                                                     .commitLogIntervals(new IntervalSet<>(flushSet.commitLogLowerBound(),
                                                                                           flushSet.commitLogUpperBound()));

        return cfs.createSSTableMultiWriter(descriptor,
                                            partitionCount,
                                            ActiveRepairService.UNREPAIRED_SSTABLE,
                                            ActiveRepairService.NO_PENDING_REPAIR,
                                            false,
                                            sstableMetadataCollector,
                                            new SerializationHeader(true,
                                                                    flushSet.metadata(),
                                                                    flushSet.columns(),
                                                                    flushSet.encodingStats()),
                                            txn);
    }
}
