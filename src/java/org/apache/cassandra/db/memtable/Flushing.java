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

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.DiskBoundaries;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.commitlog.IntervalSet;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.FBUtilities;

public class Flushing
{
    private static final Logger logger = LoggerFactory.getLogger(Flushing.class);

    private Flushing() // prevent instantiation
    {
    }

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
            Throwable t = abortRunnables(runnables, e);
            Throwables.throwIfUnchecked(t);
            throw new RuntimeException(t);
        }
    }

    static FlushRunnable flushRunnable(ColumnFamilyStore cfs,
                                       Memtable memtable,
                                       PartitionPosition from,
                                       PartitionPosition to,
                                       LifecycleTransaction txn,
                                       Directories.DataDirectory flushLocation)
    {
        Memtable.FlushablePartitionSet<?> flushSet = memtable.getFlushSet(from, to);
        SSTableFormat<?, ?> format = DatabaseDescriptor.getSelectedSSTableFormat();
        long estimatedSize = format.getWriterFactory().estimateSize(flushSet);

        Descriptor descriptor = flushLocation == null
                                ? cfs.newSSTableDescriptor(cfs.getDirectories().getWriteableLocationAsFile(estimatedSize), format)
                                : cfs.newSSTableDescriptor(cfs.getDirectories().getLocationForDisk(flushLocation), format);

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
                t = runnable.writer.abort(t);
        return t;
    }

    public static class FlushRunnable implements Callable<SSTableMultiWriter>
    {
        private final Memtable.FlushablePartitionSet<?> toFlush;

        private final SSTableMultiWriter writer;
        private final TableMetrics metrics;
        private final boolean isBatchLogTable;
        private final boolean logCompletion;

        public FlushRunnable(Memtable.FlushablePartitionSet<?> flushSet,
                             SSTableMultiWriter writer,
                             TableMetrics metrics,
                             boolean logCompletion)
        {
            this.toFlush = flushSet;
            this.writer = writer;
            this.metrics = metrics;
            this.isBatchLogTable = toFlush.metadata() == SystemKeyspace.Batches;
            this.logCompletion = logCompletion;
        }

        private void writeSortedContents()
        {
            logger.info("Writing {}, flushed range = [{}, {})", toFlush.memtable(), toFlush.from(), toFlush.to());

            // (we can't clear out the map as-we-go to free up memory,
            //  since the memtable is being used for queries in the "pending flush" category)
            for (Partition partition : toFlush)
            {
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

            if (logCompletion)
            {
                long bytesFlushed = writer.getBytesWritten();
                logger.info("Completed flushing {} ({}) for commitlog position {}",
                            writer.getFilename(),
                            FBUtilities.prettyPrintMemory(bytesFlushed),
                            toFlush.memtable().getFinalCommitLogUpperBound());
                // Update the metrics
                metrics.bytesFlushed.inc(bytesFlushed);
            }
        }

        @Override
        public SSTableMultiWriter call()
        {
            writeSortedContents();
            return writer;
            // We don't close the writer on error as the caller aborts all runnables if one happens.
        }

        @Override
        public String toString()
        {
            return "Flush " + toFlush.metadata().keyspace + '.' + toFlush.metadata().name;
        }
    }

    public static SSTableMultiWriter createFlushWriter(ColumnFamilyStore cfs,
                                                       Memtable.FlushablePartitionSet<?> flushSet,
                                                       LifecycleTransaction txn,
                                                       Descriptor descriptor,
                                                       long partitionCount)
    {
        return cfs.createSSTableMultiWriter(descriptor,
                                            partitionCount,
                                            ActiveRepairService.UNREPAIRED_SSTABLE,
                                            ActiveRepairService.NO_PENDING_REPAIR,
                                            false,
                                            new IntervalSet<>(flushSet.commitLogLowerBound(),
                                                              flushSet.commitLogUpperBound()),
                                            new SerializationHeader(true,
                                                                    flushSet.metadata(),
                                                                    flushSet.columns(),
                                                                    flushSet.encodingStats()),
                                            txn);
    }
}
