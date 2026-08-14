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
package org.apache.cassandra.index.sai.disk;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.tries.TrieSpaceExhaustedException;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.format.OnDiskFormat;
import org.apache.cassandra.index.sai.memory.RowMapping;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.SSTableFlushObserver;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ApproximateTime;
import org.apache.cassandra.utils.Throwables;

/**
 * Writes all on-disk index structures attached to a given SSTable.
 */
public class StorageAttachedIndexWriter implements SSTableFlushObserver
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final IndexDescriptor indexDescriptor;
    private final PrimaryKey.Factory primaryKeyFactory;
    private final Collection<StorageAttachedIndex> indices;
    private final Collection<PerIndexWriter> perIndexWriters;
    private final PerSSTableWriter perSSTableWriter;
    private final Stopwatch stopwatch = Stopwatch.createUnstarted();
    private final RowMapping rowMapping;
    private final OperationType opType;
    private final TableMetrics tableMetrics;

    private DecoratedKey currentKey;
    private boolean tokenOffsetWriterCompleted = false;
    private boolean aborted = false;

    private long sstableRowId = 0;
    private long totalTimeSpent = 0;

    public StorageAttachedIndexWriter(IndexDescriptor indexDescriptor,
                                      TableMetadata tableMetadata,
                                      Collection<StorageAttachedIndex> indices,
                                      LifecycleNewTracker lifecycleNewTracker,
                                      long keyCount,
                                      TableMetrics tableMetrics) throws IOException
    {
        this(indexDescriptor, tableMetadata, indices, lifecycleNewTracker, keyCount, false, tableMetrics);
    }

    public StorageAttachedIndexWriter(IndexDescriptor indexDescriptor,
                                      TableMetadata tableMetadata,
                                      Collection<StorageAttachedIndex> indices,
                                      LifecycleNewTracker lifecycleNewTracker,
                                      long keyCount,
                                      boolean perIndexComponentsOnly,
                                      TableMetrics tableMetrics) throws IOException
    {
        // We always use the version of the existing per-sstable components, if any, or the configured current version
        // if there are no previously existing per-sstable components.
        OnDiskFormat onDiskFormat = indexDescriptor.versionForNewComponents().onDiskFormat();
        this.indexDescriptor = indexDescriptor;
        // Note: I think there is a silent assumption here. That is, the PK factory we use here must be for the current
        // format version, because that is what `IndexContext.keyFactory` always uses (see ctor)
        this.primaryKeyFactory = onDiskFormat.newPrimaryKeyFactory(tableMetadata.comparator);
        this.indices = indices;
        this.opType = lifecycleNewTracker.opType();
        this.rowMapping = RowMapping.create(opType);
        this.perIndexWriters = indices.stream().map(i -> onDiskFormat.newPerIndexWriter(i,
                                                                                        indexDescriptor,
                                                                                        lifecycleNewTracker,
                                                                                        rowMapping,
                                                                                        keyCount))
                                      .filter(Objects::nonNull) // a null here means the column had no data to flush
                                      .collect(Collectors.toList());

        // If the SSTable components are already being built by another index build then we don't want
        // to build them again so use a NO-OP writer
        this.perSSTableWriter = perIndexComponentsOnly
                                ? PerSSTableWriter.NONE
                                : onDiskFormat.newPerSSTableWriter(indexDescriptor);
        this.tableMetrics = tableMetrics;
    }

    @Override
    public void begin()
    {
        logger.trace(indexDescriptor.logMessage("Starting partition iteration for storage attached index flush for SSTable {}..."), indexDescriptor.descriptor);
        stopwatch.start();
    }

    @Override
    public void startPartition(DecoratedKey key, long position)
    {
        if (aborted) return;

        currentKey = key;

        try
        {
            perSSTableWriter.startPartition(key, position);
        }
        catch (Throwable t)
        {
            logger.error(indexDescriptor.logMessage("Failed to record a partition start during an index build"), t);
            abort(t, true);
            // fail compaction task or index build task if SAI failed
            throw Throwables.unchecked(t);
        }
    }

    @Override
    public void nextUnfilteredCluster(Unfiltered unfiltered, long position)
    {
        if (aborted) return;

        // Ignore range tombstones...
        if (!unfiltered.isRow())
            return;

        try
        {
            addRow((Row)unfiltered);
        }
        catch (Throwable t)
        {
            logger.error(indexDescriptor.logMessage("Failed to record a row during an index build"), t);
            abort(t, true);
            // fail compaction task or index build task if SAI failed
            throw Throwables.unchecked(t);
        }
    }

    @Override
    public void partitionLevelDeletion(DeletionTime deletionTime, long position)
    {
        // Deletions (including partition deletions) are accounted for during reads.
    }

    @Override
    public void staticRow(Row staticRow, long position)
    {
        if (aborted) return;

        if (staticRow.isEmpty())
            return;

        try
        {
            addRow(staticRow);
        }
        catch (Throwable t)
        {
            logger.error(indexDescriptor.logMessage("Failed to record a static row during an index build"), t);
            abort(t, true);
            // fail compaction task or index build task if SAI failed
            throw Throwables.unchecked(t);
        }
    }

    @Override
    public void onSSTableWriterSwitched()
    {
        if (aborted) return;

        try
        {
            long start = ApproximateTime.nanoTime();
            for (PerIndexWriter w : perIndexWriters)
            {
                w.onSSTableWriterSwitched(stopwatch);
            }
            totalTimeSpent += (ApproximateTime.nanoTime() - start);
        }
        catch (Throwable t)
        {
            logger.error(indexDescriptor.logMessage("Failed to flush segment on sstable writer switched"), t);
            abort(t, true);
            // fail compaction task or index build task if SAI failed
            throw Throwables.unchecked(t);
        }
    }

    @Override
    public void complete(SSTable sstable)
    {
        long startComplete = ApproximateTime.nanoTime();
        try
        {
            if (aborted) return;

            long start = stopwatch.elapsed(TimeUnit.MILLISECONDS);

            logger.trace(indexDescriptor.logMessage("Completed partition iteration for index flush for SSTable {}. Elapsed time: {} ms"),
                         indexDescriptor.descriptor,
                         start);

            try
            {
                perSSTableWriter.complete(stopwatch);
                tokenOffsetWriterCompleted = true;
                long elapsed = stopwatch.elapsed(TimeUnit.MILLISECONDS);
                logger.trace(indexDescriptor.logMessage("Completed per-SSTable write for SSTable {}. Duration: {} ms. Total elapsed time: {} ms."),
                             indexDescriptor.descriptor,
                             elapsed - start,
                             elapsed);

                start = elapsed;

                rowMapping.complete();

                for (PerIndexWriter perIndexWriter : perIndexWriters)
                {
                    perIndexWriter.complete(stopwatch);

                    // The handling of components when we flush/compact is a tad backward: instead of registering the
                    // components as we write them, all the components are collected beforehand in `SSTableWriter#create`,
                    // which means this is a superset of possible components, but if any components are not written for
                    // those reason, this needs to be fixed afterward. One case for SAI component for instance is empty
                    // indexes: if a particular sstable has nothing indexed for a particular index, then only the completion
                    // marker for that index is kept on disk but no other components, so we need to remove the components
                    // that were "optimistically" added (and more generally, future index implementation may have some
                    // components that are only optionally present based on specific conditions).
                    // Note 1: for index build/rebuild on existing sstable, `SSTableWriter#create` is not used, and instead
                    //   we do only register components written (see `StorageAttachedIndexBuilder#completeSSTable`).
                    // Note 2: as hinted above, an alternative here would be to change the whole handling of components,
                    //   registering components only as they are effectively written. This is a larger refactor, with some
                    //   subtleties involved, so it is left as potential future work.
                    if (opType == OperationType.FLUSH || opType == OperationType.COMPACTION)
                    {
                        var writtenComponents = perIndexWriter.writtenComponents().allAsCustomComponents();
                        var registeredComponents = IndexDescriptor.perIndexComponentsForNewlyFlushedSSTable(perIndexWriter.indexContext());
                        var toRemove = Sets.difference(registeredComponents, writtenComponents);
                        if (!toRemove.isEmpty())
                        {
                            if (logger.isTraceEnabled())
                            {
                                logger.trace(indexDescriptor.logMessage("Removing optimistically added but not writen components from TOC of SSTable {} for index {}"),
                                             indexDescriptor.descriptor,
                                             perIndexWriter.indexContext().getIndexName());
                            }

                            // During flush, this happens as we finalize the sstable and before its size is tracked, so not
                            // passing a tracker is correct and intended (there is nothing to update in the tracker).
                            sstable.unregisterComponents(toRemove, null);
                        }
                    }
                }
                elapsed = stopwatch.elapsed(TimeUnit.MILLISECONDS);
                logger.trace(indexDescriptor.logMessage("Completed per-index writes for SSTable {}. Duration: {} ms. Total elapsed time: {} ms."),
                             indexDescriptor.descriptor,
                             elapsed - start,
                             elapsed);
            }
            catch (Throwable t)
            {
                logger.error(indexDescriptor.logMessage("Failed to complete an index build"), t);
                abort(t, true);
                // fail compaction task or index build task if SAI failed
                throw Throwables.unchecked(t);
            }
        }
        finally
        {
            totalTimeSpent += (ApproximateTime.nanoTime() - startComplete);
            tableMetrics.updateStorageAttachedIndexWritingTime(totalTimeSpent, opType);
        }
    }

    /**
     * Aborts all column index writers and, only if they have not yet completed, SSTable-level component writers.
     *
     * @param accumulator the initial exception thrown from the failed writer
     */
    @Override
    public void abort(Throwable accumulator)
    {
        abort(accumulator, false);
    }

    /**
     *
     * @param accumulator original cause of the abort
     * @param fromIndex true if the cause of the abort was the index itself, false otherwise
     */
    public void abort(Throwable accumulator, boolean fromIndex)
    {
        if (aborted)
            return;

        // Mark the write aborted, so we can short-circuit any further operations on the component writers.
        aborted = true;

        // For non-compaction and non-flush, make any indexes involved in this transaction non-queryable,
        // as they will likely not match the backing table.
        // For compaction and flush: the task should be aborted and new sstables will not be added to tracker.
        // We do not want to mark the index as non-queryable on compaction and flush because otherwise
        // the index status would be propagated to the other nodes and that would make querying the index impossible
        // also on the other nodes. If the problem with compaction or flush repeats, then it is better to fail
        // on this node only and let the rest of the cluster operate normally.
        if (fromIndex && opType != OperationType.COMPACTION && opType != OperationType.FLUSH)
            indices.forEach(StorageAttachedIndex::makeIndexNonQueryable);

        for (PerIndexWriter perIndexWriter : perIndexWriters)
        {
            try
            {
                perIndexWriter.abort(accumulator);
            }
            catch (Throwable t)
            {
                if (accumulator != null)
                {
                    accumulator.addSuppressed(t);
                }
            }
        }

        if (!tokenOffsetWriterCompleted)
        {
            // If the token/offset files have already been written successfully, they can be reused later.
            perSSTableWriter.abort(accumulator);
        }
    }

    private void addRow(Row row) throws IOException, TrieSpaceExhaustedException
    {
        // we are using System.nanoTime() instead of ApproximateTime.nanoTime() here because
        // it is verify likely that this method takes microsecronds instead of milliseconds
        // and ApproximateTime.nanoTime() precision is 2 milliseconds
        long now = System.nanoTime();
        PrimaryKey primaryKey = primaryKeyFactory.create(currentKey, row.clustering());
        perSSTableWriter.nextRow(primaryKey);
        rowMapping.add(primaryKey, sstableRowId);

        for (PerIndexWriter w : perIndexWriters)
        {
            w.addRow(primaryKey, row, sstableRowId);
        }
        sstableRowId++;

        totalTimeSpent += (System.nanoTime() - now);
    }
}
