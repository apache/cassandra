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
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.tries.InMemoryTrie;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.sstable.SSTableFlushObserver;
import org.apache.cassandra.utils.Throwables;

/**
 * Writes all on-disk index structures attached to a given SSTable.
 */
@NotThreadSafe
public class StorageAttachedIndexWriter implements SSTableFlushObserver
{
    private static final Logger logger = LoggerFactory.getLogger(StorageAttachedIndexWriter.class);

    private final IndexDescriptor indexDescriptor;
    private final Collection<PerColumnIndexWriter> perIndexWriters;
    private final PerSSTableIndexWriter perSSTableWriter;
    private final Stopwatch stopwatch = Stopwatch.createUnstarted();
    private final RowMapping rowMapping;
    private DecoratedKey currentKey;
    private boolean tokenOffsetWriterCompleted = false;
    private boolean aborted = false;

    private long sstableRowId = 0;

    public static StorageAttachedIndexWriter createFlushObserverWriter(IndexDescriptor indexDescriptor,
                                                                       Collection<StorageAttachedIndex> indexes,
                                                                       LifecycleNewTracker lifecycleNewTracker) throws IOException
    {
        return new StorageAttachedIndexWriter(indexDescriptor, indexes, lifecycleNewTracker, false);

    }

    public static StorageAttachedIndexWriter createBuilderWriter(IndexDescriptor indexDescriptor,
                                                                 Collection<StorageAttachedIndex> indexes,
                                                                 LifecycleNewTracker lifecycleNewTracker,
                                                                 boolean perIndexComponentsOnly) throws IOException
    {
        return new StorageAttachedIndexWriter(indexDescriptor, indexes, lifecycleNewTracker, perIndexComponentsOnly);
    }

    private StorageAttachedIndexWriter(IndexDescriptor indexDescriptor,
                                       Collection<StorageAttachedIndex> indexes,
                                       LifecycleNewTracker lifecycleNewTracker,
                                       boolean perIndexComponentsOnly) throws IOException
    {
        this.indexDescriptor = indexDescriptor;
        this.rowMapping = RowMapping.create(lifecycleNewTracker.opType());
        this.perIndexWriters = indexes.stream().map(index -> indexDescriptor.newPerColumnIndexWriter(index,
                                                                                                     lifecycleNewTracker,
                                                                                                     rowMapping))
                                      .filter(Objects::nonNull) // a null here means the column had no data to flush
                                      .collect(Collectors.toList());

        // If the SSTable components are already being built by another index build then we don't want
        // to build them again so use a null writer
        this.perSSTableWriter = perIndexComponentsOnly ? PerSSTableIndexWriter.NONE : indexDescriptor.newPerSSTableIndexWriter();
    }

    @Override
    public void begin()
    {
        logger.debug(indexDescriptor.logMessage("Starting partition iteration for storage-attached index flush for SSTable {}..."), indexDescriptor.sstableDescriptor);
        stopwatch.start();
    }

    @Override
    public void startPartition(DecoratedKey key, long keyPosition, long keyPositionForSASI)
    {
        if (aborted) return;
        
        currentKey = key;

        try
        {
            perSSTableWriter.startPartition(key);
        }
        catch (Throwable t)
        {
            logger.error(indexDescriptor.logMessage("Failed to record a partition during an index build"), t);
            abort(t, true);
        }
    }

    @Override
    public void nextUnfilteredCluster(Unfiltered unfiltered)
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
        }
    }

    @Override
    public void staticRow(Row staticRow)
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
        }
    }

    @Override
    public void complete()
    {
        if (aborted) return;

        long start = stopwatch.elapsed(TimeUnit.MILLISECONDS);

        logger.debug(indexDescriptor.logMessage("Completed partition iteration for index flush for SSTable {}. Elapsed time: {} ms"),
                     indexDescriptor.sstableDescriptor, start);

        try
        {
            perSSTableWriter.complete();
            tokenOffsetWriterCompleted = true;

            long elapsed = stopwatch.elapsed(TimeUnit.MILLISECONDS);

            logger.debug(indexDescriptor.logMessage("Completed per-SSTable write for SSTable {}. Duration: {} ms. Total elapsed time: {} ms."),
                         indexDescriptor.sstableDescriptor, elapsed - start, elapsed);

            start = elapsed;

            rowMapping.complete();

            for (PerColumnIndexWriter perIndexWriter : perIndexWriters)
            {
                perIndexWriter.complete(stopwatch);
            }
            elapsed = stopwatch.elapsed(TimeUnit.MILLISECONDS);
            logger.debug(indexDescriptor.logMessage("Completed per-index writes for SSTable {}. Duration: {} ms. Total elapsed time: {} ms."),
                         indexDescriptor.sstableDescriptor, elapsed - start, elapsed);
        }
        catch (Throwable t)
        {
            logger.error(indexDescriptor.logMessage("Failed to complete an index build"), t);
            abort(t, true);
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
        if (aborted) return;

        // Mark the write operation aborted, so we can short-circuit any further operations on the component writers.
        aborted = true;
        
        for (PerColumnIndexWriter perIndexWriter : perIndexWriters)
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
            perSSTableWriter.abort();
        }

        // If the abort was from an index error, propagate the error upstream so index builds, compactions, and 
        // flushes can handle it correctly.
        if (fromIndex)
            throw Throwables.unchecked(accumulator);
    }

    private void addRow(Row row) throws IOException, InMemoryTrie.SpaceExhaustedException
    {
        PrimaryKey primaryKey = indexDescriptor.hasClustering() ? indexDescriptor.primaryKeyFactory.create(currentKey, row.clustering())
                                                                : indexDescriptor.primaryKeyFactory.create(currentKey);
        perSSTableWriter.nextRow(primaryKey);
        rowMapping.add(primaryKey, sstableRowId);

        for (PerColumnIndexWriter w : perIndexWriters)
        {
            w.addRow(primaryKey, row, sstableRowId);
        }
        sstableRowId++;
    }
}
