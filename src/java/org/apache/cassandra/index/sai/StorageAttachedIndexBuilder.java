/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.cassandra.index.sai;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.compaction.CompactionInterruptedException;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.Tracker;
import org.apache.cassandra.index.SecondaryIndexBuilder;
import org.apache.cassandra.index.sai.disk.StorageAttachedIndexWriter;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.KeyIterator;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.sstable.SSTableFlushObserver;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.CountDownLatch;
import org.apache.cassandra.utils.concurrent.Ref;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;

/**
 * Multiple storage-attached indexes can start building concurrently. We need to make sure:
 * 1. Per-SSTable index files are built only once
 *      a. Per-SSTable index files already built, do nothing
 *      b. Per-SSTable index files are currently building, we need to wait until it's built in order to consider index built.
 * 2. Per-column index files are built for each column index
 */
public class StorageAttachedIndexBuilder extends SecondaryIndexBuilder
{
    protected static final Logger logger = LoggerFactory.getLogger(StorageAttachedIndexBuilder.class);

    // make sure only one builder can write to per sstable files when multiple storage-attached indexes are created simultaneously.
    private static final Map<SSTableReader, CountDownLatch> inProgress = Maps.newConcurrentMap();

    private final StorageAttachedIndexGroup group;
    private final TableMetadata metadata;
    private final Tracker tracker;
    private final TimeUUID compactionId = nextTimeUUID();
    private final boolean isFullRebuild;
    private final boolean isInitialBuild;

    private final SortedMap<SSTableReader, Set<StorageAttachedIndex>> sstables;

    private long bytesProcessed = 0;
    private final long totalSizeInBytes;

    StorageAttachedIndexBuilder(StorageAttachedIndexGroup group,
                                SortedMap<SSTableReader, Set<StorageAttachedIndex>> sstables,
                                boolean isFullRebuild,
                                boolean isInitialBuild)
    {
        this.group = group;
        this.metadata = group.metadata();
        this.sstables = sstables;
        this.tracker = group.table().getTracker();
        this.isFullRebuild = isFullRebuild;
        this.isInitialBuild = isInitialBuild;
        this.totalSizeInBytes = sstables.keySet().stream().mapToLong(SSTableReader::uncompressedLength).sum();
    }

    @Override
    public void build()
    {
        logger.debug(logMessage(String.format("Starting %s %s index build...",
                                              isInitialBuild ? "initial" : "non-initial",
                                              isFullRebuild ? "full" : "partial")));

        for (Map.Entry<SSTableReader, Set<StorageAttachedIndex>> e : sstables.entrySet())
        {
            SSTableReader sstable = e.getKey();
            Set<StorageAttachedIndex> indexes = e.getValue();

            Set<StorageAttachedIndex> existing = validateIndexes(indexes, sstable.descriptor);
            if (existing.isEmpty())
            {
                logger.debug(logMessage("{} dropped during index build"), indexes);
                continue;
            }

            if (indexSSTable(sstable, existing))
                return;
        }
    }

    private String logMessage(String message)
    {
        return String.format("[%s.%s.*] %s", metadata.keyspace, metadata.name, message);
    }

    /**
     * @return true if index build should be stopped
     */
    private boolean indexSSTable(SSTableReader sstable, Set<StorageAttachedIndex> indexes)
    {
        logger.debug(logMessage("Starting index build on {}"), sstable.descriptor);

        CountDownLatch perSSTableFileLock = null;
        StorageAttachedIndexWriter indexWriter = null;

        Ref<? extends SSTableReader> ref = sstable.tryRef();
        if (ref == null)
        {
            logger.warn(logMessage("Couldn't acquire reference to the SSTable {}. It may have been removed."), sstable.descriptor);
            return false;
        }

        try (RandomAccessReader dataFile = sstable.openDataReader();
             LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.INDEX_BUILD, sstable))
        {
            perSSTableFileLock = shouldWritePerSSTableFiles(sstable);
            // If we were unable to get the per-SSTable file lock it means that the
            // per-SSTable components are already being built, so we only want to
            // build the per-index components
            boolean perIndexComponentsOnly = perSSTableFileLock == null;
            // remove existing per column index files instead of overwriting
            IndexDescriptor indexDescriptor = IndexDescriptor.create(sstable);
            indexes.forEach(index -> indexDescriptor.deleteColumnIndex(index.termType(), index.identifier()));

            indexWriter = StorageAttachedIndexWriter.createBuilderWriter(indexDescriptor, indexes, txn, perIndexComponentsOnly);

            indexWriter.begin();

            long previousBytesRead = 0;

            try (KeyIterator keys = sstable.keyIterator())
            {
                while (keys.hasNext())
                {
                    if (isStopRequested())
                    {
                        logger.debug(indexDescriptor.logMessage("Index build has been stopped"));
                        throw new CompactionInterruptedException(getCompactionInfo());
                    }

                    DecoratedKey key = keys.next();

                    indexWriter.startPartition(key, -1, -1);

                    long position = sstable.getPosition(key, SSTableReader.Operator.EQ);
                    dataFile.seek(position);
                    ByteBufferUtil.readWithShortLength(dataFile); // key

                    try (SSTableIdentityIterator partition = SSTableIdentityIterator.create(sstable, dataFile, key))
                    {
                        // if the row has statics attached, it has to be indexed separately
                        if (metadata.hasStaticColumns())
                            indexWriter.nextUnfilteredCluster(partition.staticRow());

                        while (partition.hasNext())
                            indexWriter.nextUnfilteredCluster(partition.next());
                    }
                    long bytesRead = keys.getBytesRead();
                    bytesProcessed += bytesRead - previousBytesRead;
                    previousBytesRead = bytesRead;
                }

                completeSSTable(indexWriter, sstable, indexes, perSSTableFileLock);
            }

            return false;
        }
        catch (Throwable t)
        {
            if (indexWriter != null)
            {
                indexWriter.abort(t, true);
            }

            if (t instanceof InterruptedException)
            {
                logger.warn(logMessage("Interrupted while building indexes {} on SSTable {}"), indexes, sstable.descriptor);
                Thread.currentThread().interrupt();
                return true;
            }
            else if (t instanceof CompactionInterruptedException)
            {
                //TODO Shouldn't do this if the stop was interrupted by a truncate
                if (isInitialBuild)
                {
                    logger.error(logMessage("Stop requested while building initial indexes {} on SSTable {}."), indexes, sstable.descriptor);
                    throw Throwables.unchecked(t);
                }
                else
                {
                    logger.info(logMessage("Stop requested while building indexes {} on SSTable {}."), indexes, sstable.descriptor);
                    return true;
                }
            }
            else
            {
                logger.error(logMessage("Unable to build indexes {} on SSTable {}. Cause: {}."), indexes, sstable, t.getMessage());
                throw Throwables.unchecked(t);
            }
        }
        finally
        {
            ref.release();
            // release current lock in case of error
            if (perSSTableFileLock != null)
            {
                inProgress.remove(sstable);
                perSSTableFileLock.decrement();
            }
        }
    }

    @Override
    public CompactionInfo getCompactionInfo()
    {
        return new CompactionInfo(metadata,
                                  OperationType.INDEX_BUILD,
                                  bytesProcessed,
                                  totalSizeInBytes,
                                  compactionId,
                                  sstables.keySet());
    }

    /**
     * if the per sstable index files are already created, no need to write them again, unless found corrupted on rebuild
     * if not created, try to acquire a lock, so only one builder will generate per sstable index files
     */
    private CountDownLatch shouldWritePerSSTableFiles(SSTableReader sstable)
    {
        IndexDescriptor indexDescriptor = IndexDescriptor.create(sstable);

        // if per-table files are incomplete, full rebuild is requested, or checksum fails
        if (!indexDescriptor.isPerSSTableIndexBuildComplete()
            || isFullRebuild
            || !indexDescriptor.validatePerSSTableComponents(IndexValidation.CHECKSUM, true, false))
        {
            CountDownLatch latch = CountDownLatch.newCountDownLatch(1);
            if (inProgress.putIfAbsent(sstable, latch) == null)
            {
                // lock owner should clean up existing per-SSTable files
                group.deletePerSSTableFiles(Collections.singleton(sstable));
                return latch;
            }
        }
        return null;
    }

    private void completeSSTable(SSTableFlushObserver indexWriter,
                                 SSTableReader sstable,
                                 Set<StorageAttachedIndex> indexes,
                                 CountDownLatch latch) throws InterruptedException
    {
        indexWriter.complete();

        if (latch != null)
        {
            // current builder owns the lock
            latch.decrement();
        }
        else
        {
            /*
             * When there is no lock, it means the per sstable index files are already created, just proceed to finish.
             * When there is a lock held by another builder, wait for it to finish before finishing marking current index built.
             */
            latch = inProgress.get(sstable);
            if (latch != null)
                latch.await();
        }

        Set<StorageAttachedIndex> existing = validateIndexes(indexes, sstable.descriptor);
        if (existing.isEmpty())
        {
            logger.debug(logMessage("{} dropped during index build"), indexes);
            return;
        }

        // register custom index components into existing sstables
        sstable.registerComponents(StorageAttachedIndexGroup.getLiveComponents(sstable, existing), tracker);
        Set<StorageAttachedIndex> incomplete = group.onSSTableChanged(Collections.emptyList(), Collections.singleton(sstable), existing, IndexValidation.NONE);

        if (!incomplete.isEmpty())
        {
            // If this occurs during an initial index build, there is only one index in play, and
            // throwing here to terminate makes sense. (This allows the initialization task to fail
            // correctly and be marked as failed by the SIM.) In other cases, such as rebuilding a
            // set of indexes for a new added/streamed SSTables, we terminate pessimistically. In
            // other words, we abort the SSTable index write across all column indexes and mark
            // then non-queryable until a restart or other incremental rebuild occurs.
            throw new RuntimeException(logMessage("Failed to update views on column indexes " + incomplete + " on indexes " + indexes + '.'));
        }
    }

    /**
     *  In case of full rebuild, stop the index build if any index is dropped.
     *  Otherwise, skip dropped indexes to avoid exception during repair/streaming.
     */
    private Set<StorageAttachedIndex> validateIndexes(Set<StorageAttachedIndex> indexes, Descriptor descriptor)
    {
        Set<StorageAttachedIndex> existing = new HashSet<>();
        Set<StorageAttachedIndex> dropped = new HashSet<>();

        for (StorageAttachedIndex index : indexes)
        {
            if (group.containsIndex(index))
                existing.add(index);
            else
                dropped.add(index);
        }

        if (!dropped.isEmpty())
        {
            String droppedIndexes = dropped.stream().map(sai -> sai.identifier().indexName).collect(Collectors.toList()).toString();
            if (isFullRebuild)
                throw new RuntimeException(logMessage(String.format("%s are dropped, will stop index build.", droppedIndexes)));
            else
                logger.debug(logMessage("Skip building dropped index {} on sstable {}"), droppedIndexes, descriptor.baseFile());
        }

        return existing;
    }
}
