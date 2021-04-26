/*
 * All changes to the original code are Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

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
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.compaction.CompactionInterruptedException;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.Tracker;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.index.SecondaryIndexBuilder;
import org.apache.cassandra.index.sai.disk.StorageAttachedIndexWriter;
import org.apache.cassandra.index.sai.disk.io.CryptoUtils;
import org.apache.cassandra.index.sai.disk.io.IndexComponents;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.sstable.SSTableSimpleIterator;
import org.apache.cassandra.io.sstable.format.PartitionIndexIterator;
import org.apache.cassandra.io.sstable.format.RowIndexEntry;
import org.apache.cassandra.io.sstable.format.SSTableFlushObserver;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.concurrent.Ref;

import static org.apache.cassandra.db.compaction.TableOperation.StopTrigger.TRUNCATE;

/**
 * Multiple storage-attached indexes can start building concurrently. We need to make sure:
 * 1. Per-SSTable index files are built only once, eg. {@link IndexComponents#PER_SSTABLE_COMPONENTS}
 *      a. Per-SSTable index files already built, do nothing
 *      b. Per-SSTable index files are currently building, we need to wait until it's built in order to consider index built.
 * 2. Per-column index files are built for each column index..{@link IndexComponents#perColumnComponents(String, boolean)}
 */
public class StorageAttachedIndexBuilder extends SecondaryIndexBuilder
{
    protected static final Logger logger = LoggerFactory.getLogger(StorageAttachedIndexBuilder.class);

    // make sure only one builder can write to per sstable files when multiple storage-attached indexes are created simultaneously.
    private static final Map<SSTableReader, CountDownLatch> inProgress = Maps.newConcurrentMap();

    private final StorageAttachedIndexGroup group;
    private final TableMetadata metadata;
    private final Tracker tracker;
    private final UUID compactionId = UUIDGen.getTimeUUID();
    private final boolean isFullRebuild;
    private final boolean isInitialBuild;

    private final SortedMap<SSTableReader, Set<StorageAttachedIndex>> sstables;

    private long bytesProcessed = 0;
    private final long totalSizeInBytes;

    StorageAttachedIndexBuilder(StorageAttachedIndexGroup group, SortedMap<SSTableReader, Set<StorageAttachedIndex>> sstables, boolean isFullRebuild, boolean isInitialBuild)
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
            {
                return;
            }
        }
    }

    private String logMessage(String message) {
        return String.format("[%s.%s.*] %s", metadata.keyspace, metadata.name, message);
    }

    /**
     * @return true if index build should be stopped
     */
    private boolean indexSSTable(SSTableReader sstable, Set<StorageAttachedIndex> indexes)
    {
        CountDownLatch perSSTableFileLock = null;
        StorageAttachedIndexWriter indexWriter = null;

        Ref<? extends SSTableReader> ref = sstable.tryRef();
        if (ref == null)
        {
            logger.warn(logMessage("Couldn't acquire reference to the SSTable {}. It may have been removed."), sstable.descriptor);
            return false;
        }

        try (RandomAccessReader dataFile = sstable.openDataReader();
             LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.INDEX_BUILD))
        {
            perSSTableFileLock = shouldWriteTokenOffsetFiles(sstable);
            boolean perColumnOnly = perSSTableFileLock == null;
            // remove existing per column index files instead of overwriting
            indexes.forEach(index -> index.deleteIndexFiles(sstable));

            final CompressionParams compressionParams = CryptoUtils.getCompressionParams(sstable);

            indexWriter = new StorageAttachedIndexWriter(sstable.descriptor, indexes, txn, perColumnOnly, compressionParams);

            long previousKeyPosition = 0;
            indexWriter.begin();

            try (PartitionIndexIterator keys = sstable.allKeysIterator())
            {
                while (!keys.isExhausted())
                {
                    if (isStopRequested())
                    {
                        throw new CompactionInterruptedException(getProgress());
                    }

                    final DecoratedKey key = sstable.decorateKey(keys.key());
                    final long keyPosition = keys.keyPosition();

                    indexWriter.startPartition(key, keyPosition);

                    RowIndexEntry indexEntry = sstable.getPosition(key, SSTableReader.Operator.EQ);
                    dataFile.seek(indexEntry.position);
                    ByteBufferUtil.skipShortLength(dataFile); // key

                    /*
                     * Not directly using {@link SSTableIdentityIterator#create(SSTableReader, FileDataInput, DecoratedKey)},
                     * because we need to get position of partition level deletion and static row.
                     */
                    long partitionDeletionPosition = dataFile.getFilePointer();
                    DeletionTime partitionLevelDeletion = DeletionTime.serializer.deserialize(dataFile);
                    long staticRowPosition = dataFile.getFilePointer();

                    indexWriter.partitionLevelDeletion(partitionLevelDeletion, partitionDeletionPosition);

                    DeserializationHelper helper = new DeserializationHelper(sstable.metadata(), sstable.descriptor.version.correspondingMessagingVersion(), DeserializationHelper.Flag.LOCAL);

                    try (SSTableSimpleIterator iterator = SSTableSimpleIterator.create(sstable.metadata(), dataFile, sstable.header, helper, partitionLevelDeletion);
                         SSTableIdentityIterator partition = new SSTableIdentityIterator(sstable, key, partitionLevelDeletion, sstable.getFilename(), iterator))
                    {
                        // if the row has statics attached, it has to be indexed separately
                        if (metadata.hasStaticColumns())
                            indexWriter.nextUnfilteredCluster(partition.staticRow(), staticRowPosition);

                        while (partition.hasNext())
                        {
                            long unfilteredPosition = dataFile.getFilePointer();
                            indexWriter.nextUnfilteredCluster(partition.next(), unfilteredPosition);
                        }
                    }

                    keys.advance();
                    long dataPosition = keys.isExhausted() ? sstable.uncompressedLength() : keys.dataPosition();
                    bytesProcessed += dataPosition - previousKeyPosition;
                    previousKeyPosition = dataPosition;
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
                // TODO: Is there anything that makes more sense than just restoring the interrupt?
                logger.warn(logMessage("Interrupted while building indexes {} on SSTable {}"), indexes, sstable.descriptor);
                Thread.currentThread().interrupt();
                return true;
            }
            else if (t instanceof CompactionInterruptedException)
            {
                if (isInitialBuild && trigger() != TRUNCATE)
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
                perSSTableFileLock.countDown();
            }
        }
    }

    @Override
    public OperationProgress getProgress()
    {
        return new OperationProgress(metadata,
                                     OperationType.INDEX_BUILD,
                                     bytesProcessed,
                                     totalSizeInBytes,
                                     compactionId,
                                     sstables.keySet());
    }

    /**
     * if the per sstable index files are already created, not need to write it again, unless found corrupted on rebuild
     * if not created, try to acquire a lock, so only one builder will generate per sstable index files
     */
    private CountDownLatch shouldWriteTokenOffsetFiles(SSTableReader sstable)
    {
        // if per-table files are incomplete or checksum failed during full rebuild.
        if (!IndexComponents.isGroupIndexComplete(sstable.descriptor) ||
            (isFullRebuild && !IndexComponents.perSSTable(sstable).validatePerSSTableComponentsChecksum()))
        {
            CountDownLatch latch = new CountDownLatch(1);
            if (inProgress.putIfAbsent(sstable, latch) == null)
            {
                // lock owner should cleanup existing per-SSTable files
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
            latch.countDown();
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
        sstable.registerComponents(group.getComponents(existing), tracker);
        Set<StorageAttachedIndex> incomplete = group.onSSTableChanged(Collections.emptyList(), Collections.singleton(sstable), existing, false, false);

        if (!incomplete.isEmpty())
        {
            // If this occurs during an initial index build, there is only one index in play, and
            // throwing here to terminate makes sense. (This allows the initialization task to fail
            // correctly and be marked as failed by the SIM.) In other cases, such as rebuilding a
            // set of indexes for a new added/streamed SSTables, we terminate pessimistically. In
            // other words, we abort the SSTable index write across all column indexes and mark
            // then non-queryable until a restart or other incremental rebuild occurs.
            throw new RuntimeException(logMessage("Failed to update views on column indexes " + incomplete + " on indexes " + indexes + "."));
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
            String droppedIndexes = dropped.stream().map(sai -> sai.getContext().getIndexName()).collect(Collectors.toList()).toString();
            if (isFullRebuild)
                throw new RuntimeException(logMessage(String.format("%s are dropped, will stop index build.", droppedIndexes)));
            else
                logger.debug(logMessage("Skip building dropped index {} on sstable {}"), droppedIndexes, descriptor.baseFilename());
        }

        return existing;
    }
}
