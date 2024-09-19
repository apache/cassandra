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

package org.apache.cassandra.index.accord;

import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.compaction.CompactionInterruptedException;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.Tracker;
import org.apache.cassandra.index.SecondaryIndexBuilder;
import org.apache.cassandra.io.sstable.KeyIterator;
import org.apache.cassandra.io.sstable.SSTableFlushObserver;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.Ref;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;

public class RouteSecondaryIndexBuilder extends SecondaryIndexBuilder
{
    private static final Logger logger = LoggerFactory.getLogger(RouteSecondaryIndexBuilder.class);

    private final TimeUUID compactionId = nextTimeUUID();
    private final RouteIndex index;
    private final TableMetadata metadata;
    private final Tracker tracker;
    private final SSTableManager sstableManager;
    private final List<SSTableReader> sstables;
    private final boolean isFullRebuild;
    private final boolean isInitialBuild;
    private final long totalSizeInBytes;
    private long bytesProcessed = 0;

    public RouteSecondaryIndexBuilder(RouteIndex index,
                                      SSTableManager sstableManager,
                                      List<SSTableReader> sstables,
                                      boolean isFullRebuild,
                                      boolean isInitialBuild)
    {
        this.index = index;
        this.metadata = index.baseCfs().metadata();
        this.tracker = index.baseCfs().getTracker();
        this.sstableManager = sstableManager;
        this.sstables = sstables;
        this.isFullRebuild = isFullRebuild;
        this.isInitialBuild = isInitialBuild;
        this.totalSizeInBytes = sstables.stream().mapToLong(SSTableReader::uncompressedLength).sum();
    }

    @Override
    public CompactionInfo getCompactionInfo()
    {
        return new CompactionInfo(metadata,
                                  OperationType.INDEX_BUILD,
                                  bytesProcessed,
                                  totalSizeInBytes,
                                  compactionId,
                                  sstables);
    }

    @Override
    public void build()
    {
        if (!validateIndexes())
            return;
        for (SSTableReader sstable : sstables)
        {
            if (indexSSTable(sstable))
                return;
        }
    }

    /**
     * @return true if index build should be stopped
     */
    private boolean indexSSTable(SSTableReader sstable)
    {
        logger.debug("Starting index build on {}", sstable.descriptor);

        RouteIndexFormat.SSTableIndexWriter indexWriter = null;

        Ref<? extends SSTableReader> ref = sstable.tryRef();
        if (ref == null)
        {
            logger.warn("Couldn't acquire reference to the SSTable {}. It may have been removed.", sstable.descriptor);
            return false;
        }

        try (RandomAccessReader dataFile = sstable.openDataReader();
             LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.INDEX_BUILD, sstable))
        {
            // remove existing per column index files instead of overwriting
            IndexDescriptor indexDescriptor = IndexDescriptor.create(sstable);
            indexDescriptor.deleteIndex();

            indexWriter = new RouteIndexFormat.SSTableIndexWriter(index, indexDescriptor);
            indexWriter.begin();

            long previousBytesRead = 0;

            try (KeyIterator keys = sstable.keyIterator())
            {
                while (keys.hasNext())
                {
                    if (isStopRequested())
                    {
                        logger.debug("Index build has been stopped");
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

                completeSSTable(indexDescriptor, indexWriter, sstable);
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
                logger.warn("Interrupted while building indexes on SSTable {}", sstable.descriptor);
                Thread.currentThread().interrupt();
                return true;
            }
            else if (t instanceof CompactionInterruptedException)
            {
                //TODO Shouldn't do this if the stop was interrupted by a truncate
                if (isInitialBuild)
                {
                    logger.error("Stop requested while building initial indexes on SSTable {}.", sstable.descriptor);
                    throw Throwables.unchecked(t);
                }
                else
                {
                    logger.info("Stop requested while building indexes on SSTable {}.", sstable.descriptor);
                    return true;
                }
            }
            else
            {
                logger.error("Unable to build indexes on SSTable {}. Cause: {}.", sstable, t.getMessage());
                throw Throwables.unchecked(t);
            }
        }
        finally
        {
            ref.release();
        }
    }

    private void completeSSTable(IndexDescriptor indexDescriptor, SSTableFlushObserver indexWriter, SSTableReader sstable)
    {
        indexWriter.complete();

        if (!validateIndexes())
        {
            logger.debug("dropped during index build");
            return;
        }

        // register custom index components into existing sstables
        sstable.registerComponents(indexDescriptor.getLiveSSTableComponents(), tracker);
        sstableManager.onSSTableChanged(Collections.emptyList(), Collections.singleton(sstable));
    }

    /**
     * In case of full rebuild, stop the index build if any index is dropped.
     * Otherwise, skip dropped indexes to avoid exception during repair/streaming.
     */
    private boolean validateIndexes()
    {
        switch (index.registerStatus())
        {
            case PENDING: throw new IllegalStateException("Unable to build indexes if the index has not been registered");
            case REGISTERED: return true;
            case UNREGISTERED: break;
            default: throw new AssertionError("Unknown status: " + index.registerStatus());
        }

        if (isFullRebuild)
            throw new RuntimeException(String.format("%s are dropped, will stop index build.", index.getIndexMetadata().name));

        return false;
    }
}
