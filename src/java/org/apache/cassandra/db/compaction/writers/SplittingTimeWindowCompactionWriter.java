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
package org.apache.cassandra.db.compaction.writers;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.*;

import org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy;
import org.apache.cassandra.db.compaction.TimeWindowCompactionStrategyOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.utils.Pair;

/**
 * CompactionAwareWriter that splits input in different sstables for each time window.
 *
 * Biggest sstable will be total_compaction_size / 2, second biggest total_compaction_size / 4 etc until
 * the result would be sub 50MB, all those are put in the same
 */
public class SplittingTimeWindowCompactionWriter extends CompactionAwareWriter
{
    private static final Logger logger = LoggerFactory.getLogger(SplittingTimeWindowCompactionWriter.class);

    private final TimeWindowCompactionStrategyOptions options;
    private final Set<SSTableReader> allSSTables;
    Pair<HashMultimap<Long, SSTableReader>, Long> buckets;
    private HashMap<Long, SSTableWriter> writersByBounds;
    private Directories.DataDirectory location;
    private Pair<Long, Long> currentBounds;

    public SplittingTimeWindowCompactionWriter(ColumnFamilyStore cfs, Directories directories, LifecycleTransaction txn, Set<SSTableReader> nonExpiredSSTables)
    {
        this(cfs, directories, txn, nonExpiredSSTables, new TimeWindowCompactionStrategyOptions());
    }

    public SplittingTimeWindowCompactionWriter(ColumnFamilyStore cfs,
                                               Directories directories,
                                               LifecycleTransaction txn,
                                               Set<SSTableReader> nonExpiredSSTables,
                                               TimeWindowCompactionStrategyOptions options)
    {
        super(cfs, directories, txn, nonExpiredSSTables,false);
        this.currentBounds = Pair.create(0L, 0L);
        this.allSSTables = txn.originals();
        this.options = options;
        this.writersByBounds = new HashMap<>();
        this.buckets = TimeWindowCompactionStrategy.getBuckets(
                allSSTables,
                options.getSstableWindowUnit(),
                options.getSstableWindowSize(),
                options.getTimestampResolution()
        );
        logger.info("bucket: {}", buckets);
    }

    @Override
    public boolean realAppend(UnfilteredRowIterator partition)
    {
        // TODO: Should we group by minLocalDeletionTime instead ? (which probably makes more sense if we want to optimize full compactions).
        long timestamp = TimeUnit.MILLISECONDS.convert(
                partition.stats().minTimestamp, options.getTimestampResolution());

        logger.info("minTimestamp: {}, localDeletionTime: {}",
                partition.stats().minTimestamp,
                partition.stats().minLocalDeletionTime);
        logger.info("key: {}",
                partition.metadata().partitionKeyType.getString(partition.partitionKey().getKey()));
        Pair<Long,Long> bounds = TimeWindowCompactionStrategy.getWindowBoundsInMillis(
                options.getSstableWindowUnit(), options.getSstableWindowSize(),
                timestamp);

        logger.info("bounds: {}", bounds);

        if (currentBounds.left == 0) {
            // For the first partition, save the current context.
            currentBounds = bounds;
            logger.trace("Saving writer for {}", bounds);
            writersByBounds.put(bounds.left, sstableWriter.currentWriter());
        }

        // Then switch writers when necessary.
        if (!currentBounds.left.equals(bounds.left))
        {
            SSTableWriter writer = writersByBounds.getOrDefault(bounds.left, null);
            if (writer == null) {
                logger.trace("Creating writer for {}", bounds);
                switchCompactionLocation(location);
                writer = sstableWriter.currentWriter();
                writersByBounds.put(bounds.left, writer);
            } else {
                sstableWriter.switchWriter(writer);
            }
            logger.debug("Switching to writer {} for bounds {}", writer, bounds);
            currentBounds = bounds;
        }
        RowIndexEntry rie = sstableWriter.append(partition);
        return rie != null;
    }

    @Override
    public void switchCompactionLocation(Directories.DataDirectory location)
    {
        this.location = location;
        /* TODO: We could get a better estimate by looking at the SSTables supposed to match this.currentBounds */
        long estimatedKeys = estimatedKeys() / this.buckets.left.size();

        @SuppressWarnings("resource")
        SSTableWriter writer = SSTableWriter.create(
                cfs.newSSTableDescriptor(getDirectories().getLocationForDisk(location)),
                estimatedKeys,
                minRepairedAt,
                pendingRepair,
                cfs.metadata,
                new MetadataCollector(txn.originals(), cfs.metadata().comparator, 0),
                SerializationHeader.make(cfs.metadata(), nonExpiredSSTables),
                cfs.indexManager.listIndexes(),
                txn);

        logger.trace("Switching writer");
        sstableWriter.switchWriter(writer);
    }
}
