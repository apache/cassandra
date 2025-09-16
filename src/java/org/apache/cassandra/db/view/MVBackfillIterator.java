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

package org.apache.cassandra.db.view;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.function.LongPredicate;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.compaction.CompactionController;
import org.apache.cassandra.db.compaction.CompactionIterator;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.concurrent.Refs;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;

/**
 * Iterator for materialized view backfill operations that provides access to merged
 * base table data from SSTables. This iterator follows the same pattern as 
 * CassandraValidationIterator used in repair, but is designed specifically for
 * MV backfill operations.
 * 
 * The iterator uses CompactionIterator to properly merge data from multiple SSTables,
 * ensuring that we get the correct final state of each row for MV translation.
 */
public class MVBackfillIterator implements UnfilteredPartitionIterator
{
    private static final Logger logger = LoggerFactory.getLogger(MVBackfillIterator.class);

    /*
     * Controller for MV backfill compaction that purges tombstones and expired data
     * since we only want to backfill live data to the materialized view.
     */
    private static class MVBackfillCompactionController extends CompactionController
    {
        public MVBackfillCompactionController(ColumnFamilyStore cfs, int gcBefore)
        {
            super(cfs, gcBefore);
        }

        @Override
        public LongPredicate getPurgeEvaluator(DecoratedKey key)
        {
            // We do not need to backfill tombstones, so purge all tombstones
            return time -> true;
        }
    }

    private final ColumnFamilyStore baseCfs;
    private final Collection<Range<Token>> ranges;
    private final Refs<SSTableReader> sstables;
    private final AbstractCompactionStrategy.ScannerList scanners;
    private final MVBackfillCompactionController controller;
    private final CompactionIterator compactionIterator;
    private final long estimatedPartitions;
    private final long estimatedBytes;

    /**
     * Creates a new MVBackfillIterator for the specified base table and token ranges.
     *
     * @param baseCfs the base table column family store
     * @param ranges the token ranges to scan
     * @param nowInSec the current time in seconds for TTL evaluation
     * @throws IOException if there are issues accessing SSTables
     */
    public MVBackfillIterator(ColumnFamilyStore baseCfs, Collection<Range<Token>> ranges, int nowInSec) throws IOException
    {
        this.baseCfs = baseCfs;
        this.ranges = ranges;
        TimeUUID timeUUID = nextTimeUUID();

        // Select SSTables that intersect with our ranges
        this.sstables = baseCfs.getSSTableRefsForRanges(ranges, (s) -> true, timeUUID);
        
        if (sstables.isEmpty())
        {
            logger.debug("No SSTables found for ranges {} in {}.{}", ranges, baseCfs.keyspace.getName(), baseCfs.name);
            this.scanners = null;
            this.controller = null;
            this.compactionIterator = null;
            this.estimatedPartitions = 0;
            this.estimatedBytes = 0;
        }
        else
        {
            logger.info("Starting MV backfill scan for ranges {} with {} SSTables in {}.{}", 
                       ranges, sstables.size(), baseCfs.keyspace.getName(), baseCfs.name);

            // Create compaction controller with proper GC before time
            int gcBefore = baseCfs.gcBefore(nowInSec);
            this.controller = new MVBackfillCompactionController(baseCfs, gcBefore);

            // Get scanners for the selected SSTables - CompactionIterator will merge them
            this.scanners = baseCfs.getCompactionStrategyManager().getScanners(sstables, ranges);
            
            // Create compaction iterator for efficient scanning and merging
            this.compactionIterator = new CompactionIterator(OperationType.VIEW_BUILD, scanners.scanners, controller, nowInSec, timeUUID);

            // Calculate estimated partitions
            long allPartitions = 0;
            for (SSTableReader sstable : sstables)
                allPartitions += sstable.estimatedKeysForRanges(ranges);
            this.estimatedPartitions = allPartitions;

            // Calculate estimated bytes
            long estimatedTotalBytes = 0;
            for (SSTableReader sstable : sstables)
            {
                for (SSTableReader.PartitionPositionBounds positionsForRanges : sstable.getPositionsForRanges(ranges))
                    estimatedTotalBytes += positionsForRanges.upperPosition - positionsForRanges.lowerPosition;
            }
            this.estimatedBytes = estimatedTotalBytes;
        }
    }

    /**
     * Convenience constructor for a single token range.
     */
    public MVBackfillIterator(ColumnFamilyStore baseCfs, Range<Token> range, int nowInSec) throws IOException
    {
        this(baseCfs, Collections.singletonList(range), nowInSec);
    }

    /**
     * Checks if an SSTable intersects with any of the given ranges.
     */
    private static boolean intersects(SSTableReader sstable, Collection<Range<Token>> ranges)
    {
        Bounds<Token> sstableBounds = new Bounds<>(sstable.first.getToken(), sstable.last.getToken());
        return sstableBounds.intersects(ranges);
    }

    @Override
    public boolean hasNext()
    {
        return compactionIterator != null && compactionIterator.hasNext();
    }

    @Override
    public UnfilteredRowIterator next()
    {
        if (compactionIterator == null)
            throw new IllegalStateException("Iterator is empty");
        return compactionIterator.next();
    }

    @Override
    public void close()
    {
        Throwable first = null;

        first = closeSafe(first, compactionIterator);
        first = closeSafe(first, scanners);
        first = closeSafe(first, controller);

        // sstables.release() isn't AutoCloseable; wrap it
        first = closeSafe(first, () -> { if (sstables != null) sstables.release(); });

        if (first != null) {
            if (first instanceof RuntimeException) throw (RuntimeException) first;
            throw new RuntimeException("Failed to close resources", first);
        }
    }

    private static Throwable closeSafe(Throwable first, AutoCloseable c) {
        if (c == null) return first;
        try {
            c.close();
        } catch (Throwable t) {
            if (first == null) return t;
            first.addSuppressed(t);
        }
        return first;
    }

    /**
     * Returns the estimated number of partitions to be processed.
     */
    public long getEstimatedPartitions()
    {
        return estimatedPartitions;
    }

    /**
     * Returns the estimated total bytes to be processed.
     */
    public long getEstimatedBytes()
    {
        return estimatedBytes;
    }

    /**
     * Update the progress of the
     */
    @VisibleForTesting
    public void updateBytesRead()
    {
        if (compactionIterator != null)
        {
            compactionIterator.updateBytesRead();
        }
    }

    /**
     * Returns the current progress as bytes read.
     */
    public long getBytesRead()
    {
        return compactionIterator != null ? compactionIterator.getBytesRead() : 0;
    }

    /**
     * Returns true if this iterator has no data to process.
     */
    public boolean isEmpty()
    {
        return compactionIterator == null;
    }

    /**
     * Returns the base table metadata.
     */
    public TableMetadata metadata()
    {
        return baseCfs.metadata.get();
    }
}
