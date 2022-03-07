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

package org.apache.cassandra.db.compaction;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.RateLimiter;

import org.apache.cassandra.db.compaction.writers.SSTableDataSink;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.io.sstable.compaction.SortedStringTableCursor;
import org.apache.cassandra.io.sstable.compaction.IteratorFromCursor;
import org.apache.cassandra.io.sstable.compaction.PurgeCursor;
import org.apache.cassandra.io.sstable.compaction.SSTableCursor;
import org.apache.cassandra.io.sstable.compaction.SSTableCursorMerger;
import org.apache.cassandra.io.sstable.compaction.SkipEmptyDataCursor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableMetadata;

/**
 * Counterpart to CompactionIterator. Maintains sstable cursors, applies limiter and produces metrics. In the future it
 * should also pass information to observers and deal with expired tombstones and garbage-collection compactions.
 */
public class CompactionCursor implements SSTableCursorMerger.MergeListener, AutoCloseable
{
    private static final long MILLISECONDS_TO_UPDATE_PROGRESS = 1000;

    private final OperationType type;
    private final CompactionController controller;
    private final ImmutableSet<SSTableReader> sstables;
    private final UUID compactionId;
    private final SSTableCursor cursor;
    private final Row.Builder rowBuilder;

    private final long totalBytes;
    private volatile long currentBytes;
    private long currentProgressMillisSinceStartup;

    /**
     * Merged frequency counters for partitions and rows (AKA histograms).
     * The array index represents the number of sstables containing the row or partition minus one. So index 0 contains
     * the number of rows or partitions coming from a single sstable (therefore copied rather than merged), index 1 contains
     * the number of rows or partitions coming from two sstables and so forth.
     */
    private final long[] mergedPartitionsHistogram;
    private final long[] mergedRowsHistogram;

    private final long totalCompressedSize;

    @SuppressWarnings("resource")
    public CompactionCursor(OperationType type, Collection<SSTableReader> readers, CompactionController controller, RateLimiter limiter, int nowInSec, UUID compactionId)
    {
        this.controller = controller;
        this.type = type;
        this.compactionId = compactionId;
        this.totalCompressedSize = readers.stream().mapToLong(SSTableReader::onDiskLength).sum();
        this.mergedPartitionsHistogram = new long[readers.size()];
        this.mergedRowsHistogram = new long[readers.size()];
        this.rowBuilder = BTreeRow.sortedBuilder();
        this.sstables = ImmutableSet.copyOf(readers);
        this.cursor = makeMergedAndPurgedCursor(readers, controller, limiter, nowInSec);
        this.totalBytes = cursor.bytesTotal();
        this.currentBytes = 0;
        this.currentProgressMillisSinceStartup = System.currentTimeMillis();
    }

    private SSTableCursor makeMergedAndPurgedCursor(Collection<SSTableReader> readers,
                                                    CompactionController controller,
                                                    RateLimiter limiter,
                                                    int nowInSec)
    {
        if (readers.isEmpty())
            return SSTableCursor.empty();

        SSTableCursor merged = new SSTableCursorMerger(readers.stream()
                                                              .map(r -> new SortedStringTableCursor(r, limiter))
                                                              .collect(Collectors.toList()),
                                                       metadata(),
                                                       this);

        if (Iterables.any(readers, SSTableReader::mayHaveTombstones))
        {
            merged = new PurgeCursor(merged, controller, nowInSec);
            merged = new SkipEmptyDataCursor(merged);
        }
        return merged;
    }

    public SSTableCursor.Type copyOne(SSTableDataSink writer) throws IOException
    {
        boolean wasInitialized = true;
        if (cursor.type() == SSTableCursor.Type.UNINITIALIZED)
        {
            cursor.advance();
            wasInitialized = false;
        }

        switch (cursor.type())
        {
            case ROW:
                Row row = collectRow();
                if (!row.isEmpty())
                    writer.addUnfiltered(row);
                return SSTableCursor.Type.ROW;
            case RANGE_TOMBSTONE:
                writer.addUnfiltered(collectRangeTombstoneMarker());
                return SSTableCursor.Type.RANGE_TOMBSTONE;
            case PARTITION:
                if (wasInitialized)
                    writer.endPartition();
                maybeUpdateProgress();
                // The writer can reject a partition (e.g. due to long key). Loop until it accepts one.
                while (!writer.startPartition(cursor.partitionKey(), cursor.partitionLevelDeletion()))
                {
                    if (!skipToNextPartition())
                        return SSTableCursor.Type.EXHAUSTED;
                }
                cursor.advance();
                return SSTableCursor.Type.PARTITION;
            case EXHAUSTED:
                if (wasInitialized)
                    writer.endPartition();
                updateProgress(Long.MAX_VALUE);
                return SSTableCursor.Type.EXHAUSTED;
            default:
                throw new AssertionError();
        }
    }

    private void maybeUpdateProgress()
    {
        long now = System.currentTimeMillis();
        if (now - currentProgressMillisSinceStartup > MILLISECONDS_TO_UPDATE_PROGRESS)
            updateProgress(now);
    }

    private void updateProgress(long now)
    {
        currentBytes = cursor.bytesProcessed();
        currentProgressMillisSinceStartup = now;
    }

    private Row collectRow()
    {
        return IteratorFromCursor.collectRow(cursor, rowBuilder);
    }

    private Unfiltered collectRangeTombstoneMarker()
    {
        return IteratorFromCursor.collectRangeTombstoneMarker(cursor);
    }

    private boolean skipToNextPartition()
    {
        while (true)
        {
            switch (cursor.advance())
            {
                case EXHAUSTED:
                    return false;
                case PARTITION:
                    return true;
                default:
                    break;  // continue loop
            }
        }
    }

    /**
     * @return A {@link TableOperation} backed by this iterator. This operation can be observed for progress
     * and for interrupting provided that it is registered with a {@link TableOperationObserver}, normally the
     * metrics in the compaction manager. The caller is responsible for registering the operation and checking
     * {@link TableOperation#isStopRequested()}.
     */
    public TableOperation createOperation()
    {
        return new AbstractTableOperation() {

            @Override
            public OperationProgress getProgress()
            {
                return new OperationProgress(controller.realm.metadata(),
                                             type,
                                             bytesRead(),
                                             totalBytes(),
                                             compactionId,
                                             sstables);
            }

            @Override
            public boolean isGlobal()
            {
                return false;
            }
        };
    }

    public TableMetadata metadata()
    {
        return controller.realm.metadata();
    }

    long bytesRead()
    {
        // Note: This may be called from other threads. Reading the current positions in the sources is not safe as
        // random access readers aren't thread-safe. To avoid problems we track the progress in the processing thread
        // and store it in a volatile field.
        return currentBytes;
    }

    long totalBytes()
    {
        return totalBytes;
    }

    long totalSourcePartitions()
    {
        return Arrays.stream(mergedPartitionsHistogram).reduce(0L, Long::sum);
    }

    long totalSourceRows()
    {
        return Arrays.stream(mergedRowsHistogram).reduce(0L, Long::sum);
    }

    public long getTotalCompressedSize()
    {
        return totalCompressedSize;
    }

    long[] mergedPartitionsHistogram()
    {
        return mergedPartitionsHistogram;
    }

    long[] mergedRowsHistogram()
    {
        return mergedRowsHistogram;
    }

    public void onItem(SSTableCursor cursor, int numVersions)
    {
        switch (cursor.type())
        {
            case PARTITION:
                mergedPartitionsHistogram[numVersions - 1] += 1;
                break;
            case ROW:
                mergedRowsHistogram[numVersions - 1] += 1;
                break;
            default:
                break;
        }
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    public void close()
    {
        cursor.close();
    }

    public String toString()
    {
        return String.format("%s: %s, (%d/%d)", type, metadata(), bytesRead(), totalBytes());
    }
}
