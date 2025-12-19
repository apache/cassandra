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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.function.LongPredicate;

import com.google.common.collect.ImmutableSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Config.DiskAccessMode;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.AbstractCompactionController;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionPurger;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.DeletionTime.ReusableDeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.ReusableLivenessInfo;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Cells;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.db.rows.UnfilteredSerializer;
import org.apache.cassandra.dht.ReusableDecoratedKey;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.PartitionDescriptor;
import org.apache.cassandra.io.sstable.SSTableCursorReader;
import org.apache.cassandra.io.sstable.SSTableCursorWriter;
import org.apache.cassandra.io.sstable.UnfilteredDescriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.format.SortedTableWriter;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.db.ClusteringPrefix.Kind.EXCL_END_BOUND;
import static org.apache.cassandra.db.ClusteringPrefix.Kind.EXCL_END_INCL_START_BOUNDARY;
import static org.apache.cassandra.db.ClusteringPrefix.Kind.EXCL_START_BOUND;
import static org.apache.cassandra.db.ClusteringPrefix.Kind.INCL_END_BOUND;
import static org.apache.cassandra.db.ClusteringPrefix.Kind.INCL_END_EXCL_START_BOUNDARY;
import static org.apache.cassandra.db.ClusteringPrefix.Kind.INCL_START_BOUND;
import static org.apache.cassandra.db.compaction.CursorCompactor.CellResolution.COMPARE;
import static org.apache.cassandra.db.compaction.CursorCompactor.CellResolution.LEFT;
import static org.apache.cassandra.db.compaction.CursorCompactor.CellResolution.RIGHT;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.CELL_END;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.CELL_HEADER_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.CELL_VALUE_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.DONE;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.PARTITION_END;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.PARTITION_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.ROW_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.STATIC_ROW_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.TOMBSTONE_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.UNFILTERED_END;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.isState;

/**
 * Compacts the contents of 1..n sstables into a 1..m sstables. The compaction is driven one output partition at a time
 * by the {@link CursorCompactionPipeline}.
 * <p>
 * Compaction here implies:
 * <ul>
 *   <li>Merge source sstable data, such that only latest live values, or tombstones, are present in the output.</li>
 *   <li>Purge gc-able tombstones if possible (see PurgeFunction below).</li>
 *   <li>Invalidate cached partitions that are empty post-compaction. This avoids keeping partitions with
 *       only purgable tombstones in the row cache.</li>
 *   <li>Keep tracks of the compaction progress.</li>
 * </ul>
 * This compaction implementation does not support 2ndary indexes or trie indexes at this time.
 * <p>
*     This compaction implmentation avoids garbage creation per partition/row/cell by utilizing reader/writer code
*     which supports reusable copies of sstable entry components. The implementation consolidates and duplicates code
 *    from various classes to support the use of these reusable structures.
 * </p>
 */
public class CursorCompactor extends CompactionInfo.Holder
{
    public static boolean isSupported(AbstractCompactionStrategy.ScannerList scanners, AbstractCompactionController controller)
    {
        TableMetadata metadata = controller.cfs.metadata();
        if (unsupportedMetadata(metadata)) return false;

        for (ISSTableScanner scanner : scanners.scanners)
        {
            // TODO: implement partial range reader
            if (!scanner.isFullRange())
            {
                if (LOGGER.isDebugEnabled()) logDebugReason(metadata, "Partial scanners are not supported.");
                return false;
            }

            for (SSTableReader reader : scanner.getBackingSSTables()) {
                Version version = reader.descriptor.version;
                if (!version.isLatestVersion())
                {
                    if (LOGGER.isDebugEnabled()) logDebugReason(metadata, "Older sstable versions are not supported. version=" + version);
                    return false;
                }
            }
        }
        // BTI index writing is not supported yet
        if (!(DatabaseDescriptor.getSelectedSSTableFormat() instanceof BigFormat))
        {
            if (LOGGER.isDebugEnabled()) logDebugReason(metadata, "Only BIG sstable output format is supported. format=" + DatabaseDescriptor.getSelectedSSTableFormat());
            return false;
        }
        // TODO: Implement CompactionIterator.GarbageSkipper like functionality
        if (controller.tombstoneOption != CompactionParams.TombstoneOption.NONE)
        {
            if (LOGGER.isDebugEnabled()) logDebugReason(metadata, "Garbage skipping not implemented. controller.tombstoneOption=" + controller.tombstoneOption);
            return false;
        }
        if (LOGGER.isDebugEnabled()) LOGGER.debug("Cursor compaction for table: " + metadata.name + " keyspace: " + metadata.keyspace + " is supported.");

        return true;
    }

    public static boolean unsupportedMetadata(TableMetadata metadata)
    {
        if (metadata.keyspace.equals(SchemaConstants.ACCORD_KEYSPACE_NAME))
            return true;

        if (!metadata.partitioner.supportsReusableKeys())
        {
            if (LOGGER.isDebugEnabled()) logDebugReason(metadata, "Incompatible partitioner, does not support reusable keys:" + metadata.partitioner.getClass().getSimpleName());
            return true;
        }

        if (metadata.indexes.size() != 0)
        {
            if (LOGGER.isDebugEnabled()) logDebugReason(metadata, "Additional indexes are not supported. metadata.indexes=" + metadata.indexes);
            return true;
        }

        if (unsupportedSchema(metadata))
            return true;
        return false;
    }

    private static boolean unsupportedSchema(TableMetadata metadata)
    {
        // Cell value merge limitations
        for (ColumnMetadata column : metadata.regularAndStaticColumns())
        {
            if (column.isComplex())
            {
                if (LOGGER.isDebugEnabled()) logDebugReason(metadata, "Complex columns are not supported. column=" + column);
                return true;
            }
            else if (column.isCounterColumn())
            {
                if (LOGGER.isDebugEnabled()) logDebugReason(metadata, "Counter columns are not supported. column=" + column);
                return true;
            }
        }
        return false;
    }

    private static void logDebugReason(TableMetadata metadata, String reason)
    {
        LOGGER.debug("Cursor compaction for table: " + metadata.name + " keyspace: " + metadata.keyspace + " is not supported. REASON: " + reason);
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(CursorCompactor.class.getName());

    private final OperationType type;
    private final AbstractCompactionController controller;
    private final ActiveCompactionsTracker activeCompactions;
    private final ImmutableSet<SSTableReader> sstables;
    private final long nowInSec;
    private final TimeUUID compactionId;
    private final long totalInputBytes;
    private final StatefulCursor[] sstableCursors;
    private final boolean[] sstableCursorsEqualsNext;
    private final boolean hasStaticColumns;
    private final boolean enforceStrictLiveness;

    // Keep targetDirectory for compactions, needed for `nodetool compactionstats`
    private volatile String targetDirectory;

    private SSTableCursorWriter ssTableCursorWriter;
    private boolean finished = false;

    /*
     * counters for merged partitions/rows/cells.
     * array index represents (number of merged rows - 1), so index 0 is counter for no merge (1 row),
     * index 1 is counter for 2 rows merged, and so on.
     */
    private final long[] partitionMergeCounters;
    private final long[] staticRowMergeCounters;
    private final long[] rowMergeCounters;
    private final long[] rangeTombstonesMergeCounters;
    private final long[] cellMergeCounters;

    // Progress accounting
    private long totalBytesRead = 0;
    private long totalSourceCQLRows;
    private long totalDataBytesWritten;

    // state
    final Purger purger;

    private StatefulCursor lastSource = null;
    private final ReusableDecoratedKey detachedLastWrittenKey;

    // Partition state. Writes can be delayed if the deletion is purged, or live and partition is empty -> LIVE deletion.
    PartitionDescriptor partitionDescriptor;

    // This will be 0 if we haven't written partition header.
    int partitionHeaderLength = 0;
    private CompactionAwareWriter compactionAwareWriter;

    public CursorCompactor(OperationType type, List<ISSTableScanner> scanners, AbstractCompactionController controller, long nowInSec, TimeUUID compactionId)
    {
        this(type, scanners, controller, nowInSec, compactionId, ActiveCompactionsTracker.NOOP);
    }

    private CursorCompactor(OperationType type,
                           List<ISSTableScanner> scanners,
                           AbstractCompactionController controller,
                           long nowInSec,
                           TimeUUID compactionId,
                           ActiveCompactionsTracker activeCompactions)
    {
        this.controller = controller;
        this.type = type;
        this.nowInSec = nowInSec;
        this.compactionId = compactionId;

        long inputBytes = 0;
        for (ISSTableScanner scanner : scanners)
            inputBytes += scanner.getLengthInBytes();
        this.totalInputBytes = inputBytes;
        this.partitionMergeCounters = new long[scanners.size()];
        this.staticRowMergeCounters = new long[partitionMergeCounters.length];
        this.rowMergeCounters = new long[partitionMergeCounters.length];
        this.rangeTombstonesMergeCounters = new long[partitionMergeCounters.length];
        this.cellMergeCounters = new long[partitionMergeCounters.length];
        // note that we leak `this` from the constructor when calling beginCompaction below, this means we have to get the sstables before
        // calling that to avoid a NPE.
        this.sstables = scanners.stream().map(ISSTableScanner::getBackingSSTables).flatMap(Collection::stream).collect(ImmutableSet.toImmutableSet());
        // This is always NOOP, but keep it around in case we need it later to match CompactionIterator
        this.activeCompactions = activeCompactions == null ? ActiveCompactionsTracker.NOOP : activeCompactions;
        this.activeCompactions.beginCompaction(this); // note that CompactionTask also calls this, but CT only creates CompactionIterator with a NOOP ActiveCompactions

        TableMetadata metadata = metadata();
        this.hasStaticColumns = metadata.hasStaticColumns();
        /**
         * Pipeline should end up similar to the one in {@link CompactionIterator}:
         * [MERGED -> ?TopPartitionTracker -> GarbageSkipper -> Purger -> org.apache.cassandra.db.transform.DuplicateRowChecker -> Abortable] -> next()
         * V - Merge - This is drawing on code all over the place to iterate through the data and merge partitions/rows/cells
         * * {@link org.apache.cassandra.db.transform.Transformation}s, applied to above iterator:
         *   X - Not needed for CompactionTask usage: {@link org.apache.cassandra.metrics.TopPartitionTracker.TombstoneCounter}
         *   X - Unsupported {@link CompactionIterator.GarbageSkipper} - filters out, or "skips" data shadowed by the provided "tombstone source".
         *   V - {@link CompactionIterator.Purger} - filters out, or "purges" gc-able tombstones. Also updates bytes read on every row % 100.
         *   X - Not needed for latest version tables: {@link org.apache.cassandra.db.transform.DuplicateRowChecker}
         *   V - Abortable - aborts the compaction if the user has requested it (at a certain granularity).
         * {@link CompactionIterator#CompactionIterator(OperationType, List, AbstractCompactionController, long, TimeUUID, ActiveCompactionsTracker)}
         */

        this.sstableCursors = convertScannersToCursors(scanners, sstables, DatabaseDescriptor.getCompactionReadDiskAccessMode());
        this.sstableCursorsEqualsNext = new boolean[sstables.size()];
        this.enforceStrictLiveness = controller.cfs.metadata.get().enforceStrictLiveness();

        purger = new Purger(type, controller, nowInSec);

        detachedLastWrittenKey = metadata.partitioner.createReusableKey(128);
    }

    /**
     * @return false if finished, true if partition is written (which might require multiple partition reads)
     */
    public boolean writeNextPartition(CompactionAwareWriter compactionAwareWriter) throws IOException {
        while (!finished) {
            if (tryWriteNextPartition(compactionAwareWriter)) {
                return true;
            }
        }
        return false;
    }

    /**
     * @return true if a partition was written
     */
    private boolean tryWriteNextPartition(CompactionAwareWriter compactionAwareWriter) throws IOException
    {
        if (isStopRequested())
            throw new CompactionInterruptedException(getCompactionInfo());

        int partitionMergeLimit = prepareAndSortForPartitionMerge();
        if (partitionMergeLimit == 0)
        {
            finish();
            return false;
        }
        // Top reader is on the current key/header
        StatefulCursor currSource = sstableCursors[0];
        partitionDescriptor = currSource.currPartition();

        // possibly reached boundary of the current writer
        try
        {
            DecoratedKey key = partitionDescriptor.key();
            if (lastSource != null && currSource != lastSource && lastWrittenKey().compareTo(key) >= 0)
                throw new IllegalStateException(String.format("Last written key %s >= current key %s", lastWrittenKey(), key));

            // needed if we actually write a partition, not used otherwise
            this.compactionAwareWriter = compactionAwareWriter;

            purger.resetOnNewPartition(key);
            boolean written = mergePartitions(partitionMergeLimit);
            if (!written)
            {
                purger.onEmptyPartitionPostPurge();
                // skipping a partition means the prevPartition on the lastSource is no longer stable, copy it
                if (detachedLastWrittenKey.getKeyLength() == 0 && lastSource != null)
                    detachedLastWrittenKey.copyKey(lastSource.prevKey().getKey());
            }
            else
            {
                // last source has will have the prev key recorded, reset the copy
                if (detachedLastWrittenKey.getKeyLength() != 0)
                    detachedLastWrittenKey.reset();
            }
            return written;
        }
        finally
        {
            lastSource = currSource;
            partitionDescriptor = null;
            partitionHeaderLength = 0;
        }
    }

    /**
     * See {@link UnfilteredPartitionIterators#merge(List, UnfilteredPartitionIterators.MergeListener)}
     */
    private boolean mergePartitions(int partitionMergeLimit) throws IOException
    {
        partitionMergeCounters[partitionMergeLimit - 1]++;

        // Pick "max" pDeletion
        /** {@link UnfilteredRowIterators.UnfilteredRowMergeIterator#collectPartitionLevelDeletion(List, UnfilteredRowIterators.MergeListener)}*/
        final DeletionTime mergedDeletion = mergePartitionDeletions(partitionMergeLimit);

        // maybe purge? If the partition is written out, this will be the deletion we write.
        final DeletionTime toWritePartitionDeletion = maybePurgedOutputDeletion(mergedDeletion);
        if (toWritePartitionDeletion != DeletionTime.LIVE) {
            startPartition(toWritePartitionDeletion);
        }
        // active deletion tracks the open deletion within a partition, so will change to track range tombstones
        DeletionTime activeDeletion = mergedDeletion;

        // Merge any common static rows
        if (hasStaticColumns)
        {
            int staticRowMergeLimit = prepareAndSortStaticForMerge(partitionMergeLimit);
            if (staticRowMergeLimit != 0)
            {
                mergeRows(staticRowMergeLimit, activeDeletion, true, false);
            }
            if (isPartitionStarted())
            {
                if (staticRowMergeLimit == 0) ssTableCursorWriter.writeEmptyStaticRow();
                partitionHeaderLength = (int) (ssTableCursorWriter.getPosition() - ssTableCursorWriter.getPartitionStart());
            }
        }

        // Merge any common normal rows
        int unfilteredMergeLimit = partitionMergeLimit;
        boolean isFirstUnfiltered = true;
        int unfilteredCount = 0;
        UnfilteredDescriptor lastClustering = null;
        while (true)
        {
            unfilteredMergeLimit = prepareAndSortUnfilteredForMerge(partitionMergeLimit, unfilteredMergeLimit);
            if (unfilteredMergeLimit == 0)
                break;
            int flags = sstableCursors[0].unfiltered().flags();
            if (UnfilteredSerializer.isRow(flags))
            {
                if (mergeRows(unfilteredMergeLimit, activeDeletion, false, isFirstUnfiltered))
                {
                    isFirstUnfiltered = false;
                    unfilteredCount++;
                    lastClustering = sstableCursors[0].unfiltered();
                }
            }
            else if (UnfilteredSerializer.isTombstoneMarker(flags)) {
                // the tombstone processing *maybe* writes a marker, and *maybe* changes the `activeOpenRangeDeletion`
                if (mergeRangeTombstones(unfilteredMergeLimit, mergedDeletion, isFirstUnfiltered))
                {
                    isFirstUnfiltered = false;
                    unfilteredCount++;
                    lastClustering = sstableCursors[0].unfiltered();
                }
                if (activeOpenRangeDeletion == DeletionTime.LIVE) {
                    activeDeletion = mergedDeletion;
                }
                else {
                    activeDeletion = activeOpenRangeDeletion;
                }
            }
            else {
                throw new IllegalStateException("Unexpected unfiltered type (not row or tombstone):" + flags);
            }
            // move along
            continueReadingAfterMerge(unfilteredMergeLimit, UNFILTERED_END);
        }

        boolean partitionWritten = isPartitionStarted();
        if (partitionWritten)
        {
            ssTableCursorWriter.writePartitionEnd(partitionDescriptor.keyBytes(), partitionDescriptor.keyLength(), toWritePartitionDeletion, partitionHeaderLength);
            // update metadata tracking of min/max clustering on last unfiltered
            if (unfilteredCount > 1) {
                ssTableCursorWriter.updateClusteringMetadata(lastClustering);
            }
        }
        // move along
        continueReadingAfterMerge(partitionMergeLimit, PARTITION_END);
        return partitionWritten;
    }

    private void startPartition(DeletionTime toWritePartitionDeletion) throws IOException
    {
        maybeSwitchWriter(compactionAwareWriter);
        partitionHeaderLength = ssTableCursorWriter.writePartitionStart(
                                    partitionDescriptor.keyBytes(),
                                    partitionDescriptor.keyLength(),
                                    toWritePartitionDeletion);
    }

    private DeletionTime maybePurgedOutputDeletion(DeletionTime mergedDeletion) throws IOException
    {
        final DeletionTime toWritePartitionDeletion;

        if (!mergedDeletion.isLive() && !purger.shouldPurge(mergedDeletion))
        {
            toWritePartitionDeletion = mergedDeletion;
        }
        else
        {
            toWritePartitionDeletion = DeletionTime.LIVE;
        }
        return toWritePartitionDeletion;
    }

    private DeletionTime mergePartitionDeletions(int partitionMergeLimit)
    {
        DeletionTime mergedDeletion = partitionDescriptor.deletionTime();
        for (int i = 1; i < partitionMergeLimit; i++)
        {
            DeletionTime otherDeletionTime = sstableCursors[i].currPartition().deletionTime();
            if (!mergedDeletion.supersedes(otherDeletionTime))
                mergedDeletion = otherDeletionTime;
        }
        return mergedDeletion;
    }

    /**
     * We have a common clustering and need to merge data. Cells might be different in different rows, but collision is
     * likely at this stage (probably).
     * {@link Row.Merger#merge(DeletionTime)}
     */
    private boolean mergeRows(int rowMergeLimit, DeletionTime partitionActiveDeletion, boolean isStatic, boolean isFirstUnfiltered) throws IOException
    {
        if (isStopRequested())
            throw new CompactionInterruptedException(getCompactionInfo());

        if (isStatic)
        {
            staticRowMergeCounters[rowMergeLimit - 1]++;
        }
        else
        {
            rowMergeCounters[rowMergeLimit - 1]++;
        }

        // merge deletion/liveness
        /** {@link Row.Merger#merge(DeletionTime)}*/
        UnfilteredDescriptor row = sstableCursors[0].unfiltered();

        LivenessInfo mergedRowInfo = row.livenessInfo();
        DeletionTime mergedRowDeletion = row.deletionTime();

        for (int i = 1; i < rowMergeLimit; i++)
        {
            // TODO: can validate state here
            row = sstableCursors[i].unfiltered();
            // TODO: maybe flags more optimal(avoid ref loads and comaparisons etc)
            if (row.livenessInfo().supersedes(mergedRowInfo))
                mergedRowInfo = row.livenessInfo();
            if (row.deletionTime().supersedes(mergedRowDeletion))
                mergedRowDeletion = row.deletionTime();
        }

        /**
         * See: {@link BTreeRow#purge(DeletionPurger, long, boolean)}
         */
        DeletionTime rowActiveDeletion = partitionActiveDeletion;
        if (mergedRowDeletion.supersedes(rowActiveDeletion))
        {
            rowActiveDeletion = mergedRowDeletion; // deletion is in effect before purge takes effect
            mergedRowDeletion = purger.shouldPurge(mergedRowDeletion) ? DeletionTime.LIVE : mergedRowDeletion;
        }
        else
        {
            // partition delete takes over
            mergedRowDeletion = DeletionTime.LIVE;
        }

        if (rowActiveDeletion.deletes(mergedRowInfo) || purger.shouldPurge(mergedRowInfo, nowInSec))
        {
            mergedRowInfo = LivenessInfo.EMPTY;
        }

        boolean isRowDropped = mergedRowDeletion.isLive() && mergedRowInfo.isEmpty();

        if (!isRowDropped)
        {
            lateStartRow(mergedRowInfo, mergedRowDeletion, isStatic);
        }

        if (isRowDropped && enforceStrictLiveness)
        {
            skipRowsOnStrictLiveness(rowMergeLimit, isStatic);
        }
        else
        {
            int cellMergeLimit = rowMergeLimit;
            // loop through the columns and copy/merge each cell
            while (true)
            {
                // advance cursors that need to read the cell header
                for (int i = 0; i < cellMergeLimit; i++)
                {
                    int readerState = sstableCursors[i].state();
                    if (readerState == CELL_HEADER_START)
                    {
                        sstableCursors[i].readCellHeader();
                    }
                }
                // Sort rows by cells
                cellMergeLimit = prepareAndSortCellsForMerge(rowMergeLimit, cellMergeLimit);
                if (cellMergeLimit == 0)
                    break;
                isRowDropped = mergeCells(cellMergeLimit, rowActiveDeletion, mergedRowInfo, isRowDropped, isStatic);
                // move along
                continueReadingAfterMerge(cellMergeLimit, CELL_END);
            }
            if (!isRowDropped)
                ssTableCursorWriter.writeRowEnd(sstableCursors[0].unfiltered(), isFirstUnfiltered);
        }
        if (isRowDropped && isStatic &&
            isPartitionStarted())
            // if the partition write has not started, keep delaying it, might be an empty partition (purged+no data)
        {
            ssTableCursorWriter.writeEmptyStaticRow();
        }
        return !isRowDropped;
    }

    private void skipRowsOnStrictLiveness(int rowMergeLimit, boolean isStatic) throws IOException
    {
        for (int i = 0; i < rowMergeLimit; i++)
        {
            if (sstableCursors[i].state() != UNFILTERED_END){
                if (isStatic)
                    sstableCursors[i].skipStaticRow();
                else
                    sstableCursors[i].skipUnfiltered();
            }
        }
    }

    private DataOutputBuffer tempCellBuffer1 = new DataOutputBuffer();
    private DataOutputBuffer tempCellBuffer2 = new DataOutputBuffer();
    private final byte[] copyColumnValueBuffer = new byte[4096]; // used to copy cell contents (maybe piecemeal if very large, since we don't have a direct read option)

    /**
     * {@link Row.Merger.ColumnDataReducer#getReduced()} <-- applied the delete before reconcile, should not make a difference?
     * {@link Cells#reconcile(Cell, Cell)}
     */
    private boolean mergeCells(int cellMergeLimit, DeletionTime activeDeletion, LivenessInfo rowLiveness, boolean isRowDropped, boolean isStatic) throws IOException
    {
        cellMergeCounters[cellMergeLimit - 1]++;
        // Nothing to sort, we basically need to pick the correct data to copy.
        // -> the latest data.
        // TODO: handle counters/complex cells
        StatefulCursor cellSource = sstableCursors[0];
        SSTableCursorReader.CellCursor cellCursor = cellSource.cellCursor();
        ReusableLivenessInfo cellLiveness = cellCursor.cellLiveness;
        DataOutputBuffer tempCellBuffer = null;

        if (cellCursor.cellColumn.isComplex())
            throw new UnsupportedOperationException("TODO: Not ready for complex cells.");
        if (cellCursor.cellColumn.isCounterColumn())
            throw new UnsupportedOperationException("TODO: Not ready for counter cells.");

        /** See: {@link Cells#reconcile(Cell, Cell)} */
        // Find latest cell value/delete info, only one cell can win(for now... same timestamp handling awaits)!
        for (int i = 1; i < cellMergeLimit; i++)
        {
            StatefulCursor oCellSource = sstableCursors[i];
            SSTableCursorReader.CellCursor oCellCursor = oCellSource.cellCursor();
            ReusableLivenessInfo oCellLiveness = oCellCursor.cellLiveness;

            CellResolution cellResolution = resolveRegular(cellLiveness, oCellLiveness);
            if (cellResolution == LEFT) {
                if (oCellSource.state() == CELL_VALUE_START) oCellSource.skipCellValue();
            }
            else if (cellResolution == RIGHT) {
                if (cellSource.state() == CELL_VALUE_START) cellSource.skipCellValue();
                cellSource = oCellSource;
                cellCursor = oCellCursor;
                cellLiveness = oCellLiveness;
                tempCellBuffer = null;
            }
            else { // COMPARE
                if (activeDeletion.deletes(oCellLiveness)) {
                    if (oCellSource.state() == CELL_VALUE_START) oCellSource.skipCellValue();
                }
                else {
                    // copy out the values for comparison
                    if (cellSource.state() == CELL_VALUE_START)
                    {
                        if (tempCellBuffer != null)
                            throw new IllegalStateException("tempCellBuffer should be null if cellSource has a value to be read.");
                        tempCellBuffer1.clear();

                        cellSource.copyCellValue(tempCellBuffer1, copyColumnValueBuffer);

                        tempCellBuffer = tempCellBuffer1; // assume cell1 is going to be bigger
                    }
                    else if (tempCellBuffer == null) {
                        // potential trash value in buffer1
                        tempCellBuffer1.clear();
                    }
                    else if (tempCellBuffer != tempCellBuffer1) {
                        throw new IllegalStateException("tempCellBuffer should be tempCellBuffer1 if cellSource has been read.");
                    }
                    tempCellBuffer2.clear();
                    if (oCellSource.state() == CELL_VALUE_START)
                        oCellSource.copyCellValue(tempCellBuffer2, copyColumnValueBuffer);

                    int compare = Arrays.compareUnsigned(tempCellBuffer1.getData(), 0, tempCellBuffer1.getLength(), tempCellBuffer2.getData(), 0, tempCellBuffer2.getLength());
                    if (compare >= 0) {
                        // swap the buffers
                        tempCellBuffer = tempCellBuffer1;
                        tempCellBuffer1 = tempCellBuffer2;
                        tempCellBuffer2 = tempCellBuffer;

                        // tempCellBuffer != null -> tempCellBuffer == tempCellBuffer1
                        tempCellBuffer = tempCellBuffer1;

                        cellSource = oCellSource;
                        cellCursor = oCellCursor;
                        cellLiveness = oCellLiveness;
                    }
                }
            }
        }


        /**
         * {@link Cell.Serializer#serialize}
         */
        int cellFlags = cellCursor.cellFlags;

        /** {@link org.apache.cassandra.db.rows.AbstractCell#purge(org.apache.cassandra.db.DeletionPurger, long)} */
        // if `isExpiring` => has ttl, and TTL has lapsed, convert the TTL to a tombstone
        if (Cell.Serializer.isExpiring(cellFlags) && cellLiveness.isExpired(nowInSec)) {
            cellLiveness.ttlToTombstone();
            // remove the value, this is a tombstone now
            if (Cell.Serializer.hasValue(cellFlags))
            {
                cellFlags = cellFlags | Cell.Serializer.HAS_EMPTY_VALUE_MASK;
                if (cellSource.state() == CELL_VALUE_START)
                {
                    if (tempCellBuffer != null) throw new IllegalStateException("Either copied buffer or ready to copy reader, not both.");
                    cellSource.skipCellValue();
                }
                else if (tempCellBuffer != null) {
                    tempCellBuffer = null;
                }
                else
                {
                    throw new IllegalStateException("Flags and state contradict");
                }
            }
        }

        if (activeDeletion.deletes(cellLiveness) || purger.shouldPurge(cellLiveness, nowInSec))
        {
            if (Cell.Serializer.hasValue(cellFlags))
            {
                // we're dropping the cell, but could do: cellFlags = cellFlags | Cell.Serializer.HAS_EMPTY_VALUE_MASK;
                if (cellSource.state() == CELL_VALUE_START)
                {
                    if (tempCellBuffer != null) throw new IllegalStateException("Either copied buffer or ready to copy reader, not both.");
                    cellSource.skipCellValue();
                }
                else if (tempCellBuffer != null) {
                    // we're dropping the cell, but could do: tempCellBuffer = null;
                }
                else
                {
                    throw new IllegalStateException("Flags and state contradict");
                }
            }
        }
        else
        {
            if (isRowDropped)
            {
                isRowDropped = false;
                lateStartRow(isStatic);
            }
            /** {@link org.apache.cassandra.db.rows.Cell.Serializer#serialize(Cell, ColumnMetadata, DataOutputPlus, LivenessInfo, SerializationHeader)} */
            boolean isDeleted = cellLiveness.isTombstone();
            boolean isExpiring = cellLiveness.isExpiring();
            boolean useRowTimestamp = !rowLiveness.isEmpty() && cellLiveness.timestamp() == rowLiveness.timestamp();
            boolean useRowTTL = isExpiring && rowLiveness.isExpiring() &&
                                cellLiveness.ttl() == rowLiveness.ttl() &&
                                cellLiveness.localExpirationTime() == rowLiveness.localExpirationTime();
            // Re-write cell flags to reflect resulting contents
            cellFlags &= Cell.Serializer.HAS_EMPTY_VALUE_MASK;
            if (isDeleted) cellFlags |= Cell.Serializer.IS_DELETED_MASK;
            if (isExpiring) cellFlags |= Cell.Serializer.IS_EXPIRING_MASK;
            if (useRowTimestamp) cellFlags |= Cell.Serializer.USE_ROW_TIMESTAMP_MASK;
            if (useRowTTL) cellFlags |= Cell.Serializer.USE_ROW_TTL_MASK;
            ssTableCursorWriter.writeCellHeader(cellFlags, cellLiveness, cellSource.cellCursor().cellColumn);
            if (Cell.Serializer.hasValue(cellFlags)) {
                if (cellSource.state() == CELL_VALUE_START)
                {
                    if (tempCellBuffer != null) throw new IllegalStateException("Either copied buffer or ready to copy reader, not both.");
                    ssTableCursorWriter.writeCellValue(cellSource, copyColumnValueBuffer);
                }
                else if (tempCellBuffer != null)
                {
                    ssTableCursorWriter.writeCellValue(tempCellBuffer);
                }
                else
                {
                    throw new IllegalStateException("Flags and state contradict");
                }
            }

        }
        return isRowDropped;
    }

    enum CellResolution
    {
        LEFT, RIGHT, COMPARE
    }

    private static CellResolution resolveRegular(LivenessInfo left, LivenessInfo right)
    {
        long leftTimestamp = left.timestamp();
        long rightTimestamp = right.timestamp();
        if (leftTimestamp != rightTimestamp)
            return leftTimestamp > rightTimestamp ? LEFT : RIGHT;

        long leftLocalDeletionTime = left.localExpirationTime();
        long rightLocalDeletionTime = right.localExpirationTime();

        boolean leftIsExpiringOrTombstone = leftLocalDeletionTime != Cell.NO_DELETION_TIME;
        boolean rightIsExpiringOrTombstone = rightLocalDeletionTime != Cell.NO_DELETION_TIME;

        if (leftIsExpiringOrTombstone | rightIsExpiringOrTombstone)
        {
            // Tombstones always win reconciliation with live cells of the same timstamp
            // CASSANDRA-14592: for consistency of reconciliation, regardless of system clock at time of reconciliation
            // this requires us to treat expiring cells (which will become tombstones at some future date) the same wrt regular cells
            if (leftIsExpiringOrTombstone != rightIsExpiringOrTombstone)
                return leftIsExpiringOrTombstone ? LEFT : RIGHT;

            // for most historical consistency, we still prefer tombstones over expiring cells.
            // While this leads to an inconsistency over which is chosen
            // (i.e. before expiry, the pure tombstone; after expiry, whichever is more recent)
            // this inconsistency has no user-visible distinction, as at this point they are both logically tombstones
            // (the only possible difference is the time at which the cells become purgeable)
            boolean leftIsTombstone = !left.isExpiring(); // !isExpiring() == isTombstone(), but does not need to consider localDeletionTime()
            boolean rightIsTombstone = !right.isExpiring();
            if (leftIsTombstone != rightIsTombstone)
                return leftIsTombstone ? LEFT : RIGHT;

            // ==> (leftIsExpiring && rightIsExpiring) or (leftIsTombstone && rightIsTombstone)
            // if both are expiring, we do not want to consult the value bytes if we can avoid it, as like with C-14592
            // the value bytes implicitly depend on the system time at reconciliation, as a
            // would otherwise always win (unless it had an empty value), until it expired and was translated to a tombstone
            if (leftLocalDeletionTime != rightLocalDeletionTime)
                return leftLocalDeletionTime > rightLocalDeletionTime ? LEFT : RIGHT;
        }
        return COMPARE;
    }

    DeletionTime activeOpenRangeDeletion = DeletionTime.LIVE;
    final List<ReusableDeletionTime> openMarkers = new ArrayList<>();
    final ArrayDeque<ReusableDeletionTime> reusableMarkersPool = new ArrayDeque<>();

    /**
     * We have a common clustering and need to merge tombstones. Alternatively, we have a series of range tombstones
     * whose intersections mutate from bounds into boundary (a combination of 2 bounds). We also need to purge any GC'ed
     * deletes.
     *
     * {@link RangeTombstoneMarker.Merger#merge()}
     *
     * @return true if written, false otherwise
     */
    private boolean mergeRangeTombstones(int rangeTombstoneMergeLimit, DeletionTime partitionDeletion, boolean isFirstUnfiltered) throws IOException
    {
        if (rangeTombstoneMergeLimit == 0)
        {
            throw new IllegalStateException();
        }
        rangeTombstonesMergeCounters[rangeTombstoneMergeLimit - 1]++;
        DeletionTime previousDeletionTimeInMerged = DeletionTime.LIVE;
        if (activeOpenRangeDeletion != DeletionTime.LIVE) {
            previousDeletionTimeInMerged = getDeletionTimeReusableCopy(activeOpenRangeDeletion);
        }
        try
        {
            updateOpenMarkers(rangeTombstoneMergeLimit, partitionDeletion);

            DeletionTime newDeletionTimeInMerged = activeOpenRangeDeletion;
            if (previousDeletionTimeInMerged.equals(newDeletionTimeInMerged))
                return false;

            // we will stomp on the unfiltered descriptor and write it out
            UnfilteredDescriptor rangeTombstone = sstableCursors[0].unfiltered();
            boolean isBeforeClustering = rangeTombstone.clusteringKind().comparedToClustering < 0;

            // Combining the merge and purge code
            if (previousDeletionTimeInMerged == DeletionTime.LIVE)
            {
                if (purger.shouldPurge(newDeletionTimeInMerged))
                {
                    return false;
                }
                else
                {
                    rangeTombstone.clusteringKind(isBeforeClustering ? INCL_START_BOUND : EXCL_START_BOUND);
                    rangeTombstone.deletionTime().reset(newDeletionTimeInMerged);
                }
            }
            else if (newDeletionTimeInMerged == DeletionTime.LIVE)
            {
                if (purger.shouldPurge(previousDeletionTimeInMerged))
                {
                    return false;
                }
                else
                {
                    rangeTombstone.clusteringKind(isBeforeClustering ? EXCL_END_BOUND : INCL_END_BOUND);
                    rangeTombstone.deletionTime().reset(previousDeletionTimeInMerged);
                }
            }
            else
            {
                boolean shouldPurgeClose = purger.shouldPurge(previousDeletionTimeInMerged);
                boolean shouldPurgeOpen = purger.shouldPurge(newDeletionTimeInMerged);

                if (shouldPurgeClose && shouldPurgeOpen)
                    return false;

                if (shouldPurgeClose)
                {
                    rangeTombstone.clusteringKind(isBeforeClustering ? INCL_START_BOUND : EXCL_START_BOUND);
                    rangeTombstone.deletionTime().reset(newDeletionTimeInMerged);
                }
                else if (shouldPurgeOpen)
                {
                    rangeTombstone.clusteringKind(isBeforeClustering ? EXCL_END_BOUND : INCL_END_BOUND);
                    rangeTombstone.deletionTime().reset(previousDeletionTimeInMerged);
                }
                else {
                    // Boundary
                    rangeTombstone.clusteringKind(isBeforeClustering ? EXCL_END_INCL_START_BOUNDARY : INCL_END_EXCL_START_BOUNDARY);
                    rangeTombstone.deletionTime().reset(previousDeletionTimeInMerged); // close
                    rangeTombstone.deletionTime2().reset(newDeletionTimeInMerged); // open
                }
            }

            if (isPartitionStartDelayed())
            {
                lateStartPartition(false);
                ssTableCursorWriter.writeRangeTombstone(rangeTombstone, true);
            }
            else {
                ssTableCursorWriter.writeRangeTombstone(rangeTombstone, isFirstUnfiltered);
            }
            return true;
        }
        finally
        {
            if (previousDeletionTimeInMerged != DeletionTime.LIVE)
            {
                reusableMarkersPool.offer((ReusableDeletionTime) previousDeletionTimeInMerged);
            }
        }
    }

    private void updateOpenMarkers(int rangeTombstoneMergeLimit, DeletionTime partitionDeletion)
    {
        /** Similar to {@link RangeTombstoneMarker.Merger#updateOpenMarkers()} but we validate a close exists for every open.*/
        for (int i = 0; i < rangeTombstoneMergeLimit; i++)
        {
            UnfilteredDescriptor rangeTombstone = sstableCursors[i].unfiltered();
            if (rangeTombstone.isStartBound())
            {
                DeletionTime openRangeDeletion = rangeTombstone.deletionTime();
                addOpenRangeDeletion(partitionDeletion, openRangeDeletion);
            }
            else if (rangeTombstone.isEndBound())
            {
                DeletionTime closeRangeDeletion = rangeTombstone.deletionTime();
                removeOpenRangeDeletion(partitionDeletion, closeRangeDeletion, rangeTombstone);
            }
            else if (rangeTombstone.isBoundary())
            {
                DeletionTime closeRangeDeletion = rangeTombstone.deletionTime();
                removeOpenRangeDeletion(partitionDeletion, closeRangeDeletion, rangeTombstone);
                DeletionTime openRangeDeletion = rangeTombstone.deletionTime2();
                addOpenRangeDeletion(partitionDeletion, openRangeDeletion);
            }
            else
                throw new IllegalStateException("Unexpected bound type:" + rangeTombstone.clusteringKind());
        }

        if (activeOpenRangeDeletion == null)
        {
            recalculateActiveOpen();
        }
    }

    private void recalculateActiveOpen()
    {
        // active open has been invalidated by a close bound matching it, need to scan the list for new max
        int size = openMarkers.size();
        if (size == 0)
        {
            activeOpenRangeDeletion = DeletionTime.LIVE;
            return;
        }
        // find max open marker
        DeletionTime maxOpenDeletion = openMarkers.get(0);
        for (int i = 1; i < size; i++)
        {
            DeletionTime openDeletionTime = openMarkers.get(i);
            if (openDeletionTime.supersedes(maxOpenDeletion))
                maxOpenDeletion = openDeletionTime;
        }
        activeOpenRangeDeletion = maxOpenDeletion;
    }

    private void removeOpenRangeDeletion(DeletionTime partitionDeletion, DeletionTime closeRangeDeletion, UnfilteredDescriptor rangeTombstone)
    {
        // filter out markers that are deleted by the `partitionDelete`
        if (partitionDeletion != DeletionTime.LIVE && !closeRangeDeletion.supersedes(partitionDeletion))
        {
            return;
        }
        // a close marker should have a matching open in the list
        int j = 0;
        int size = openMarkers.size();
        ReusableDeletionTime reusableOpenMarker = null;
        for (; j < size;j++) {
            reusableOpenMarker = openMarkers.get(j);
            if (reusableOpenMarker.equals(closeRangeDeletion))
                break;
        }
        if (j == size)
            throw new IllegalStateException("Expected an open marker for this closing marker:" + rangeTombstone);

        reusableMarkersPool.offer(reusableOpenMarker);
        if (activeOpenRangeDeletion == reusableOpenMarker) {
            // trigger recalculation
            activeOpenRangeDeletion = null;
        }
        if (size == 1) {
            openMarkers.clear();
        }
        else {
            // avoid expensive array copy, take the last element
            ReusableDeletionTime deletionTime = openMarkers.remove(size - 1);
            if (j != size - 1)
            {
                // overwrite the matched marker (if it was not the last one)
                openMarkers.set(j, deletionTime);
            }
        }
    }

    private void addOpenRangeDeletion(DeletionTime partitionDeletion, DeletionTime openRangeDeletion)
    {
        // filter out markers that are deleted by the `partitionDelete`
        if (partitionDeletion != DeletionTime.LIVE && !openRangeDeletion.supersedes(partitionDeletion))
        {
            return;
        }

        ReusableDeletionTime reusable = getDeletionTimeReusableCopy(openRangeDeletion);
        openMarkers.add(reusable);
        if (activeOpenRangeDeletion != null && // invalidated by remove, so full scan is required
            (activeOpenRangeDeletion == DeletionTime.LIVE || reusable.supersedes(activeOpenRangeDeletion))) {
            activeOpenRangeDeletion = reusable;
        }
    }

    private ReusableDeletionTime getDeletionTimeReusableCopy(DeletionTime openRangeDeletion)
    {
        ReusableDeletionTime reusable = reusableMarkersPool.pollLast();
        if (reusable == null) {
            reusable = ReusableDeletionTime.copy(openRangeDeletion);
        }
        else {
            reusable.reset(openRangeDeletion);
        }
        return reusable;
    }

    private boolean isPartitionStarted()
    {
        return partitionHeaderLength != 0;
    }

    private boolean isPartitionStartDelayed()
    {
        return !isPartitionStarted();
    }

    private void continueReadingAfterMerge(int mergeLimit, int endState)
    {
        for (int i = 0; i < mergeLimit; i++)
        {
            if (sstableCursors[i].state() == endState){
                sstableCursors[i].continueReading();
            }
        }
    }

    private void lateStartRow(boolean isStatic) throws IOException
    {
        lateStartRow(LivenessInfo.EMPTY, DeletionTime.LIVE, isStatic);
    }

    private void lateStartRow(LivenessInfo livenessInfo, DeletionTime deletionTime, boolean isStatic) throws IOException
    {
        if (isPartitionStartDelayed())
        {
            lateStartPartition(isStatic);
        }
        ssTableCursorWriter.writeRowStart(livenessInfo, deletionTime, isStatic);
    }

    private void lateStartPartition(boolean isStatic) throws IOException
    {
        startPartition(DeletionTime.LIVE);
        // Did we miss writing an empty static row?
        if (!isStatic)
        {
            if(ssTableCursorWriter.writeEmptyStaticRow())
                partitionHeaderLength = (int) (ssTableCursorWriter.getPosition() - ssTableCursorWriter.getPartitionStart());
        }
    }

    private void finish()
    {
        // only finish writing once
        if (!finished)
        {
            finished = true;
            writerRollover();
        }
    }

    private void maybeSwitchWriter(CompactionAwareWriter writerProvider)
    {
        assert !finished;
        // Set last key, so this is ready to be closed.
        SSTableWriter newWriter = writerProvider.maybeSwitchWriter(partitionDescriptor.key());
        if (newWriter != null)
        {
            writerRollover();

            ssTableCursorWriter = new SSTableCursorWriter((SortedTableWriter) newWriter);
            ssTableCursorWriter.setFirst(partitionDescriptor.keyBuffer());
        }
        assert ssTableCursorWriter != null;
    }

    private void writerRollover()
    {
        if (ssTableCursorWriter != null) {
            totalDataBytesWritten += ssTableCursorWriter.getPosition();
            ssTableCursorWriter.setLast(lastWrittenKey().getKey());
        }
        ssTableCursorWriter = null;
    }

    private DecoratedKey lastWrittenKey()
    {
        if (detachedLastWrittenKey.getKeyLength() != 0)
            return detachedLastWrittenKey;
        else
            return lastSource.prevKey();
    }

    // SORT AND COMPARE

    /**
     * Prepares the cursors array for partition merge.
     * <p>
     * The cursors are in one of 3 states:
     * <ul>
     *     <li>PARTITION_START - Partition header needs to be loaded in preparation for merge. This is the starting state of all cursors.</li>
     *     <li>STATIC_ROW_START | ROW_START | TOMBSTONE_START | PARTITION_END - header is loaded. Already sorted.</li>
     *     <li>DONE - Exhausted cursors. This is the end state of all cursors.</li>
     * </ul>
     * After each `mergePartitions` iteration, the recently progressed cursors are at the beginning of the array and are
     * either at a new PARTITION_START or DONE.
     * We prepare all the cursors in the PARTITION_START state for sorting by loading the key and delete time. We also
     * need to push all the DONE cursors to the back of the list.
     *
     * Once the bounds of the sorting are known we insert sort the freshly read/done cursors into the pre-sorted
     * remaining array. After the sort we find the next merge limit, which is to say how many of the top partition keys
     * are equal.
     *
     * @return the next merge limit, or 0 if all cursors are DONE
     */
    private int prepareAndSortForPartitionMerge() throws IOException
    {
        // start by loading in new partition keys from any readers for which we just merged partitions => are
        // on partition edge. Exhausted cursors are at the bottom. Mid-read partitions are in the middle.
        int perturbedCursors = 0;
        for (; perturbedCursors < sstableCursors.length; perturbedCursors++)
        {
            StatefulCursor sstableCursor = sstableCursors[perturbedCursors];
            int sstableCursorState = sstableCursor.state();

            if (sstableCursorState == PARTITION_START)
            {
                sstableCursor.readPartitionHeader();
                updateTotalBytesRead(sstableCursor);
            }
            else if (isState(sstableCursorState, STATIC_ROW_START | ROW_START | TOMBSTONE_START | PARTITION_END))
            {
                // The cursors after this point are sorted, and unmoved
                break;
            }
            else if (sstableCursorState == DONE)
            {
                if (sstableCursor.resetAfterDone())
                {
                    updateTotalBytesRead(sstableCursor);
                }
                else
                {
                    break;
                }
            }
            else
            {
                throw new IllegalStateException("Cursor is in an unexpected state:" + sstableCursor);
            }
        }
        // no cursors were moved => all done
        if (perturbedCursors == 0)
        {
            assert sstableCursors.length == 0 || sstableCursors[0].state() == DONE;
            return 0;
        }

        sortPerturbedCursors(perturbedCursors, sstableCursors.length, CursorCompactor::compareByPartitionKey);
        // top cursor is DONE -> all cursors are DONE
        int state = sstableCursors[0].state();
        if(state == DONE)
        {
            return 0;
        }
        assert isState(state, STATIC_ROW_START | ROW_START | TOMBSTONE_START | PARTITION_END);

        int partitionMergeLimit = 1;
        for (; partitionMergeLimit < sstableCursors.length; partitionMergeLimit++)
        {
            if (!sstableCursorsEqualsNext[partitionMergeLimit-1])
                break;
        }
        return partitionMergeLimit;
    }


    private int prepareAndSortUnfilteredForMerge(int partitionMergeLimit, int prevMergeLimit) throws IOException
    {
        // move cursors that need to move passed the row header
        for (int i = 0; i < prevMergeLimit; i++)
        {
            StatefulCursor sstableCursor = sstableCursors[i];
            int readerState = sstableCursor.state();
            if (readerState == ROW_START)
            {
                totalSourceCQLRows++;
                sstableCursor.readRowHeader();
            }
            if (readerState == TOMBSTONE_START)
            {
                sstableCursor.readTombstoneMarker();
            }
            if (readerState == STATIC_ROW_START)
                throw new IllegalStateException("Unexpected static row after static row merge:" + sstableCursor);
        }

        // Sort rows by their clustering
        sortPerturbedCursors(prevMergeLimit, partitionMergeLimit, CursorCompactor::compareByRowClustering);
        int state = sstableCursors[0].state();
        if (state == PARTITION_END)
        {
            return 0;
        }
        assert isState(state, UNFILTERED_END | CELL_HEADER_START);
        int unfilteredMergeLimit = 1;
        for (; unfilteredMergeLimit < partitionMergeLimit; unfilteredMergeLimit++)
        {
            if (!sstableCursorsEqualsNext[unfilteredMergeLimit-1])
                break;
        }
        return unfilteredMergeLimit;
    }

    private int prepareAndSortStaticForMerge(int partitionMergeLimit) throws IOException
    {
        sortPerturbedCursors(partitionMergeLimit, partitionMergeLimit, CursorCompactor::compareByStatic);
        int state = sstableCursors[0].state();
        if (state != STATIC_ROW_START)
        {
            assert isState(state, ROW_START|TOMBSTONE_START|PARTITION_END);
            return 0;
        }
        totalSourceCQLRows++;
        sstableCursors[0].readStaticRowHeader();
        int staticRowMergeLimit = 1;
        for (; staticRowMergeLimit < partitionMergeLimit; staticRowMergeLimit++)
        {
            if (sstableCursorsEqualsNext[staticRowMergeLimit - 1])
            {
                totalSourceCQLRows++;
                sstableCursors[staticRowMergeLimit].readStaticRowHeader();
            }
            else
                break;
        }

        return staticRowMergeLimit;
    }

    private int prepareAndSortCellsForMerge(int rowMergeLimit, int prevCellMergeLimit)
    {
        sortPerturbedCursors(prevCellMergeLimit, rowMergeLimit, CursorCompactor::compareByColumn);
        // next row/partition/done
        if (sstableCursors[0].state() == UNFILTERED_END)
            return 0;

        int state = sstableCursors[0].state();
        if (isState(state, UNFILTERED_END | CELL_HEADER_START))
            return 0;

        int cellMergeLimit = 1;
        for (; cellMergeLimit < rowMergeLimit; cellMergeLimit++)
        {
            if (!sstableCursorsEqualsNext[cellMergeLimit - 1])
                break;
        }
        return cellMergeLimit;
    }

    private static int compareByPartitionKey(StatefulCursor c1, StatefulCursor c2)
    {
        if (c1 == c2) return 0;
        int tint = c1.state();
        int oint = c2.state();
        if (tint == DONE && oint == DONE) return 0;
        if (tint == DONE) return 1;
        if (oint == DONE) return -1;
        return c1.currentKey().compareTo(c2.currentKey());
    }

    private static int compareByStatic(StatefulCursor c1, StatefulCursor c2)
    {
        if (c1 == c2) return 0;
        int tState = c1.state();
        int oState = c2.state();

        if (tState == PARTITION_END && oState == PARTITION_END) return 0;
        if (tState == PARTITION_END) return 1;
        if (oState == PARTITION_END) return -1;

        return -Boolean.compare(tState == STATIC_ROW_START, oState == STATIC_ROW_START);
    }

    private static int compareByRowClustering(StatefulCursor c1, StatefulCursor c2)
    {
        if (c1 == c2) return 0;
        int tState = c1.state();
        int oState = c2.state();

        if (tState == PARTITION_END && oState == PARTITION_END) return 0;
        if (tState == PARTITION_END) return 1;
        if (oState == PARTITION_END) return -1;
        // Either have cells, or an empty row
        boolean tIsAfterHeader = isState(tState, CELL_HEADER_START | UNFILTERED_END);
        boolean oIsAfterHeader = isState(oState, CELL_HEADER_START | UNFILTERED_END);
        if (tIsAfterHeader && oIsAfterHeader)
            return ClusteringComparator.compare(c1.unfiltered(), c2.unfiltered());
        else
            throw new IllegalStateException("We only sort through rows ready to be merged/copied. c1 = " + c1 + ", c2 = " + c2);
    }

    private static int compareByColumn(StatefulCursor c1, StatefulCursor c2)
    {
        if (c1 == c2) return 0;
        int tState = c1.state();
        int oState = c2.state();
        if (tState == UNFILTERED_END && oState == UNFILTERED_END) return 0;
        if (tState == UNFILTERED_END) return 1;
        if (oState == UNFILTERED_END) return -1;

        boolean tIsAfterHeader = isState(tState, CELL_VALUE_START | CELL_END);
        boolean oIsAfterHeader = isState(oState, CELL_VALUE_START | CELL_END);
        if (tIsAfterHeader && oIsAfterHeader)
            return c1.cellCursor().cellColumn.compareTo(c2.cellCursor().cellColumn);
        else
            throw new IllegalStateException("We only sort through cells ready to be merged/copied. c1 = " + c1 + ", c2 = " + c2);
    }

    // Purge

    /**
     * We are combining code from:
     * - {@link org.apache.cassandra.db.compaction.CompactionIterator.Purger}
     * - {@link org.apache.cassandra.db.partitions.PurgeFunction}
     * - {@link DeletionPurger}
     * The original code leans on the {@link org.apache.cassandra.db.transform.Transformation} abstraction and the
     * iterator infrastructure which is not fit for purpose here.
     */
    static class Purger implements DeletionPurger
    {
        private final long nowInSec;

        private final long oldestUnrepairedTombstone;
        private final boolean onlyPurgeRepairedTombstones;
        private final boolean shouldIgnoreGcGraceForAnyKey;
        private final OperationType type;

        private boolean ignoreGcGraceSeconds;
        private final AbstractCompactionController controller;

        private DecoratedKey partitionKey;
        private LongPredicate purgeEvaluator;

        private long compactedUnfiltered;

        Purger(OperationType type, AbstractCompactionController controller, long nowInSec)
        {
            oldestUnrepairedTombstone = controller.compactingRepaired() ? Long.MAX_VALUE : Integer.MIN_VALUE;
            onlyPurgeRepairedTombstones = controller.cfs.getCompactionStrategyManager().onlyPurgeRepairedTombstones();
            shouldIgnoreGcGraceForAnyKey = controller.cfs.shouldIgnoreGcGraceForAnyKey();
            this.nowInSec = nowInSec;
            this.controller = controller;
            this.type = type;
        }

        void resetOnNewPartition(DecoratedKey key)
        {
            partitionKey = key;
            purgeEvaluator = null;
            ignoreGcGraceSeconds = shouldIgnoreGcGraceForAnyKey && controller.cfs.shouldIgnoreGcGraceForKey(partitionKey);
        }

        void onEmptyPartitionPostPurge()
        {
            if (type == OperationType.COMPACTION)
                controller.cfs.invalidateCachedPartition(partitionKey);
        }

        @Override
        public boolean shouldPurge(long timestamp, long localDeletionTime)
        {
            return !(onlyPurgeRepairedTombstones && localDeletionTime >= oldestUnrepairedTombstone)
                   && (localDeletionTime < controller.gcBefore || ignoreGcGraceSeconds)
                   && getPurgeEvaluator().test(timestamp);
        }

        /*
         * Evaluates whether a tombstone with the given deletion timestamp can be purged. This is the minimum
         * timestamp for any sstable containing `currentKey` outside of the set of sstables involved in this compaction.
         * This is computed lazily on demand as we only need this if there is tombstones and this a bit expensive
         * (see #8914).
         */
        private LongPredicate getPurgeEvaluator()
        {
            if (purgeEvaluator == null)
            {
                purgeEvaluator = controller.getPurgeEvaluator(partitionKey);
            }
            return purgeEvaluator;
        }
    }

    // ACCOUNTING CODE
    public TableMetadata metadata()
    {
        return controller.cfs.metadata();
    }

    public CompactionInfo getCompactionInfo()
    {
        return new CompactionInfo(controller.cfs.metadata(),
                                  type,
                                  getBytesRead(),
                                  totalInputBytes,
                                  compactionId,
                                  sstables,
                                  targetDirectory);
    }

    public boolean isGlobal()
    {
        return false;
    }

    public void setTargetDirectory(final String targetDirectory)
    {
        this.targetDirectory = targetDirectory;
    }

    public long[] getMergedParitionsCounts()
    {
        return partitionMergeCounters;
    }

    public long[] getMergedRowsCounts()
    {
        return rowMergeCounters;
    }

    public long[] getMergedCellsCounts()
    {
        return cellMergeCounters;
    }

    public long getTotalSourceCQLRows()
    {
        return totalSourceCQLRows;
    }

    public long getBytesRead()
    {
        return totalBytesRead;
    }

    private void updateTotalBytesRead(StatefulCursor cursor)
    {
        totalBytesRead += cursor.bytesReadSinceSnapshot();
    }

    public String toString()
    {
        return this.getCompactionInfo().toString();
    }

    public long getTotalBytesScanned()
    {
        return getBytesRead();
    }

    private static boolean isPaxos(ColumnFamilyStore cfs)
    {
        return cfs.name.equals(SystemKeyspace.PAXOS) && cfs.getKeyspaceName().equals(SchemaConstants.SYSTEM_KEYSPACE_NAME);
    }

    private long sumHistogram(long[] histogram)
    {
        long sum = 0;
        for (long count : histogram)
        {
            sum += count;
        }
        return sum;
    }

    private static String mergeHistogramToString(long[] histogram)
    {
        StringBuilder sb = new StringBuilder();
        long sum = 0;
        sb.append("[");
        for (int i = 0; i < histogram.length; i++)
        {
            if (histogram[i] != 0)
            {
                sb.append(i + 1).append(":").append(histogram[i]).append(", ");
                sum += (i + 1) * histogram[i];
            }
        }
        if (sb.length() > 1)
            sb.setLength(sb.length() - 1); //trim trailing comma
        sb.append("] = ").append(sum);
        return sb.toString();
    }

    /**
     * Closes scanner-opened readers before opening cursor-specific readers with the configured disk access mode.
     * In cursor compaction, scanners are only used for metadata; closing them avoids holding redundant file
     * descriptors and prevents conflicts when scan and non-scan readers for the same file share thread-local
     * buffer state on the same thread.
     */
    private static StatefulCursor[] convertScannersToCursors(List<ISSTableScanner> scanners, ImmutableSet<SSTableReader> sstables,
                                                             DiskAccessMode diskAccessMode)
    {
        for (ISSTableScanner scanner : scanners)
            scanner.close();

        StatefulCursor[] cursors = new StatefulCursor[sstables.size()];
        int i = 0;
        try
        {
            for (SSTableReader reader : sstables)
                cursors[i++] = new StatefulCursor(reader, diskAccessMode);
            return cursors;
        }
        catch (RuntimeException | Error e)
        {
            Throwables.closeNonNullAndAddSuppressed(e, cursors);
            throw e;
        }
    }

    public void close()
    {
        try
        {
            finish();

            for (SSTableCursorReader reader : sstableCursors)
            {
                reader.close();
            }
        }
        finally
        {
            activeCompactions.finishCompaction(this);
        }

        if (LOGGER.isInfoEnabled())
        {
            LOGGER.info("Compaction ended {}: { data bytes read = {}, data bytes written = {}, " +
                        " input (keys = {}, rows = {}, cells = {}), " +
                        " output (keys = {}, rows = {}, cells = {})}",
                        this.compactionId, getTotalBytesScanned(), totalDataBytesWritten,
                        mergeHistogramToString(partitionMergeCounters), mergeHistogramToString(rowMergeCounters), mergeHistogramToString(cellMergeCounters),
                        sumHistogram(partitionMergeCounters), sumHistogram(rowMergeCounters), sumHistogram(cellMergeCounters));
        }
    }

    private void sortPerturbedCursors(int perturbedLimit, int mergeLimit, Comparator<? super StatefulCursor> comparator) {
        for (; perturbedLimit > 0; perturbedLimit--) {
            bubbleInsertElementToPreSorted(sstableCursors, sstableCursorsEqualsNext, perturbedLimit, mergeLimit, comparator);
        }
    }

    /**
     * Use bubble sort to insert the <code>sortedFrom - 1</code> element into a pre-sorted array, and track element
     * equality to next element to help in finding merge ranges.
     * </p>
     * We use this method to sort the cursor array on 3 levels:
     * <ul>
     *     <li>Partition - insert sort the newly read partitions into the full list, comparing on pKey</li>
     *     <li>Unfiltered - insert sort the newly read rows into the sub-list of merging partitions, comparing on clustering</li>
     *     <li>Cell - insert sort the newly read cells into the sub-list of merging rows, comparing on column</li>
     * </ul>
     *
     * @param preSortedArray partially pre-sorted array of elements to be sorted in place
     * @param equalsNext tracking the equality between each element and the next in the sorted array
     * @param sortedFrom elements from <code>sortedFrom</code> are assumed sorted
     * @param sortedTo the limit of our sort effort
     * @param comparator comparing elements in the array
     * @param <T> element type
     */
    public static <T> void bubbleInsertElementToPreSorted(T[] preSortedArray,
                                                          boolean[] equalsNext,
                                                          int sortedFrom,
                                                          int sortedTo,
                                                          Comparator<T> comparator)
    {
        int insertInto = sortedFrom - 1;
        T newElement = preSortedArray[insertInto];
        for (; insertInto < sortedTo - 1; insertInto++)
        {
            int cmp = comparator.compare(newElement, preSortedArray[insertInto + 1]);
            if (cmp < 0)
            {
                equalsNext[insertInto] = false;
                break;
            }
            else if (cmp == 0) {
                equalsNext[insertInto] = true;
                break;
            }
            else
            {
                // skip the section of equal elements who are smaller than the new element
                for (; insertInto < sortedTo - 1; insertInto++)
                {
                    if (!equalsNext[insertInto + 1])
                    {
                        break;
                    }
                    preSortedArray[insertInto] = preSortedArray[insertInto + 1];
                    equalsNext[insertInto] = true;
                }
                preSortedArray[insertInto] = preSortedArray[insertInto + 1];
                equalsNext[insertInto] = false;
            }
        }
        preSortedArray[insertInto] = newElement;
    }
}