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
import java.util.List;
import java.util.function.LongPredicate;

import com.google.common.collect.ImmutableSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Config.DiskAccessMode;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.AbstractCompactionController;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionPurger;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.DeletionTime.ReusableDeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellLivenessInfo;
import org.apache.cassandra.db.rows.CellLivenessInfo.Resolution;
import org.apache.cassandra.db.rows.Cells;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.ReusableCellLivenessInfo;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.db.rows.UnfilteredSerializer;
import org.apache.cassandra.io.sstable.ClusteringDescriptor;
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
import static org.apache.cassandra.db.rows.CellLivenessInfo.Resolution.LEFT;
import static org.apache.cassandra.db.rows.CellLivenessInfo.Resolution.RIGHT;
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
 *   <li>Keeps track of the compaction progress.</li>
 * </ul>
 * This compaction implementation does not support 2ndary indexes, trie (BTI) sstable output,
 * complex (collection/UDT) columns, or counter columns, and it stands aside for a compaction that
 * ignores gc grace for a key; see {@link #isSupported} and {@link #unsupportedMetadata} for the full
 * set of gates.
 * <p>
 *     This compaction implementation avoids garbage creation per partition/row/cell by utilizing reader/writer code
 *     which supports reusable copies of sstable entry components. The implementation consolidates and duplicates code
 *     from various classes to support the use of these reusable structures.
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
                if (unsupportedHeaderColumns(metadata, reader))
                    return false;
            }
        }
        // BTI index writing is not supported yet
        if (!(DatabaseDescriptor.getSelectedSSTableFormat() instanceof BigFormat))
        {
            if (LOGGER.isDebugEnabled()) logDebugReason(metadata, "Only the BIG sstable output format is supported. format=" + DatabaseDescriptor.getSelectedSSTableFormat());
            return false;
        }
        // TODO: Implement CompactionIterator.GarbageSkipper like functionality
        if (controller.tombstoneOption != CompactionParams.TombstoneOption.NONE)
        {
            if (LOGGER.isDebugEnabled()) logDebugReason(metadata, "Garbage skipping not implemented. controller.tombstoneOption=" + controller.tombstoneOption);
            return false;
        }
        // ColumnFamilyStore.forceCompactionKeysIgnoringGcGrace, whose shipped caller is nodetool
        // forcecompact, is the only thing that puts a key in this set, and so the only way a purge is
        // decided with localDeletionTime >= gcBefore (see Purger.shouldPurge). That is the case the cursor
        // cannot reproduce. BTreeRow.purge returns the row untouched whenever nowInSec is below the row's
        // minimum local deletion time, so the iterator settles row-level purging all-or-nothing over the
        // whole row before touching a cell, while a streaming cursor commits to the row's deletion and
        // liveness before it has walked the row's cells. Under an ordinary gcBefore that short-circuit
        // costs nothing, because such a row holds nothing purgeable either way.
        // The set is per-table and lives for the whole force compaction, so a background compaction that
        // starts inside that window falls back as well. Coarse, but the fallback is to the reference
        // implementation, so it costs throughput and not correctness.
        if (controller.cfs.shouldIgnoreGcGraceForAnyKey())
        {
            if (LOGGER.isDebugEnabled()) logDebugReason(metadata, "Ignoring gc_grace_seconds for a key is not supported (nodetool forcecompact).");
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

    /**
     * {@link #unsupportedSchema} only sees the columns the table has NOW. Dropping a column
     * removes it from {@link TableMetadata#regularAndStaticColumns()}, but every sstable written
     * before the drop still lists it in that sstable's own serialization header, with its original
     * type intact — {@code TableMetadata.recordColumnDrop} stores {@code type.expandUserTypes()},
     * so a dropped non-frozen collection is still a multi-cell column there. The reader builds its
     * column arrays from the header, not from the schema, so such a column reaches the cell cursor,
     * which reads a single cell where the complex framing
     * ({@code [complex deletion?][vint cellsCount][cells]}) requires a count-led run, and misparses
     * the row.
     *
     * Whether that misparse then surfaces at all is not something the reader can decide sensibly:
     * {@link #mergeCells} rejects complex columns outright, but reaching it depends on the
     * dropped-column filter, and the timestamp that filter compares against the drop horizon was
     * itself decoded from the misread framing rather than written by any cell. So the column is
     * discarded silently or rejected loudly according to bytes that mean nothing here — neither
     * outcome can be relied on, which is why the screening belongs at the gate.
     *
     * Dropping a non-frozen collection is legal and cursor compaction is on by default, so the
     * schema check alone lets that misparse through. Screen the columns the readers will actually
     * present. The fallback is permanent for this table: the ghost column stays in those headers
     * until the pre-drop sstables are rewritten by the iterator path.
     */
    private static boolean unsupportedHeaderColumns(TableMetadata metadata, SSTableReader reader)
    {
        // RegularAndStaticColumns iterates statics then regulars, so this covers both
        for (ColumnMetadata column : reader.header.columns())
        {
            // the header records its own type for any column whose type has since changed, so ask
            // it as well as the schema: that is the type the reader will decode against
            AbstractType<?> diskType = reader.header.getType(column);
            if (column.isComplex() || column.isCounterColumn()
                || (diskType != null && (diskType.isMultiCell() || diskType.isCounter())))
            {
                if (LOGGER.isDebugEnabled()) logDebugReason(metadata, "Complex and counter columns are not supported, and " + reader.descriptor + " still carries one in its header (dropped from the schema?). column=" + column);
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
    private final long totalCompressedInputBytes;
    private final StatefulCursor[] sstableCursors;
    private final boolean[] sstableCursorsEqualsNext;
    private final boolean hasStaticColumns;
    private final boolean enforceStrictLiveness;

    /**
     * Scratch for {@link #anyMergedCellDeadAtNow}, which walks a row's cells and then puts the cursors
     * back: the cursor ORDER and the equals-next flags its sorts overwrite, and the per-cursor state it
     * needs to know which cursors to rewind. Null unless the table enforces strict liveness, so a
     * non-view compaction does not allocate them at all.
     */
    private final StatefulCursor[] probeCursorOrder;
    private final boolean[] probeEqualsNext;
    private final int[] probeCursorState;

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
    /**
     * The PartitionDescriptor instance holding the last WRITTEN partition's header, owned by the write side.
     * Obtained by swapping this field's previous contents into the writing cursor's prev slot, so the key is
     * never copied. Non-final: it IS the floater, and every steal exchanges it.
     */
    private PartitionDescriptor lastWrittenPartition;
    /**
     * The cursor whose partition was written most recently, and from which the steal is still owed. Cleared by
     * the steal, so a skipped partition leaves nothing to take and the held instance keeps describing the last
     * partition actually written.
     */
    private StatefulCursor lastWrittenPartitionSource = null;
    /** {@link StatefulCursor#partitionSwaps()} on that cursor at the moment the write happened. */
    private long lastWrittenPartitionSourceSwaps = 0;
    /**
     * The UnfilteredDescriptor instance holding the last unfiltered WRITTEN to the current output partition,
     * owned by the write side.
     *
     * sstableCursors[*].unfiltered() descriptors are reusable: a cursor overwrites its descriptor in place as
     * soon as it reads its next unfiltered — including unfiltereds that then merge away or purge and never
     * reach the output — so a reference borrowed during the merge loop can, by the time the partition closes,
     * describe a clustering that was never written. This instance is obtained instead by swapping the field's
     * previous contents into the writing cursor's slot, so the clustering is never copied. Non-final: it IS
     * the floater, and every steal exchanges it.
     */
    private UnfilteredDescriptor lastWrittenUnfiltered;
    /**
     * Unfiltereds written to the current output partition, reset once per partition in
     * {@link #mergePartitions}, immediately before the unfiltered loop rather than inside it.
     *
     * The zero case is load-bearing rather than an optimisation, because the instance above is
     * compaction-global: for a partition written from only a partition-level deletion and/or a static row it
     * still describes a PREVIOUS partition's unfiltered, so feeding it to the covered-clustering update would
     * widen this output's Statistics.db slice to a clustering no partition of it holds there. On the very
     * first partition it describes nothing at all — a fresh ClusteringDescriptor leaves its kind null, which
     * would NPE in MetadataCollector rather than widen anything.
     */
    private int unfilteredsWrittenToPartition = 0;

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
        // mirror CompactionIterator.purger(): accord-enabled (and accord-migrating) tables
        // purge and expire relative to gcBefore — derived from accord's durability bounds by
        // CompactionTask.getCompactionController — retaining data accord may still read at
        // earlier timestamps; every nowInSec use below is a purge/expiry decision
        TableMetadata tableMetadata = controller.cfs.metadata();
        this.nowInSec = tableMetadata.isAccordEnabled() || tableMetadata.migratingFromAccord()
                        ? controller.gcBefore
                        : nowInSec;
        this.compactionId = compactionId;

        long inputBytes = 0;
        long compressedInputBytes = 0;
        for (ISSTableScanner scanner : scanners)
        {
            inputBytes += scanner.getLengthInBytes();
            compressedInputBytes += scanner.getCompressedLengthInBytes();
        }
        this.totalInputBytes = inputBytes;
        this.totalCompressedInputBytes = compressedInputBytes;
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
        // the INPUT headers decide whether static rows can occur in this merge (and the output
        // header, SerializationHeader.make, is their union): after ALTER TABLE ... DROP of the
        // last static column, current metadata has no static columns but older sstables
        // legitimately still carry static rows
        boolean anyStaticColumns = false;
        for (SSTableReader sstable : this.sstables)
            anyStaticColumns |= sstable.header.hasStatic();
        this.hasStaticColumns = anyStaticColumns;
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
        this.probeCursorOrder = enforceStrictLiveness ? new StatefulCursor[sstableCursors.length] : null;
        this.probeEqualsNext = enforceStrictLiveness ? new boolean[sstableCursors.length] : null;
        this.probeCursorState = enforceStrictLiveness ? new int[sstableCursors.length] : null;

        purger = new Purger(type, controller);

        lastWrittenPartition = new PartitionDescriptor(metadata.partitioner.createReusableKey(0));
        lastWrittenUnfiltered = new UnfilteredDescriptor(metadata.comparator.subtypes().toArray(AbstractType[]::new));
        // The unfiltered steal hands one cursor's descriptor to another, and to the write side's instance built
        // from the table comparator above, so all of them must PARSE a clustering identically. A descriptor
        // parses with its OWN clusteringTypes and reads exactly two things from them, per column: whether the
        // value is fixed-length, and how long (SSTableCursorReader.readUnfilteredClustering). That is what the
        // assert covers.
        assert clusteringParsingAgrees() : "the cursors disagree on how to parse a clustering: " + metadata;
    }

    /** @see #lastWrittenUnfiltered */
    private boolean clusteringParsingAgrees()
    {
        AbstractType<?>[] writeSide = lastWrittenUnfiltered.clusteringTypes();
        for (StatefulCursor cursor : sstableCursors)
        {
            AbstractType<?>[] readSide = cursor.unfiltered().clusteringTypes();
            if (readSide.length != writeSide.length)
                return false;
            for (int i = 0; i < readSide.length; i++)
            {
                if (readSide[i].isValueLengthFixed() != writeSide[i].isValueLengthFixed())
                    return false;
                if (readSide[i].isValueLengthFixed()
                    && readSide[i].valueLengthIfFixed() != writeSide[i].valueLengthIfFixed())
                    return false;
            }
        }
        return true;
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
        // The round's slot advances have happened, so the steal owed by the last written partition is due. This
        // sits ahead of the finish() exit because that path consumes the value too, through writerRollover's
        // setLast; on it the advance came from resetAfterDone rather than from a read.
        takeOwedPartitionSteal();
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
            // The check begins once a partition has been written, because there is nothing to check against
            // before that: no output exists to be out of order with, and no writer is created until the first
            // write, so setLast is unreachable in that window.
            if (hasWrittenPartition() && lastSource != null && currSource != lastSource && lastWrittenKey().compareTo(key) >= 0)
                throw new IllegalStateException(String.format("Last written key %s >= current key %s", lastWrittenKey(), key));

            // needed if we actually write a partition, not used otherwise
            this.compactionAwareWriter = compactionAwareWriter;

            purger.resetOnNewPartition(key);
            boolean written = mergePartitions(partitionMergeLimit);
            if (!written)
            {
                purger.onEmptyPartitionPostPurge();
            }
            else
            {
                // the steal is owed by this cursor, and lands one round later — see detachPrevPartition
                lastWrittenPartitionSource = currSource;
                lastWrittenPartitionSourceSwaps = currSource.partitionSwaps();
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
                // No steal here, because this call's return value is not consumed -- but the consumed
                // static descriptor is still in cursor 0's slot when the unfiltered loop below opens, and
                // STATIC_CLUSTERING sorts ahead of every row, so that loop's first round calls mergeRows on
                // it again, inside the branch that DOES steal; see the assert there for why that round
                // writes nothing.
                mergeRows(staticRowMergeLimit, activeDeletion, true, false);
                // Without this, a cursor that had a static row is left at UNFILTERED_END with its
                // unfiltered() descriptor still showing STATIC_CLUSTERING -- which sorts ahead of every
                // real row -- so the normal-row loop below would sort it first and spuriously re-merge
                // the same, already-fully-consumed position as a phantom regular row. Harmless to
                // output (the phantom row has no new liveness/deletion, so it's dropped unwritten), but
                // it inflates rowMergeCounters once per static-row partition, feeding
                // system.compaction_history.rows_merged and the logged totalSourceRows.
                continueReadingAfterMerge(staticRowMergeLimit, UNFILTERED_END);
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
        unfilteredsWrittenToPartition = 0;
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
                    // The static descriptor reaches this loop — STATIC_CLUSTERING sorts ahead of every row —
                    // and must never be WRITTEN from it: its clustering has length 0, so it belongs in neither
                    // the covered-clustering slice nor an index block. What keeps it out is that the round
                    // writes nothing, and it writes nothing only because CQL gives a static row no
                    // primary-key liveness: UpdateStatement adds liveness inside updatesRegularRows(), and
                    // DeleteStatement's static branch emits cell tombstones only, so the row is dropped.
                    //
                    // That is a property of the write path, not of this one, which is why it is asserted.
                    // A static row that does carry liveness — PartitionUpdate.simpleBuilder's row() with no
                    // clustering values produces one — reaches the write and hands the covered-clustering
                    // max a STATIC_CLUSTERING, which fails in MetadataCollector.finalizeMetadata when the
                    // sstable is closed. That failure predates the detach; the assert only moves it to the
                    // point where the cause is still on the stack.
                    assert sstableCursors[0].unfiltered().clusteringKind() != ClusteringPrefix.Kind.STATIC_CLUSTERING
                         : "a static descriptor was written from the unfiltered loop";
                    isFirstUnfiltered = false;
                    unfilteredsWrittenToPartition++;
                    detachWrittenUnfiltered();
                }
            }
            else if (UnfilteredSerializer.isTombstoneMarker(flags)) {
                // the tombstone processing *maybe* writes a marker, and *maybe* changes the `activeOpenRangeDeletion`
                if (mergeRangeTombstones(unfilteredMergeLimit, mergedDeletion, isFirstUnfiltered))
                {
                    isFirstUnfiltered = false;
                    unfilteredsWrittenToPartition++;
                    detachWrittenUnfiltered();
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
            // The trailing index block's last name and the covered-clustering max are the same value, the
            // clustering of the last unfiltered written here; a partition that wrote none has no trailing
            // block to cut, hence null.
            ClusteringDescriptor lastName = unfilteredsWrittenToPartition > 0 ? lastWrittenClustering() : null;
            ssTableCursorWriter.writePartitionEnd(partitionDescriptor.keyBytes(), partitionDescriptor.keyLength(), toWritePartitionDeletion, partitionHeaderLength, lastName);
            // update metadata tracking of min/max clustering on last unfiltered. The count guard is
            // load-bearing rather than an optimisation — see unfilteredsWrittenToPartition.
            if (unfilteredsWrittenToPartition > 1) {
                ssTableCursorWriter.updateClusteringMetadata(lastName);
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
        // Row.Deletion.isShadowable(): deprecated (CASSANDRA-11500), reachable only on old
        // Materialized View data. Tracked alongside mergedRowDeletion because the shadowing step
        // below reads it, and because it has to survive to the output write attached to whichever
        // deletion wins.
        boolean mergedRowShadowable = row.isShadowableDeletion();

        for (int i = 1; i < rowMergeLimit; i++)
        {
            // TODO: can validate state here
            row = sstableCursors[i].unfiltered();
            // TODO: maybe flags more optimal(avoid ref loads and comaparisons etc)
            if (row.livenessInfo().supersedes(mergedRowInfo))
                mergedRowInfo = row.livenessInfo();
            if (row.deletionTime().supersedes(mergedRowDeletion))
            {
                mergedRowDeletion = row.deletionTime();
                mergedRowShadowable = row.isShadowableDeletion();
            }
        }

        /**
         * {@link Row.Deletion#isShadowedBy(LivenessInfo)}, applied where
         * {@link Row.Merger#merge(DeletionTime)} applies it: after the N-way merge and BEFORE the
         * active-deletion comparison below. A shadowable deletion yields to a primary-key liveness that
         * postdates it, so the row keeps the cells the deletion would otherwise shadow — and the output
         * carries neither the deletion nor its {@code HAS_SHADOWABLE_DELETION} extension flag.
         *
         * Safe against exposing a cell the deletion itself shadowed: {@code BTreeRow.Builder} drops such
         * cells at write time, so no on-disk row carries one for this step to uncover.
         */
        if (mergedRowShadowable && mergedRowInfo.timestamp() > mergedRowDeletion.markedForDeleteAt())
        {
            mergedRowDeletion = DeletionTime.LIVE;
            mergedRowShadowable = false; // a live deletion is never shadowable
        }

        /**
         * See: {@link BTreeRow#purge(DeletionPurger, long, boolean)}
         */
        DeletionTime rowActiveDeletion = partitionActiveDeletion;
        // Whether BTreeRow.purge's hasDeletion(nowInSec) guard would be open for reasons OTHER than the
        // row's cells; see the strict-liveness branch below for why only the purger's two clearances
        // count here.
        boolean rowHasDeletionAtNow = false;
        if (mergedRowDeletion.supersedes(rowActiveDeletion))
        {
            rowActiveDeletion = mergedRowDeletion; // deletion is in effect before purge takes effect
            if (purger.shouldPurge(mergedRowDeletion))
            {
                mergedRowDeletion = DeletionTime.LIVE;
                mergedRowShadowable = false; // a live deletion is never shadowable
                rowHasDeletionAtNow = true;
            }
        }
        else
        {
            // partition delete takes over
            mergedRowDeletion = DeletionTime.LIVE;
            mergedRowShadowable = false; // a live deletion is never shadowable
        }

        // Split from the single disjunction it reads as so the strict-liveness branch below can tell the
        // two clearances apart: the reference's minLocalDeletionTime is computed AFTER the active
        // deletion has emptied the liveness and BEFORE the purger runs, so only a purger clearance is
        // visible to it. Splitting is behaviour-neutral, the assignment of the EMPTY singleton over an
        // already-empty liveness included: an empty liveness carries NO_TIMESTAMP, which is
        // Long.MIN_VALUE, so DeletionTime.deletes reports every deletion as deleting it and the first
        // branch is always the one taken.
        if (rowActiveDeletion.deletes(mergedRowInfo))
        {
            mergedRowInfo = LivenessInfo.EMPTY;
        }
        else if (purger.shouldPurge(mergedRowInfo, nowInSec))
        {
            // A purged liveness contributed a reference term at or below nowInSec, so the guard is open.
            // Not because "not live at nowInSec" implies an expiration at or below it -- an
            // EXPIRED_LIVENESS_TTL move marker is not live at any nowInSec while its localExpirationTime
            // can be above it, and ReusableLivenessInfo.isLive answers on that flag. It is because
            // shouldPurge requires localDeletionTime < gcBefore, and gcBefore <= nowInSec on every path
            // that reaches here: gcBefore is now - gc_grace with gc_grace non-negative, and an
            // accord-enabled table takes nowInSec := gcBefore outright. The one escape from
            // localDeletionTime < gcBefore is ignoreGcGraceSeconds -- and isSupported refuses the table
            // while any key ignores gc grace, so that gate is load-bearing for this arm too, which is
            // why it is named here.
            rowHasDeletionAtNow |= !mergedRowInfo.isEmpty();
            mergedRowInfo = LivenessInfo.EMPTY;
        }

        boolean isRowDropped = mergedRowDeletion.isLive() && mergedRowInfo.isEmpty();

        if (!isRowDropped)
        {
            lateStartRow(mergedRowInfo, mergedRowDeletion, mergedRowShadowable, isStatic);
        }

        /**
         * Strict liveness ({@link org.apache.cassandra.schema.TableMetadata#enforceStrictLiveness}, true
         * only for a view whose primary key holds a base non-PK column) drops a row with no primary-key
         * liveness and no row deletion -- but {@link BTreeRow#purge} reaches that drop only past its
         * opening {@code if (!hasDeletion(nowInSec)) return this;}, i.e. only when
         * {@code nowInSec >= minLocalDeletionTime} of the MERGED row. Without that precondition the
         * cursor deleted rows the iterator returns untouched, cells included.
         *
         * {@code minLocalDeletionTime} is the minimum of three terms, all taken on the merged row as
         * {@link Row.Merger#merge} leaves it -- after the active deletion has emptied the liveness and
         * cleared the deletion it does not outrank, and before the purger runs:
         *
         *   liveness: {@code isExpiring() ? localExpirationTime() : Cell.MAX_DELETION_TIME}
         *   deletion: {@code isLive() ? Cell.MAX_DELETION_TIME : Long.MIN_VALUE}
         *   cells:    {@code Long.MIN_VALUE} for a tombstone, else the cell's local deletion time
         *
         * Reaching this branch at all means the post-purge deletion is live and the post-purge liveness
         * empty, which collapses the first two terms to the purger's own two clearances. If the purger
         * cleared the deletion, the pre-purge deletion was not live and the deletion term is
         * {@code Long.MIN_VALUE}. If it cleared the liveness, the liveness was not live at
         * {@code nowInSec}, which for a non-empty liveness means expiring and expired, so the liveness
         * term is at or below {@code nowInSec}. Either way the guard is open. Otherwise both terms are
         * {@code Cell.MAX_DELETION_TIME} and the cells decide it alone -- which is what
         * {@link #anyMergedCellDeadAtNow} answers, and why it is only consulted when neither clearance
         * fired.
         */
        if (isRowDropped && enforceStrictLiveness
            && (rowHasDeletionAtNow || anyMergedCellDeadAtNow(rowMergeLimit, rowActiveDeletion, isStatic)))
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

    /**
     * Whether the merged row carries any cell that is not live at {@code nowInSec} — a cell tombstone,
     * or an expiring cell already past its expiry. That is the cells' contribution to
     * {@link BTreeRow#purge}'s {@code hasDeletion(nowInSec)} guard: {@code Cell.minDeletionTime()} is
     * {@code Long.MIN_VALUE} for a tombstone and the local deletion time otherwise, and
     * {@code Cell.NO_DELETION_TIME} for a plain live cell, so "at or below {@code nowInSec}" and "not
     * live at {@code nowInSec}" name the same cells.
     *
     * Only the merge WINNER of each column counts, because only the winner reaches the reference's
     * merged row, and only if it survives the active deletion — which the reference applies before
     * reconciling. Taking the winner across all cells and then testing it against the active deletion is
     * the same answer: the winner carries the greatest timestamp, so if the active deletion deletes it,
     * it deletes every cell of that column and the column contributes nothing. The winner is decided by
     * {@link CellLivenessInfo#resolve} alone, and its {@code COMPARE} verdict never leaves the two sides
     * disagreeing about being live: it is returned either because both carry
     * {@code Cell.NO_DELETION_TIME}, which makes both live outright, or because they agree on timestamp,
     * deletion time and TTL. So the value comparison this skips cannot move the answer.
     *
     * <b>Walks the row's cells and rewinds every cursor it moved</b>, so the caller's own cell loop runs
     * against the state it would have seen had the probe not run — the cursor ORDER and the equals-next
     * flags included, which the cell sorts below overwrite; restoring both is defensive rather than
     * provably needed, since the real loop's first sort re-sorts the whole group anyway. Only reached
     * under {@code enforceStrictLiveness}, so no non-view compaction pays for the second walk or its
     * scratch. Decided as soon as one dead cell is found, so this is the one walk that always runs to
     * the end of the row.
     */
    private boolean anyMergedCellDeadAtNow(int rowMergeLimit, DeletionTime rowActiveDeletion, boolean isStatic)
    {
        System.arraycopy(sstableCursors, 0, probeCursorOrder, 0, rowMergeLimit);
        System.arraycopy(sstableCursorsEqualsNext, 0, probeEqualsNext, 0, rowMergeLimit);
        for (int i = 0; i < rowMergeLimit; i++)
            probeCursorState[i] = sstableCursors[i].state();

        boolean anyDead = false;
        int cellMergeLimit = rowMergeLimit;
        while (!anyDead)
        {
            for (int i = 0; i < cellMergeLimit; i++)
            {
                if (sstableCursors[i].state() == CELL_HEADER_START)
                    sstableCursors[i].readCellHeader();
            }
            cellMergeLimit = prepareAndSortCellsForMerge(rowMergeLimit, cellMergeLimit);
            if (cellMergeLimit == 0)
                break;

            ReusableCellLivenessInfo winner = sstableCursors[0].cellCursor().cellLiveness;
            for (int i = 1; i < cellMergeLimit; i++)
            {
                ReusableCellLivenessInfo challenger = sstableCursors[i].cellCursor().cellLiveness;
                if (CellLivenessInfo.resolve(winner, challenger) == RIGHT)
                    winner = challenger;
            }
            anyDead = !rowActiveDeletion.deletesCellAt(winner.timestamp()) && !winner.isLive(nowInSec);

            for (int i = 0; i < cellMergeLimit; i++)
            {
                if (sstableCursors[i].state() == CELL_VALUE_START)
                    sstableCursors[i].skipCellValue();
            }
            continueReadingAfterMerge(cellMergeLimit, CELL_END);
        }

        System.arraycopy(probeCursorOrder, 0, sstableCursors, 0, rowMergeLimit);
        System.arraycopy(probeEqualsNext, 0, sstableCursorsEqualsNext, 0, rowMergeLimit);
        for (int i = 0; i < rowMergeLimit; i++)
        {
            // Every member of the group is at CELL_HEADER_START if it has a cell in this row, else
            // UNFILTERED_END: the row-header loaders return only those two, and every member has been
            // through one. It is the sort ORDER that keeps other states out, not a throw -- both
            // comparators pass PARTITION_END and sort it to the back, so it either empties the group or
            // ends it before itself; and compareByStatic sorts STATIC_ROW_START ahead of every non-static
            // cursor, so a static group is all-static and prepareAndSortStaticForMerge has loaded every
            // member of it.
            // Only the first state needs rewinding, and only the first CAN be -- rewindRowCells restores
            // the walk to the row's FIRST cell, so a cursor recorded mid-row would have its earlier cells
            // re-presented to the merge. Asserted rather than tolerated for that reason. With assertions
            // off a mid-row cursor is left where the probe put it, and the cell-desync check catches that
            // only when the probe's walk had RUN OUT the row, which clears unfilteredEnd; on the
            // early-exit path -- the one a dropped row usually takes -- the second walk consumes to the
            // same declared end and the missing cells go unreported. That asymmetry is the reason the
            // assert is here rather than a tolerated branch.
            int recordedState = probeCursorState[i];
            assert recordedState == CELL_HEADER_START || recordedState == UNFILTERED_END
                 : "unexpected merge-group state before the strict-liveness probe: " + sstableCursors[i];
            if (recordedState == CELL_HEADER_START)
                sstableCursors[i].rewindRowCells(isStatic);
        }
        return anyDead;
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
        ReusableCellLivenessInfo cellLiveness = cellCursor.cellLiveness;
        DataOutputBuffer tempCellBuffer = null;

        if (!metadata().regularAndStaticColumns().contains(cellCursor.cellColumn))
        {
            for (int i = 0; i < cellMergeLimit; i++)
            {
                if (sstableCursors[i].state() == CELL_VALUE_START)
                    sstableCursors[i].skipCellValue();
            }
            return isRowDropped;
        }

        if (cellCursor.cellColumn.isComplex())
            throw new UnsupportedOperationException("TODO: Not ready for complex cells.");
        if (cellCursor.cellColumn.isCounterColumn())
            throw new UnsupportedOperationException("TODO: Not ready for counter cells.");

        /** See: {@link Cells#reconcile(Cell, Cell)} */
        // Find the winning cell across sources: CellLivenessInfo.resolve is the full decision the reference
        // path takes, including the same-timestamp tie-breaks (deleted/expiring beats live, greater deletion
        // time, lower TTL), and COMPARE is the value comparison it defers to us. No narrowing needed here,
        // unlike Cells.resolveRegular: ReusableCellLivenessInfo has no subclasses to pollute the profile.
        for (int i = 1; i < cellMergeLimit; i++)
        {
            StatefulCursor oCellSource = sstableCursors[i];
            SSTableCursorReader.CellCursor oCellCursor = oCellSource.cellCursor();
            ReusableCellLivenessInfo oCellLiveness = oCellCursor.cellLiveness;

            Resolution cellResolution = CellLivenessInfo.resolve(cellLiveness, oCellLiveness);
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
                if (activeDeletion.deletesCellAt(oCellLiveness.timestamp())) {
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

                    // Matches Cells.resolveRegular: left (the current winner) wins ties, the
                    // challenger only wins with a strictly greater value
                    // (compareValues(left, right) >= 0 ? left : right). The reference
                    // comparison is over the RAW value bytes (ValueAccessor.compare, plain
                    // unsigned lexicographic): these buffers hold the WIRE form, which for
                    // variable-length types carries a leading length vint — comparing that
                    // prefix orders by LENGTH first (the vint's leading byte encodes it) and
                    // picks the wrong winner for ties between different-length values.
                    // Fixed-length types carry no vint (and equal lengths).
                    int skip1 = 0, skip2 = 0;
                    if (cellCursor.cellType.valueLengthIfFixed() < 0)
                    {
                        skip1 = tempCellBuffer1.getLength() == 0 ? 0 : wireVintSize(tempCellBuffer1.getData()[0]);
                        skip2 = tempCellBuffer2.getLength() == 0 ? 0 : wireVintSize(tempCellBuffer2.getData()[0]);
                    }
                    int compare = Arrays.compareUnsigned(tempCellBuffer1.getData(), skip1, tempCellBuffer1.getLength(),
                                                         tempCellBuffer2.getData(), skip2, tempCellBuffer2.getLength());
                    if (compare < 0) {
                        // challenger wins: swap buffers so tempCellBuffer1 holds the winner's value
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

        if (activeDeletion.deletesCellAt(cellLiveness.timestamp()) || purger.shouldPurge(cellLiveness, nowInSec))
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
            /** {@link org.apache.cassandra.db.rows.Cell.Serializer#serialize(Cell, ColumnMetadata, DataOutputPlus, LivenessInfo, org.apache.cassandra.db.SerializationHeader)} */
            boolean isDeleted = cellLiveness.isTombstone();
            // Cell.Serializer treats deleted/expiring as mutually exclusive (else-if below), so a
            // tombstone must never carry IS_EXPIRING or a TTL field.
            boolean isExpiring = cellLiveness.isExpiring();
            boolean useRowTimestamp = !rowLiveness.isEmpty() && cellLiveness.timestamp() == rowLiveness.timestamp();
            boolean useRowTTL = isExpiring && rowLiveness.isExpiring() &&
                                cellLiveness.ttl() == rowLiveness.ttl() &&
                                cellLiveness.localDeletionTime() == rowLiveness.localExpirationTime();
            // Re-write cell flags to reflect resulting contents
            cellFlags &= Cell.Serializer.HAS_EMPTY_VALUE_MASK;
            if (isDeleted) cellFlags |= Cell.Serializer.IS_DELETED_MASK;
            else if (isExpiring) cellFlags |= Cell.Serializer.IS_EXPIRING_MASK;
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

    /**
     * Byte length of the leading unsigned vint in a wire-form variable-length value:
     * non-negative first byte = single-byte vint (VIntCoding's own callers guard the same
     * way before consulting numberOfExtraBytesToRead, which expects the SIGNED byte).
     */
    private static int wireVintSize(byte firstByte)
    {
        return firstByte >= 0 ? 1
               : 1 + org.apache.cassandra.utils.vint.VIntCoding.numberOfExtraBytesToRead(firstByte);
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
        lateStartRow(LivenessInfo.EMPTY, DeletionTime.LIVE, false, isStatic);
    }

    private void lateStartRow(LivenessInfo livenessInfo, DeletionTime deletionTime, boolean isShadowable, boolean isStatic) throws IOException
    {
        if (isPartitionStartDelayed())
        {
            lateStartPartition(isStatic);
        }
        ssTableCursorWriter.writeRowStart(livenessInfo, deletionTime, isShadowable, isStatic);
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

    private boolean hasWrittenPartition()
    {
        return lastWrittenPartition.keyLength() != 0;
    }

    private DecoratedKey lastWrittenKey()
    {
        assert hasWrittenPartition() : "no partition has been written yet";
        return lastWrittenPartition.key();
    }

    /**
     * Takes the owed steal, if any. Called at the top of a round, after the prepare-and-sort that performs the
     * round's reads and before either consumer of the value: the writer-order check below, and
     * {@code writerRollover}'s setLast reached from startPartition or from finish().
     *
     * The deferral is what makes it correct. On the cursor that wrote key k1, the round that reads k2 has
     * already swapped k1 into prev and compared it against k2, so prev is consumed and can be taken; the next
     * round swaps k2 into prev and loads k3 into the floater we handed back. Exactly one read must have
     * happened in between, which the assert states.
     */
    private void takeOwedPartitionSteal()
    {
        if (lastWrittenPartitionSource == null)
            return;
        assert lastWrittenPartitionSource.partitionSwaps() == lastWrittenPartitionSourceSwaps + 1
             : "the partition steal is not one slot advance behind its write: now "
               + lastWrittenPartitionSource.partitionSwaps() + ", was " + lastWrittenPartitionSourceSwaps;
        lastWrittenPartition = lastWrittenPartitionSource.detachPrevPartition(lastWrittenPartition);
        lastWrittenPartitionSource = null;
    }

    /**
     * Takes the instance describing the unfiltered just written from the cursor it was read into, handing that
     * cursor the floater in exchange.
     *
     * sstableCursors[0] is the source because it is the instance the output was written from: the merged group
     * shares one clustering, mergeRows writes cursor 0's descriptor, and mergeRangeTombstones stomps the
     * clustering kind on that same instance before writing it — which the covered-clustering update then keys
     * its boundary skip on.
     *
     * Immediate, not deferred by a round like the partition steal — see
     * {@link StatefulCursor#detachUnfiltered} for why that is safe with a single slot.
     */
    private void detachWrittenUnfiltered()
    {
        lastWrittenUnfiltered = sstableCursors[0].detachUnfiltered(lastWrittenUnfiltered);
    }

    /** The clustering of the last unfiltered written to the current output partition. */
    private ClusteringDescriptor lastWrittenClustering()
    {
        assert unfilteredsWrittenToPartition > 0 : "no unfiltered has been written to this partition";
        return lastWrittenUnfiltered;
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

        PARTITION_KEY_SORT.sortPerturbed(sstableCursors, sstableCursorsEqualsNext, perturbedCursors, sstableCursors.length);
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
        ROW_CLUSTERING_SORT.sortPerturbed(sstableCursors, sstableCursorsEqualsNext, prevMergeLimit, partitionMergeLimit);
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
        STATIC_SORT.sortPerturbed(sstableCursors, sstableCursorsEqualsNext, partitionMergeLimit, partitionMergeLimit);
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
        COLUMN_SORT.sortPerturbed(sstableCursors, sstableCursorsEqualsNext, prevCellMergeLimit, rowMergeLimit);
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

    // One dedicated, separately-compiled sort per comparison kind (see PreSortedBubbleInsert's
    // javadoc): each singleton is only ever constructed with its own compareByXxx reference, so
    // its copy's comparator.compare() call site stays monomorphic/inlinable instead of megamorphic
    // across all 4 comparators sharing one call site.
    private static final PartitionKeyMergeSort<StatefulCursor> PARTITION_KEY_SORT =
        new PartitionKeyMergeSort<>(CursorCompactor::compareByPartitionKey);
    private static final RowClusteringMergeSort<StatefulCursor> ROW_CLUSTERING_SORT =
        new RowClusteringMergeSort<>(CursorCompactor::compareByRowClustering);
    private static final StaticMergeSort<StatefulCursor> STATIC_SORT =
        new StaticMergeSort<>(CursorCompactor::compareByStatic);
    private static final ColumnMergeSort<StatefulCursor> COLUMN_SORT =
        new ColumnMergeSort<>(CursorCompactor::compareByColumn);

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
        private final long oldestUnrepairedTombstone;
        private final boolean onlyPurgeRepairedTombstones;
        private final boolean shouldIgnoreGcGraceForAnyKey;
        private final OperationType type;

        private boolean ignoreGcGraceSeconds;
        private final AbstractCompactionController controller;

        private DecoratedKey partitionKey;
        private LongPredicate purgeEvaluator;

        private long compactedUnfiltered;

        Purger(OperationType type, AbstractCompactionController controller)
        {
            oldestUnrepairedTombstone = controller.compactingRepaired() ? Long.MAX_VALUE : Integer.MIN_VALUE;
            onlyPurgeRepairedTombstones = controller.cfs.getCompactionStrategyManager().onlyPurgeRepairedTombstones();
            shouldIgnoreGcGraceForAnyKey = controller.cfs.shouldIgnoreGcGraceForAnyKey();
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
                                  totalCompressedInputBytes,
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

}
