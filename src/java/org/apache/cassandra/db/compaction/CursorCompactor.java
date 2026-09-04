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
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.LongPredicate;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

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
import org.apache.cassandra.io.sstable.format.SSTableSimpleScanner;
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

import static org.apache.cassandra.config.Config.PaxosStatePurging.legacy;
import static org.apache.cassandra.config.DatabaseDescriptor.paxosStatePurging;
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
 * counter columns, or a multi-cell column that the schema has dropped, and it stands aside for a
 * compaction that ignores gc grace for a key; see {@link #isSupported} and
 * {@link #unsupportedMetadata} for the full set of gates.
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

        if (unsupportedScanners(metadata, scanners))
            return false;
        // BTI index writing is not supported yet
        if (!(DatabaseDescriptor.getSelectedSSTableFormat() instanceof BigFormat))
        {
            LOGGER.debug("Cursor compaction is not supported for {}.{}: only the BIG sstable output format is supported, not {}",
                         metadata.keyspace, metadata.name, DatabaseDescriptor.getSelectedSSTableFormat());
            return false;
        }
        // TODO: Implement CompactionIterator.GarbageSkipper like functionality
        if (controller.tombstoneOption != CompactionParams.TombstoneOption.NONE)
        {
            LOGGER.debug("Cursor compaction is not supported for {}.{}: garbage skipping is not implemented, controller.tombstoneOption={}",
                         metadata.keyspace, metadata.name, controller.tombstoneOption);
            return false;
        }
        // Only ColumnFamilyStore.forceCompactionKeysIgnoringGcGrace puts a key in this set, and its
        // shipped caller is nodetool forcecompact. It is therefore the only way a purge is decided
        // with localDeletionTime >= gcBefore (see Purger.shouldPurge), and that is the case the
        // cursor cannot reproduce. BTreeRow.purge returns the row untouched whenever nowInSec is
        // below the row's minimum local deletion time. The iterator therefore settles row-level
        // purging all-or-nothing over the whole row, before it touches a cell. A streaming cursor
        // instead commits to the row's deletion and liveness before it walks the row's cells. Under
        // an ordinary gcBefore that short-circuit costs nothing, because such a row holds nothing
        // purgeable either way.
        // The set is per-table and lives for the whole force compaction, so a background compaction
        // that starts inside that window falls back as well. The gate is coarse, but it falls back
        // to the reference implementation, so it costs throughput and not correctness.
        // CompactionIterator swaps in PaxosPurger for system.paxos when paxos state purging is not
        // legacy. This path has one purger and cannot, so it declines the table and the iterator
        // path takes it.
        if (isPaxos(controller.cfs) && paxosStatePurging() != legacy)
        {
            LOGGER.debug("Cursor compaction is not supported for {}.{}: non-legacy paxos state purging on system.paxos is not supported, paxosStatePurging={}",
                         metadata.keyspace, metadata.name, DatabaseDescriptor.paxosStatePurging());
            return false;
        }
        if (controller.cfs.shouldIgnoreGcGraceForAnyKey())
        {
            LOGGER.debug("Cursor compaction is not supported for {}.{}: ignoring gc_grace_seconds for a key is not supported (nodetool forcecompact)",
                         metadata.keyspace, metadata.name);
            return false;
        }
        LOGGER.debug("Cursor compaction for table: {} keyspace: {} is supported.", metadata.name, metadata.keyspace);

        return true;
    }

    /** True if any scanner reads a partial range the cursor cannot bound, or holds an sstable the cursor path cannot read. */
    private static boolean unsupportedScanners(TableMetadata metadata, AbstractCompactionStrategy.ScannerList scanners)
    {
        for (ISSTableScanner scanner : scanners.scanners)
        {
            // A cursor reads the data-file segments an SSTableSimpleScanner was built over, so a
            // partial range of that scanner is fine. Every other scanner selects partitions by
            // token as it iterates, and the cursor has no such filter.
            //
            // The instanceof comes first on purpose: isFullRange() calls hasNext(), which seeks to
            // the scanner's first range, and there is no reason to seek for a scanner we accept.
            if (!(scanner instanceof SSTableSimpleScanner) && !scanner.isFullRange())
            {
                LOGGER.debug("Cursor compaction is not supported for {}.{}: partial scanners are supported only for SSTableSimpleScanner, not {}",
                             metadata.keyspace, metadata.name, scanner);
                return true;
            }
            if (unsupportedSSTables(metadata, scanner))
                return true;
        }
        return false;
    }

    private static boolean unsupportedSSTables(TableMetadata metadata, ISSTableScanner scanner)
    {
        for (SSTableReader reader : scanner.getBackingSSTables())
        {
            Version version = reader.descriptor.version;
            if (!version.isLatestVersion())
            {
                LOGGER.debug("Cursor compaction is not supported for {}.{}: sstable version {} is not the latest",
                             metadata.keyspace, metadata.name, version);
                return true;
            }
            if (unsupportedHeaderColumns(metadata, reader))
                return true;
        }
        return false;
    }

    public static boolean unsupportedMetadata(TableMetadata metadata)
    {
        if (metadata.keyspace.equals(SchemaConstants.ACCORD_KEYSPACE_NAME))
            return true;

        if (!metadata.partitioner.supportsReusableKeys())
        {
            LOGGER.debug("Cursor compaction is not supported for {}.{}: partitioner {} does not support reusable keys",
                         metadata.keyspace, metadata.name, metadata.partitioner.getClass().getSimpleName());
            return true;
        }

        if (metadata.indexes.size() != 0)
        {
            LOGGER.debug("Cursor compaction is not supported for {}.{}: additional indexes are not supported, metadata.indexes={}",
                         metadata.keyspace, metadata.name, metadata.indexes);
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
            if (column.isCounterColumn())
            {
                LOGGER.debug("Cursor compaction is not supported for {}.{}: counter columns are not supported, column={}",
                             metadata.keyspace, metadata.name, column);
                return true;
            }
        }
        return false;
    }

    /**
     * Rejects a dropped column that an sstable header still lists.
     *
     * WHAT THIS GATE DECIDES
     *
     * {@link #unsupportedSchema} sees only the columns the table has now. A drop removes the column
     * from {@link TableMetadata#regularAndStaticColumns()}. Each sstable written before the drop
     * still lists that column in its own serialization header, with the original type. The reader
     * builds its column arrays from the header, not from the schema, so a dropped column still
     * reaches the cell cursor. This gate is the only screen that sees it.
     *
     * The output has a slot for such a column either way. {@code SerializationHeader.make} builds
     * the output header from the headers of the input sstables, not from the current schema.
     *
     * SUPPORT MATRIX, which path runs per dropped column shape
     *
     *   dropped SIMPLE column                     cursor    SUPPORTED
     *   dropped MULTI-CELL column, re-added       cursor    SUPPORTED
     *   dropped MULTI-CELL column, still dropped  iterator  NOT SUPPORTED here, and BROKEN there
     *   dropped COUNTER column                    iterator  NOT SUPPORTED here
     *
     * A dropped SIMPLE column never fires the gate, and
     * {@code DroppedColumnDifferentialCompactionTest} covers it. Neither does a re-added multi-cell
     * column, because the gate tests {@code metadata.getColumn(name) == null} and a re-add puts the
     * column back in the schema; {@code droppedThenReaddedComplexColumnDeletionNotResurrected}
     * covers that. A dropped COUNTER column has no cursor support at all: {@link #mergeCells} throws
     * for a counter cell, and {@link #unsupportedSchema} rejects a counter the schema still has. The
     * iterator path handles it. The remaining shape, a multi-cell column still out of the schema, is
     * the reason this gate exists.
     *
     * THE DROPPED MULTI-CELL CASE, AND WHY THE FALLBACK TARGET IS NOT SAFE EITHER
     *
     * The cursor reads, merges and writes complex framing correctly, so this gate is NOT about
     * parsing. It is closed because of the state of the fallback target.
     *
     * The drop filter is gated on the timestamp, not on the column, so a cell written above the
     * drop time survives the read. What happens next depends on the data:
     *
     *   - every cell and complex deletion at or below the drop time: the iterator compacts normally
     *     and the filter removes the data. The fallback costs throughput only.
     *   - any cell above the drop time: the ITERATOR fails. It throws a NullPointerException, for a
     *     regular column and for a static one alike. The one exception is a table whose dropped
     *     column was its ONLY static column: {@code UnfilteredRowIterators.mergeStaticRows} returns
     *     {@code Rows.EMPTY_STATIC_ROW} on an empty column set, so it discards the whole static
     *     block before it builds a merger, and no exception is thrown. The table cannot compact on
     *     either path, and closing this gate only chooses which path fails. See CASSANDRA-21607.
     *
     * A surviving cell is also data that a later re-add of the column must not bring back, which is
     * the second half of CASSANDRA-21607. Note that the re-added row of the matrix above inherits
     * that: both paths carry such a cell once the column is back in the schema.
     *
     * OPEN THIS GATE TO MULTI-CELL COLUMNS ONCE CASSANDRA-21607 IS FIXED, that is, once the read
     * filter discards a dropped column's cells whatever their timestamp. Parity with the iterator
     * path needs no further work in this class. It needs a reference that does not fail.
     *
     * THE FALLBACK IS PERMANENT FOR THIS TABLE
     *
     * A drop cannot be undone in the headers, and no compaction clears it.
     * {@code SerializationHeader.make} builds the output header as the union of the INPUT headers'
     * columns, so every compaction copies the dropped column forward into the header it writes. The
     * iterator path does this as well. One pre-drop sstable therefore sends every future compaction
     * of the table to the iterator, for the life of the table.
     *
     * See CASSANDRA-21463.
     */
    private static boolean unsupportedHeaderColumns(TableMetadata metadata, SSTableReader reader)
    {
        // RegularAndStaticColumns iterates statics then regulars, so this covers both
        for (ColumnMetadata column : reader.header.columns())
        {
            if (isDroppedMultiCellOrCounterColumn(metadata, column, reader.header.getType(column)))
            {
                LOGGER.atDebug()
                      .setMessage("Cursor compaction for table: {} keyspace: {} is not supported. REASON: A multi-cell or counter column dropped from the schema is still carried in the header of {}, which the cursor path does not yet cover. column={}")
                      .addArgument(metadata.name)
                      .addArgument(metadata.keyspace)
                      .addArgument(() -> reader.descriptor)
                      .addArgument(() -> column)
                      .log();
                return true;
            }
        }
        return false;
    }

    /**
     * True if {@code column} is a complex or counter column that the schema has dropped.
     *
     * @param diskType the type the header records for {@code column}. Test this type as well as
     *                 the schema type, because the reader decodes against the header type.
     */
    private static boolean isDroppedMultiCellOrCounterColumn(TableMetadata metadata, ColumnMetadata column, AbstractType<?> diskType)
    {
        boolean isMultiCellOrCounter = column.isComplex() || column.isCounterColumn()
                                      || (diskType != null && (diskType.isMultiCell() || diskType.isCounter()));
        return isMultiCellOrCounter && metadata.getColumn(column.name) == null;
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
     * Scratch for {@link #anyMergedCellDeadAtNow}, which walks a row's cells and then puts the
     * cursors back. The arrays hold the cursor ORDER and the equals-next flags that its sorts
     * overwrite, and the per-cursor state that tells it which cursors to rewind. All three are
     * null unless the table enforces strict liveness.
     */
    private final StatefulCursor[] probeCursorOrder;
    private final boolean[] probeEqualsNext;
    private final int[] probeCursorState;
    // Scratch space for the complex-deletion test in anyMergedCellDeadAtNow. Same reason as above.
    private final DeletionTime.ReusableDeletionTime probeComplexDeletion;
    /**
     * The complex column the probe last folded. All cells of one complex column share a single fold
     * result, so it is computed again only when the lead cursor moves to a new column.
     * {@link #mergeCells} caches {@link #mergedComplexDeletion} the same way.
     */
    private ColumnMetadata probeComplexColumn;

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
     * The UnfilteredDescriptor holding the last unfiltered written to the current output partition.
     * A cursor overwrites its own descriptor on its next read, so this is the floater.
     */
    private UnfilteredDescriptor lastWrittenUnfiltered;
    /**
     * Unfiltereds written to the current output partition. At zero,
     * {@link #lastWrittenUnfiltered} still holds an earlier partition's clustering, or nothing on
     * the first partition.
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
        this.probeComplexDeletion = enforceStrictLiveness ? DeletionTime.ReusableDeletionTime.live() : null;

        purger = new Purger(type, controller);

        lastWrittenPartition = new PartitionDescriptor(metadata.partitioner.createReusableKey(0));
        lastWrittenUnfiltered = new UnfilteredDescriptor(metadata.comparator.subtypes().toArray(AbstractType[]::new));
        // A steal moves a descriptor between cursors, and to the write-side instance built from
        // the table comparator above. Each parses a clustering with its own clusteringTypes, so
        // all of them must parse identically.
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
     * Merges the partition's static row, if the table has static columns. Mirrors
     * UnfilteredPartitionIterators.merge(List, MergeListener).
     */
    private void mergeStaticRow(int partitionMergeLimit, DeletionTime activeDeletion) throws IOException
    {
        if (!hasStaticColumns)
            return;

        int staticRowMergeLimit = prepareAndSortStaticForMerge(partitionMergeLimit);
        if (staticRowMergeLimit != 0)
        {
            // No steal here: this call's return value is not consumed.
            mergeRows(staticRowMergeLimit, activeDeletion, true, false);
            // Required. A cursor left at UNFILTERED_END still holds the static descriptor, and
            // STATIC_CLUSTERING sorts ahead of every row, so the unfiltered loop re-merges the
            // consumed position as a phantom row. The output is unaffected; rowMergeCounters is
            // not, and it feeds system.compaction_history.rows_merged.
            continueReadingAfterMerge(staticRowMergeLimit, UNFILTERED_END);
        }
        if (isPartitionStarted())
        {
            if (staticRowMergeLimit == 0) ssTableCursorWriter.writeEmptyStaticRow();
            partitionHeaderLength = (int) (ssTableCursorWriter.getPosition() - ssTableCursorWriter.getPartitionStart());
        }
    }

    /** Merges the partition's rows and range tombstone markers, in clustering order. */
    private void mergeUnfiltereds(int partitionMergeLimit, DeletionTime mergedDeletion, DeletionTime activeDeletion) throws IOException
    {
        int unfilteredMergeLimit = partitionMergeLimit;
        boolean isFirstUnfiltered = true;
        unfilteredsWrittenToPartition = 0;
        while (true)
        {
            unfilteredMergeLimit = prepareAndSortUnfilteredForMerge(partitionMergeLimit, unfilteredMergeLimit);
            if (unfilteredMergeLimit == 0)
                return;

            int flags = sstableCursors[0].unfiltered().flags();
            if (UnfilteredSerializer.isRow(flags))
            {
                isFirstUnfiltered = writeMergedRow(unfilteredMergeLimit, activeDeletion, isFirstUnfiltered);
            }
            else if (UnfilteredSerializer.isTombstoneMarker(flags))
            {
                isFirstUnfiltered = writeMergedMarker(unfilteredMergeLimit, mergedDeletion, isFirstUnfiltered);
                activeDeletion = activeOpenRangeDeletion == DeletionTime.LIVE ? mergedDeletion : activeOpenRangeDeletion;
            }
            else
            {
                throw new IllegalStateException("Unexpected unfiltered type (not row or tombstone):" + flags);
            }
            // move along
            continueReadingAfterMerge(unfilteredMergeLimit, UNFILTERED_END);
        }
    }

    /** @return false once anything has been written to the partition */
    private boolean writeMergedRow(int unfilteredMergeLimit, DeletionTime activeDeletion, boolean isFirstUnfiltered) throws IOException
    {
        if (!mergeRows(unfilteredMergeLimit, activeDeletion, false, isFirstUnfiltered))
            return isFirstUnfiltered;

        // A static descriptor must never be written from the unfiltered loop: its clustering has
        // length 0.
        assert sstableCursors[0].unfiltered().clusteringKind() != ClusteringPrefix.Kind.STATIC_CLUSTERING
             : "a static descriptor was written from the unfiltered loop";
        unfilteredsWrittenToPartition++;
        detachWrittenUnfiltered();
        return false;
    }

    /**
     * The tombstone processing maybe writes a marker, and maybe changes
     * {@link #activeOpenRangeDeletion}.
     *
     * @return false once anything has been written to the partition
     */
    private boolean writeMergedMarker(int unfilteredMergeLimit, DeletionTime mergedDeletion, boolean isFirstUnfiltered) throws IOException
    {
        if (!mergeRangeTombstones(unfilteredMergeLimit, mergedDeletion, isFirstUnfiltered))
            return isFirstUnfiltered;

        unfilteredsWrittenToPartition++;
        detachWrittenUnfiltered();
        return false;
    }

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

        mergeStaticRow(partitionMergeLimit, activeDeletion);
        mergeUnfiltereds(partitionMergeLimit, mergedDeletion, activeDeletion);

        boolean partitionWritten = isPartitionStarted();
        if (partitionWritten)
        {
            // The trailing index block's last name and the covered-clustering max are the same value, the
            // clustering of the last unfiltered written here; a partition that wrote none has no trailing
            // block to cut, hence null.
            ClusteringDescriptor lastName = unfilteredsWrittenToPartition > 0 ? lastWrittenClustering() : null;
            ssTableCursorWriter.writePartitionEnd(partitionDescriptor.keyBytes(), partitionDescriptor.keyLength(), toWritePartitionDeletion, partitionHeaderLength, lastName);
            // Update min/max clustering metadata. The count guard is required; see
            // unfilteredsWrittenToPartition.
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
     * We have a common clustering and need to merge data.
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

        foldRowLivenessAndDeletion(rowMergeLimit);
        DeletionTime rowActiveDeletion = applyRowPurge(partitionActiveDeletion);

        boolean isRowDropped = mergedRow.deletion.isLive() && mergedRow.info.isEmpty();

        if (!isRowDropped)
        {
            lateStartRow(mergedRow.info, mergedRow.deletion, mergedRow.shadowable, isStatic);
        }

        /**
         * Strict liveness ({@link org.apache.cassandra.schema.TableMetadata#enforceStrictLiveness})
         * drops a row with no primary-key liveness and no row deletion. {@link BTreeRow#purge} reaches
         * that drop only past its opening {@code if (!hasDeletion(nowInSec)) return this;}, so the
         * cursor applies the same precondition. Without it the cursor deletes rows the iterator
         * returns untouched, cells included.
         */
        if (isRowDropped && enforceStrictLiveness
            && (mergedRow.hasDeletionAtNow || anyMergedCellDeadAtNow(rowMergeLimit, rowActiveDeletion, isStatic)))
        {
            skipRowsOnStrictLiveness(rowMergeLimit, isStatic);
        }
        else
        {
            isRowDropped = mergeRowCells(rowMergeLimit, rowActiveDeletion, isRowDropped, isStatic);
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

    /**
     * The merged row's liveness and deletion. One instance, reused for every row.
     */
    private static final class MergedRow
    {
        LivenessInfo info;
        DeletionTime deletion;
        /**
         * Row.Deletion.isShadowable(): deprecated (CASSANDRA-11500), reachable only on old
         * Materialized View data. Tracked alongside {@link #deletion} because the shadowing step
         * reads it, and because it has to survive to the output write attached to whichever
         * deletion wins.
         */
        boolean shadowable;
        /**
         * Whether BTreeRow.purge's hasDeletion(nowInSec) guard would be open for reasons OTHER than
         * the row's cells. See the strict-liveness branch in {@link #mergeRows} for why only the
         * purger's two clearances count.
         */
        boolean hasDeletionAtNow;
    }

    private final MergedRow mergedRow = new MergedRow();

    /**
     * Folds every source's liveness and deletion into {@link #mergedRow}, then clears a deletion
     * the liveness shadows.
     *
     * @see Row.Merger#merge(DeletionTime)
     * @see Row.Deletion#isShadowedBy(LivenessInfo)
     */
    private void foldRowLivenessAndDeletion(int rowMergeLimit)
    {
        UnfilteredDescriptor row = sstableCursors[0].unfiltered();
        mergedRow.info = row.livenessInfo();
        mergedRow.deletion = row.deletionTime();
        mergedRow.shadowable = row.isShadowableDeletion();

        for (int i = 1; i < rowMergeLimit; i++)
        {
            // TODO: can validate state here
            row = sstableCursors[i].unfiltered();
            // TODO: maybe flags more optimal(avoid ref loads and comaparisons etc)
            if (row.livenessInfo().supersedes(mergedRow.info))
                mergedRow.info = row.livenessInfo();
            if (row.deletionTime().supersedes(mergedRow.deletion))
            {
                mergedRow.deletion = row.deletionTime();
                mergedRow.shadowable = row.isShadowableDeletion();
            }
        }

        // Placed as Row.Merger.merge(DeletionTime) places it. No shadowed cell resurfaces:
        // BTreeRow.Builder drops those at write time.
        if (mergedRow.shadowable && mergedRow.info.timestamp() > mergedRow.deletion.markedForDeleteAt())
        {
            mergedRow.deletion = DeletionTime.LIVE;
            mergedRow.shadowable = false; // a live deletion is never shadowable
        }
    }

    /**
     * Applies the partition's deletion and the purger to {@link #mergedRow}.
     *
     * @return the deletion in effect for the row's cells
     * @see BTreeRow#purge(DeletionPurger, long, boolean)
     */
    private DeletionTime applyRowPurge(DeletionTime partitionActiveDeletion)
    {
        DeletionTime rowActiveDeletion = partitionActiveDeletion;
        mergedRow.hasDeletionAtNow = false;
        if (mergedRow.deletion.supersedes(rowActiveDeletion))
        {
            rowActiveDeletion = mergedRow.deletion; // deletion is in effect before purge takes effect
            if (purger.shouldPurge(mergedRow.deletion))
            {
                mergedRow.deletion = DeletionTime.LIVE;
                mergedRow.shadowable = false; // a live deletion is never shadowable
                mergedRow.hasDeletionAtNow = true;
            }
        }
        else
        {
            // partition delete takes over
            mergedRow.deletion = DeletionTime.LIVE;
            mergedRow.shadowable = false; // a live deletion is never shadowable
        }

        // Only the purger arm records a clearance: BTreeRow.purge computes minLocalDeletionTime
        // after the active deletion empties the liveness, and before the purger runs.
        if (rowActiveDeletion.deletes(mergedRow.info))
        {
            mergedRow.info = LivenessInfo.EMPTY;
        }
        else if (purger.shouldPurge(mergedRow.info, nowInSec))
        {
            // shouldPurge requires localDeletionTime < gcBefore, and gcBefore <= nowInSec here, so
            // the reference term is at or below nowInSec.
            mergedRow.hasDeletionAtNow |= !mergedRow.info.isEmpty();
            mergedRow.info = LivenessInfo.EMPTY;
        }
        return rowActiveDeletion;
    }

    /** Walks the row's columns, merging and writing each cell group. */
    private boolean mergeRowCells(int rowMergeLimit, DeletionTime rowActiveDeletion, boolean isRowDropped, boolean isStatic) throws IOException
    {
        int cellMergeLimit = rowMergeLimit;
        currentComplexColumn = null;
        while (true)
        {
            // advance cursors that need to read the cell header
            for (int i = 0; i < cellMergeLimit; i++)
            {
                if (sstableCursors[i].state() == CELL_HEADER_START)
                    sstableCursors[i].readCellHeader();
            }
            // Sort rows by cells
            cellMergeLimit = prepareAndSortCellsForMerge(rowMergeLimit, cellMergeLimit);
            if (cellMergeLimit == 0)
                return isRowDropped;
            isRowDropped = mergeCells(rowMergeLimit, cellMergeLimit, rowActiveDeletion, mergedRow.info, isRowDropped, isStatic);
            // move along
            continueReadingAfterMerge(cellMergeLimit, CELL_END);
        }
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
     * True if the merged row has a cell that is not live at {@code nowInSec}. Such a cell is either
     * a cell tombstone or an expiring cell that is past its expiry time.
     *
     * This is the cells' part of the {@code hasDeletion(nowInSec)} guard in {@link BTreeRow#purge}.
     * {@code Cell.minDeletionTime()} gives {@code Long.MIN_VALUE} for a tombstone,
     * {@code Cell.NO_DELETION_TIME} for a live cell, and the local deletion time in all other
     * cases. Thus "at or below {@code nowInSec}" and "not live at {@code nowInSec}" select the same
     * cells.
     *
     * Only the merge winner of each column counts. Only the winner goes into the reference merged
     * row, and only if it survives the active deletion, which the reference applies before it
     * reconciles. This method picks the winner first and then tests the active deletion, which
     * gives the same answer: the winner has the highest timestamp, so if the active deletion
     * deletes the winner, it deletes every cell of that column.
     *
     * {@link CellLivenessInfo#resolve} alone selects the winner. Its {@code COMPARE} result never
     * leaves the two cells in disagreement about liveness. {@code resolve} returns
     * {@code COMPARE} in two cases only:
     * <ul>
     *   <li>both cells hold {@code Cell.NO_DELETION_TIME}, so both are live;</li>
     *   <li>both cells hold the same timestamp, deletion time and TTL.</li>
     * </ul>
     * Therefore the cell-value comparison that this method skips cannot change the answer.
     *
     * A complex column also has its own deletion, which is a second and independent term.
     * {@link BTreeRow#minDeletionTime(org.apache.cassandra.db.rows.ComplexColumnData)} always folds
     * a non-live complex deletion in as {@code Long.MIN_VALUE}, whatever the cells below it
     * contribute. A column can hold a dead deletion and a live cell together, and the deletion
     * alone still opens the guard. {@link #foldAndClampComplexDeletion} computes the same merged
     * deletion that {@link #mergeCells} uses to shadow older cells, before the purge.
     *
     * This method walks the row's cells and then rewinds every cursor it moved. The caller's cell
     * loop therefore runs against the state it would see if the probe had not run. The rewind also
     * restores the cursor order and the equals-next flags, which the cell sorts overwrite. That
     * restore is a safety measure: the real loop sorts the whole group again in any case.
     */
    private boolean anyMergedCellDeadAtNow(int rowMergeLimit, DeletionTime rowActiveDeletion, boolean isStatic)
    {
        System.arraycopy(sstableCursors, 0, probeCursorOrder, 0, rowMergeLimit);
        System.arraycopy(sstableCursorsEqualsNext, 0, probeEqualsNext, 0, rowMergeLimit);
        for (int i = 0; i < rowMergeLimit; i++)
            probeCursorState[i] = sstableCursors[i].state();

        boolean anyDead = false;
        int cellMergeLimit = rowMergeLimit;
        probeComplexColumn = null;
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

            anyDead = probeGroupHasDeadCell(rowMergeLimit, cellMergeLimit, rowActiveDeletion);

            for (int i = 0; i < cellMergeLimit; i++)
            {
                if (sstableCursors[i].state() == CELL_VALUE_START)
                    sstableCursors[i].skipCellValue();
            }
            continueReadingAfterMerge(cellMergeLimit, CELL_END);
        }

        restoreCursorsAfterProbe(rowMergeLimit, isStatic);
        return anyDead;
    }

    /**
     * True if the current cell group contributes a cell that is not live at {@code nowInSec}, or a
     * non-live complex deletion.
     *
     * <p>BTreeRow.minDeletionTime(ComplexColumnData) always folds in the column's own deletion,
     * above what its cells contribute. A non-live complex deletion gives Long.MIN_VALUE even when
     * the cells below it are live, so that test decides the group on its own and needs no cell
     * state. It is also the only safe test when the position produced no cell.
     */
    private boolean probeGroupHasDeadCell(int rowMergeLimit, int cellMergeLimit, DeletionTime rowActiveDeletion)
    {
        SSTableCursorReader.CellCursor leadCellCursor = sstableCursors[0].cellCursor();
        if (leadCellCursor.cellColumn.isComplex())
        {
            if (!ColumnMetadata.sameName(probeComplexColumn, leadCellCursor.cellColumn))
            {
                probeComplexColumn = leadCellCursor.cellColumn;
                foldAndClampComplexDeletion(rowMergeLimit, probeComplexColumn, rowActiveDeletion, probeComplexDeletion);
            }
            if (!probeComplexDeletion.isLive())
                return true;
        }
        // The producedCell test guards the read below: a deletion-only position (see mergeCells) has
        // no valid cell fields, because cellLiveness still holds the values of an earlier cell. A
        // live complex deletion above zero cells has nothing left to decide.
        if (!leadCellCursor.producedCell)
            return false;

        // The column is simple, or it is complex with a live deletion. Its cells alone decide it.
        ReusableCellLivenessInfo winner = leadCellCursor.cellLiveness;
        for (int i = 1; i < cellMergeLimit; i++)
        {
            ReusableCellLivenessInfo challenger = sstableCursors[i].cellCursor().cellLiveness;
            if (CellLivenessInfo.resolve(winner, challenger) == RIGHT)
                winner = challenger;
        }
        return !rowActiveDeletion.deletesCellAt(winner.timestamp()) && !winner.isLive(nowInSec);
    }

    /** Puts every cursor the probe moved back where the caller's cell loop expects it. */
    private void restoreCursorsAfterProbe(int rowMergeLimit, boolean isStatic)
    {
        System.arraycopy(probeCursorOrder, 0, sstableCursors, 0, rowMergeLimit);
        System.arraycopy(probeEqualsNext, 0, sstableCursorsEqualsNext, 0, rowMergeLimit);
        for (int i = 0; i < rowMergeLimit; i++)
        {
            // rewindRowCells restores the walk to the row's FIRST cell, so a cursor recorded mid-row
            // would re-present its earlier cells to the merge.
            int recordedState = probeCursorState[i];
            assert recordedState == CELL_HEADER_START || recordedState == UNFILTERED_END
                 : "unexpected merge-group state before the strict-liveness probe: " + sstableCursors[i];
            if (recordedState == CELL_HEADER_START)
                sstableCursors[i].rewindRowCells(isStatic);
        }
    }

    // current output complex column state (reset per row)
    private ColumnMetadata currentComplexColumn;
    private boolean complexColumnStarted;
    private final DeletionTime.ReusableDeletionTime mergedComplexDeletion = DeletionTime.ReusableDeletionTime.live();
    // The same merged deletion, but for cell-drop decisions. It is separate because the two roles
    // differ: the output drops a purgeable deletion, but that deletion must still delete the older
    // cells below it. The iterator does the same. It applies the un-purged deletion at merge time
    // (Row.Merger.ColumnDataReducer) and purges it only afterwards (ComplexColumnData.purge).
    private final DeletionTime.ReusableDeletionTime shadowComplexDeletion = DeletionTime.ReusableDeletionTime.live();

    private DataOutputBuffer tempCellBuffer1 = new DataOutputBuffer();
    private DataOutputBuffer tempCellBuffer2 = new DataOutputBuffer();
    // Fallback transfer buffer for cell-content copies. The usual path reads directly into the
    // target DataOutputBuffer array: see SSTableCursorReader.copyCellContents.
    private final byte[] copyColumnValueBuffer = new byte[4096];

    /**
     * Computes the complex deletion of {@code column} across every source that holds it, and
     * clamps the result to {@code activeDeletion}. The result goes into {@code scratch}.
     *
     * {@link #mergeCells} calls this before it uses the result to shadow the column's older cells.
     * {@link #anyMergedCellDeadAtNow} calls it to ask the same question: does the merged row hold
     * a non-live complex deletion for this column? One method computes the fold for both callers,
     * so the two answers cannot disagree.
     *
     * This method does not purge the result. Both callers need the un-purged value:
     * <ul>
     *   <li>a deletion that the output drops as purged must still shadow an older cell;</li>
     *   <li>the strict-liveness guard asks about the row as {@link Row.Merger#merge} leaves it,
     *       which is after the clamp and before the purge.</li>
     * </ul>
     */
    private void foldAndClampComplexDeletion(int rowMergeLimit, ColumnMetadata column, DeletionTime activeDeletion,
                                             DeletionTime.ReusableDeletionTime scratch)
    {
        scratch.resetLive();
        for (int i = 0; i < rowMergeLimit; i++)
        {
            StatefulCursor c = sstableCursors[i];
            if (isState(c.state(), CELL_VALUE_START | CELL_END)
                && ColumnMetadata.sameName(c.cellCursor().cellColumn, column))
            {
                DeletionTime d = c.cellCursor().complexDeletion;
                if (d.supersedes(scratch))
                    scratch.reset(d);
            }
        }
        // The keep-condition of ColumnDataReducer: a deletion survives only if it supersedes the
        // active deletion. Every real deletion supersedes LIVE.
        if (!scratch.supersedes(activeDeletion))
            scratch.resetLive();
    }

    /**
     * Starts a new output complex column. This method folds the merged deletion of the column and
     * keeps the un-purged copy that shadows the older cells.
     *
     * If the merged deletion survives, this method opens the column now, because the output must
     * carry a surviving deletion even when no cell survives. If the deletion is live, the column
     * opens later, at its first surviving cell. A column with no surviving cell and no deletion
     * writes nothing, in the same way that the iterator drops an empty
     * {@code ComplexColumnData}.
     *
     * @return the new value of {@code isRowDropped}
     */
    private boolean startNewComplexColumn(int rowMergeLimit, ColumnMetadata column, DeletionTime activeDeletion,
                                          boolean isRowDropped, boolean isStatic) throws IOException
    {
        currentComplexColumn = column;
        complexColumnStarted = false;
        // The fold clamps against the active row, partition or range deletion. Equal deletions do
        // not survive that clamp: a row delete and a column delete can share a timestamp and a
        // second, and ColumnDataReducer then drops the complex deletion. Keeping it here would
        // write a spurious HAS_COMPLEX_DELETION flag and spurious deletion bytes.
        foldAndClampComplexDeletion(rowMergeLimit, currentComplexColumn, activeDeletion, mergedComplexDeletion);
        // The deletion purges like any other tombstone, but only in the output. It must still
        // shadow the older cells of this column during the merge: see shadowComplexDeletion. A
        // purge before the shadow step would bring those cells back.
        shadowComplexDeletion.reset(mergedComplexDeletion);
        if (purger.shouldPurge(mergedComplexDeletion))
            mergedComplexDeletion.resetLive();
        if (!mergedComplexDeletion.isLive())
            isRowDropped = openRowAndComplexColumn(isRowDropped, isStatic, true);
        return isRowDropped;
    }

    /**
     * Opens the output row if this is its first surviving content. Then opens the current complex
     * column if it is not open.
     *
     * @return the new value of {@code isRowDropped}
     */
    private boolean openRowAndComplexColumn(boolean isRowDropped, boolean isStatic, boolean isComplexColumn) throws IOException
    {
        if (isRowDropped)
        {
            isRowDropped = false;
            lateStartRow(isStatic);
        }
        if (isComplexColumn && !complexColumnStarted)
        {
            ssTableCursorWriter.startComplexColumn(currentComplexColumn, mergedComplexDeletion);
            complexColumnStarted = true;
        }
        return isRowDropped;
    }

    /**
     * {@link Row.Merger.ColumnDataReducer#getReduced()} <-- applied the delete before reconcile, should not make a difference?
     * {@link Cells#reconcile(Cell, Cell)}
     */
    private boolean mergeCells(int rowMergeLimit, int cellMergeLimit, DeletionTime activeDeletion, LivenessInfo rowLiveness, boolean isRowDropped, boolean isStatic) throws IOException
    {
        cellMergeCounters[cellMergeLimit - 1]++;
        // Nothing to sort, we basically need to pick the correct data to copy.
        // -> the latest data.
        // TODO: handle counter cells
        StatefulCursor firstSource = sstableCursors[0];
        SSTableCursorReader.CellCursor firstCursor = firstSource.cellCursor();
        cellWinner.set(firstSource, firstCursor, firstCursor.cellLiveness);

        if (firstCursor.cellColumn.isCounterColumn())
            throw new UnsupportedOperationException("TODO: Not ready for counter cells.");

        // All cells in this group have the same column, because the group is the merge minimum.
        // The winner changes below, but the column does not.
        final boolean isComplexColumn = firstCursor.cellColumn.isComplex();

        DeletionTime effectiveDeletion = activeDeletion;
        if (isComplexColumn)
        {
            // At a new complex column, every source that holds the column is positioned at it.
            // The streams are in column order, this column is the merge minimum, and a
            // deletion-only position sorts before the cells. The merged deletion is therefore
            // known before the first cell of the column is written.
            if (!ColumnMetadata.sameName(currentComplexColumn, firstCursor.cellColumn))
                isRowDropped = startNewComplexColumn(rowMergeLimit, firstCursor.cellColumn, activeDeletion, isRowDropped, isStatic);
            // The shadow deletion is non-live only if it superseded the active deletion at the fold.
            if (!shadowComplexDeletion.isLive())
                effectiveDeletion = shadowComplexDeletion;

            if (!firstCursor.producedCell)
            {
                // A deletion-only group. The fold above already used its deletion.
                return isRowDropped;
            }
        }

        selectWinningCell(cellMergeLimit, effectiveDeletion);


        /** {@link Cell.Serializer#serialize} */
        int cellFlags = applyExpiredTtl(cellWinner.cursor.cellFlags);

        if (effectiveDeletion.deletesCellAt(cellWinner.liveness.timestamp())
            || purger.shouldPurge(cellWinner.liveness, nowInSec))
        {
            if (Cell.Serializer.hasValue(cellFlags))
                discardWinnerValue();
            return isRowDropped;
        }

        isRowDropped = openRowAndComplexColumn(isRowDropped, isStatic, isComplexColumn);
        writeMergedCell(rewriteCellFlags(cellFlags, rowLiveness), isComplexColumn);
        return isRowDropped;
    }

    /** Writes the winning cell: header, then path for a complex column, then value if it has one. */
    private void writeMergedCell(int cellFlags, boolean isComplexColumn) throws IOException
    {
        // The winner's own cursor supplies the column. The name test in writeCellHeader needs that
        // instance.
        ssTableCursorWriter.writeCellHeader(cellFlags, cellWinner.liveness, cellWinner.cursor.cellColumn);
        if (isComplexColumn)
            ssTableCursorWriter.writeCellPath(cellWinner.cursor.cellPathBuffer, cellWinner.cursor.cellPathLength);
        if (!Cell.Serializer.hasValue(cellFlags))
            return;

        if (winnerValueLocation() == ValueLocation.IN_SOURCE)
            ssTableCursorWriter.writeCellValue(cellWinner.source, copyColumnValueBuffer);
        else
            ssTableCursorWriter.writeCellValue(cellWinner.buffer, tempCellValueLength1);
    }

    /**
     * The winning cell of one merge group, and where its value is. One instance, reused for every
     * group.
     */
    private static final class CellWinner
    {
        StatefulCursor source;
        SSTableCursorReader.CellCursor cursor;
        ReusableCellLivenessInfo liveness;
        /** Null while the value is still unread in {@link #source}, else the buffer holding a copy. */
        DataOutputBuffer buffer;

        /** Takes over as winner, with the value still unread in its source. */
        void set(StatefulCursor source, SSTableCursorReader.CellCursor cursor, ReusableCellLivenessInfo liveness)
        {
            takeOver(source, cursor, liveness);
            this.buffer = null;
        }

        /** Takes over as winner without disturbing {@link #buffer}, which the caller has just set. */
        void takeOver(StatefulCursor source, SSTableCursorReader.CellCursor cursor, ReusableCellLivenessInfo liveness)
        {
            this.source = source;
            this.cursor = cursor;
            this.liveness = liveness;
        }
    }

    private final CellWinner cellWinner = new CellWinner();

    /** Where the winning cell's value is.  Any other state is a defect. */
    private enum ValueLocation { IN_SOURCE, IN_BUFFER }

    /**
     * Leaves {@link #cellWinner} holding the cell that survives the group.
     *
     * <p>{@link CellLivenessInfo#resolve} makes the whole liveness decision.  COMPARE means it
     * defers to the value comparison.  Unlike {@link Cells#resolveRegular}, this call site needs no
     * narrowing: {@link ReusableCellLivenessInfo} has no subclasses, so the liveness accessors
     * already bind from one type.
     *
     * @see Cells#reconcile(Cell, Cell)
     */
    private void selectWinningCell(int cellMergeLimit, DeletionTime effectiveDeletion) throws IOException
    {
        for (int i = 1; i < cellMergeLimit; i++)
        {
            StatefulCursor challenger = sstableCursors[i];
            SSTableCursorReader.CellCursor challengerCursor = challenger.cellCursor();
            ReusableCellLivenessInfo challengerLiveness = challengerCursor.cellLiveness;

            Resolution resolution = CellLivenessInfo.resolve(cellWinner.liveness, challengerLiveness);
            if (resolution == LEFT)
                skipValueIfUnread(challenger);
            else if (resolution == RIGHT)
            {
                skipValueIfUnread(cellWinner.source);
                cellWinner.set(challenger, challengerCursor, challengerLiveness);
            }
            else if (effectiveDeletion.deletesCellAt(challengerLiveness.timestamp()))
                skipValueIfUnread(challenger);
            else if (challengerValueWins(challenger))
                cellWinner.takeOver(challenger, challengerCursor, challengerLiveness);
        }
    }

    private static void skipValueIfUnread(StatefulCursor cursor) throws IOException
    {
        if (cursor.state() == CELL_VALUE_START)
            cursor.skipCellValue();
    }

    /**
     * Compares the challenger's value against the winner's and reports whether it wins.  The winner
     * keeps ties, as {@link Cells#resolveRegular} does.
     *
     * <p>Both values are copied into the two reusable buffers first, because a value still in its
     * source can only be read once.  When the challenger wins, the buffers are swapped rather than
     * copied, so {@code tempCellBuffer1} always holds the winner's value.
     */
    private boolean challengerValueWins(StatefulCursor challenger) throws IOException
    {
        if (cellWinner.source.state() == CELL_VALUE_START)
        {
            if (cellWinner.buffer != null)
                throw new IllegalStateException("tempCellBuffer should be null if cellSource has a value to be read.");
            tempCellBuffer1.clear();
            cellWinner.source.copyCellValue(tempCellBuffer1, copyColumnValueBuffer);
            tempCellValueLength1 = cellWinner.source.lastCellValueLength();
            cellWinner.buffer = tempCellBuffer1; // assume cell1 is going to be bigger
        }
        else if (cellWinner.buffer == null)
        {
            tempCellBuffer1.clear(); // potential trash value in buffer1
            tempCellValueLength1 = 0;
        }
        else if (cellWinner.buffer != tempCellBuffer1)
            throw new IllegalStateException("tempCellBuffer should be tempCellBuffer1 if cellSource has been read.");

        tempCellBuffer2.clear();
        tempCellValueLength2 = 0;
        if (challenger.state() == CELL_VALUE_START)
        {
            challenger.copyCellValue(tempCellBuffer2, copyColumnValueBuffer);
            tempCellValueLength2 = challenger.lastCellValueLength();
        }

        // These buffers hold the wire form: a variable-length type prefixes its value with a length
        // vint, and the reference compares the raw value bytes. Skip the vint, or a lexicographic
        // compare orders by length first.
        int skip1 = 0, skip2 = 0;
        if (cellWinner.cursor.cellType.valueLengthIfFixed() < 0)
        {
            skip1 = tempCellBuffer1.getLength() == 0 ? 0 : wireVintSize(tempCellBuffer1.getData()[0]);
            skip2 = tempCellBuffer2.getLength() == 0 ? 0 : wireVintSize(tempCellBuffer2.getData()[0]);
        }
        if (Arrays.compareUnsigned(tempCellBuffer1.getData(), skip1, tempCellBuffer1.getLength(),
                                   tempCellBuffer2.getData(), skip2, tempCellBuffer2.getLength()) >= 0)
            return false;

        DataOutputBuffer swap = tempCellBuffer1;
        tempCellBuffer1 = tempCellBuffer2;
        tempCellBuffer2 = swap;
        int swapLength = tempCellValueLength1;
        tempCellValueLength1 = tempCellValueLength2;
        tempCellValueLength2 = swapLength;
        cellWinner.buffer = tempCellBuffer1;
        return true;
    }

    /**
     * The raw value bytes each temp buffer holds, tracked in step with the buffers themselves so
     * that the swap above keeps them paired. {@code tempCellValueLength1} is the winner's, because
     * {@link CellWinner#buffer} is {@code tempCellBuffer1} whenever it is set. The collection
     * guardrails measure a value this way; the buffer's own length also counts the leading vint of a
     * variable-length type.
     */
    private int tempCellValueLength1;
    private int tempCellValueLength2;

    /**
     * Reports where the winning cell's value is, and fails if the source state and the buffer
     * disagree.
     */
    private ValueLocation winnerValueLocation()
    {
        if (cellWinner.source.state() == CELL_VALUE_START)
        {
            if (cellWinner.buffer != null)
                throw new IllegalStateException("Either copied buffer or ready to copy reader, not both.");
            return ValueLocation.IN_SOURCE;
        }
        if (cellWinner.buffer != null)
            return ValueLocation.IN_BUFFER;
        throw new IllegalStateException("Flags and state contradict");
    }

    /** Drops the winning cell's value, wherever it is. */
    private void discardWinnerValue() throws IOException
    {
        if (winnerValueLocation() == ValueLocation.IN_SOURCE)
            cellWinner.source.skipCellValue();
        else
            cellWinner.buffer = null;
    }

    /**
     * Converts a lapsed TTL into a tombstone and drops the value the cell no longer carries.
     *
     * @return the flags to serialize
     * @see org.apache.cassandra.db.rows.AbstractCell#purge(org.apache.cassandra.db.DeletionPurger, long)
     */
    private int applyExpiredTtl(int cellFlags) throws IOException
    {
        if (!Cell.Serializer.isExpiring(cellFlags) || !cellWinner.liveness.isExpired(nowInSec))
            return cellFlags;

        cellWinner.liveness.ttlToTombstone();
        if (!Cell.Serializer.hasValue(cellFlags))
            return cellFlags;

        discardWinnerValue();
        return cellFlags | Cell.Serializer.HAS_EMPTY_VALUE_MASK;
    }

    /**
     * Rewrites the flags to match what the merge actually produced.  Cell.Serializer treats deleted
     * and expiring as mutually exclusive, so a tombstone never carries IS_EXPIRING or a TTL field.
     *
     * @see Cell.Serializer#serialize(Cell, ColumnMetadata, DataOutputPlus, LivenessInfo, org.apache.cassandra.db.SerializationHeader)
     */
    private int rewriteCellFlags(int cellFlags, LivenessInfo rowLiveness)
    {
        ReusableCellLivenessInfo cellLiveness = cellWinner.liveness;
        boolean isExpiring = cellLiveness.isExpiring();
        boolean useRowTimestamp = !rowLiveness.isEmpty() && cellLiveness.timestamp() == rowLiveness.timestamp();
        boolean useRowTTL = isExpiring && rowLiveness.isExpiring()
                            && cellLiveness.ttl() == rowLiveness.ttl()
                            && cellLiveness.localDeletionTime() == rowLiveness.localExpirationTime();

        int flags = cellFlags & Cell.Serializer.HAS_EMPTY_VALUE_MASK;
        if (cellLiveness.isTombstone()) flags |= Cell.Serializer.IS_DELETED_MASK;
        else if (isExpiring) flags |= Cell.Serializer.IS_EXPIRING_MASK;
        if (useRowTimestamp) flags |= Cell.Serializer.USE_ROW_TIMESTAMP_MASK;
        if (useRowTTL) flags |= Cell.Serializer.USE_ROW_TTL_MASK;
        return flags;
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

            if (!shapeMergedMarker(rangeTombstone, previousDeletionTimeInMerged, newDeletionTimeInMerged, isBeforeClustering))
                return false;

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

    /**
     * Rewrites the descriptor as the marker this merge step produces: a start bound, an end bound,
     * or a boundary that closes one deletion and opens another. Combines the merge and the purge,
     * so a marker whose deletions are all purged is never written.
     *
     * @return false if nothing survives the purge and the caller must write nothing
     */
    private boolean shapeMergedMarker(UnfilteredDescriptor rangeTombstone,
                                      DeletionTime previousDeletionTimeInMerged,
                                      DeletionTime newDeletionTimeInMerged,
                                      boolean isBeforeClustering)
    {
        boolean purgeClose = previousDeletionTimeInMerged != DeletionTime.LIVE
                             && purger.shouldPurge(previousDeletionTimeInMerged);
        boolean purgeOpen = newDeletionTimeInMerged != DeletionTime.LIVE
                            && purger.shouldPurge(newDeletionTimeInMerged);

        boolean opens = newDeletionTimeInMerged != DeletionTime.LIVE && !purgeOpen;
        boolean closes = previousDeletionTimeInMerged != DeletionTime.LIVE && !purgeClose;

        if (opens && closes)
        {
            rangeTombstone.clusteringKind(isBeforeClustering ? EXCL_END_INCL_START_BOUNDARY : INCL_END_EXCL_START_BOUNDARY);
            rangeTombstone.deletionTime().reset(previousDeletionTimeInMerged); // close
            rangeTombstone.deletionTime2().reset(newDeletionTimeInMerged); // open
            return true;
        }
        if (opens)
        {
            rangeTombstone.clusteringKind(isBeforeClustering ? INCL_START_BOUND : EXCL_START_BOUND);
            rangeTombstone.deletionTime().reset(newDeletionTimeInMerged);
            return true;
        }
        if (closes)
        {
            rangeTombstone.clusteringKind(isBeforeClustering ? EXCL_END_BOUND : INCL_END_BOUND);
            rangeTombstone.deletionTime().reset(previousDeletionTimeInMerged);
            return true;
        }
        return false;
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
     * Takes the steal owed to a cursor, if any. It waits a round because the descriptor only reaches
     * the prev slot on the next read. See {@link StatefulCursor#detachPrevPartition}.
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

    /** Steals the descriptor of the unfiltered just written; see {@link StatefulCursor#detachUnfiltered}. */
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
    /**
     * Loads a new partition key from every cursor sitting on a partition edge, which is every
     * cursor whose partition the last merge consumed. Exhausted cursors are at the bottom and
     * mid-read partitions are in the middle, so the walk stops at the first cursor that has not
     * moved.
     *
     * @return the count of cursors at the top of the array that may now be out of order
     */
    private int readPartitionHeadersOnEdge() throws IOException
    {
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
                return perturbedCursors;
            }
            else if (sstableCursorState == DONE)
            {
                if (!sstableCursor.resetAfterDone())
                    return perturbedCursors;
                updateTotalBytesRead(sstableCursor);
            }
            else
            {
                throw new IllegalStateException("Cursor is in an unexpected state:" + sstableCursor);
            }
        }
        return perturbedCursors;
    }

    private int prepareAndSortForPartitionMerge() throws IOException
    {
        int perturbedCursors = readPartitionHeadersOnEdge();
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
        // move cursors that need to move past the row header
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
        // The row's cells are done: the lead cursor is at the next row, partition or EOF. Read
        // state() once; it validates the current cell when corrupt-tombstone validation is on.
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
        new ColumnMergeSort<>(CursorCompactor::compareByColumnAndPath);

    /**
     * A cursor at its terminal state sorts after one that is not. Each comparator has its own
     * terminal state, so it passes that in.
     *
     * <p>Takes the states rather than the cursors: {@link StatefulCursor#state()} validates the
     * current cell when corrupt-tombstone validation is on, so a caller reads it exactly once.
     *
     * @return the comparison, or {@link #NO_TERMINAL_DECISION} when both cursors are live and the
     *         caller must compare them itself
     */
    @VisibleForTesting
    static int compareByTerminalState(int s1, int s2, int terminalState)
    {
        if (s1 == terminalState && s2 == terminalState) return 0;
        if (s1 == terminalState) return 1;
        if (s2 == terminalState) return -1;
        return NO_TERMINAL_DECISION;
    }

    /** Outside the range of any real comparison, so it cannot collide with one. */
    @VisibleForTesting
    static final int NO_TERMINAL_DECISION = Integer.MIN_VALUE;

    private static int compareByPartitionKey(StatefulCursor c1, StatefulCursor c2)
    {
        if (c1 == c2) return 0;
        int byTerminal = compareByTerminalState(c1.state(), c2.state(), DONE);
        if (byTerminal != NO_TERMINAL_DECISION) return byTerminal;
        return c1.currentKey().compareTo(c2.currentKey());
    }

    private static int compareByStatic(StatefulCursor c1, StatefulCursor c2)
    {
        if (c1 == c2) return 0;
        int tState = c1.state();
        int oState = c2.state();

        int byTerminal = compareByTerminalState(tState, oState, PARTITION_END);
        if (byTerminal != NO_TERMINAL_DECISION) return byTerminal;

        return -Boolean.compare(tState == STATIC_ROW_START, oState == STATIC_ROW_START);
    }

    private static int compareByRowClustering(StatefulCursor c1, StatefulCursor c2)
    {
        if (c1 == c2) return 0;
        int tState = c1.state();
        int oState = c2.state();

        int byTerminal = compareByTerminalState(tState, oState, PARTITION_END);
        if (byTerminal != NO_TERMINAL_DECISION) return byTerminal;
        // Either have cells, or an empty row
        boolean tIsAfterHeader = isState(tState, CELL_HEADER_START | UNFILTERED_END);
        boolean oIsAfterHeader = isState(oState, CELL_HEADER_START | UNFILTERED_END);
        if (tIsAfterHeader && oIsAfterHeader)
            return ClusteringComparator.compare(c1.unfiltered(), c2.unfiltered());
        else
            throw new IllegalStateException("We only sort through rows ready to be merged/copied. c1 = " + c1 + ", c2 = " + c2);
    }

    private static int compareByColumnAndPath(StatefulCursor c1, StatefulCursor c2)
    {
        if (c1 == c2) return 0;
        int tState = c1.state();
        int oState = c2.state();
        int byTerminal = compareByTerminalState(tState, oState, UNFILTERED_END);
        if (byTerminal != NO_TERMINAL_DECISION) return byTerminal;

        boolean tIsAfterHeader = isState(tState, CELL_VALUE_START | CELL_END);
        boolean oIsAfterHeader = isState(oState, CELL_VALUE_START | CELL_END);
        if (!(tIsAfterHeader && oIsAfterHeader))
            throw new IllegalStateException("We only sort through cells ready to be merged/copied. c1 = " + c1 + ", c2 = " + c2);

        SSTableCursorReader.CellCursor cc1 = c1.cellCursor();
        SSTableCursorReader.CellCursor cc2 = c2.cellCursor();
        int byColumn = cc1.cellColumn.compareTo(cc2.cellColumn);
        if (byColumn != 0 || !cc1.cellColumn.isComplex())
            return byColumn;
        // The two cursors are at the same complex column. A deletion-only position has no cell,
        // and sorts before every cell, so the deletion sources of the column come first.
        if (!cc1.producedCell || !cc2.producedCell)
            return Boolean.compare(cc1.producedCell, cc2.producedCell);
        // The cell cursor resolves cellPathType once per column. Pass it here, because this
        // comparator runs once per cell per source.
        return comparePaths(cc1.cellColumn, cc1.cellPathType, cc1.cellPathWindow(), cc2.cellPathWindow());
    }

    /**
     * Compares two cell paths of one complex column. The order must agree with
     * {@link ColumnMetadata#cellPathComparator()}, which sets both the cell order that flush
     * writes to disk and the merge grouping of the iterator.
     */
    @VisibleForTesting
    static int comparePaths(ColumnMetadata column, ByteBuffer p1, ByteBuffer p2)
    {
        return comparePaths(column, ColumnMetadata.pathNameComparator(column.type), p1, p2);
    }

    /**
     * @param pathType the type that compares the path bytes: see
     *                 {@link ColumnMetadata#pathNameComparator}. Map keys, set elements and list
     *                 timeuuids compare by their type, not as raw bytes.
     *                 {@link SSTableCursorReader.CellCursor} resolves this type once per column.
     */
    private static int comparePaths(ColumnMetadata column, AbstractType<?> pathType, ByteBuffer p1, ByteBuffer p2)
    {
        // Today only CollectionType and UserType are multi-cell, and pathNameComparator handles
        // both, so no column that reaches this comparator has a null path type. If a new
        // multi-cell type is added and is not handled, fail here. Do not fall back to raw byte
        // order, because that order can differ from the reference order.
        if (pathType == null)
            throw new IllegalStateException("No cell-path comparator for multi-cell type: " + column.type + " (column " + column.name + ")");
        return pathType.compare(p1, p2);
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

    public long[] getMergedRowsCounts()
    {
        return rowMergeCounters;
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
     * <p>
     * An {@link SSTableSimpleScanner} hands its data-file segments to the cursor, so a scanner over part of an
     * sstable gives a cursor over that part. Any other scanner passed {@link #unsupportedScanners}, so it is a
     * full-range one, and each of its sstables gets a whole-file cursor.
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
            for (ISSTableScanner scanner : scanners)
            {
                if (scanner instanceof SSTableSimpleScanner)
                {
                    SSTableReader reader = Iterables.getOnlyElement(scanner.getBackingSSTables());
                    cursors[i++] = new StatefulCursor(reader, ((SSTableSimpleScanner) scanner).positionBounds(), diskAccessMode);
                }
                else
                {
                    for (SSTableReader reader : scanner.getBackingSSTables())
                        cursors[i++] = new StatefulCursor(reader, diskAccessMode);
                }
            }
            assert i == cursors.length : "cursor count " + i + " differs from sstable count " + cursors.length;
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

        // Every argument is a supplier: the builder is a no-op when INFO is off, so none of these
        // histograms is built or summed unless the line is actually logged.
        LOGGER.atInfo()
              .setMessage("Compaction ended {}: { data bytes read = {}, data bytes written = {}, input (keys = {}, static rows = {}, rows = {}, range tombstones = {}, cells = {}), output (keys = {}, static rows = {}, rows = {}, range tombstones = {}, cells = {})}")
              .addArgument(compactionId)
              .addArgument(this::getTotalBytesScanned)
              .addArgument(() -> totalDataBytesWritten)
              .addArgument(() -> mergeHistogramToString(partitionMergeCounters))
              .addArgument(() -> mergeHistogramToString(staticRowMergeCounters))
              .addArgument(() -> mergeHistogramToString(rowMergeCounters))
              .addArgument(() -> mergeHistogramToString(rangeTombstonesMergeCounters))
              .addArgument(() -> mergeHistogramToString(cellMergeCounters))
              .addArgument(() -> sumHistogram(partitionMergeCounters))
              .addArgument(() -> sumHistogram(staticRowMergeCounters))
              .addArgument(() -> sumHistogram(rowMergeCounters))
              .addArgument(() -> sumHistogram(rangeTombstonesMergeCounters))
              .addArgument(() -> sumHistogram(cellMergeCounters))
              .log();
    }

}
