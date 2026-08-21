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

package org.apache.cassandra.io.sstable;

import java.io.IOException;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.config.Config.DiskAccessMode;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.ReusableLivenessInfo;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.ReusableCellLivenessInfo;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.db.rows.UnfilteredSerializer;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.ResizableByteBuffer;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.tools.Util;
import org.apache.cassandra.utils.concurrent.Ref;

import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.CELL_END;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.CELL_HEADER_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.CELL_VALUE_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.DONE;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.PARTITION_END;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.PARTITION_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.ROW_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.SEEK;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.STATIC_ROW_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.TOMBSTONE_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.UNFILTERED_END;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.isState;

public class SSTableCursorReader implements AutoCloseable
{
    private static final ColumnMetadata[] COLUMN_METADATA_TYPE = new ColumnMetadata[0];
    private final boolean hasStaticColumns;

    public interface State
    {
        /** start of file, after partition end but before EOF */
        int PARTITION_START = 1;
        int STATIC_ROW_START = 1 << 1;
        int ROW_START = 1 << 2;
        /** common to row/static row cells */
        int CELL_HEADER_START = 1 << 3;
        int CELL_VALUE_START = 1 << 4;
        int CELL_END = 1 << 5;
        int TOMBSTONE_START = 1 << 6;

        /** common to rows/tombstones. Call continue(); for next unfiltered, or maybe partition end */
        int UNFILTERED_END = 1 << 7;
        /** at {@link UnfilteredSerializer#isEndOfPartition(int)} */
        int PARTITION_END = 1 << 8;
        /** EOF */
        int DONE = 1 << 9;

        /* Special case for seeking in file */
        int SEEK = 1 << 10;
        static boolean isState(int state, int mask) {
            return (state & mask) != 0;
        }
    }

    /** {@link CellCursor#readCellHeader()} result: no cell surfaced — every remaining
     *  column in this row was dropped-column filtered */
    static final int CELL_NONE_REMAINING = -1;
    /** {@link CellCursor#readCellHeader()} result: cell surfaced with no value (tombstone) */
    static final int CELL_NO_VALUE = 0;
    /** {@link CellCursor#readCellHeader()} result: cell surfaced with a value */
    static final int CELL_HAS_VALUE = 1;

    public class CellCursor {
        public ReusableLivenessInfo rowLiveness;
        public Columns columns;

        public int columnsSize;
        public int cellFlags;
        public final ReusableCellLivenessInfo cellLiveness = new ReusableCellLivenessInfo();
        public CellPath cellPath;
        public AbstractType<?> cellType;
        public ColumnMetadata cellColumn;
        private ColumnMetadata[] columnsArray;
        private AbstractType<?>[] cellTypeArray;
        // Parallel to columnsArray: each column's drop horizon, or DeserializationHelper.NO_DROP_HORIZON
        // if none. That sentinel is Long.MIN_VALUE and so is NOT out of band for a timestamp — the drop
        // test must go through DeserializationHelper.isDroppedAtHorizon, which checks the sentinel first;
        // a bare "timestamp <= droppedTimeArray[i]" filters a cell timestamped LivenessInfo.NO_TIMESTAMP
        // on a column that was never dropped, which the iterator path keeps. Built once per superset
        // change (see init) so the per-cell drop check is a plain array read instead of a
        // ByteBuffer-keyed map lookup on sstableHasDroppedColumns tables.
        private long[] droppedTimeArray;

        // Remaining PRESENT columns of this row as a bitmask over columnsArray indices.
        // Garbage-free sparse-row iteration: rows that do not contain every header column
        // pass the missing-columns mask (or, for >= 64-column supersets, present-mask
        // words) instead of a freshly allocated Columns subset, so the identity cache
        // below only rebuilds on a genuine superset change (stable per reader).
        private long presentMask;
        // >= 64-column supersets: present-mask words (bit i of word i/64 = superset column
        // i present), walked word by word. Grow-once scratch, consumed destructively.
        private long[] presentWords;
        private int presentWordsCount;
        private int presentWordIndex;

        void init (Columns columns, long missingColumnsMask, long[] presentColumnsWords, ReusableLivenessInfo rowLiveness)
        {
            // the sstable-scoped dropped-column flag is only sound while the superset comes from
            // this sstable's header; a schema-derived Columns here would under-filter silently
            assert columns == serializationHeader.columns(false) || columns == serializationHeader.columns(true)
                 : "cell superset must be one of this sstable's header column sets";
            if (this.columns != columns)
            {
                // This will be a problem with changing columns
                this.columns = columns;
                columnsArray = columns.toArray(COLUMN_METADATA_TYPE);
                cellTypeArray = new AbstractType<?>[columnsArray.length];
                droppedTimeArray = sstableHasDroppedColumns ? new long[columnsArray.length] : null;
                for (int i = 0; i < columnsArray.length; i++)
                {
                    cellTypeArray[i] = serializationHeader.getType(columnsArray[i]);
                    if (sstableHasDroppedColumns)
                        droppedTimeArray[i] = deserializationHelper.droppedTimeOrMin(columnsArray[i]);
                }
                columnsSize = columns.size();
            }
            if (columnsSize >= 64)
            {
                // word-mask walk over the superset; the descriptor decoded the large-subset
                // wire format into presentColumnsWords (null = all columns present)
                int nWords = (columnsSize + 63) >>> 6;
                if (presentWords == null || presentWords.length < nWords)
                    presentWords = new long[nWords]; // grow-once, amortized zero
                presentWordsCount = nWords;
                presentWordIndex = 0;
                if (presentColumnsWords != null)
                {
                    System.arraycopy(presentColumnsWords, 0, presentWords, 0, nWords);
                }
                else
                {
                    java.util.Arrays.fill(presentWords, 0, nWords, -1L);
                    if ((columnsSize & 63) != 0)
                        presentWords[nWords - 1] = -1L >>> (64 - (columnsSize & 63));
                }
                presentMask = 0;
            }
            else
            {
                // Build the present-columns bitmask from the wire's MISSING-columns mask:
                //   -1L >>> (64 - n)   is the "n low ones" template (e.g. n=3 -> 0b111): all 64
                //                      bits set, then shifted so exactly the n column bits remain.
                //                      n == 0 must be special-cased because Java shifts are mod 64
                //                      (>>> 64 is a no-op, NOT zero).
                //   ~missingColumnsMask flips missing->present but also sets every bit ABOVE the
                //                      column range, so it is ANDed with the template to trim them.
                presentMask = ~missingColumnsMask & (columnsSize == 0 ? 0 : (-1L >>> (64 - columnsSize)));
            }
            this.rowLiveness = rowLiveness;
            cellFlags = 0;
            cellPath = null;
            cellType = null;
        }

        public boolean hasNext()
        {
            return columnsSize >= 64 ? columnsRemain() : presentMask != 0;
        }

        private boolean columnsRemain()
        {
            // advance to the next non-empty word; position is retained across calls
            while (presentWordIndex < presentWordsCount)
            {
                if (presentWords[presentWordIndex] != 0)
                    return true;
                presentWordIndex++;
            }
            return false;
        }

        /**
         * For Cell deserialization see {@link Cell.Serializer#deserialize}
         *
         * Dropped-column filtering happens here, mirroring the iterator's deserialization:
         * cells of a dropped column written at or before the drop are consumed and never
         * surfaced; the loop advances to the next column in that case. A dropped column
         * that turns out to be the row's last remaining column leaves NO cell at all for
         * this position (distinct from a genuine valueless cell/tombstone), which is why
         * this returns a tri-state rather than a plain hasValue boolean: the caller must
         * skip straight past the row/unfiltered end rather than stopping at a cell that
         * doesn't exist.
         *
         * @return 1 if the next cell has a value, 0 if it has none (tombstone), -1 if no
         *         cell remains in this row (all trailing columns were dropped-filtered)
         */
        int readCellHeader() throws IOException
        {
            if (!hasNext()) throw new IllegalStateException();

            for (;;)
            {
                // HOTSPOT: suprisingly expensive
                int currIndex;
                if (columnsSize >= 64)
                {
                    // columnsRemain() (via hasNext() above, or the loop-continue path below)
                    // parked presentWordIndex on a non-empty word; same low-to-high bit walk
                    // as the single-mask path below
                    long word = presentWords[presentWordIndex];
                    currIndex = (presentWordIndex << 6) + Long.numberOfTrailingZeros(word);
                    presentWords[presentWordIndex] = word & (word - 1);
                }
                else
                {
                    // Bit i of presentMask corresponds to the i-th column of the superset in
                    // its iteration order — the SAME order the serializer assigned bits and
                    // the same order cells appear on disk. Walking bits low-to-high therefore
                    // visits cells in exactly their on-disk order:
                    //   numberOfTrailingZeros = index of the lowest set bit (next present column)
                    //   x & (x - 1)           = clears that lowest set bit (subtracting 1 borrows
                    //                           through the trailing zeros; the AND kills both)
                    currIndex = Long.numberOfTrailingZeros(presentMask);
                    presentMask &= presentMask - 1;
                }
                cellColumn = columnsArray[currIndex];
                cellType = cellTypeArray[currIndex];
                cellFlags = dataReader.readUnsignedByte();
                // TODO: specialize common case where flags == HAS_VALUE | USE_ROW_TS?
                boolean hasValue = Cell.Serializer.hasValue(cellFlags);
                boolean isDeleted = Cell.Serializer.isDeleted(cellFlags);
                boolean isExpiring = Cell.Serializer.isExpiring(cellFlags);
                boolean useRowTimestamp = Cell.Serializer.useRowTimestamp(cellFlags);
                boolean useRowTTL = Cell.Serializer.useRowTTL(cellFlags);

                long timestamp = useRowTimestamp ? rowLiveness.timestamp() : serializationHeader.readTimestamp(dataReader);

                long localDeletionTime = useRowTTL
                                         ? rowLiveness.localExpirationTime()
                                         : (isDeleted || isExpiring ? serializationHeader.readLocalDeletionTime(dataReader) : Cell.NO_DELETION_TIME);

                int ttl = useRowTTL ? rowLiveness.ttl() : (isExpiring ? serializationHeader.readTTL(dataReader) : Cell.NO_TTL);
                localDeletionTime = Cell.decodeLocalDeletionTime(localDeletionTime, ttl, deserializationHelper);

                cellLiveness.reset(timestamp, ttl, localDeletionTime);
                // Complex (multi-cell) columns never reach the cell cursor: CursorCompactor's
                // unsupportedSchema and unsupportedHeaderColumns gates both reject them before
                // compaction starts, including a dropped complex column still recorded in an
                // older sstable's header — see unsupportedHeaderColumns's javadoc for why that
                // needs its own check. cellPath is therefore always null on this path; assert
                // the invariant instead of paying a per-cell isComplex() dispatch and a dead
                // deserialize call.
                assert !cellColumn.isComplex() : "complex column reached the cell cursor: " + cellColumn;
                cellPath = null;

                // Equivalent to deserializationHelper.isDropped(cellColumn, timestamp, false), but
                // via the precomputed per-superset array instead of a ByteBuffer-keyed map lookup
                // per cell (isDropped's isComplex=false path would look up droppedColumns.get(
                // column.name.bytes) every time; this reader never primes startOfComplexColumn's
                // cache, so isComplex=true was never an option here either). isDroppedAtHorizon
                // carries the sentinel test that keeps the two forms equivalent — see its javadoc.
                if (sstableHasDroppedColumns && DeserializationHelper.isDroppedAtHorizon(timestamp, droppedTimeArray[currIndex]))
                {
                    // mirror UnfilteredSerializer.readSimpleColumn: cells of a dropped column
                    // written at or before the drop are discarded on read
                    if (hasValue)
                        cellType.skipValue(dataReader);
                    if (!hasNext())
                        return CELL_NONE_REMAINING; // caller must skip past the row/unfiltered end
                    continue;
                }
                return hasValue ? CELL_HAS_VALUE : CELL_NO_VALUE;
            }
        }
    }

    private final Ref<SSTableReader> ssTableReaderRef;
    private final AbstractType<?>[] clusteringColumnTypes;
    private final DeserializationHelper deserializationHelper;
    private final SerializationHeader serializationHeader;
    // True when a column of THIS sstable's header carries a drop horizon; the helper's
    // identically-purposed flag is table-scoped, hence the name. Sstable scope is sound because the
    // cell cursor's superset comes from serializationHeader.columns() — asserted in CellCursor.init
    // — so a column absent from this header can never reach readCellHeader. Held as a field so an
    // sstable with no dropped column builds no droppedTimeArray at all (see CellCursor.init) and
    // costs a boolean field test per cell rather than an array load and a sentinel compare.
    private final boolean sstableHasDroppedColumns;

    // need to be closed
    private final SSTableReader ssTableReader;
    private final RandomAccessReader dataReader;
    private final DeletionTime.Serializer deletionTimeSerializer;

    private final CellCursor staticRowCellCursor = new CellCursor();
    private final CellCursor rowCellCursor = new CellCursor();


    private CellCursor cellCursor;

    // SHARED STATIC_ROW/ROW/TOMB
    private int basicUnfilteredFlags = 0;
    private int extendedFlags = 0;

    // Where the unfiltered whose cells are being read must end: dataStart + unfilteredSize, captured
    // from the descriptor when the header was read. Nothing else checks that the cell walk consumes
    // exactly the body the header declared, so without this a cell-level desync runs on into the NEXT
    // unfiltered and surfaces far from its cause.
    // Only the cell-walk funnel below consumes it, and it is cleared there and on markers. Paths that
    // end an unfiltered WITHOUT walking cells — a row with no present columns, skipRowCells, a seek —
    // leave it holding the closed row's end, which is why the check is scoped to that one funnel
    // rather than applied wherever an unfiltered ends.
    private static final long NO_UNFILTERED_END = -1;
    private long unfilteredEnd = NO_UNFILTERED_END;

    // Where the cell walk of the unfiltered currently being read BEGAN, so it can be walked again;
    // see rewindRowCells. Not derivable from the descriptor: its dataStart() is the start of the row
    // BODY, which still has previousUnfilteredSize, the liveness, the deletion and the missing-columns
    // subset ahead of the first cell.
    private long unfilteredCellsStart = NO_UNFILTERED_END;

    private int state = PARTITION_START;

    public static SSTableCursorReader fromDescriptor(Descriptor desc) throws IOException
    {
        TableMetadata metadata = Util.metadataFromSSTable(desc);
        SSTableReader reader = SSTableReader.openNoValidation(null, desc, TableMetadataRef.forOfflineTools(metadata));
        return new SSTableCursorReader(reader, metadata, reader.ref(), null);
    }

    public SSTableCursorReader(SSTableReader reader)
    {
        this(reader, reader.metadata(), null, null);
    }

    public SSTableCursorReader(SSTableReader reader, DiskAccessMode diskAccessMode)
    {
        this(reader, reader.metadata(), null, diskAccessMode);
    }

    private SSTableCursorReader(SSTableReader reader, TableMetadata metadata, Ref<SSTableReader> readerRef, DiskAccessMode diskAccessMode)
    {
        ssTableReader = reader;
        ssTableReaderRef = readerRef;
        Version version = reader.descriptor.version;
        deletionTimeSerializer = DeletionTime.getSerializer(version);
        ImmutableList<ColumnMetadata> clusteringColumns = metadata.clusteringColumns();
        int clusteringColumnCount = clusteringColumns.size();
        clusteringColumnTypes = new AbstractType<?>[clusteringColumnCount];
        for (int i = 0; i < clusteringColumnTypes.length; i++)
        {
            clusteringColumnTypes[i] = clusteringColumns.get(i).type;
        }
        deserializationHelper = new DeserializationHelper(metadata, version.correspondingMessagingVersion(), DeserializationHelper.Flag.LOCAL, null);
        serializationHeader = reader.header;
        sstableHasDroppedColumns = anyDroppedColumn(deserializationHelper, serializationHeader);

        dataReader = reader.openDataReaderForScan(diskAccessMode);
        // the HEADER decides whether this sstable can contain static rows: after
        // ALTER TABLE ... DROP of the last static column, current metadata has no static
        // columns but older sstables legitimately still carry static rows
        hasStaticColumns = serializationHeader.hasStatic();
    }

    @Override
    public void close()
    {
        dataReader.close();
        if (ssTableReaderRef != null)
            ssTableReaderRef.close();
    }

    private static boolean anyDroppedColumn(DeserializationHelper deserializationHelper, SerializationHeader header)
    {
        if (!deserializationHelper.hasDroppedColumns())
            return false;
        // RegularAndStaticColumns iterates statics then regulars, so this covers both
        for (ColumnMetadata column : header.columns())
        {
            if (deserializationHelper.isDroppedColumn(column))
                return true;
        }
        return false;
    }

    private void resetOnPartitionStart()
    {
        basicUnfilteredFlags = 0;
        extendedFlags = 0;
    }

    public int seekPartition(long position)
    {
        state = SEEK;
        if (position == 0)
        {
            dataReader.seek(position);
            state = PARTITION_START;
        }
        else {
            // verify partition start is after a partition end marker
            dataReader.seek(position - 1);
            try
            {
                basicUnfilteredFlags = dataReader.readUnsignedByte();
            }
            catch (Exception e)
            {
                return corruptSSTable(e);
            }
            // end of partition
            if (!UnfilteredSerializer.isEndOfPartition(basicUnfilteredFlags)) {
                throw new IllegalArgumentException("Seeking to a partition at: " + position + " did not result in a valid state");
            }
            state = dataReader.isEOF() ? DONE : PARTITION_START;
        }
        resetOnPartitionStart();
        return state;
    }

    public int seekUnfiltered(long position)
    {
        state = SEEK;
        // partition elements (Unfiltered) have flags
        dataReader.seek(position);
        int state = 0;
        try
        {
            state = checkNextFlagsAfterStaticRowOrUnfilteredStart(false);
        }
        catch (IOException e)
        {
            return corruptSSTable(e);
        }
        if (!isState(state , ROW_START | TOMBSTONE_START | DONE)) throw new IllegalStateException();
        return state;
    }

    // struct partition {
    //   struct partition_header header
    //   optional<struct row> row
    //   struct unfiltered unfiltereds[];
    //};
    public int readPartitionHeader(PartitionDescriptor header)
    {
        if (state != PARTITION_START) throw new IllegalStateException();
        resetOnPartitionStart();
        try
        {
            header.load(dataReader, deletionTimeSerializer);
            return checkNextFlagsAfterPartitionStart(false);
        }
        catch (Exception e)
        {
            return corruptSSTable(e);
        }
    }

    // struct static_row {
    //    byte flags;          // preloaded
    //    byte extended_flags; // preloaded
    //    varint row_body_size;
    //    varint prev_unfiltered_size; // for backward traversing, ignored
    //    optional<struct liveness_info> liveness_info;
    //    optional<struct delta_deletion_time> deletion_time;
    // ***  We read the columns in a separate method ***
    //      optional<varint[]> missing_columns;
    //      cell[] cells; // potentially only some
    //};
    public int readStaticRowHeader(UnfilteredDescriptor unfilteredDescriptor)
    {
        if (state != STATIC_ROW_START) throw new IllegalStateException();
        try
        {
            unfilteredDescriptor.loadStaticRow(dataReader, serializationHeader, deserializationHelper, basicUnfilteredFlags, extendedFlags);
            unfilteredEnd = unfilteredDescriptor.dataStart() + unfilteredDescriptor.size();
            unfilteredCellsStart = dataReader.getPosition();
        }
        catch (IOException e)
        {
            return corruptSSTable(e);
        }

        staticRowCellCursor.init(unfilteredDescriptor.rowColumns(), unfilteredDescriptor.missingColumnsMask(),
                                 unfilteredDescriptor.presentColumnsWords(), unfilteredDescriptor.livenessInfo());
        cellCursor = staticRowCellCursor;
        if (!staticRowCellCursor.hasNext())
        {
            try
            {
                return checkNextFlagsAfterStaticRowOrUnfilteredStart(false);
            }
            catch (Exception e)
            {
                return corruptSSTable(e);
            }
        }
        else
        {
            return state = State.CELL_HEADER_START;
        }
    }

    public int copyCellValue(DataOutputPlus writer, byte[] buffer) throws IOException
    {
        if (state != CELL_VALUE_START) throw new IllegalStateException();
        if (cellCursor.cellType == null) throw new IllegalStateException();
        int length = cellCursor.cellType.valueLengthIfFixed();
        copyCellContents(writer, buffer, length);

        try
        {
            if (!cellCursor.hasNext())
            {
                try
                {
                    return checkNextFlagsAfterCellValuesEnd();
                }
                catch (Exception e)
                {
                    return corruptSSTable(e);
                }
            }
            return state = State.CELL_END;
        }
        catch (Exception e)
        {
            return corruptSSTable(e);
        }
    }

    // TODO: move to cell cursor? maybe avoid copy through buffer?
    private void copyCellContents(DataOutputPlus writer, byte[] transferBuffer, int length) throws IOException
    {
        if (length < 0)
        {
            // variable length: the wire carries a length vint, which is mirrored to the output
            try
            {
                length = dataReader.readUnsignedVInt32();
            }
            catch (IOException e)
            {
                corruptSSTable(e);
            }
            // both checks mirror AbstractType.read, the reference for this wire format
            if (length < 0)
                corruptSSTable("Corrupt (negative) value length encountered");
            if (length > DatabaseDescriptor.getMaxValueSize())
                corruptSSTable(String.format("Corrupt value length %d encountered, as it exceeds the maximum of %d, " +
                                             "which is set via max_value_size in cassandra.yaml",
                                             length, DatabaseDescriptor.getMaxValueSize()));
            writer.writeUnsignedVInt32(length);
        }
        // Fixed-length values carry no vint but are not bounded by the transfer buffer either:
        // valueLengthIfFixed() is 6144 for a vector<float, 1536>. Both cases copy in chunks.
        int remaining = length;
        while (remaining > 0)
        {
            int chunk = Math.min(remaining, transferBuffer.length);
            try
            {
                dataReader.readFully(transferBuffer, 0, chunk);
            }
            catch (Exception e)
            {
                corruptSSTable(e);
            }
            writer.write(transferBuffer, 0, chunk);
            remaining -= chunk;
        }
    }

    // struct row {
    //    byte flags;
    //    optional<struct clustering_block[]> clustering_blocks;
    //    varint row_body_size;
    //    varint prev_unfiltered_size; // for backward traversing, ignored
    //    optional<struct liveness_info> liveness_info;
    //    optional<struct delta_deletion_time> deletion_time;
    // ***  We read the columns in a separate step ***
    //    optional<varint[]> missing_columns;
    //    cell[] cells; // potentially only some
    //};
    public int readRowHeader(UnfilteredDescriptor unfilteredDescriptor)
    {
        if (state != State.ROW_START) throw new IllegalStateException();
        if (!UnfilteredSerializer.isRow(basicUnfilteredFlags)) throw new IllegalStateException();
        try
        {
            unfilteredDescriptor.loadRow(dataReader, serializationHeader, deserializationHelper, basicUnfilteredFlags, extendedFlags);
            unfilteredEnd = unfilteredDescriptor.dataStart() + unfilteredDescriptor.size();
            unfilteredCellsStart = dataReader.getPosition();

            rowCellCursor.init(unfilteredDescriptor.rowColumns(), unfilteredDescriptor.missingColumnsMask(),
                               unfilteredDescriptor.presentColumnsWords(), unfilteredDescriptor.livenessInfo());
            cellCursor = rowCellCursor;
            if (!rowCellCursor.hasNext())
            {
                return checkNextFlagsAfterStaticRowOrUnfilteredStart(false);
            }
            else
            {
                return state = State.CELL_HEADER_START;
            }
        }
        catch (Exception e)
        {
            return corruptSSTable(e);
        }
    }

    // TODO: introduce cell header class
    public int readCellHeader()
    {
        if (state != State.CELL_HEADER_START) throw new IllegalStateException();
        try
        {
            int cell = cellCursor.readCellHeader();
            if (cell == CELL_NONE_REMAINING)
            {
                // no cell surfaced at all (every remaining column was dropped-column
                // filtered): nothing is current, so advance straight past the would-be
                // CELL_END stop instead of surfacing a cell-less position
                checkNextFlagsAfterCellValuesEnd();
                return continueReading();
            }
            if (cell == CELL_HAS_VALUE)
            {
                return state = State.CELL_VALUE_START;
            }
            if (!cellCursor.hasNext())
                return checkNextFlagsAfterCellValuesEnd();
            return state = State.CELL_END;
        }
        catch (Exception e)
        {
            return corruptSSTable(e);
        }
    }

    public int skipCellValue()
    {
        if (state != State.CELL_VALUE_START) throw new IllegalStateException();
        try
        {
            cellCursor.cellType.skipValue(dataReader);
            return !cellCursor.hasNext() ? checkNextFlagsAfterCellValuesEnd() : (state = State.CELL_HEADER_START);
        }
        catch (Exception e)
        {
            return corruptSSTable(e);
        }
    }

    /**
     * See: {@link org.apache.cassandra.db.rows.UnfilteredSerializer#serialize(RangeTombstoneMarker, SerializationHelper, DataOutputPlus, long, int)}
     * <pre>
     * struct range_tombstone_marker {
     *   byte flags = IS_MARKER;
     *   byte kind_ordinal;
     *   be16 bound_values_count;
     *   struct clustering_block[] clustering_blocks;
     *   varint marker_body_size;
     *   varint prev_unfiltered_size;
     * };
     * struct range_tombstone_bound_marker : range_tombstone_marker {
     *   struct delta_deletion_time deletion_time;
     * };
     * struct range_tombstone_boundary_marker : range_tombstone_marker {
     *   struct delta_deletion_time end_deletion_time;
 *       struct delta_deletion_time start_deletion_time;
     * };
     * </pre>
     *
     */
    public int readTombstoneMarker(UnfilteredDescriptor unfilteredDescriptor)
    {
        try
        {
            if (state != TOMBSTONE_START) throw new IllegalStateException();
            if (!UnfilteredSerializer.isTombstoneMarker(basicUnfilteredFlags)) throw new IllegalStateException();
            unfilteredDescriptor.loadTombstone(dataReader, serializationHeader, basicUnfilteredFlags);
            // a marker has no cells, and loadTombstone does not set dataStart, so the descriptor still
            // holds the previous row's — close the window rather than leave that reachable
            unfilteredEnd = NO_UNFILTERED_END;
            return checkNextFlagsAfterStaticRowOrUnfilteredStart(false);
        }
        catch (Exception e)
        {
            return corruptSSTable(e);
        }
    }


    /**
     * {@link ClusteringPrefix.Serializer#deserializeValuesWithoutSize}
     */
    static void readUnfilteredClustering(RandomAccessReader dataReader, AbstractType<?>[] types, int clusteringColumnsBound, ResizableByteBuffer clustering) throws IOException
    {
        clustering.resetBuffer();
        if (clusteringColumnsBound == 0) {
            return;
        }
        long clusteringBlockHeader = 0;
        int fixedLengthClusteringLength = 0;
        for (int clusteringIndex = 0; clusteringIndex < clusteringColumnsBound; clusteringIndex++)
        {
            // struct clustering_block {
            //    varint clustering_block_header;
            //    simple_cell[] clustering_cells;
            // };
            if (clusteringIndex % 32 == 0)
            {
                if (fixedLengthClusteringLength != 0) {
                    clustering.loadPart(dataReader, fixedLengthClusteringLength);
                    fixedLengthClusteringLength = 0;
                }
                clusteringBlockHeader = dataReader.readUnsignedVInt();
                clustering.writeUnsignedVInt(clusteringBlockHeader);
            }

            // load value if present
            if ((clusteringBlockHeader & 0b11) == 0)
            {
                AbstractType<?> type = types[clusteringIndex];
                if (type.isValueLengthFixed())
                {
                    fixedLengthClusteringLength += type.valueLengthIfFixed();
                }
                else
                {
                    if (fixedLengthClusteringLength != 0) {
                        clustering.loadPart(dataReader, fixedLengthClusteringLength);
                        fixedLengthClusteringLength = 0;
                    }
                    int varLength = dataReader.readUnsignedVInt32();
                    clustering.writeUnsignedVInt(varLength);
                    clustering.loadPart(dataReader, varLength);
                }
            }
            clusteringBlockHeader = clusteringBlockHeader >>> 2;
        }
        if (fixedLengthClusteringLength != 0) clustering.loadPart(dataReader, fixedLengthClusteringLength);
        if (clusteringBlockHeader != 0) {
            throw new IOException("Clustering block upper bits (those not associated with keys) expected to be 0:" + clusteringBlockHeader);
        }
    }

    private static void skipClustering(RandomAccessReader dataReader, AbstractType<?>[] types, int clusteringColumnsBound) throws IOException
    {
        long clusteringBlockHeader = 0;
        for (int clusteringIndex = 0; clusteringIndex < clusteringColumnsBound; clusteringIndex++)
        {
            // struct clustering_block {
            //    varint clustering_block_header;
            //    simple_cell[] clustering_cells;
            // };
            if (clusteringIndex % 32 == 0)
            {
                clusteringBlockHeader = dataReader.readUnsignedVInt();
            }
            // skip value if present
            if ((clusteringBlockHeader & 0b11) == 0)
            {
                AbstractType<?> type = types[clusteringIndex];
                int len = type.isValueLengthFixed() ? type.valueLengthIfFixed() : dataReader.readUnsignedVInt32();
                dataReader.skipBytes(len);
            }
            clusteringBlockHeader = clusteringBlockHeader >>> 2;
        }
        if (clusteringBlockHeader != 0) {
            throw new IOException("Clustering block upper bits (those not associated with keys) expected to be 0:" + clusteringBlockHeader);
        }
    }

    /**
     * {@link UnfilteredSerializer#deserializeRowBody(DataInputPlus, SerializationHeader, DeserializationHelper, int, int, Row.Builder)}
     */
    static void readLivenessInfo(RandomAccessReader dataReader, SerializationHeader serializationHeader, DeserializationHelper deserializationHelper, int flags, ReusableLivenessInfo livenessInfo) throws IOException
    {
        long timestamp = LivenessInfo.NO_TIMESTAMP;
        int ttl = LivenessInfo.NO_TTL;
        long localExpirationTime = LivenessInfo.NO_EXPIRATION_TIME;
        if (UnfilteredSerializer.hasTimestamp(flags))
        {
            // struct liveness_info {
            //    varint64 delta_timestamp;
            //    optional<varint32> delta_ttl;
            //    optional<varint64> delta_local_deletion_time;
            //};
            timestamp = serializationHeader.readTimestamp(dataReader);
            if (UnfilteredSerializer.hasTTL(flags))
            {
                ttl = serializationHeader.readTTL(dataReader);
                localExpirationTime = Cell.decodeLocalDeletionTime(serializationHeader.readLocalDeletionTime(dataReader), ttl, deserializationHelper);
            }
        }
        livenessInfo.reset(timestamp, ttl, localExpirationTime);
    }

    // SKIPPING
    public int skipPartition()
    {
        if (state == PARTITION_END)
            return continueReading();

        if (state == PARTITION_START)
        {
            try
            {
                int partitionKeyLength = dataReader.readUnsignedShort();
                dataReader.skipBytes(partitionKeyLength);

                // PARTITION DELETION TIME
                deletionTimeSerializer.skip(dataReader);
                checkNextFlagsAfterPartitionStart(true);
            }
            catch (Exception e)
            {
                return corruptSSTable(e);
            }
        }
        else if (!isState(state, STATIC_ROW_START | ROW_START | TOMBSTONE_START | PARTITION_END))
        {
            throw new IllegalStateException("Unexpected state: " + state);
        }

        while (!isState(state,PARTITION_START | DONE))
        {
            switch (state)
            {
                case STATIC_ROW_START:
                    state = skipStaticRow(true);
                    break;
                case ROW_START:
                case TOMBSTONE_START:
                    state = skipUnfiltered(true);
                    break;
            }
        }
        return state;
    }

    public int skipStaticRow(boolean autoContinue)
    {
        if (state != State.STATIC_ROW_START) throw new IllegalStateException();

        try
        {
            long rowSize = dataReader.readUnsignedVInt();
            dataReader.seek(dataReader.getPosition() + rowSize);
            return checkNextFlagsAfterStaticRowOrUnfilteredStart(autoContinue);
        }
        catch (IOException e)
        {
            return corruptSSTable(e);
        }
    }

    public int skipUnfiltered(boolean autoContinue)
    {
        if (!isState(state, ROW_START | TOMBSTONE_START))
            throw new IllegalStateException();

        AbstractType<?>[] types = clusteringColumnTypes;
        int clusteringColumnsBound = types.length;
        // tombstone markers have `kind` & `clusteringColumnsBound`
        try
        {
            if (!UnfilteredSerializer.isRow(basicUnfilteredFlags))
            {
                dataReader.readByte();// byte kind =
                clusteringColumnsBound = dataReader.readUnsignedShort();
            }
            /**
             * {@link org.apache.cassandra.db.ClusteringPrefix.Deserializer}
             */
            skipClustering(dataReader, types, clusteringColumnsBound);
            // same for row/tombstone
            long rowSize = dataReader.readUnsignedVInt();
            dataReader.seek(dataReader.getPosition() + rowSize);

            return checkNextFlagsAfterStaticRowOrUnfilteredStart(autoContinue);
        }
        catch (Exception e)
        {
            return corruptSSTable(e);
        }
    }

    public int skipRowCells(long unfilteredDataStart, long unfilteredSize, boolean autoContinue)
    {
        if (!(isState(state,CELL_HEADER_START | CELL_VALUE_START | CELL_END))) throw new IllegalStateException();

        try
        {
            dataReader.seek(unfilteredDataStart + unfilteredSize);
            return checkNextFlagsAfterStaticRowOrUnfilteredStart(autoContinue);
        }
        catch (IOException e)
        {
            return corruptSSTable(e);
        }
    }

    /**
     * Re-positions the cell walk of the row (or static row) that {@code unfilteredDescriptor} describes
     * back to its FIRST cell, so the same cells can be walked a second time.
     *
     * The descriptor is what makes this possible without re-reading the row header: everything
     * {@link #readRowHeader} feeds into {@code CellCursor.init} — the column superset, the
     * missing-columns mask or the present-column words, the row liveness — survives there untouched, so
     * re-running init restores the walk state readRowHeader left. {@code presentColumnsWords()} is
     * COPIED into the cursor's own scratch by init rather than walked in place, which is what makes a
     * second init from the same descriptor sound. Two of {@code CellCursor}'s fields, {@code cellColumn}
     * and {@code cellLiveness}, are NOT reset by init and so still describe the last cell of the first
     * walk; nothing reads either before the next {@code readCellHeader} overwrites them
     * ({@code compareByColumn} refuses any state but {@code CELL_VALUE_START}/{@code CELL_END}).
     *
     * The position comes from {@link #unfilteredCellsStart} rather than the descriptor, whose
     * {@code dataStart()} is the start of the row BODY and so sits ahead of the first cell.
     * {@code unfilteredEnd} is restored from the descriptor: {@link #checkNextFlagsAfterCellValuesEnd}
     * clears it when a walk completes, and its cell-desync check needs it on the second walk as much as
     * on the first.
     *
     * <b>{@link #basicUnfilteredFlags} and {@link #extendedFlags} are deliberately NOT restored.</b> A
     * first walk that ran to the end of the row has overwritten them with the NEXT unfiltered's flags,
     * and this leaves them there. That is sound only because every way out of the second walk re-reads
     * those same bytes at the same offset before anything consults them — {@code
     * checkNextFlagsAfterCellValuesEnd} if the cells are walked again, {@code
     * checkNextFlagsAfterStaticRowOrUnfilteredStart} if {@link #skipRowCells} skips them — so a caller
     * that ends the second walk any other way has to restore them itself.
     *
     * @param isStatic which of the two cell cursors this row's cells are walked with, as
     *        {@link #readStaticRowHeader} and {@link #readRowHeader} choose it
     */
    protected int rewindRowCells(UnfilteredDescriptor unfilteredDescriptor, boolean isStatic)
    {
        dataReader.seek(unfilteredCellsStart);
        unfilteredEnd = unfilteredDescriptor.dataStart() + unfilteredDescriptor.size();
        CellCursor rewound = isStatic ? staticRowCellCursor : rowCellCursor;
        rewound.init(unfilteredDescriptor.rowColumns(), unfilteredDescriptor.missingColumnsMask(),
                     unfilteredDescriptor.presentColumnsWords(), unfilteredDescriptor.livenessInfo());
        cellCursor = rewound;
        // Callers only rewind a cursor that WAS in a cell state, which is exactly the state the row
        // header loaders leave when the row has a present column. No cell surfacing here would mean the
        // descriptor and the state the caller recorded disagree.
        assert rewound.hasNext() : "rewound a row whose columns are all absent: " + unfilteredDescriptor;
        return state = State.CELL_HEADER_START;
    }

    public int continueReading() {
        // TODO: can be optimized by pre-calculating next state when the flags are read
        switch (state)
        {
            case PARTITION_END:
                state = dataReader.isEOF() ? DONE : PARTITION_START;
                break;
            case UNFILTERED_END:
                if (UnfilteredSerializer.isEndOfPartition(basicUnfilteredFlags))
                {
                    state = PARTITION_END;
                }
                else
                {
                    state = UnfilteredSerializer.isRow(basicUnfilteredFlags) ? ROW_START : TOMBSTONE_START;
                }
                break;
            case CELL_END:
                if (cellCursor.hasNext())
                {
                    state = CELL_HEADER_START;
                }
                else
                {
                    state = UNFILTERED_END;
                }
                break;
            default:
                throw new IllegalStateException("Cannot continue reading in current state: " + state);
        }
        return state;
    }

    private int checkNextFlagsAfterPartitionStart(boolean autoContinue) throws IOException
    {
        long preFlagsPosition = dataReader.getPosition();
        basicUnfilteredFlags = dataReader.readUnsignedByte();
        if (UnfilteredSerializer.isEndOfPartition(basicUnfilteredFlags))
        {
            state = !autoContinue ? PARTITION_END :
                                    dataReader.isEOF() ? DONE : PARTITION_START;
        }
        else
        {
            readRowExtendedFlags(basicUnfilteredFlags, true, preFlagsPosition);
            if (UnfilteredSerializer.isStatic(extendedFlags))
            {
                state = STATIC_ROW_START;
                validateStaticRowFlags(preFlagsPosition);
            }
            else
            {
                state = UnfilteredSerializer.isRow(basicUnfilteredFlags) ? ROW_START : TOMBSTONE_START;
            }
        }
        return state;
    }

    /**
     * Reads the extended-flags byte for {@code basicFlags} if present, storing it (0 if absent)
     * in the {@link #extendedFlags} field for the row loader to pick up. Extended flags are only
     * meaningful on a row (not a marker): {@code IS_STATIC}, and — deprecated since 4.0
     * (CASSANDRA-11500), reachable only on old Materialized View data — {@code HAS_SHADOWABLE_DELETION}
     * on either a static or a non-static row. A static row is only legal as the partition's first
     * unfiltered, hence {@code allowStatic}.
     */
    private void readRowExtendedFlags(int basicFlags, boolean allowStatic, long preFlagsPosition) throws IOException
    {
        if (!UnfilteredSerializer.isExtended(basicFlags))
        {
            extendedFlags = 0;
            return;
        }
        if (!UnfilteredSerializer.isRow(basicFlags))
        {
            corruptSSTable("Marker at: " + preFlagsPosition + " has extended flags, flags: " + basicFlags);
            return;
        }
        extendedFlags = dataReader.readUnsignedByte();
        if (!allowStatic && UnfilteredSerializer.isStatic(extendedFlags))
        {
            corruptSSTable("Unexpected static row (flags=" + basicFlags + ") mid-partition, at position: " + preFlagsPosition);
        }
    }

    private void validateStaticRowFlags(long preFlagsPosition)
    {
        if (!UnfilteredSerializer.isRow(basicUnfilteredFlags))
        {
            corruptSSTable("Static row at: " + preFlagsPosition + " is not a row, flags: " + basicUnfilteredFlags);
        }
        if (!hasStaticColumns)
        {
            corruptSSTable("Row at: " + preFlagsPosition + " is static, but table has no static columns " + ssTableReader.metadata());
        }
    }

    private int checkNextFlagsAfterStaticRowOrUnfilteredStart(boolean autoContinue) throws IOException
    {
        long preFlagsPosition = dataReader.getPosition();
        int flags = this.basicUnfilteredFlags = dataReader.readUnsignedByte();
        readRowExtendedFlags(flags, false, preFlagsPosition);

        if (!autoContinue) {
            return this.state = UNFILTERED_END;
        }
        else
        {
            return this.state = nextStateMidPartition(flags);
        }
    }

    private int checkNextFlagsAfterCellValuesEnd() throws IOException
    {
        // The cell walk must have consumed exactly the body the row header declared. Checked here, the
        // single point at which a cell-value walk COMPLETES, and BEFORE the next unfiltered's flag byte
        // is read below, so the position is directly comparable. Aborting the compaction is the intended
        // outcome: a desync means the cells written from here would not match the iterator path's.
        //
        // A decoder defect is as likely as on-disk damage here, but that's the same ambiguity
        // UnfilteredSerializer.readRow's own catch (RuntimeException | AssertionError) already accepts;
        // route through the same corruption handling rather than surface a raw AssertionError.
        if (dataReader.getPosition() != unfilteredEnd)
            corruptSSTable("cell desync: cells consumed to " + dataReader.getPosition()
                            + ", unfiltered body declared end " + unfilteredEnd);
        unfilteredEnd = NO_UNFILTERED_END;

        long preFlagsPosition = dataReader.getPosition();
        int flags = this.basicUnfilteredFlags = dataReader.readUnsignedByte();
        readRowExtendedFlags(flags, false, preFlagsPosition);
        return this.state = CELL_END;
    }

    private int corruptSSTable(Exception e)
    {
        ssTableReader.markSuspect();
        if (e instanceof CorruptSSTableException)
            throw (CorruptSSTableException) e;

        throw new CorruptSSTableException(e, ssTableReader.getFilename());
    }

    protected int corruptSSTable(String message)
    {
        return corruptSSTable(new IllegalStateException(message));
    }

    private int nextStateMidPartition(int basicUnfilteredFlags)
    {
        if (UnfilteredSerializer.isEndOfPartition(basicUnfilteredFlags))
        {
            return dataReader.isEOF() ? DONE : PARTITION_START;
        }
        else if (UnfilteredSerializer.isRow(basicUnfilteredFlags))
        {
            return ROW_START;
        }
        else
        {
            return TOMBSTONE_START;
        }
    }

    public boolean isEOF() {
        return state == DONE || dataReader.isEOF();
    }

    public int state()
    {
        return state;
    }

    public long position() {
        return dataReader.getFilePointer();
    }

    public long uncompressedLength()
    {
        return ssTableReader.uncompressedLength();
    }

    public SSTableReader ssTableReader()
    {
        return ssTableReader;
    }

    public CellCursor cellCursor()
    {
        return cellCursor;
    }
}
