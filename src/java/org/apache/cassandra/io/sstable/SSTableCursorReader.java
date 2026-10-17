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
import java.nio.ByteBuffer;
import java.util.Collection;

import com.google.common.annotations.VisibleForTesting;
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
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.ReusableCellLivenessInfo;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.db.rows.UnfilteredSerializer;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReader.PartitionPositionBounds;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
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

        /** common to rows and tombstones. Call {@link SSTableCursorReader#continueReading} for the
         *  next unfiltered, or for the partition end */
        int UNFILTERED_END = 1 << 7;
        /** at {@link UnfilteredSerializer#isEndOfPartition(int)} */
        int PARTITION_END = 1 << 8;
        /** no segment left to read; EOF for a whole-file cursor */
        int DONE = 1 << 9;

        static boolean isState(int state, int mask) {
            return (state & mask) != 0;
        }
    }

    /** {@link CellCursor#readCellHeader()} result: no cell. No column left in this row gives
     *  one. */
    static final int CELL_NONE_REMAINING = -1;
    /** {@link CellCursor#readCellHeader()} result: a cell with no value, that is, a tombstone;
     *  or a complex column with no cell. */
    static final int CELL_NO_VALUE = 0;
    /** {@link CellCursor#readCellHeader()} result: a cell with a value. */
    static final int CELL_HAS_VALUE = 1;
    /** {@link CellCursor#openNextColumnRun()} result: a column with a cell to read is open. This
     *  value is outside the range of the three results above, which that method passes through. */
    static final int COLUMN_RUN_OPEN = 2;
    /** {@link CellCursor#discardDroppedCell(boolean)} result: read the next cell. Never returned
     *  to a caller of {@link CellCursor#readCellHeader()}, so it sits outside their range. */
    private static final int CELL_DROPPED_CONTINUE = 3;

    // If true, a complex column with no cells becomes a position where the cursor stops. See
    // CellCursor.surfaceDeletionOnlyComplexColumn for what the cursor holds at that position.
    //
    // Defaults to true: a deletion-only complex column must reach any consumer that merges or
    // writes rows, or the column-level deletion silently disappears. Turn this off only for a
    // pure consumption walk that has no use for a position carrying no cell, such as an
    // allocation microbenchmark.
    private boolean pauseAtEmptyComplexColumns = true;

    public void pauseAtEmptyComplexColumns(boolean pause)
    {
        this.pauseAtEmptyComplexColumns = pause;
    }

    public class CellCursor {
        public ReusableLivenessInfo rowLiveness;
        public Columns columns;

        public int columnsSize;
        public int cellFlags;
        public final ReusableCellLivenessInfo cellLiveness = new ReusableCellLivenessInfo();
        // The cell path of the current cell, held as raw bytes in a scratch buffer that only
        // grows. A length below zero means the cell has no path.
        public byte[] cellPathBuffer = new byte[32];
        public int cellPathLength = -1;
        // Raw value bytes of the cell that copyCellValue last copied, without the length vint that
        // a variable-length type puts on the wire. Guardrails.collectionSize measures a collection
        // with Cell.dataSize, which counts the value this way.
        public int cellValueLength;
        private ByteBuffer cellPathWindow;

        /**
         * A ByteBuffer view of the current cell path. The view is valid until the next
         * readCellHeader call.
         *
         * Each CellCursor holds ONE view and returns that same object every time. Compare the
         * paths of two DIFFERENT cursors only.
         */
        public ByteBuffer cellPathWindow()
        {
            if (cellPathWindow == null || cellPathWindow.array() != cellPathBuffer)
                cellPathWindow = ByteBuffer.wrap(cellPathBuffer);
            cellPathWindow.limit(cellPathLength).position(0);
            return cellPathWindow;
        }
        // State of the current complex column. The deletion is LIVE if the column has no
        // deletion, and also if the row carries no complex deletion at all.
        public int remainingCellsInColumn;
        public final DeletionTime.ReusableDeletionTime complexDeletion = DeletionTime.ReusableDeletionTime.live();
        // True if the last readCellHeader call gave a cell. It is false at a complex column that
        // has no cell, and after the CELL_NONE_REMAINING result.
        public boolean producedCell;
        private boolean rowHasComplexDeletion;
        public AbstractType<?> cellType;
        public ColumnMetadata cellColumn;
        // The type that compares the path bytes of the current cell: see
        // ColumnMetadata.pathNameComparator. It is null for a simple column.
        public AbstractType<?> cellPathType;
        private ColumnMetadata[] columnsArray;
        private AbstractType<?>[] cellTypeArray;
        // One entry per column of columnsArray: the path comparator type of a complex column, or
        // null for a simple column. Built with cellTypeArray, once per change of the column set.
        private AbstractType<?>[] pathTypeArray;
        // Parallel to columnsArray: each column's drop horizon, or DeserializationHelper.NO_DROP_HORIZON
        // if none. Null when this sstable has no dropped column. Read it only through isDroppedAt,
        // which holds both the null guard and the sentinel test. A bare
        // "timestamp <= droppedTimeArray[i]" drops a cell timestamped LivenessInfo.NO_TIMESTAMP on a
        // column that was never dropped, which the iterator path keeps.
        private long[] droppedTimeArray;
        // The columnsArray index of cellColumn. It is kept for every cell of a complex column, so
        // the drop test below stays an array read and does not become a lookup per cell.
        private int cellColumnIndex;

        // Remaining PRESENT columns of this row as a bitmask over columnsArray indices.
        // A row that does not carry every header column passes this mask, or the present-mask
        // words for a superset of 64 columns or more. It passes no freshly allocated Columns
        // subset, so the column identity test in init rebuilds only on a superset change.
        private long presentMask;
        // >= 64-column supersets: present-mask words. Bit i of word i/64 is set if superset
        // column i is present. The array grows once, and the walk clears the bits it consumes.
        private long[] presentWords;
        private int presentWordsCount;
        private int presentWordIndex;

        /**
         * Rebuilds the per-superset arrays for a new column set. Only a changed superset pays for
         * this, so the usual row reuses the arrays.
         */
        private void rebuildColumnArrays(Columns columns)
        {
            // This will be a problem with changing columns
            this.columns = columns;
            columnsArray = columns.toArray(COLUMN_METADATA_TYPE);
            cellTypeArray = new AbstractType<?>[columnsArray.length];
            pathTypeArray = new AbstractType<?>[columnsArray.length];
            droppedTimeArray = sstableHasDroppedColumns ? new long[columnsArray.length] : null;
            for (int i = 0; i < columnsArray.length; i++)
            {
                cellTypeArray[i] = serializationHeader.getType(columnsArray[i]);
                pathTypeArray[i] = columnsArray[i].isComplex()
                                   ? ColumnMetadata.pathNameComparator(columnsArray[i].type)
                                   : null;
                if (sstableHasDroppedColumns)
                    droppedTimeArray[i] = deserializationHelper.droppedTimeOrMin(columnsArray[i]);
            }
            columnsSize = columns.size();
        }

        /**
         * Prepares the word-mask walk over a superset of 64 columns or more. The descriptor decoded
         * the large-subset wire format into {@code presentColumnsWords}; null means every column is
         * present.
         */
        private void initPresentWords(long[] presentColumnsWords)
        {
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

        void init (Columns columns, long missingColumnsMask, long[] presentColumnsWords,
                   boolean rowHasComplexDeletion, ReusableLivenessInfo rowLiveness)
        {
            // The dropped-column flag applies to this sstable, not to the table. A column set
            // taken from the schema would filter too few cells, and would give no error.
            assert columns == serializationHeader.columns(false) || columns == serializationHeader.columns(true)
                 : "cell superset must be one of this sstable's header column sets";
            this.rowHasComplexDeletion = rowHasComplexDeletion;
            remainingCellsInColumn = 0;
            complexDeletion.resetLive();
            if (this.columns != columns)
                rebuildColumnArrays(columns);

            if (columnsSize >= 64)
                initPresentWords(presentColumnsWords);
            else
                // The AND trims bits the flip sets above the column range. columnsSize == 0 needs
                // its own arm: Java shifts are mod 64.
                presentMask = ~missingColumnsMask & (columnsSize == 0 ? 0 : (-1L >>> (64 - columnsSize)));

            this.rowLiveness = rowLiveness;
            cellFlags = 0;
            cellPathLength = -1;
            cellType = null;
            cellPathType = null;
            producedCell = false;
        }

        public boolean hasNext()
        {
            return remainingCellsInColumn > 0 || columnsRemain();
        }

        private boolean columnsRemain()
        {
            if (columnsSize < 64)
                return presentMask != 0;
            // advance to the next non-empty word; position is retained across calls
            while (presentWordIndex < presentWordsCount)
            {
                if (presentWords[presentWordIndex] != 0)
                    return true;
                presentWordIndex++;
            }
            return false;
        }

        /** Gives a complex column that has no cell a position of its own. At that position
         *  cellColumn and complexDeletion describe the column, and no cell field is valid. */
        private int surfaceDeletionOnlyComplexColumn()
        {
            producedCell = false;
            cellPathLength = -1;
            return CELL_NO_VALUE;
        }

        /**
         * The dropped-column filter for one decoded timestamp. Gives the same answer as
         * {@code deserializationHelper.isDropped(columnsArray[columnIndex], timestamp, ...)}, and as
         * {@code isDroppedComplexDeletion} for a complex deletion, but reads the prepared
         * {@link #droppedTimeArray} instead of a map keyed by ByteBuffer, which would be a lookup
         * per cell. {@link DeserializationHelper#isDroppedAtHorizon} holds the sentinel test that
         * keeps the two forms equal: see its javadoc.
         *
         * The {@code sstableHasDroppedColumns} test is required, not an optimization:
         * {@link #droppedTimeArray} is null when this sstable's header has no dropped column.
         */
        private boolean isDroppedAt(long timestamp, int columnIndex)
        {
            return sstableHasDroppedColumns
                   && DeserializationHelper.isDroppedAtHorizon(timestamp, droppedTimeArray[columnIndex]);
        }

        /**
         * Moves to the next column of the row that has a cell to read. On the way it consumes the
         * header of each complex column, which holds a deletion and a cell count. If it returns
         * {@link #COLUMN_RUN_OPEN}, then {@code remainingCellsInColumn} is above zero.
         *
         * @return {@link #COLUMN_RUN_OPEN} if a cell is ready to read;
         *         {@link #CELL_NONE_REMAINING} if the row has no column left;
         *         {@link #CELL_NO_VALUE} if a complex column with no cells became a position of
         *         its own: see {@link #surfaceDeletionOnlyComplexColumn}
         */
        private int openNextColumnRun() throws IOException
        {
            while (remainingCellsInColumn == 0)
            {
                if (!columnsRemain())
                    return CELL_NONE_REMAINING; // the last complex columns held no cells
                // HOTSPOT: suprisingly expensive
                int currIndex = takeNextPresentColumn();
                cellColumnIndex = currIndex;
                cellColumn = columnsArray[currIndex];
                cellType = cellTypeArray[currIndex];
                cellPathType = pathTypeArray[currIndex];
                if (!cellColumn.isComplex())
                {
                    remainingCellsInColumn = 1;
                    break;
                }
                readComplexDeletion(currIndex);
                remainingCellsInColumn = dataReader.readUnsignedVInt32();
                if (remainingCellsInColumn == 0 && pauseAtEmptyComplexColumns)
                    return surfaceDeletionOnlyComplexColumn();
                // A count of zero and no pause: continue to the next column.
            }
            return COLUMN_RUN_OPEN;
        }

        /**
         * Takes the next present column out of the walk and returns its superset index.
         *
         * <p>Bit i is the i-th column of the column set, in the order the set gives them. The
         * serializer assigned the bits in that same order, and the cells are on disk in that same
         * order, so a walk from the lowest bit to the highest reads the cells in their disk order.
         * numberOfTrailingZeros gives the lowest set bit, and {@code x & (x - 1)} clears it,
         * because the subtraction borrows through the trailing zeros and the AND removes both.
         */
        private int takeNextPresentColumn()
        {
            if (columnsSize < 64)
            {
                int currIndex = Long.numberOfTrailingZeros(presentMask);
                presentMask &= presentMask - 1;
                return currIndex;
            }
            // columnsRemain left presentWordIndex on a word that has a set bit.
            long word = presentWords[presentWordIndex];
            int currIndex = (presentWordIndex << 6) + Long.numberOfTrailingZeros(word);
            presentWords[presentWordIndex] = word & (word - 1);
            return currIndex;
        }

        /** Reads a complex column's own deletion, or clears it when the row carries none. */
        private void readComplexDeletion(int currIndex) throws IOException
        {
            if (!rowHasComplexDeletion)
            {
                complexDeletion.resetLive();
                return;
            }
            serializationHeader.readDeletionTime(dataReader, complexDeletion);
            // Do what DeserializationHelper.isDroppedComplexDeletion does: drop a complex deletion
            // at or before the drop time of its column.
            if (isDroppedAt(complexDeletion.markedForDeleteAt(), currIndex))
                complexDeletion.resetLive();
        }

        /**
         * Reads the header of the next cell.
         *
         * For the format of one cell, see {@link Cell.Serializer#deserialize}. For a complex
         * column, see UnfilteredSerializer.readComplexColumn. On disk each complex column holds:
         * <ul>
         *   <li>a DeletionTime, but only if the row flag HAS_COMPLEX_DELETION is set;</li>
         *   <li>a vint cell count;</li>
         *   <li>the cells, in cell-path order.</li>
         * </ul>
         *
         * This method also applies the dropped-column filter, as the iterator does. It consumes
         * the cells and the complex deletions of a dropped column and never surfaces them.
         *
         * A dropped column can be the last column of the row, and a complex column with no cells
         * can be the last position. This method then has no cell to give, which is not the same
         * as a cell with no value.
         *
         * @return 1 if the next cell has a value;
         *         0 if it has no value, that is, a tombstone or a complex column with no cells;
         *         -1 if the row has no cell left
         */
        int readCellHeader() throws IOException
        {
            if (!hasNext()) throw new IllegalStateException();

            for (;;)
            {
                producedCell = false;
                int opened = openNextColumnRun();
                if (opened != COLUMN_RUN_OPEN)
                    return opened;

                remainingCellsInColumn--;
                producedCell = true;

                long timestamp = readCellLiveness();
                readCellPath();

                boolean hasValue = Cell.Serializer.hasValue(cellFlags);
                if (!isDroppedAt(timestamp, cellColumnIndex))
                    return hasValue ? CELL_HAS_VALUE : CELL_NO_VALUE;

                int dropped = discardDroppedCell(hasValue);
                if (dropped != CELL_DROPPED_CONTINUE)
                    return dropped;
            }
        }

        /**
         * Reads the cell's flags, timestamp, TTL and local deletion time into {@link #cellLiveness}.
         *
         * @return the cell's timestamp, which the dropped-column filter also needs
         */
        private long readCellLiveness() throws IOException
        {
            cellFlags = dataReader.readUnsignedByte();
            // TODO: specialize common case where flags == HAS_VALUE | USE_ROW_TS?
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
            return timestamp;
        }

        /**
         * Reads the cell path of a complex column, growing the buffer when it must.  A simple
         * column has no path and takes a length of -1.
         */
        private void readCellPath() throws IOException
        {
            if (!cellColumn.isComplex())
            {
                cellPathLength = -1;
                return;
            }
            // CollectionPathSerializer writes a vint length and then the path bytes. It writes that
            // format for every complex column, UDTs included.
            int pathLength = dataReader.readUnsignedVInt32();
            // The reference, ByteBufferUtil.readWithVIntLength, rejects a negative length.
            // Unchecked, it would size the buffer below and index into it.
            validateClusteringValueLength(pathLength);
            if (cellPathBuffer.length < pathLength)
                cellPathBuffer = new byte[Math.max(pathLength, cellPathBuffer.length * 2)]; // doubles, so the cost is amortized
            dataReader.readFully(cellPathBuffer, 0, pathLength);
            cellPathLength = pathLength;
        }

        /**
         * Discards a cell of a dropped column that was written at or before the drop, as
         * UnfilteredSerializer.readSimpleColumn and readComplexColumn do.
         *
         * @return {@link #CELL_DROPPED_CONTINUE} when the caller must read the next cell, else the
         *         state to return to the caller's caller
         */
        private int discardDroppedCell(boolean hasValue) throws IOException
        {
            if (hasValue)
                cellType.skipValue(dataReader);
            if (remainingCellsInColumn == 0 && cellColumn.isComplex() && pauseAtEmptyComplexColumns && !complexDeletion.isLive())
            {
                // The filter dropped every cell of this complex column, but the deletion of the
                // column itself survives and must reach the merge.
                return surfaceDeletionOnlyComplexColumn();
            }
            if (!hasNext())
                return CELL_NONE_REMAINING; // the caller must move past the end of the row
            return CELL_DROPPED_CONTINUE;
        }
    }

    private final Ref<SSTableReader> ssTableReaderRef;
    private final AbstractType<?>[] clusteringColumnTypes;
    private final DeserializationHelper deserializationHelper;
    private final SerializationHeader serializationHeader;
    // True when a column of THIS sstable's header carries a drop horizon. The helper's flag of the
    // same purpose is table-scoped, which is why the name differs. The sstable scope is sound
    // because the cell cursor's superset comes from serializationHeader.columns(), which
    // CellCursor.init asserts, so a column absent from this header never reaches readCellHeader.
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

    // Where the unfiltered being read must end: dataStart + unfilteredSize. Paths that end an
    // unfiltered without walking cells leave it stale, so only the cell-walk funnel checks it.
    private static final long NO_UNFILTERED_END = -1;
    private long unfilteredEnd = NO_UNFILTERED_END;

    // The first cell of the unfiltered being read; see rewindRowCells. The descriptor cannot
    // give it: dataStart() is the row BODY, ahead of previousUnfilteredSize, the liveness, the
    // deletion and the missing-columns subset.
    private long unfilteredCellsStart = NO_UNFILTERED_END;

    // The [start, end) data-file segments this cursor reads, in ascending order. Each starts and
    // ends on a partition boundary. The cursor is DONE once the last one is read, EOF or not.
    private final PartitionPositionBounds[] segments;
    private int segmentIndex;
    private long segmentStart;
    private long segmentEnd;
    private long bytesReadInPreviousSegments;

    private int state;

    public static SSTableCursorReader fromDescriptor(Descriptor desc) throws IOException
    {
        TableMetadata metadata = Util.metadataFromSSTable(desc);
        SSTableReader reader = SSTableReader.openNoValidation(null, desc, TableMetadataRef.forOfflineTools(metadata));
        return new SSTableCursorReader(reader, metadata, reader.ref(), null, null);
    }

    public SSTableCursorReader(SSTableReader reader)
    {
        this(reader, reader.metadata(), null, null, null);
    }

    public SSTableCursorReader(SSTableReader reader, DiskAccessMode diskAccessMode)
    {
        this(reader, reader.metadata(), null, null, diskAccessMode);
    }

    /**
     * A cursor over the given ranges of uncompressed positions, under the contract of
     * {@link org.apache.cassandra.io.sstable.format.SSTableSimpleScanner}: each range starts and
     * ends on a partition boundary, and the ranges are non-overlapping and ascending.
     *
     * @param bounds the segments to read, or null for the whole file
     */
    public SSTableCursorReader(SSTableReader reader, Collection<PartitionPositionBounds> bounds, DiskAccessMode diskAccessMode)
    {
        this(reader, reader.metadata(), null, bounds, diskAccessMode);
    }

    /** @param bounds the segments to read, or null for the whole file */
    private SSTableCursorReader(SSTableReader reader, TableMetadata metadata, Ref<SSTableReader> readerRef,
                                Collection<PartitionPositionBounds> bounds, DiskAccessMode diskAccessMode)
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

        segments = bounds == null
                   ? new PartitionPositionBounds[]{ new PartitionPositionBounds(0, dataReader.length()) }
                   : bounds.toArray(new PartitionPositionBounds[0]);
        try
        {
            state = advanceSegment();
        }
        catch (RuntimeException | Error e)
        {
            dataReader.close();
            throw e;
        }
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

    /**
     * The state after an end-of-partition marker: the next partition of this segment, the first
     * partition of the next segment, or DONE.
     */
    private int afterPartitionEnd()
    {
        return dataReader.getPosition() < segmentEnd ? PARTITION_START : advanceSegment();
    }

    /**
     * Enters the next segment that has bytes, as {@code SSTableSimpleScanner.advanceRange} does,
     * and leaves the reader at its first partition.
     *
     * @return PARTITION_START, or DONE when no segment is left
     */
    private int advanceSegment()
    {
        while (segmentIndex < segments.length)
        {
            PartitionPositionBounds next = segments[segmentIndex++];
            if (segmentEnd > next.lowerPosition)
                throw new IllegalArgumentException("Ranges supplied to SSTableCursorReader must be non-overlapping and in ascending order.");
            if (next.upperPosition < next.lowerPosition)
                throw new IllegalArgumentException("A range supplied to SSTableCursorReader ends before it starts: "
                                                   + next.lowerPosition + " > " + next.upperPosition);
            // An empty range carries no partition. Skip it WITHOUT touching segmentStart, segmentEnd
            // or the byte accounting: bytesRead() is bytesReadInPreviousSegments plus the progress
            // through the current segment, so moving those to a range the reader never visits makes
            // the count go backwards. The scanner avoids this by seeking to the empty range's start;
            // not seeking is cheaper and reads nothing outside a range this cursor covers.
            if (next.lowerPosition == next.upperPosition)
                continue;

            bytesReadInPreviousSegments += segmentEnd - segmentStart;
            segmentStart = next.lowerPosition;
            segmentEnd = next.upperPosition;
            try
            {
                seekPartition(segmentStart);
            }
            catch (IOException e)
            {
                return corruptSSTable(e);
            }
            return PARTITION_START;
        }
        return DONE;
    }

    /**
     * Seeks to the start of a partition. Every partition but the file's first follows an
     * end-of-partition marker, and reading that byte leaves the reader at the partition start.
     */
    private void seekPartition(long position) throws IOException
    {
        if (position == 0)
        {
            if (dataReader.getPosition() != 0)
                dataReader.seek(0);
            return;
        }
        dataReader.seek(position - 1);
        // The exact byte, not isEndOfPartition, which is (b & 1) != 0 and so accepts 128 of the 256
        // values. The writer emits END_OF_PARTITION alone for this marker, so the stronger test
        // costs nothing and rejects a mis-sized bound that happens to land on an odd byte.
        int marker = dataReader.readUnsignedByte();
        if (marker != UnfilteredSerializer.END_OF_PARTITION)
            throw new IOException("Seeking to a partition at: " + position + " did not land after an end-of-partition marker; found 0x"
                                  + Integer.toHexString(marker));
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
                                 unfilteredDescriptor.presentColumnsWords(),
                                 unfilteredDescriptor.hasComplexDeletion(),
                                 unfilteredDescriptor.livenessInfo());
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

    /**
     * Raw value bytes of the cell that {@link #copyCellValue} last copied, without the length vint
     * that a variable-length type puts on the wire. Valid until the next call.
     */
    public int lastCellValueLength()
    {
        return cellCursor.cellValueLength;
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

    // TODO: move to cell cursor?
    private void copyCellContents(DataOutputPlus writer, byte[] transferBuffer, int length) throws IOException
    {
        if (length < 0)
        {
            // variable length: the wire carries a length vint, and this mirrors it to the output
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
        cellCursor.cellValueLength = length;
        // In production every writer is a DataOutputBuffer that holds a heap array. Read the value
        // bytes straight into that array. This needs no loop for a value that is larger than the
        // transfer buffer, and such values occur: valueLengthIfFixed is 6144 for a
        // vector<float, 1536>.
        //
        // Catch IOException only. That is what a failed read of the input throws, and the code
        // above already checks the length. An exception from the growth of the output buffer is a
        // defect in this process, not damaged data, and it must not mark the sstable as corrupt.
        //
        // hasArray() guards against a direct-backed DataOutputBuffer: readFully requires a heap
        // array, so a direct-backed instance falls through to the transfer-buffer loop below
        // instead of taking this fast path unsafely.
        if (writer instanceof DataOutputBuffer && ((DataOutputBuffer) writer).hasArray())
        {
            try
            {
                ((DataOutputBuffer) writer).readFully(dataReader, length);
            }
            catch (IOException e)
            {
                corruptSSTable(e);
            }
            return;
        }
        // Fallback for any other DataOutputPlus: copy in blocks the size of the transfer buffer.
        int remaining = length;
        while (remaining > 0)
        {
            int chunk = Math.min(remaining, transferBuffer.length);
            try
            {
                dataReader.readFully(transferBuffer, 0, chunk);
            }
            catch (IOException e)
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
                               unfilteredDescriptor.presentColumnsWords(),
                               unfilteredDescriptor.hasComplexDeletion(),
                               unfilteredDescriptor.livenessInfo());
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
                // There is no cell. Either the dropped-column filter removed every remaining
                // column, or the last complex columns held no cells. Nothing is current, so move
                // past the CELL_END stop and do not give a position that has no cell.
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
     *   struct delta_deletion_time start_deletion_time;
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
            // A marker has no cells, and loadTombstone does not set dataStart, so the descriptor
            // still holds the previous row's dataStart. Close the window instead of leaving that
            // value reachable.
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
                fixedLengthClusteringLength = flushFixedLengthRun(dataReader, clustering, fixedLengthClusteringLength);
                clusteringBlockHeader = dataReader.readUnsignedVInt();
                clustering.writeUnsignedVInt(clusteringBlockHeader);
            }

            // load value if present
            if ((clusteringBlockHeader & 0b11) == 0)
                fixedLengthClusteringLength = readClusteringValue(dataReader, clustering, types[clusteringIndex], fixedLengthClusteringLength);

            clusteringBlockHeader = clusteringBlockHeader >>> 2;
        }
        flushFixedLengthRun(dataReader, clustering, fixedLengthClusteringLength);
        if (clusteringBlockHeader != 0) {
            throw new IOException("Clustering block upper bits (those not associated with keys) expected to be 0:" + clusteringBlockHeader);
        }
    }

    /**
     * Copies the pending run of fixed-length components in one read. They are adjacent on disk, so
     * the run is only broken by a variable-length component or by a block boundary.
     *
     * @return the new pending run length, always 0
     */
    private static int flushFixedLengthRun(RandomAccessReader dataReader, ResizableByteBuffer clustering, int fixedLengthClusteringLength) throws IOException
    {
        if (fixedLengthClusteringLength != 0)
            clustering.loadPart(dataReader, fixedLengthClusteringLength);
        return 0;
    }

    /**
     * Reads one present clustering component. A fixed-length component joins the pending run; a
     * variable-length one flushes the run first, because its length vint sits between them.
     *
     * @return the pending fixed-length run after this component
     */
    private static int readClusteringValue(RandomAccessReader dataReader, ResizableByteBuffer clustering,
                                           AbstractType<?> type, int fixedLengthClusteringLength) throws IOException
    {
        if (type.isValueLengthFixed())
            return fixedLengthClusteringLength + type.valueLengthIfFixed();

        flushFixedLengthRun(dataReader, clustering, fixedLengthClusteringLength);
        int varLength = dataReader.readUnsignedVInt32();
        validateClusteringValueLength(varLength);
        clustering.writeUnsignedVInt(varLength);
        clustering.loadPart(dataReader, varLength);
        return 0;
    }

    /**
     * Rejects a clustering value length the wire cannot have produced honestly. Both checks mirror
     * AbstractType.read, the reference for this format. readUnsignedVInt32 can return a negative
     * int, which is why the first check exists: an unchecked negative length reaches
     * {@link java.io.DataInput#skipBytes} as a silent no-op, and a buffer sizer as a defect.
     *
     * <p>Every caller of this walk wraps it and reports a {@code CorruptSSTableException}.
     */
    @VisibleForTesting
    static void validateClusteringValueLength(int length) throws IOException
    {
        if (length < 0)
            throw new IOException("Corrupt (negative) clustering value length encountered: " + length);
        if (length > DatabaseDescriptor.getMaxValueSize())
            throw new IOException(String.format("Corrupt clustering value length %d encountered, as it exceeds the maximum of %d, " +
                                                "which is set via max_value_size in cassandra.yaml",
                                                length, DatabaseDescriptor.getMaxValueSize()));
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
                int len = type.valueLengthIfFixed();
                if (!type.isValueLengthFixed())
                {
                    len = dataReader.readUnsignedVInt32();
                    validateClusteringValueLength(len);
                }
                // skipBytesFully, not skipBytes: skipBytes clamps at EOF and returns how far it
                // got, so a corrupt length would leave the walk reading a value's own bytes as the
                // next block header, with nothing downstream to notice.
                dataReader.skipBytesFully(len);
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
     * The position comes from {@link #unfilteredCellsStart}, not from the descriptor's
     * {@code dataStart()}, which is the start of the row BODY.
     *
     * {@link #basicUnfilteredFlags} and {@link #extendedFlags} are NOT restored. A caller that ends the
     * second walk without re-reading those bytes must restore them itself.
     *
     * @param isStatic which of the two cell cursors this row's cells are walked with
     */
    protected int rewindRowCells(UnfilteredDescriptor unfilteredDescriptor, boolean isStatic)
    {
        dataReader.seek(unfilteredCellsStart);
        unfilteredEnd = unfilteredDescriptor.dataStart() + unfilteredDescriptor.size();
        CellCursor rewound = isStatic ? staticRowCellCursor : rowCellCursor;
        rewound.init(unfilteredDescriptor.rowColumns(), unfilteredDescriptor.missingColumnsMask(),
                     unfilteredDescriptor.presentColumnsWords(),
                     unfilteredDescriptor.hasComplexDeletion(),
                     unfilteredDescriptor.livenessInfo());
        cellCursor = rewound;
        // Callers only rewind a cursor that WAS in a cell state, which is exactly the state the row
        // header loaders leave when the row has a present column.
        assert rewound.hasNext() : "rewound a row whose columns are all absent: " + unfilteredDescriptor;
        return state = State.CELL_HEADER_START;
    }

    public int continueReading() {
        // TODO: can be optimized by pre-calculating next state when the flags are read
        switch (state)
        {
            case PARTITION_END:
                state = afterPartitionEnd();
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
            state = !autoContinue ? PARTITION_END : afterPartitionEnd();
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
     * Reads the extended-flags byte for {@code basicFlags} if present, and stores it in the
     * {@link #extendedFlags} field for the row loader. The field holds 0 if the byte is absent.
     * Extended flags are meaningful on a row only, never on a marker. They are {@code IS_STATIC},
     * and {@code HAS_SHADOWABLE_DELETION} on either a static or a non-static row. CASSANDRA-11500
     * deprecated the second one in 4.0, and only old Materialized View data still carries it.
     * A static row is legal only as the partition's first unfiltered, hence {@code allowStatic}.
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
        // The cell walk must have consumed exactly the body the row header declared. Checked here,
        // before the next unfiltered's flag byte is read, so the two positions are comparable.
        //
        // A decoder defect is as likely as on-disk damage, but UnfilteredSerializer.readRow accepts
        // the same ambiguity in its catch, so route through corruption handling rather than raise a
        // raw AssertionError.
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
            return afterPartitionEnd();
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

    /** Bytes read of the segments, as {@code SSTableSimpleScanner.getBytesScanned} counts them: a seek between segments counts nothing. */
    public long bytesRead()
    {
        return bytesReadInPreviousSegments + dataReader.getFilePointer() - segmentStart;
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
