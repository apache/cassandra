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

import org.apache.cassandra.db.ReusableLivenessInfo;
import org.apache.cassandra.io.util.ResizableByteBuffer;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.db.rows.UnfilteredSerializer;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.tools.Util;
import org.apache.cassandra.utils.concurrent.Ref;

import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.*;

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

    public class CellCursor {
        public ReusableLivenessInfo rowLiveness;
        public Columns columns;

        public int columnsSize;
        public int columnsIndex;
        public int cellFlags;
        public final ReusableLivenessInfo cellLiveness = new ReusableLivenessInfo();
        public CellPath cellPath;
        public AbstractType<?> cellType;
        public ColumnMetadata cellColumn;
        private ColumnMetadata[] columnsArray;
        private AbstractType<?>[] cellTypeArray;

        void init (Columns columns, ReusableLivenessInfo rowLiveness)
        {
            if (this.columns != columns)
            {
                // This will be a problem with changing columns
                this.columns = columns;
                columnsArray = columns.toArray(COLUMN_METADATA_TYPE);
                cellTypeArray = new AbstractType<?>[columnsArray.length];
                for (int i = 0; i < columnsArray.length; i++)
                {
                    ColumnMetadata cellColumn = columnsArray[i];
                    cellTypeArray[i]  = serializationHeader.getType(cellColumn);
                }
                columnsSize = columns.size();
            }
            this.rowLiveness = rowLiveness;
            columnsIndex = 0;
            cellFlags = 0;
            cellPath = null;
            cellType = null;
        }

        public boolean hasNext()
        {
            return columnsIndex < columnsSize;
        }

        /**
         * For Cell deserialization see {@link Cell.Serializer#deserialize}
         *
         * @return true if the cell has a value, false otherwise
         */
        boolean readCellHeader() throws IOException
        {
            if (!(columnsIndex < columnsSize)) throw new IllegalStateException();

            // HOTSPOT: suprisingly expensive
            int currIndex = columnsIndex++;
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
            cellPath = cellColumn.isComplex()
                            ? cellColumn.cellPathSerializer().deserialize(dataReader)
                            : null;
            return hasValue;
        }
    }

    private final Ref<SSTableReader> ssTableReaderRef;
    private final AbstractType<?>[] clusteringColumnTypes;
    private final DeserializationHelper deserializationHelper;
    private final SerializationHeader serializationHeader;

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

    private int state = PARTITION_START;

    public static SSTableCursorReader fromDescriptor(Descriptor desc) throws IOException
    {
        TableMetadata metadata = Util.metadataFromSSTable(desc);
        SSTableReader reader = SSTableReader.openNoValidation(null, desc, TableMetadataRef.forOfflineTools(metadata));
        return new SSTableCursorReader(reader, metadata, reader.ref());
    }

    public SSTableCursorReader(SSTableReader reader)
    {
        this(reader, reader.metadata(), null);
    }

    private SSTableCursorReader(SSTableReader reader, TableMetadata metadata, Ref<SSTableReader> readerRef)
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

        dataReader = reader.openDataReader();
        hasStaticColumns = metadata.hasStaticColumns();
    }

    @Override
    public void close()
    {
        dataReader.close();
        if (ssTableReaderRef != null)
            ssTableReaderRef.close();
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
        }
        catch (IOException e)
        {
            return corruptSSTable(e);
        }

        staticRowCellCursor.init(unfilteredDescriptor.rowColumns(), unfilteredDescriptor.livenessInfo());
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
        if (length >= 0)
        {
            try
            {
                dataReader.readFully(transferBuffer, 0, length);
            }
            catch (Exception e)
            {
                corruptSSTable(e);
            }
            writer.write(transferBuffer, 0, length);
        }
        else
        {
            try
            {
                length = dataReader.readUnsignedVInt32();
            }
            catch (IOException e)
            {
                corruptSSTable(e);
            }
            if (length < 0)
                corruptSSTable("Corrupt (negative) value length encountered");
            writer.writeUnsignedVInt32(length);
            int remaining = length;
            while (remaining > 0)
            {
                int readLength = Math.min(remaining, transferBuffer.length);
                try
                {
                    dataReader.readFully(transferBuffer, 0, readLength);
                }
                catch (Exception e)
                {
                    corruptSSTable(e);
                }
                writer.write(transferBuffer, 0, readLength);
                remaining -= readLength;
            }
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
            unfilteredDescriptor.loadRow(dataReader, serializationHeader, deserializationHelper, basicUnfilteredFlags);

            rowCellCursor.init(unfilteredDescriptor.rowColumns(), unfilteredDescriptor.livenessInfo());
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
            if (cellCursor.readCellHeader())
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
        else if (UnfilteredSerializer.isExtended(basicUnfilteredFlags))
        {
            state = STATIC_ROW_START;
            extendedFlags = dataReader.readUnsignedByte();
            validateStaticRowFlags(preFlagsPosition);
        }
        else
        {
            state = UnfilteredSerializer.isRow(basicUnfilteredFlags) ? ROW_START : TOMBSTONE_START;
        }
        return state;
    }

    private void validateStaticRowFlags(long preFlagsPosition)
    {
        if (!UnfilteredSerializer.isStatic(extendedFlags))
        {
            corruptSSTable("Row at: " + preFlagsPosition + " has extended flags but is not static, extendedFlags: " + extendedFlags);
        }
        if (!UnfilteredSerializer.isRow(basicUnfilteredFlags))
        {
            corruptSSTable("Static row at: " + preFlagsPosition + " is not a row, flags: " + basicUnfilteredFlags);
        }
        if (!hasStaticColumns)
        {
            corruptSSTable("Row at: " + preFlagsPosition + " is static, but table has no static columns " + ssTableReader.metadata());
        }
        if (UnfilteredSerializer.deletionIsShadowable(extendedFlags))
        {
            throw new UnsupportedOperationException("Static row at: " + preFlagsPosition + " has deletionIsShadowable, which is deprecated since 4.0");
        }
    }

    private int checkNextFlagsAfterStaticRowOrUnfilteredStart(boolean autoContinue) throws IOException
    {
        int flags = this.basicUnfilteredFlags = dataReader.readUnsignedByte();
        if (UnfilteredSerializer.isExtended(flags))
        {
            corruptSSTable("Unexpected static row (flags=" + flags + ") mid-partition, at position: " + (dataReader.getPosition() - 1));
        }

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
        int flags = this.basicUnfilteredFlags = dataReader.readUnsignedByte();
        if (UnfilteredSerializer.isExtended(flags))
        {
            corruptSSTable("Unexpected static row (flags=" + flags + ") mid-partition, at position: " + (dataReader.getPosition() - 1));
        }
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
