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
import java.util.Arrays;

import com.google.common.annotations.VisibleForTesting;

import org.agrona.collections.IntArrayList;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.DeletionTime.ReusableDeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.db.guardrails.Threshold;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellLivenessInfo;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.db.rows.UnfilteredSerializer;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SortedTableWriter;
import org.apache.cassandra.io.sstable.format.big.BigTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.Ref;

import static org.apache.cassandra.db.rows.UnfilteredSerializer.HAS_ALL_COLUMNS;
import static org.apache.cassandra.db.rows.UnfilteredSerializer.HAS_DELETION;
import static org.apache.cassandra.db.rows.UnfilteredSerializer.HAS_TIMESTAMP;
import static org.apache.cassandra.db.rows.UnfilteredSerializer.HAS_TTL;
import static org.apache.cassandra.db.rows.UnfilteredSerializer.IS_MARKER;
import static org.apache.cassandra.db.rows.UnfilteredSerializer.isExtended;

public class SSTableCursorWriter implements AutoCloseable
{
    private static final UnfilteredSerializer SERIALIZER = UnfilteredSerializer.serializer;
    private static final ColumnMetadata[] EMPTY_COL_META = new ColumnMetadata[0];
    private final SortedTableWriter<?,?> ssTableWriter;
    private final SequentialWriter dataWriter;
    private final SortedTableWriter.AbstractIndexWriter indexWriter;
    private final DeletionTime.Serializer deletionTimeSerializer;
    private final MetadataCollector metadataCollector;
    private final SerializationHeader serializationHeader;
    private final boolean hasStaticColumns;

    private long partitionStart;
    // File position of the first byte of the previous non-static unfiltered. It holds the partition
    // start while the partition has none. previousUnfilteredSize is the distance from this position,
    // as SortedTablePartitionWriter.addUnfiltered computes it. A row or a marker writes that distance.
    // A static row writes 0 and does not advance this position.
    private long previousUnfilteredStart;
    // The dataWriter position that writeRowStart encoded previousUnfilteredSize against. Nothing may
    // write to dataWriter between writeRowStart and writeRowEnd, or that distance goes stale.
    private long rowStartPosition;
    // ROW contents, needed because of the order of writing and the var int fields
    private int rowFlags; // discovered as we go along
    private int rowExtendedFlags;
    private final DataOutputBuffer rowHeaderBuffer = new DataOutputBuffer(); // holds the contents between FLAGS and SIZE
    private final DataOutputBuffer rowBuffer = new DataOutputBuffer();
    private final ReusableDeletionTime openMarker = ReusableDeletionTime.live();

    // How the writer holds the complex columns of the current row.
    //
    // The cells go into rowBuffer, as the cells of a simple column do. For each complex column a
    // marker records three things: where the cells of that column start in rowBuffer, the merged
    // deletion of the column, and the number of cells that survived.
    //
    // The writer cannot emit the header of a complex column at that time. The header holds the cell
    // count, and the row flag that controls the deletion field is known only at the end of the row.
    // writeRowEnd therefore computes the length of the cell section by arithmetic, and then writes
    // the row in parts: a block of rowBuffer, one marker header, the next block, and so on. The cell
    // bytes are copied one time only.
    //
    // A row that has no complex column writes rowBuffer in one block.
    private static final int COMPLEX_MARKERS_INITIAL = 8;
    private int complexMarkerCount;
    private int[] markerStartOffset = new int[COMPLEX_MARKERS_INITIAL];
    private int[] markerEndOffset = new int[COMPLEX_MARKERS_INITIAL];
    private int[] markerCellCount = new int[COMPLEX_MARKERS_INITIAL];
    private long[] markerDeletionMfda = new long[COMPLEX_MARKERS_INITIAL];
    private long[] markerDeletionLdt = new long[COMPLEX_MARKERS_INITIAL];
    private ColumnMetadata[] markerColumn = new ColumnMetadata[COMPLEX_MARKERS_INITIAL];
    // What Guardrails.collectionSize and Guardrails.itemsPerCollection measure: the size and the
    // count of the LIVE cells of the column, as ComplexColumnData.purge(PURGE_ALL, nowInSec) leaves
    // them. Both stay zero while the guardrails are off.
    private long[] markerLiveDataSize = new long[COMPLEX_MARKERS_INITIAL];
    private int[] markerLiveCellCount = new int[COMPLEX_MARKERS_INITIAL];
    private final DeletionTime.ReusableDeletionTime reusableMarkerDeletion = DeletionTime.ReusableDeletionTime.live();
    // True if any complex column of this row has a non-live merged deletion. startComplexColumn keeps
    // it current, so writeRowEnd sets HAS_COMPLEX_DELETION without a second walk of the markers.
    private boolean rowHasComplexDeletion;
    private ColumnMetadata lastCellColumn;

    // SortedTableWriter applies these two in guardCollectionSize, once per row per complex column.
    // The cursor writer never builds a Row, so it measures the column while it writes the cells.
    //
    // Everything here costs nothing while both guardrails are off, which is the shipped default:
    // collectionGuardsDisabled is then true, and no cell touches the two marker totals.
    private final boolean collectionGuardsDisabled;
    private final long collectionSizeWarn = Guardrails.collectionSize.warnValue(null);
    private final long itemsPerCollectionWarn = Guardrails.itemsPerCollection.warnValue(null);
    // The partition being written, kept for the guardrail message. writePartitionStart owns it.
    private byte[] partitionKey;
    private int partitionKeyLength;
    // Read once per row, so that every cell of the row judges expiry against one instant.
    private long nowInSec;
    // True while the current complex column is a collection the guardrails measure.
    private boolean markerIsGuardedCollection;
    // True while the cell being written is a live cell of such a column.
    private boolean cellCountsTowardsCollection;

    private final ColumnMetadata[] staticColumns;
    private final ColumnMetadata[] regularColumns;
    private final IntArrayList missingColumns = new IntArrayList();
    private ColumnMetadata[] columns; // points to static/regular
    private int columnsWrittenCount = 0;
    private int nextCellIndex = 0;
    // Format-specific index production. BIG writes promoted blocks, Index.db, a bloom filter and a
    // summary.
    private final CursorIndexWriter cursorIndexWriter;

    private SSTableCursorWriter(
        Descriptor desc,
        SortedTableWriter<?,?> ssTableWriter,
        SequentialWriter dataWriter,
        SortedTableWriter.AbstractIndexWriter indexWriter,
        MetadataCollector metadataCollector,
        SerializationHeader serializationHeader)
    {
        this.ssTableWriter = ssTableWriter;
        this.dataWriter = dataWriter;
        this.indexWriter = indexWriter;
        this.deletionTimeSerializer = DeletionTime.getSerializer(desc.version);
        this.metadataCollector = metadataCollector;
        this.serializationHeader = serializationHeader;
        hasStaticColumns = serializationHeader.hasStatic();
        staticColumns = hasStaticColumns ? serializationHeader.columns(true).toArray(EMPTY_COL_META) : EMPTY_COL_META;
        regularColumns = serializationHeader.columns(false).toArray(EMPTY_COL_META);
        this.cursorIndexWriter = new BigCursorIndexWriter((BigTableWriter.IndexWriter) indexWriter,
                                                           this.deletionTimeSerializer);
        // Same two conditions SortedTableWriter settles once, in its own constructor and in
        // guardCollectionSize: both guardrails off, or a system keyspace.
        this.collectionGuardsDisabled =
            (!Guardrails.collectionSize.enabled() && !Guardrails.itemsPerCollection.enabled())
            || SchemaConstants.isSystemKeyspace(ssTableWriter.metadata().keyspace);
    }

    public SSTableCursorWriter(SortedTableWriter<?,?> ssTableWriter)
    {
        this(ssTableWriter.descriptor,
             ssTableWriter,
             ssTableWriter.dataWriter,
             ssTableWriter.indexWriter,
             ssTableWriter.metadataCollector,
             ssTableWriter.partitionWriter.getHeader());
    }

    @Override
    public void close()
    {
        SSTableReader finish = ssTableWriter.finish(false);
        if (finish != null) {
            Ref<SSTableReader> ref = finish.ref();
            if (ref != null) ref.close();
        }
        ssTableWriter.close();
    }

    public long getPartitionStart()
    {
        return partitionStart;
    }

    public long getPosition()
    {
        return dataWriter.position();
    }

    public int writePartitionStart(byte[] partitionKey, int partitionKeyLength, DeletionTime partitionDeletionTime) throws IOException
    {
        openMarker.resetLive();

        this.partitionKey = partitionKey;
        this.partitionKeyLength = partitionKeyLength;
        partitionStart = dataWriter.position();
        previousUnfilteredStart = partitionStart;
        writePartitionHeader(partitionKey, partitionKeyLength, partitionDeletionTime);
        cursorIndexWriter.startPartition(partitionStart, dataWriter.position());
        // immediately after startPartition this is the partition header length — always small
        return Math.toIntExact(cursorIndexWriter.indexBlockStartOffset());
    }

    /**
     * @param lastName the clustering of the last non-static unfiltered written to this partition, needed as
     *                 the last name of a trailing index block; null if the partition wrote none.
     */
    public void writePartitionEnd(byte[] partitionKey, int partitionKeyLength, DeletionTime partitionDeletionTime,
                                  int headerLength, ClusteringDescriptor lastName) throws IOException
    {
        SERIALIZER.writeEndOfPartition(dataWriter);
        long partitionEnd = dataWriter.position();
        long partitionSize = partitionEnd - partitionStart;
        addPartitionMetadata(partitionKey, partitionKeyLength, partitionSize, partitionDeletionTime);

        /** {@link SortedTableWriter#endPartition(DecoratedKey, DeletionTime)}
         lastWrittenKey = key; // tracked for verification, see {@link SortedTableWriter#verifyPartition(DecoratedKey)}, checking the key size and sorting
         // first/last are retained for metadata {@link org.apache.cassandra.io.sstable.format.SSTableWriter#finalizeMetadata()}. They are also exposed via
         // getters from the writer, but usage is unclear.
         last = lastWrittenKey;
         if (first == null)
         first = lastWrittenKey;
         // this is implemented differently for BIG/BTI
         createRowIndexEntry(key, partitionLevelDeletion, partitionEnd - 1);
         */
        cursorIndexWriter.endPartition(partitionKey, partitionKeyLength, headerLength, partitionDeletionTime, partitionEnd, lastName);
    }


    final long guardrailsPartitionSizeWarning = Guardrails.partitionSize.warnValue(null);
    final long guardrailsPartitionTombstonesWarning = Guardrails.partitionTombstones.warnValue(null);

    /**
     *  update metadata like {@link SortedTableWriter#endPartition} and {@link SortedTableWriter#startPartition}
     */
    private void addPartitionMetadata(byte[] partitionKey, int partitionKeyLength, long partitionSize, DeletionTime partitionDeletionTime)
    {
        if (partitionSize > guardrailsPartitionSizeWarning)
            guardPartitionThreshold(Guardrails.partitionSize, partitionKey, partitionKeyLength, partitionSize);

        if (metadataCollector.totalTombstones > guardrailsPartitionTombstonesWarning)
            guardPartitionThreshold(Guardrails.partitionTombstones, partitionKey, partitionKeyLength, metadataCollector.totalTombstones);

        metadataCollector.updatePartitionDeletion(partitionDeletionTime);
        metadataCollector.addPartitionSizeInBytes(partitionSize);
        metadataCollector.addKey(partitionKey, 0, partitionKeyLength);
        metadataCollector.addCellPerPartitionCount();
    }

    /**
     * The fixed part of {@link org.apache.cassandra.db.rows.AbstractCell#dataSize()}: a timestamp, a
     * TTL and a local deletion time. The value and the cell path are added as they are written.
     */
    private static final int CELL_FIXED_DATA_SIZE = TypeSizes.LONG_SIZE + TypeSizes.INT_SIZE + TypeSizes.LONG_SIZE;

    /** Prepares the collection guardrails for one row. */
    private void startCollectionGuards()
    {
        markerIsGuardedCollection = false;
        cellCountsTowardsCollection = false;
        if (!collectionGuardsDisabled)
            nowInSec = FBUtilities.nowInSeconds();
    }

    /**
     * Applies {@code collection_size} and {@code items_per_collection} to every collection of the
     * row that is about to be written, as
     * {@link SortedTableWriter#addRow} does through {@code guardCollectionSize}.
     *
     * <p>Call this after {@code closeComplexMarkers}, which closes the last marker, and before the
     * row reaches the data file, so that a failing guardrail stops the write as it does on the
     * iterator path.
     */
    private void guardCollectionSizes(ClusteringDescriptor rHeader)
    {
        if (collectionGuardsDisabled || complexMarkerCount == 0)
            return;

        for (int i = 0; i < complexMarkerCount; i++)
        {
            ColumnMetadata column = markerColumn[i];
            if (!column.type.isCollection() || !column.type.isMultiCell())
                continue;

            long size = markerLiveDataSize[i];
            int count = markerLiveCellCount[i];
            if (size <= collectionSizeWarn && count <= itemsPerCollectionWarn)
                continue;
            if (!Guardrails.collectionSize.triggersOn(size, null)
                && !Guardrails.itemsPerCollection.triggersOn(count, null))
                continue;

            String message = String.format("%s in row %s in table %s",
                                           column.name.toString(),
                                           primaryKeyLiteral(rHeader),
                                           ssTableWriter.metadata());
            Guardrails.collectionSize.guard(size, message, true, null);
            Guardrails.itemsPerCollection.guard(count, message, true, null);
        }
    }

    /**
     * The primary key of the row being written, in CQL form, for a guardrail message.
     */
    private String primaryKeyLiteral(ClusteringDescriptor rHeader)
    {
        Clustering<?> clustering = rHeader == null
                                   ? Clustering.STATIC_CLUSTERING
                                   : (Clustering<?>) rHeader.toClusteringPrefix(ssTableWriter.metadata().comparator.subtypes());
        return ssTableWriter.metadata()
                            .primaryKeyAsCQLLiteral(ByteBuffer.wrap(partitionKey, 0, partitionKeyLength), clustering);
    }

    private void guardPartitionThreshold(Threshold guardrail, byte[] partitionKey, int partitionKeyLength, long size)
    {
        if (guardrail.triggersOn(size, null))
        {
            String message = String.format("%s.%s:%s on sstable %s",
                    ssTableWriter.metadata().keyspace,
                    ssTableWriter.metadata().name,
                    ssTableWriter.metadata().partitionKeyType.getString(ByteBuffer.wrap(partitionKey, 0, partitionKeyLength)),
                    ssTableWriter.getFilename());
            guardrail.guard(size, message, true, null);
        }
    }

    private void writePartitionHeader(byte[] partitionKey, int partitionKeyLength, DeletionTime partitionDeletionTime) throws IOException
    {
        dataWriter.writeShort(partitionKeyLength);
        dataWriter.write(partitionKey, 0, partitionKeyLength);
        deletionTimeSerializer.serialize(partitionDeletionTime, dataWriter);
    }

    public boolean writeEmptyStaticRow() throws IOException
    {
        if (!hasStaticColumns)
            return false;
        rowFlags = UnfilteredSerializer.EXTENSION_FLAG;
        rowExtendedFlags = UnfilteredSerializer.IS_STATIC;
        columns = staticColumns;
        // TODO: this case may not need the row buffers.
        rowHeaderBuffer.clear();
        rowHeaderBuffer.writeUnsignedVInt(0L); // previousUnfilteredSize, always 0 for a static row
        rowBuffer.clear();
        complexMarkerCount = 0;
        rowHasComplexDeletion = false;
        lastCellColumn = null;
        columnsWrittenCount = 0;
        missingColumns.clear();
        startCollectionGuards();
        writeRowEnd(null, false);

        cursorIndexWriter.staticRowWritten(dataWriter.position());
        return true;
    }

    public void writeRowStart(LivenessInfo livenessInfo, DeletionTime deletionTime, boolean isShadowable, boolean isStatic) throws IOException
    {
        // Row.Deletion's constructor enforces this: a live deletion is never shadowable. A caller
        // (CursorCompactor.mergeRows) must reset isShadowable whenever it resets deletionTime to LIVE.
        assert !deletionTime.isLive() || !isShadowable : "shadowable deletion must not be live";
        rowExtendedFlags = 0;
        if (isStatic)
            rowExtendedFlags |= UnfilteredSerializer.IS_STATIC;
        if (isShadowable)
            rowExtendedFlags |= UnfilteredSerializer.HAS_SHADOWABLE_DELETION;
        rowFlags = rowExtendedFlags != 0 ? UnfilteredSerializer.EXTENSION_FLAG : 0;
        columns = isStatic ? staticColumns : regularColumns;
        // The row body carries its size ahead of its bytes, and rewriting that size afterwards costs
        // more than buffering does. The liveness fields and the cell data therefore go to buffers
        // first, because the length of both varies with the timestamps they hold.
        rowHeaderBuffer.clear();
        // previousUnfilteredSize leads the row body, ahead of the liveness data. dataWriter has not been
        // written since the previous unfiltered finished, so its position is this unfiltered's first byte.
        rowStartPosition = dataWriter.position();
        rowHeaderBuffer.writeUnsignedVInt(isStatic ? 0 : rowStartPosition - previousUnfilteredStart);
        missingColumns.clear();
        rowBuffer.clear();
        columnsWrittenCount = 0;
        startCollectionGuards();
        nextCellIndex = 0;
        complexMarkerCount = 0;
        rowHasComplexDeletion = false;
        lastCellColumn = null;

        // copy TS/TTL/deletion data
        rowFlags |= writeRowTimeData(livenessInfo, deletionTime, rowHeaderBuffer);
    }

    /**
     * See {@link UnfilteredSerializer#serialize(Row, SerializationHelper, DataOutputPlus, long, int)}
     */
    private int writeRowTimeData(LivenessInfo livenessInfo, DeletionTime deletionTime, DataOutputPlus writer) throws IOException
    {
        int flags = 0;
        boolean writtenLivenessMetadata = false;

        if (!livenessInfo.isEmpty())
        {
            flags |= HAS_TIMESTAMP;
            serializationHeader.writeTimestamp(livenessInfo.timestamp(), writer);
            metadataCollector.update(livenessInfo);
            writtenLivenessMetadata = true;
        }
        if (livenessInfo.isExpiring())
        {
            flags |= HAS_TTL;
            serializationHeader.writeTTL(livenessInfo.ttl(), writer);
            serializationHeader.writeLocalDeletionTime(livenessInfo.localExpirationTime(), writer);
            if (!writtenLivenessMetadata) metadataCollector.update(livenessInfo);
        }
        if (!deletionTime.isLive())
        {
            flags |= HAS_DELETION;
            writeDeletionTime(deletionTime, writer);
        }

        /**
         * The metadata calls above match {@link org.apache.cassandra.db.rows.Rows#collectStats}.
         * writeCellHeader collects the cell metadata.
         */
        return flags;
    }

    private void writeDeletionTime(DeletionTime deletionTime, DataOutputPlus writer) throws IOException
    {
        serializationHeader.writeDeletionTime(deletionTime, writer);
        metadataCollector.update(deletionTime);
    }

    /**
     * Gives the number of bytes that {@link SerializationHeader#writeDeletionTime} writes.
     *
     * Do not use SerializationHeader.deletionTimeSerializedSize here. That method sizes the
     * localDeletionTime field from the delta as a long. The write casts the delta to an int first,
     * and writeUnsignedVInt32 then sign-extends it back to a long. A delta between 2^31 and 2^32
     * therefore sizes as 5 bytes and writes as 9.
     *
     * Such a delta occurs. localDeletionTime is unsigned up to about the year 2106, and a deletion
     * classed as INVALID sits at 2^32-2. The row-size vint must count the bytes that the write
     * emits, so this method uses the same cast.
     */
    private long deletionTimeWrittenSize(DeletionTime deletionTime)
    {
        long localDeletionTimeDelta = (int) (deletionTime.localDeletionTime() - serializationHeader.stats().minLocalDeletionTime);
        return serializationHeader.timestampSerializedSize(deletionTime.markedForDeleteAt())
             + TypeSizes.sizeofUnsignedVInt(localDeletionTimeDelta);
    }

    /**
     * Opens a complex column of the current row, with the merged deletion of that column.
     *
     * Call this method before you call writeCellHeader for any cell of the column. You can also
     * call it alone, for a column that has a deletion but no surviving cell.
     */
    public void startComplexColumn(ColumnMetadata column, DeletionTime mergedDeletion) throws IOException
    {
        closeOpenComplexMarker();
        advanceColumnSubset(column);
        if (complexMarkerCount == markerStartOffset.length)
        {
            // The arrays live for the life of the writer, and the schema bounds their size, so this
            // growth costs only at the start.
            int n = markerStartOffset.length * 2;
            markerStartOffset = Arrays.copyOf(markerStartOffset, n);
            markerEndOffset = Arrays.copyOf(markerEndOffset, n);
            markerCellCount = Arrays.copyOf(markerCellCount, n);
            markerDeletionMfda = Arrays.copyOf(markerDeletionMfda, n);
            markerDeletionLdt = Arrays.copyOf(markerDeletionLdt, n);
            markerColumn = Arrays.copyOf(markerColumn, n);
            markerLiveDataSize = Arrays.copyOf(markerLiveDataSize, n);
            markerLiveCellCount = Arrays.copyOf(markerLiveCellCount, n);
        }
        markerStartOffset[complexMarkerCount] = rowBuffer.getLength();
        markerEndOffset[complexMarkerCount] = -1;
        markerCellCount[complexMarkerCount] = 0;
        markerDeletionMfda[complexMarkerCount] = mergedDeletion.markedForDeleteAt();
        markerDeletionLdt[complexMarkerCount] = mergedDeletion.localDeletionTime();
        markerColumn[complexMarkerCount] = column;
        // ComplexColumnData.dataSize opens with complexDeletion.dataSize, which DeletionTime fixes
        // at 12 for a live deletion as well as a real one.
        markerLiveDataSize[complexMarkerCount] = DeletionTime.LIVE.dataSize();
        markerLiveCellCount[complexMarkerCount] = 0;
        // guardCollectionSize measures a multi-cell collection and nothing else, so a non-frozen UDT
        // is out.
        markerIsGuardedCollection = !collectionGuardsDisabled
                                    && column.type.isCollection() && column.type.isMultiCell();
        rowHasComplexDeletion |= !mergedDeletion.isLive();
        complexMarkerCount++;
        lastCellColumn = column;
        columnsWrittenCount++;
        // Do not collect tombstone statistics here. writeDeletionTime updates the collector when
        // the row is written, so a count here would be a second count of the same deletion.
    }

    private void closeOpenComplexMarker()
    {
        if (complexMarkerCount > 0 && markerEndOffset[complexMarkerCount - 1] < 0)
            markerEndOffset[complexMarkerCount - 1] = rowBuffer.getLength();
    }

    private void advanceColumnSubset(ColumnMetadata cellColumn)
    {
        for (; nextCellIndex < columns.length; nextCellIndex++) {
            if (columns[nextCellIndex].compareTo(cellColumn) == 0)
                break;
            missingColumns.addInt(nextCellIndex);
        }
        if (nextCellIndex == columns.length)
            throw new IllegalStateException("Column not found: " + cellColumn +" or cell writes out of order, or bug.");
        nextCellIndex++;
    }

    /** Adds the cell path of the current complex cell to the cell stream, as a vint length and
     *  then the path bytes. */
    public void writeCellPath(byte[] pathBuffer, int pathLength) throws IOException
    {
        // CellPath.dataSize is the sum of the raw component bytes, which for one component is the
        // path length itself.
        if (cellCountsTowardsCollection)
            markerLiveDataSize[complexMarkerCount - 1] += pathLength;
        rowBuffer.writeUnsignedVInt32(pathLength);
        rowBuffer.write(pathBuffer, 0, pathLength);
    }

    public void writeCellHeader(int cellFlags, CellLivenessInfo cellLiveness, ColumnMetadata cellColumn) throws IOException
    {
        if (cellColumn.isComplex())
        {
            // startComplexColumn advanced the column subset and counted the column, so count only
            // the cell here. Compare the column names, not the references: the winning cell can come
            // from an sstable whose header holds a different ColumnMetadata instance for this column.
            if (!ColumnMetadata.sameName(lastCellColumn, cellColumn))
                throw new IllegalStateException("complex cell without startComplexColumn: " + cellColumn);
            markerCellCount[complexMarkerCount - 1]++;
            // A tombstone and a lapsed TTL both survive this far, and neither one counts: the
            // reference purges the column with DeletionPurger.PURGE_ALL before it measures.
            cellCountsTowardsCollection = markerIsGuardedCollection && cellLiveness.isLive(nowInSec);
            if (cellCountsTowardsCollection)
            {
                markerLiveCellCount[complexMarkerCount - 1]++;
                markerLiveDataSize[complexMarkerCount - 1] += CELL_FIXED_DATA_SIZE;
            }
        }
        else
        {
            closeOpenComplexMarker();
            advanceColumnSubset(cellColumn);
            lastCellColumn = cellColumn;
            columnsWrittenCount++;
            cellCountsTowardsCollection = false;
        }
        writeCellHeader(cellFlags, cellLiveness, rowBuffer);
    }

    private void writeCellHeader(int cellFlags, CellLivenessInfo cellLiveness, DataOutputPlus writer) throws IOException
    {
        writer.writeByte(cellFlags);
        if (!Cell.Serializer.useRowTimestamp(cellFlags)) {
            long timestamp = cellLiveness.timestamp();
            serializationHeader.writeTimestamp(timestamp, writer);
        }
        if (!Cell.Serializer.useRowTTL(cellFlags)) {
            boolean isDeleted = Cell.Serializer.isDeleted(cellFlags);
            boolean isExpiring = Cell.Serializer.isExpiring(cellFlags);
            if (isDeleted || isExpiring) {
                serializationHeader.writeLocalDeletionTime(cellLiveness.localDeletionTime(), writer);
            }
            if (isExpiring) {
                serializationHeader.writeTTL(cellLiveness.ttl(), writer);
            }
        }
        /**
         * matching {@link org.apache.cassandra.db.rows.Cells#collectStats};
         */
        metadataCollector.updateCellLiveness(cellLiveness);
    }

    public int writeCellValue(SSTableCursorReader cursor, byte[] copyColumnValueBuffer) throws IOException
    {
        int state = cursor.copyCellValue(rowBuffer, copyColumnValueBuffer);
        if (cellCountsTowardsCollection)
            markerLiveDataSize[complexMarkerCount - 1] += cursor.lastCellValueLength();
        return state;
    }

    /**
     * @param rawValueLength the value bytes the buffer holds, without the length vint that a
     *                       variable-length type puts ahead of them
     */
    public void writeCellValue(DataOutputBuffer tempCellBuffer, int rawValueLength) throws IOException
    {
        if (cellCountsTowardsCollection)
            markerLiveDataSize[complexMarkerCount - 1] += rawValueLength;
        rowBuffer.write(tempCellBuffer.getData(), 0, tempCellBuffer.getLength());
    }

    public void writeRowEnd(UnfilteredDescriptor rHeader, boolean updateClusteringMetadata) throws IOException
    {
        boolean isExtended = isExtended(rowFlags);
        boolean isStatic = isExtended && UnfilteredSerializer.isStatic(rowExtendedFlags);
        int columnsLength = columns.length;

        // Each marker adds a header before its cells: a deletion, but only if HAS_COMPLEX_DELETION
        // is set, and then a vint cell count. The loop below sizes those header bytes, because the
        // row-size vint counts them.
        //
        // One marker with a non-live deletion sets HAS_COMPLEX_DELETION. Every complex column of the
        // row then writes a deletion, LIVE ones included, as UnfilteredSerializer does. The flags
        // byte leads the row, so the loop must set the flag before this method writes anything.
        boolean hasComplexDeletion = rowHasComplexDeletion;
        // Must run before anything is written and before writeRowCellSection: it sets the row
        // flags and closes the marker whose end offset that method reads.
        long cellSectionLength = rowBuffer.getLength() + closeComplexMarkers(hasComplexDeletion);
        guardCollectionSizes(rHeader);

        writeRowColumnsSubset(columnsLength);
        assert isStatic || dataWriter.position() == rowStartPosition
               : "dataWriter moved between writeRowStart and writeRowEnd: " + rowStartPosition + " != " + dataWriter.position();
        long unfilteredStartPosition = rowStartPosition;
        /** See: {@link UnfilteredSerializer#serialize} */
        dataWriter.writeByte(rowFlags);
        if (isExtended)
        {
            dataWriter.writeByte(rowExtendedFlags);
        }

        if (!isStatic)
        {
            byte[] clustering = rHeader.clusteringBytes();
            int clusteringLength = rHeader.clusteringLength();
            dataWriter.write(clustering, 0, clusteringLength);
            previousUnfilteredStart = unfilteredStartPosition;
        }
        // This size covers the whole row body, previousUnfilteredSize included. That field is the
        // first vint of rowHeaderBuffer. UnfilteredSerializer.serialize reaches the same total by a
        // different route: it adds the width of that vint to the size of a body buffer that leaves
        // the field out.
        //
        // cellSectionLength covers rowBuffer plus the marker headers that the loop below writes
        // straight to the data file.
        dataWriter.writeUnsignedVInt32(Math.toIntExact(rowHeaderBuffer.getLength() + cellSectionLength));

        dataWriter.write(rowHeaderBuffer.getData(), 0, rowHeaderBuffer.getLength());
        writeRowCellSection(hasComplexDeletion);

        long unfilteredEndPosition = getPosition();

        /**
         * These calls, and the cell metadata updates above, match
         * {@link org.apache.cassandra.db.rows.Rows#collectStats}. The iterator path collects row
         * stats for non-empty rows only:
         * {@link org.apache.cassandra.io.sstable.format.SortedTableWriter#addStaticRow} guards with
         * !row.isEmpty(). A static-column table whose partition holds no static value still writes
         * an empty static row, and that row must not count towards totalRows or totalColumnsSet. A
         * row is empty when it has no cell, no liveness timestamp, no TTL and no row deletion.
         */
        boolean rowIsEmpty = columnsWrittenCount == 0
                             && (rowFlags & (HAS_TIMESTAMP | HAS_TTL | HAS_DELETION)) == 0;
        if (!rowIsEmpty)
        {
            // Match Rows.collectStats and StatsAccumulation.accumulateOnColumnData. A complex
            // column counts towards totalColumnsSet only if it gave one cell or more. A column that
            // has a deletion and no cell still appears in the column subset above, but not here.
            metadataCollector.updateColumnSetPerRow(columnsWrittenCount - deletionOnlyMarkers);
        }

        if (isStatic)
        {
            // Nothing writes to dataWriter between the unfilteredEndPosition read above and here.
            cursorIndexWriter.staticRowWritten(unfilteredEndPosition);
        }
        else
        {
            updateMetadataAndIndexBlock(rHeader, unfilteredStartPosition, unfilteredEndPosition, updateClusteringMetadata);
        }
    }

    /**
     * The count of markers that have no cell. totalColumnsSet does not count them.
     *
     * <p>Set by {@link #closeComplexMarkers}, read by {@link #writeRowEnd} after it. Both live in
     * the same row write, so nothing else may read this.
     */
    private int deletionOnlyMarkers;

    /**
     * Closes the open complex marker, sets HAS_COMPLEX_DELETION, counts the deletion-only markers,
     * and measures the header bytes each marker adds before its cells. {@link #writeRowEnd} must
     * call this before it writes anything and before {@link #writeRowCellSection}, which reads the
     * marker end offsets this closes.
     *
     * <p>The flag has to be set here rather than later, because the flags byte leads the row. One
     * marker with a non-live deletion sets it, and every complex column of the row then writes a
     * deletion, LIVE ones included, as UnfilteredSerializer does.
     *
     * <p>The row-size vint counts the header bytes, hence the measurement.
     *
     * @return the bytes to add to the cell section length
     */
    private long closeComplexMarkers(boolean hasComplexDeletion)
    {
        deletionOnlyMarkers = 0;
        if (complexMarkerCount == 0)
            return 0;

        closeOpenComplexMarker();
        if (hasComplexDeletion)
            rowFlags |= UnfilteredSerializer.HAS_COMPLEX_DELETION;

        long headerLength = 0;
        for (int i = 0; i < complexMarkerCount; i++)
        {
            if (hasComplexDeletion)
            {
                reusableMarkerDeletion.reset(markerDeletionMfda[i], markerDeletionLdt[i]);
                headerLength += deletionTimeWrittenSize(reusableMarkerDeletion);
            }
            headerLength += TypeSizes.sizeofUnsignedVInt(markerCellCount[i]);
            if (markerCellCount[i] == 0)
                deletionOnlyMarkers++;
        }
        return headerLength;
    }

    /** Records which of the row's columns were written, either as a flag or as a subset encoding. */
    private void writeRowColumnsSubset(int columnsLength) throws IOException
    {
        if (columnsWrittenCount == columnsLength)
        {
            rowFlags |= HAS_ALL_COLUMNS;
            return;
        }
        if (columnsWrittenCount == 0)
        {
            // Same as Columns.serializer.serializeSubset(Columns.NONE, serializationHeader.columns(isStatic), rowHeaderBuffer)
            if (columnsLength < 64)
                // all the bits are set, because all the columns are missing, value is always positive
                rowHeaderBuffer.writeUnsignedVInt(-1L >>> (64 - columnsLength));
            else
                // large-subset form: all columns are missing, so no index follows the count
                rowHeaderBuffer.writeUnsignedVInt32(columnsLength);
            return;
        }
        if (columnsWrittenCount < columnsLength)
        {
            for (; nextCellIndex < columnsLength; nextCellIndex++)
                missingColumns.addInt(nextCellIndex);

            encodeColumnsSubset(missingColumns, columnsLength, rowHeaderBuffer);
        }
    }

    /** Writes the blocks of rowBuffer, and the header of each complex marker between them. */
    private void writeRowCellSection(boolean hasComplexDeletion) throws IOException
    {
        byte[] rowData = rowBuffer.getData();
        if (complexMarkerCount == 0)
        {
            dataWriter.write(rowData, 0, rowBuffer.getLength());
            return;
        }

        int pos = 0;
        for (int i = 0; i < complexMarkerCount; i++)
        {
            int start = markerStartOffset[i];
            dataWriter.write(rowData, pos, start - pos);
            if (hasComplexDeletion)
            {
                reusableMarkerDeletion.reset(markerDeletionMfda[i], markerDeletionLdt[i]);
                writeDeletionTime(reusableMarkerDeletion, dataWriter);
            }
            dataWriter.writeUnsignedVInt32(markerCellCount[i]);
            int end = markerEndOffset[i];
            dataWriter.write(rowData, start, end - start);
            pos = end;
        }
        dataWriter.write(rowData, pos, rowBuffer.getLength() - pos);
    }

    /**
     * See: {@link org.apache.cassandra.io.sstable.format.SortedTableWriter#addRangeTomstoneMarker}
     */
    public void writeRangeTombstone(UnfilteredDescriptor rangeTombstone, boolean updateClusteringMetadata) throws IOException
    {
        int tombstoneKind = rangeTombstone.clusteringKindEncoded();
        ClusteringPrefix.Kind kind = ClusteringPrefix.Kind.fromOrdinal(tombstoneKind);
        long unfilteredStartPosition = getPosition();
        /** See: {@link org.apache.cassandra.db.rows.UnfilteredSerializer#serialize */
        dataWriter.writeByte((byte)IS_MARKER);
        /** See: {@link org.apache.cassandra.db.ClusteringBoundOrBoundary.Serializer#serialize} */
        dataWriter.writeByte(tombstoneKind);
        dataWriter.writeShort(rangeTombstone.clusteringColumnsBound());

        int clusteringLength = rangeTombstone.clusteringLength();
        if (clusteringLength != 0)
        {
            byte[] clustering = rangeTombstone.clusteringBytes();
            dataWriter.write(clustering, 0, clusteringLength);
        }
        rowHeaderBuffer.clear();
        // previousUnfilteredSize leads the marker body, ahead of the deletion times
        rowHeaderBuffer.writeUnsignedVInt(unfilteredStartPosition - previousUnfilteredStart);
        previousUnfilteredStart = unfilteredStartPosition;

        if (kind.isBoundary())
        {
            writeDeletionTime(rangeTombstone.deletionTime(), rowHeaderBuffer);
            writeDeletionTime(rangeTombstone.deletionTime2(), rowHeaderBuffer);
            openMarker.reset(rangeTombstone.deletionTime2());
        }
        else
        {
            writeDeletionTime(rangeTombstone.deletionTime(), rowHeaderBuffer);
            if (kind.isOpen(false))
                openMarker.reset(rangeTombstone.deletionTime());
            else
                openMarker.resetLive();
        }

        // The size spans the whole marker body, previousUnfilteredSize included.
        // UnfilteredSerializer.serialize(RangeTombstoneMarker) reaches the same total by a different
        // route: it adds that field's vint width to a body size that leaves the field out.
        dataWriter.writeUnsignedVInt32(rowHeaderBuffer.getLength());
        dataWriter.write(rowHeaderBuffer.getData(), 0, rowHeaderBuffer.getLength());

        long unfilteredEndPosition = getPosition();

        /** {@link org.apache.cassandra.io.sstable.format.big.BigFormatPartitionWriter#addUnfiltered(org.apache.cassandra.db.rows.Unfiltered)} */
        // The index writer cuts a new index block when this marker takes the block past its size.
        updateMetadataAndIndexBlock(rangeTombstone, unfilteredStartPosition, unfilteredEndPosition, updateClusteringMetadata);
    }

    private void updateMetadataAndIndexBlock(UnfilteredDescriptor unfilteredDescriptor,
                                             long unfilteredStartPosition,
                                             long unfilteredEndPosition,
                                             boolean updateClusteringMetadata) throws IOException
    {
        if (updateClusteringMetadata) updateClusteringMetadata(unfilteredDescriptor);
        cursorIndexWriter.rowWritten(unfilteredDescriptor, unfilteredStartPosition, unfilteredEndPosition, openMarker);
    }

    public void updateClusteringMetadata(ClusteringDescriptor clusteringDescriptor)
    {
        metadataCollector.updateClusteringValues(clusteringDescriptor);
    }

    /**
     * Garbage-free equivalent of {@link org.apache.cassandra.db.Columns.Serializer}'s
     * serializeSubset for a PARTIAL subset; callers handle the all-present and all-missing fast
     * paths. The bytes must stay identical to the upstream serializer's.
     *
     * @param missingColumns ascending superset positions of the columns ABSENT from the row
     */
    @VisibleForTesting
    static void encodeColumnsSubset(IntArrayList missingColumns, int supersetCount, DataOutputBuffer out) throws IOException
    {
        if (supersetCount < 64)
        {
            // set a bit for every missing column (Columns.Serializer.encodeBitmap)
            long mask = 0;
            for (int i = 0; i < missingColumns.size(); i++)
                mask |= 1L << missingColumns.getInt(i);
            out.writeUnsignedVInt(mask);
            return;
        }

        out.writeUnsignedVInt32(missingColumns.size());
        // Mode selection must mirror Columns.Serializer.serializeLargeSubset AND its
        // deserializer exactly: present-index mode iff presentCount < supersetCount / 2. An
        // equivalent-looking test on the missing count disagrees for an odd superset size.
        int presentCount = supersetCount - missingColumns.size();
        if (presentCount < supersetCount / 2)
        {
            // Write present column indices: the gaps between the missing indices, and the tail
            // after the last missing index. Dropping that tail drops every present column that
            // sorts after the last missing one, and the deserializer then reads row-body bytes as
            // column indices.
            int presentIndex = 0;
            for (int i = 0; i < missingColumns.size(); i++)
            {
                int missingIndex = missingColumns.getInt(i);
                for (; presentIndex < missingIndex; presentIndex++)
                    out.writeUnsignedVInt32(presentIndex);
                presentIndex = missingIndex + 1;
            }
            for (; presentIndex < supersetCount; presentIndex++)
                out.writeUnsignedVInt32(presentIndex);
        }
        else
        {
            // write missing columns (indexed loop: agrona's for-each would box per element)
            for (int i = 0; i < missingColumns.size(); i++)
                out.writeUnsignedVInt32(missingColumns.getInt(i));
        }
    }

    public void setLast(ByteBuffer key)
    {
        IPartitioner partitioner = ssTableWriter.getPartitioner();
        DecoratedKey last = partitioner.decorateKey(ByteBufferUtil.clone(key));
        ssTableWriter.setLast(last);
    }

    public void setFirst(ByteBuffer key)
    {
        IPartitioner partitioner = ssTableWriter.getPartitioner();
        DecoratedKey first = partitioner.decorateKey(ByteBufferUtil.clone(key));
        ssTableWriter.setFirst(first);
        ssTableWriter.setLast(first);
    }

    public IPartitioner partitioner()
    {
        return ssTableWriter.getPartitioner();
    }

    public DeletionTime openMarker() {
        return openMarker;
    }
}
