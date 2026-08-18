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

import com.google.common.annotations.VisibleForTesting;

import org.agrona.collections.IntArrayList;

import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.DeletionTime.ReusableDeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.SerializationHeader;
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
import org.apache.cassandra.utils.ByteBufferUtil;
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
    // File position of the previous non-static unfiltered's first byte, or the partition start while the
    // partition has none. previousUnfilteredSize is the distance from it, exactly as the iterator path
    // computes it (see SortedTablePartitionWriter.addUnfiltered): rows/markers write the distance from
    // the previous unfiltered's start; static rows write 0 and do not advance it.
    private long previousUnfilteredStart;
    // The dataWriter position writeRowStart encoded previousUnfilteredSize against. Nothing may write to
    // dataWriter between writeRowStart and writeRowEnd or that distance is stale, so writeRowEnd asserts
    // the position has not moved.
    private long rowStartPosition;
    // ROW contents, needed because of the order of writing and the var int fields
    private int rowFlags; // discovered as we go along
    private int rowExtendedFlags;
    private final DataOutputBuffer rowHeaderBuffer = new DataOutputBuffer(); // holds the contents between FLAGS and SIZE
    private final DataOutputBuffer rowBuffer = new DataOutputBuffer();
    private final ReusableDeletionTime openMarker = ReusableDeletionTime.live();

    private final ColumnMetadata[] staticColumns;
    private final ColumnMetadata[] regularColumns;
    private final IntArrayList missingColumns = new IntArrayList();
    private ColumnMetadata[] columns; // points to static/regular
    private int columnsWrittenCount = 0;
    private int nextCellIndex = 0;
    // Format-specific index production (BIG: promoted blocks + Index.db + bloom filter/summary;
    // other formats own their own index structures accordingly)
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
        // TOD: we should be able to skip the use of the row buffers in this special case, maybe it doesn't matter
        rowHeaderBuffer.clear();
        rowHeaderBuffer.writeUnsignedVInt(0L); // previousUnfilteredSize, always 0 for a static row
        rowBuffer.clear();
        columnsWrittenCount = 0;
        missingColumns.clear();
        writeRowEnd(null, false);

        cursorIndexWriter.staticRowWritten(dataWriter.position());
        return true;
    }

    public void writeRowStart(LivenessInfo livenessInfo, DeletionTime deletionTime, boolean isShadowable, boolean isStatic) throws IOException
    {
        // Row.Deletion's own constructor enforces this: a live deletion is never shadowable. Callers
        // (CursorCompactor.mergeRows) must reset isShadowable alongside every reset of deletionTime to
        // LIVE; this catches a future caller that adds a reset path and misses that pairing.
        assert !deletionTime.isLive() || !isShadowable : "shadowable deletion must not be live";
        rowExtendedFlags = 0;
        if (isStatic)
            rowExtendedFlags |= UnfilteredSerializer.IS_STATIC;
        if (isShadowable)
            rowExtendedFlags |= UnfilteredSerializer.HAS_SHADOWABLE_DELETION;
        rowFlags = rowExtendedFlags != 0 ? UnfilteredSerializer.EXTENSION_FLAG : 0;
        columns = isStatic ? staticColumns : regularColumns;
        // NOTE: Data after this point needs a computed ahead of write size. This, combined with the cost of rewriting
        // the size after the writing completes, means we have to buffer the row timestamps (most likely to differ in length)
        // and the row columns data (will differ if they use their own timestamps, probably). Unfortunate.
        // rest of header
        rowHeaderBuffer.clear();
        // previousUnfilteredSize leads the row body, ahead of the liveness data. dataWriter has not been
        // written since the previous unfiltered finished, so its position is this unfiltered's first byte.
        rowStartPosition = dataWriter.position();
        rowHeaderBuffer.writeUnsignedVInt(isStatic ? 0 : rowStartPosition - previousUnfilteredStart);
        missingColumns.clear();
        rowBuffer.clear();
        columnsWrittenCount = 0;
        nextCellIndex = 0;

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
         * Metadata calls matching: {@link org.apache.cassandra.db.rows.Rows#collectStats}
         * But the collection of data is conditional and the cell metadata is collected elsewhere.
         */
        return flags;
    }

    private void writeDeletionTime(DeletionTime deletionTime, DataOutputPlus writer) throws IOException
    {
        serializationHeader.writeDeletionTime(deletionTime, writer);
        metadataCollector.update(deletionTime);
    }

    public void writeCellHeader(int cellFlags, CellLivenessInfo cellLiveness, ColumnMetadata cellColumn) throws IOException
    {
        for (; nextCellIndex < columns.length; nextCellIndex++) {
            if (columns[nextCellIndex].compareTo(cellColumn) == 0)
                break;
            missingColumns.addInt(nextCellIndex);
        }
        if (nextCellIndex == columns.length)
            throw new IllegalStateException("Column not found: " + cellColumn +" or cell writes out of order, or bug.");
        nextCellIndex++;
        writeCellHeader(cellFlags, cellLiveness, rowBuffer);
    }

    private void writeCellHeader(int cellFlags, CellLivenessInfo cellLiveness, DataOutputPlus writer) throws IOException
    {
        columnsWrittenCount++;
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
        return cursor.copyCellValue(rowBuffer, copyColumnValueBuffer);
    }

    public void writeCellValue(DataOutputBuffer tempCellBuffer) throws IOException
    {
        rowBuffer.write(tempCellBuffer.getData(), 0, tempCellBuffer.getLength());
    }

    public void writeRowEnd(UnfilteredDescriptor rHeader, boolean updateClusteringMetadata) throws IOException
    {
        boolean isExtended = isExtended(rowFlags);
        boolean isStatic = isExtended && UnfilteredSerializer.isStatic(rowExtendedFlags);
        int columnsLength = columns.length;
        if (columnsWrittenCount == columnsLength)
        {
            rowFlags |= HAS_ALL_COLUMNS;
        }
        else if (columnsWrittenCount == 0) {
            // Same as Columns.serializer.serializeSubset(Columns.NONE, serializationHeader.columns(isStatic), rowHeaderBuffer)
            if (columnsLength < 64) {
                // all the bits are set, because all the columns are missing, value is always positive
                rowHeaderBuffer.writeUnsignedVInt(-1L >>> (64 - columnsLength));
            }
            else {
                // no columns are present, nothing to write
                rowHeaderBuffer.writeUnsignedVInt32(columnsLength);
            }
        }
        else if (columnsWrittenCount < columnsLength)
        {
            for (; nextCellIndex < columnsLength; nextCellIndex++)
                missingColumns.addInt(nextCellIndex);

            encodeColumnsSubset(missingColumns, columnsLength, rowHeaderBuffer);
        }
        // rowStartPosition was already captured in writeRowStart, and the invariant this class
        // documents (nothing writes to dataWriter between writeRowStart and writeRowEnd) means
        // a fresh read here would always equal it; reuse it instead of re-reading. The verifying
        // read moves into the assert itself (evaluated before any of this method's own writes),
        // so it costs nothing with assertions disabled.
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
        // The size spans the whole row body, previousUnfilteredSize included: it is the leading vint of
        // rowHeaderBuffer. UnfilteredSerializer.serialize reaches the same bytes by adding that field's
        // vint width to the size of a body buffer that excludes it.
        dataWriter.writeUnsignedVInt32(rowHeaderBuffer.getLength() + rowBuffer.getLength());

        dataWriter.write(rowHeaderBuffer.getData(), 0, rowHeaderBuffer.getLength());
        dataWriter.write(rowBuffer.getData(), 0, rowBuffer.getLength());

        long unfilteredEndPosition = getPosition();

        /**
         * Matching the: {@link org.apache.cassandra.db.rows.Rows#collectStats} along with above cell level metadata updates.
         * The iterator path only collects row stats for non-empty rows
         * ({@link org.apache.cassandra.io.sstable.format.SortedTableWriter#addStaticRow} guards with
         * !row.isEmpty()): an empty static row is still WRITTEN for static-column tables whose
         * partition has no static values, but it must not count towards totalRows/totalColumnsSet.
         * Empty == no cells, no liveness timestamp/TTL, no row deletion.
         */
        boolean rowIsEmpty = columnsWrittenCount == 0
                             && (rowFlags & (HAS_TIMESTAMP | HAS_TTL | HAS_DELETION)) == 0;
        if (!rowIsEmpty)
            metadataCollector.updateColumnSetPerRow(columnsWrittenCount);

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

        // The size spans the whole marker body, previousUnfilteredSize included; see
        // UnfilteredSerializer.serialize(RangeTombstoneMarker...), which reaches the same bytes by
        // adding that field's vint width to a body size that excludes it.
        dataWriter.writeUnsignedVInt32(rowHeaderBuffer.getLength());
        dataWriter.write(rowHeaderBuffer.getData(), 0, rowHeaderBuffer.getLength());

        long unfilteredEndPosition = getPosition();

        /** {@link org.apache.cassandra.io.sstable.format.big.BigFormatPartitionWriter#addUnfiltered(org.apache.cassandra.db.rows.Unfiltered)} */
        // if we hit the index block size that we have to index after, go ahead and index it.
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
     * serializeSubset for a PARTIAL subset (not all-present, not all-missing — callers
     * handle those fast paths): the < 64 missing-bitmap form, or the large-subset form
     * (delta vint + present- or missing-index vints). Hand-mirrored because the upstream
     * serializer requires a materialized Columns + iterator per call — a per-row allocation
     * this path must not make. The mirror has already drifted from the upstream serializer
     * once, producing corrupt output; {@code CursorColumnsSubsetEncodingTest} pins it byte-for-byte
     * against the upstream serializer across sizes, shapes, and both mode boundaries.
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
        // deserializer exactly: present-index mode iff presentCount < supersetCount / 2.
        // The previous condition (missing > supersetCount / 2) agreed for even superset
        // sizes but flipped the mode for odd sizes at missing == supersetCount/2 + 1,
        // which the deserializer then read in the WRONG mode — corrupted output.
        int presentCount = supersetCount - missingColumns.size();
        if (presentCount < supersetCount / 2)
        {
            // write present column indices: the gaps between missing indices, INCLUDING the
            // tail after the last missing index — the previous tail loop's bound was the
            // last missing index itself (vacuously empty), so present columns sorting after
            // the last missing one were silently dropped from the encoding and the
            // deserializer consumed row-body bytes as column indices — corrupted output
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
