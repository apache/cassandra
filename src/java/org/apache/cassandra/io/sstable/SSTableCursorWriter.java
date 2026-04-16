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

import com.google.common.primitives.Ints;

import org.agrona.collections.IntArrayList;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.DeletionTime.ReusableDeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.ReusableLivenessInfo;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.db.guardrails.Threshold;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredSerializer;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SortedTableWriter;
import org.apache.cassandra.io.sstable.format.big.BigFormatPartitionWriter;
import org.apache.cassandra.io.sstable.format.big.BigTableWriter;
import org.apache.cassandra.io.sstable.format.big.RowIndexEntry;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.utils.ByteArrayUtil;
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
    /**
     * See: {@link BloomFilter#reusableIndexes}
     */
    private final long[] reusableIndexes = new long[21];
    private final boolean hasStaticColumns;

    private long partitionStart;
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
    // Index info
    private final DataOutputBuffer rowIndexEntries = new DataOutputBuffer();
    private final IntArrayList rowIndexEntriesOffsets = new IntArrayList();
    private final ClusteringDescriptor rowIndexEntryLastClustering;
    private boolean hasDistinctLastClustering = false;
    private int indexBlockStartOffset;
    private int rowIndexEntryOffset;
    private final int indexBlockThreshold;

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
        this.indexBlockThreshold = DatabaseDescriptor.getColumnIndexSize(BigFormatPartitionWriter.DEFAULT_GRANULARITY);
        rowIndexEntryLastClustering = new ClusteringDescriptor(serializationHeader.clusteringTypes().toArray(AbstractType[]::new));
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
        rowIndexEntries.clear();
        rowIndexEntriesOffsets.clear();
        rowIndexEntryOffset = 0;
        openMarker.resetLive();
        hasDistinctLastClustering = false;

        partitionStart = dataWriter.position();
        writePartitionHeader(partitionKey, partitionKeyLength, partitionDeletionTime);
        updateIndexBlockStartOffset(dataWriter.position());
        return indexBlockStartOffset;
    }

    public void writePartitionEnd(byte[] partitionKey, int partitionKeyLength, DeletionTime partitionDeletionTime, int headerLength) throws IOException
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
        appendBIGIndex(partitionKey, partitionKeyLength, partitionStart, headerLength, partitionDeletionTime, partitionEnd);
    }

    private void appendBIGIndex(byte[] key, int keyLength, long partitionStart, int headerLength, DeletionTime partitionDeletionTime, long partitionEnd) throws IOException
    {
        /**
         * {@link BigTableWriter#createRowIndexEntry(DecoratedKey, DeletionTime, long)}
         * {@link BigTableWriter.IndexWriter#append(DecoratedKey, RowIndexEntry, long, ByteBuffer)}
         *
         */
        BigTableWriter.IndexWriter indexWriter = (BigTableWriter.IndexWriter) this.indexWriter;
        SequentialWriter indexFileWriter = indexWriter.writer;
        ((BloomFilter)indexWriter.bf).add(key, 0, keyLength, reusableIndexes);
        long indexStart = indexFileWriter.position();
        try
        {
            ByteArrayUtil.writeWithShortLength(key, 0, keyLength, indexFileWriter);

            indexFileWriter.writeUnsignedVInt(partitionStart);

            // if the list of entries has one or fewer entries, no point in index entries.
            /** See: {@link org.apache.cassandra.io.sstable.format.big.RowIndexEntry#create} */
            if (rowIndexEntriesOffsets.size() <= 1)
            {
                /**
                 * {@link RowIndexEntry#serialize(DataOutputPlus, ByteBuffer)}
                 */
                indexFileWriter.writeUnsignedVInt32(0); // size
            }
            else {
                // add last block
                long indexBlockSize = (partitionEnd - partitionStart - 1) - indexBlockStartOffset;
                if (indexBlockSize != 0) {
                    addIndexBlock(partitionEnd - 1, indexBlockSize);
                }
                // if we have intermeddiate index info elements we also need to serialize the partitionDeletionTime
                /** {@link RowIndexEntry.IndexedEntry#serialize(DataOutputPlus, ByteBuffer) */
                // size up to the offsets?
                int endOfEntries = rowIndexEntries.getLength();
                // Write the headerLength, partitionDeletionTime and rowIndexEntriesOffsets.size() after the entries,
                // just to calculate size.
                rowIndexEntries.writeUnsignedVInt((long)headerLength);
                deletionTimeSerializer.serialize(partitionDeletionTime, rowIndexEntries);

                rowIndexEntries.writeUnsignedVInt32(rowIndexEntriesOffsets.size()); // number of entries

                // bytes until offsets
                int entriesAndOffsetsSize = rowIndexEntries.getLength() + rowIndexEntriesOffsets.size() * 4;
                assert entriesAndOffsetsSize > 0;
                indexFileWriter.writeUnsignedVInt32(entriesAndOffsetsSize); // size != 0
                // copy the header elements
                indexFileWriter.write(rowIndexEntries.getData(), endOfEntries, rowIndexEntries.getLength() - endOfEntries);
                indexFileWriter.write(rowIndexEntries.getData(), 0, endOfEntries);
                for (int i = 0; i < rowIndexEntriesOffsets.size(); i++)
                {
                    int offset = rowIndexEntriesOffsets.get(i);
                    indexFileWriter.writeInt(offset);
                }
            }
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, indexFileWriter.getPath());
        }
        indexWriter.summary.maybeAddEntry(key, 0, keyLength, indexStart);
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
        // NOTE: if we are to write this value (which is not used), this is where we should compute it.
        rowHeaderBuffer.writeUnsignedVInt32(0);
        rowBuffer.clear();
        columnsWrittenCount = 0;
        missingColumns.clear();
        writeRowEnd(null, false);

        updateIndexBlockStartOffset(dataWriter.position());
        return true;
    }

    public void writeRowStart(LivenessInfo livenessInfo, DeletionTime deletionTime, boolean isStatic) throws IOException
    {
        if (isStatic) {
            rowFlags = UnfilteredSerializer.EXTENSION_FLAG;
            rowExtendedFlags = UnfilteredSerializer.IS_STATIC;
            columns = staticColumns;
        }
        else {
            rowFlags = 0;
            rowExtendedFlags = 0;
            columns = regularColumns;
        }
        // NOTE: Data after this point needs a computed ahead of write size. This, combined with the cost of rewriting
        // the size after the writing completes, means we have to buffer the row timestamps (most likely to differ in length)
        // and the row columns data (will differ if they use their own timestamps, probably). Unfortunate.
        // rest of header
        rowHeaderBuffer.clear();
        missingColumns.clear();
        rowBuffer.clear();
        columnsWrittenCount = 0;
        nextCellIndex = 0;

        // NOTE: if we are to write this value (which is not used), this is where we should compute it.
        rowHeaderBuffer.writeUnsignedVInt32(0);

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

    public void writeCellHeader(int cellFlags, ReusableLivenessInfo cellLiveness, ColumnMetadata cellColumn) throws IOException
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

    private void writeCellHeader(int cellFlags, ReusableLivenessInfo cellLiveness, DataOutputPlus writer) throws IOException
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
                // TODO: is this conversion from LET to LDT correct?
                serializationHeader.writeLocalDeletionTime(cellLiveness.localExpirationTime(), writer);
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

            if (columnsLength < 64) {
                // set a bit for every missing column
                long mask = 0;
                for (int missingIndex : missingColumns) {
                    mask |= (1L << missingIndex);
                }
                rowHeaderBuffer.writeUnsignedVInt(mask);
            }
            else {
                encodeLargeColumnsSubset();
            }
        }
        long unfilteredStartPosition = dataWriter.position();
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
        }

        // Now that we know the size, write it + the rest of the data
        dataWriter.writeUnsignedVInt32(rowHeaderBuffer.getLength() + rowBuffer.getLength());

        dataWriter.write(rowHeaderBuffer.getData(), 0, rowHeaderBuffer.getLength());
        dataWriter.write(rowBuffer.getData(), 0, rowBuffer.getLength());

        long unfilteredEndPosition = getPosition();

        /**
         * Matching the: {@link org.apache.cassandra.db.rows.Rows#collectStats} along with above cell level metadata updates
         */
        metadataCollector.updateColumnSetPerRow(columnsWrittenCount);

        if (isStatic)
        {
            updateIndexBlockStartOffset(dataWriter.position());
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
        ClusteringPrefix.Kind kind = ClusteringPrefix.Kind.values()[tombstoneKind];
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
        // TODO: previousUnfilteredSize
        rowHeaderBuffer.writeUnsignedVInt32(0);

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

        dataWriter.writeUnsignedVInt32(rowHeaderBuffer.getLength());
        dataWriter.write(rowHeaderBuffer.getData(), 0, rowHeaderBuffer.getLength());

        long unfilteredEndPosition = getPosition();

        /** {@link org.apache.cassandra.io.sstable.format.big.BigFormatPartitionWriter#addUnfiltered(Unfiltered)} */
        // if we hit the index block size that we have to index after, go ahead and index it.
        updateMetadataAndIndexBlock(rangeTombstone, unfilteredStartPosition, unfilteredEndPosition, updateClusteringMetadata);
    }

    private void updateMetadataAndIndexBlock(UnfilteredDescriptor unfilteredDescriptor,
                                             long unfilteredStartPosition,
                                             long unfilteredEndPosition,
                                             boolean updateClusteringMetadata) throws IOException
    {
        if (updateClusteringMetadata) updateClusteringMetadata(unfilteredDescriptor);
        // write the first clustering into rowIndexEntries buffer (we will need it unless we never write the first entry)
        if (currentOffsetInPartition(unfilteredStartPosition) == indexBlockStartOffset || (rowIndexEntryOffset == rowIndexEntries.position()))
        {
            writeClusteringToRowIndexEntries(unfilteredDescriptor);
        }
        else
        {
            rowIndexEntryLastClustering.copy(unfilteredDescriptor);
            hasDistinctLastClustering = true;
        }

        /** {@link BigFormatPartitionWriter#addUnfiltered(Unfiltered)} */
        // if we hit the index block size that we have to index after, go ahead and index it.
        long indexBlockSize = currentOffsetInPartition(unfilteredEndPosition) - indexBlockStartOffset;
        if (indexBlockSize >= this.indexBlockThreshold)
            addIndexBlock(unfilteredEndPosition, indexBlockSize);
    }

    public void updateClusteringMetadata(UnfilteredDescriptor unfilteredDescriptor)
    {
        metadataCollector.updateClusteringValues(unfilteredDescriptor);
    }

    /**
     *  See:
     *  {@link BigFormatPartitionWriter#addIndexBlock()}
     *  - {@link org.apache.cassandra.io.sstable.IndexInfo.Serializer#serialize(org.apache.cassandra.io.sstable.IndexInfo, org.apache.cassandra.io.util.DataOutputPlus)}
     */
    private void addIndexBlock(long endOfRowPosition, long indexBlockSize) throws IOException
    {
        if (rowIndexEntriesOffsets.isEmpty() && rowIndexEntryOffset != 0) {
            throw new IllegalStateException();
        }

        // serialize the index info
        /** {@link org.apache.cassandra.io.sstable.IndexInfo.Serializer#serialize(org.apache.cassandra.io.sstable.IndexInfo, org.apache.cassandra.io.util.DataOutputPlus)}*/
        rowIndexEntriesOffsets.addInt(rowIndexEntryOffset);

        // first clustering is already in, write last entry
        if (!hasDistinctLastClustering)
        {
            // first entry is the last entry, copy it
            byte[] entriesData = rowIndexEntries.getData();
            long endOfFirstEntry = rowIndexEntries.position();
            rowIndexEntries.write(entriesData, rowIndexEntryOffset, (int) (endOfFirstEntry - rowIndexEntryOffset));
        }
        else
        {
            writeClusteringToRowIndexEntries(rowIndexEntryLastClustering);
            rowIndexEntryLastClustering.resetClustering();
        }
        hasDistinctLastClustering = false;

        rowIndexEntries.writeUnsignedVInt((long)indexBlockStartOffset);
        rowIndexEntries.writeVInt(indexBlockSize - IndexInfo.Serializer.WIDTH_BASE);

        boolean isDeleteTimePresent = !openMarker.isLive();
        rowIndexEntries.writeBoolean(isDeleteTimePresent);
        if (isDeleteTimePresent)
            deletionTimeSerializer.serialize(openMarker, rowIndexEntries);
        // next block starts
        rowIndexEntryOffset = Ints.checkedCast(rowIndexEntries.position());
        updateIndexBlockStartOffset(endOfRowPosition);
    }

    private void updateIndexBlockStartOffset(long endOfRowPosition)
    {
        indexBlockStartOffset = (int) (endOfRowPosition - partitionStart);
    }

    private void writeClusteringToRowIndexEntries(ClusteringDescriptor clustering) throws IOException
    {
        ClusteringPrefix.Kind kind = clustering.clusteringKind();
        rowIndexEntries.writeByte(kind.ordinal());
        if (kind != ClusteringPrefix.Kind.CLUSTERING)
            rowIndexEntries.writeShort(clustering.clusteringColumnsBound());
        rowIndexEntries.write(clustering.clusteringBytes(), 0, clustering.clusteringLength());
    }

    private long currentOffsetInPartition(long position)
    {
        return position - partitionStart;
    }

    private void encodeLargeColumnsSubset() throws IOException
    {
        // no columns are present, nothing to write
        rowHeaderBuffer.writeUnsignedVInt32(missingColumns.size());
        if (missingColumns.size() > columns.length / 2)
        {
            // write present columns
            int presentIndex = 0;
            int missingIndex = 0;
            for (int i = 0; i < missingColumns.size(); i++)
            {
                missingIndex = missingColumns.get(i);
                for (; presentIndex < missingIndex; presentIndex++)
                    rowHeaderBuffer.writeUnsignedVInt32(presentIndex);
                presentIndex = missingIndex + 1;
            }
            if (missingIndex < columns.length-1) {
                for (; presentIndex < missingIndex; presentIndex++)
                    rowHeaderBuffer.writeUnsignedVInt32(presentIndex);
            }
        }
        else
        {
            // write missing columns
            for (int missingIndex : missingColumns) {
                rowHeaderBuffer.writeUnsignedVInt32(missingIndex);
            }
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
