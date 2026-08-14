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

package org.apache.cassandra.index.sai.disk.v5;

import java.io.IOException;
import java.util.Arrays;
import java.util.PrimitiveIterator;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.IntArrayList;
import org.apache.cassandra.index.sai.disk.vector.OnDiskOrdinalsMap;
import org.apache.cassandra.index.sai.disk.vector.OrdinalsView;
import org.apache.cassandra.index.sai.disk.vector.RowIdsView;
import org.apache.cassandra.index.sai.utils.SingletonIntIterator;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.RandomAccessReader;

import static java.lang.Math.max;
import static java.lang.Math.min;

public class V5OnDiskOrdinalsMap implements OnDiskOrdinalsMap
{
    private static final Logger logger = LoggerFactory.getLogger(V5OnDiskOrdinalsMap.class);

    private static final OneToOneRowIdsView ONE_TO_ONE_ROW_IDS_VIEW = new OneToOneRowIdsView();
    private static final EmptyOrdinalsView EMPTY_ORDINALS_VIEW = new EmptyOrdinalsView();
    private static final EmptyRowIdsView EMPTY_ROW_IDS_VIEW = new EmptyRowIdsView();

    private final FileHandle fh;
    private final long ordToRowOffset;
    private final long segmentEnd;
    private final int maxOrdinal;
    private final int maxRowId;
    private final long rowToOrdinalOffset;
    @VisibleForTesting
    final V5VectorPostingsWriter.Structure structure;

    private final Supplier<OrdinalsView> ordinalsViewSupplier;
    private final Supplier<RowIdsView> rowIdsViewSupplier;

    // cached values for OneToMany structure
    private Int2ObjectHashMap<int[]> extraRowsByOrdinal = null;
    private int[] extraRowIds = null;
    private int[] extraOrdinals = null;


    public V5OnDiskOrdinalsMap(FileHandle fh, long segmentOffset, long segmentLength)
    {
        this.segmentEnd = segmentOffset + segmentLength;
        this.fh = fh;
        try (var reader = fh.createReader())
        {
            reader.seek(segmentOffset);
            int magic = reader.readInt();
            if (magic != V5VectorPostingsWriter.MAGIC)
            {
                throw new RuntimeException("Invalid magic number in V5OnDiskOrdinalsMap");
            }
            this.structure = V5VectorPostingsWriter.Structure.values()[reader.readInt()];
            this.maxOrdinal = reader.readInt();
            this.maxRowId = reader.readInt();
            this.ordToRowOffset = reader.getFilePointer();
            if (structure == V5VectorPostingsWriter.Structure.ONE_TO_ONE)
            {
                this.rowToOrdinalOffset = segmentEnd;
            }
            else
            {
                reader.seek(segmentEnd - 8);
                this.rowToOrdinalOffset = reader.readLong();
            }

            if (maxOrdinal < 0)
            {
                this.rowIdsViewSupplier = () -> EMPTY_ROW_IDS_VIEW;
                this.ordinalsViewSupplier = () -> EMPTY_ORDINALS_VIEW;
            }
            else if (structure == V5VectorPostingsWriter.Structure.ONE_TO_ONE)
            {
                this.rowIdsViewSupplier = () -> ONE_TO_ONE_ROW_IDS_VIEW;
                this.ordinalsViewSupplier = () -> new OneToOneOrdinalsView(maxOrdinal + 1);
            }
            else if (structure == V5VectorPostingsWriter.Structure.ONE_TO_MANY)
            {
                cacheExtraRowIds(reader);
                cacheExtraRowOrdinals(reader);
                this.rowIdsViewSupplier = OneToManyRowIdsView::new;
                this.ordinalsViewSupplier = OneToManyOrdinalsView::new;
            }
            else
            {
                this.rowIdsViewSupplier = GenericRowIdsView::new;
                this.ordinalsViewSupplier = GenericOrdinalsView::new;
            }

            assert rowToOrdinalOffset <= segmentEnd : "rowOrdinalOffset " + rowToOrdinalOffset + " is not less than or equal to segmentEnd " + segmentEnd;
        }
        catch (Exception e)
        {
            throw new RuntimeException("Error initializing OnDiskOrdinalsMap at segment " + segmentOffset, e);
        }
    }

    @Override
    public V5VectorPostingsWriter.Structure getStructure()
    {
        return structure;
    }

    private void cacheExtraRowIds(RandomAccessReader reader) throws IOException
    {
        extraRowsByOrdinal = new Int2ObjectHashMap<>();
        reader.seek(ordToRowOffset);
        int entryCount = reader.readInt();
        for (int i = 0; i < entryCount; i++)
        {
            int ordinal = reader.readInt();
            int postingsSize = reader.readInt();
            if (postingsSize > 0)
                postingsSize++; // add the ordinal itself
            int[] rowIds = new int[postingsSize];
            if (postingsSize > 0)
            {
                rowIds[0] = ordinal;
                for (int j = 1; j < postingsSize; j++)
                    rowIds[j] = reader.readInt();
            }
            extraRowsByOrdinal.put(ordinal, rowIds);
        }
    }

    private void cacheExtraRowOrdinals(RandomAccessReader reader) throws IOException
    {
        var extraRowIdsList = new IntArrayList();
        var extraOrdinalsList = new IntArrayList();
        reader.seek(rowToOrdinalOffset);
        while (reader.getFilePointer() < segmentEnd - 8)
        {
            extraRowIdsList.add(reader.readInt());
            extraOrdinalsList.add(reader.readInt());
        }

        extraRowIds = extraRowIdsList.toIntArray();
        extraOrdinals = extraOrdinalsList.toIntArray();
    }

    public RowIdsView getRowIdsView()
    {
        return rowIdsViewSupplier.get();
    }

    private class GenericRowIdsView implements RowIdsView
    {
        RandomAccessReader reader = fh.createReader();

        @Override
        public PrimitiveIterator.OfInt getSegmentRowIdsMatching(int vectorOrdinal) throws IOException
        {
            Preconditions.checkArgument(vectorOrdinal <= maxOrdinal, "vectorOrdinal %s is out of bounds %s", vectorOrdinal, maxOrdinal);

            // read index entry
            try
            {
                reader.seek(ordToRowOffset + vectorOrdinal * 8L);
            }
            catch (Exception e)
            {
                throw new RuntimeException(String.format("Error seeking to index offset for ordinal %d with ordToRowOffset %d",
                                                         vectorOrdinal, ordToRowOffset), e);
            }
            var offset = reader.readLong();
            // seek to and read rowIds
            try
            {
                reader.seek(offset);
            }
            catch (Exception e)
            {
                throw new RuntimeException(String.format("Error seeking to rowIds offset for ordinal %d with ordToRowOffset %d",
                                                         vectorOrdinal, ordToRowOffset), e);
            }
            var postingsSize = reader.readInt();

            // Optimize for the most common case
            if (postingsSize == 1)
                return new SingletonIntIterator(reader.readInt());

            var rowIds = new int[postingsSize];
            for (var i = 0; i < rowIds.length; i++)
            {
                rowIds[i] = reader.readInt();
            }
            return Arrays.stream(rowIds).iterator();
        }

        @Override
        public void close()
        {
            reader.close();
        }
    }

    public OrdinalsView getOrdinalsView()
    {
        return ordinalsViewSupplier.get();
    }

    @NotThreadSafe
    private class GenericOrdinalsView implements OrdinalsView
    {
        RandomAccessReader reader = fh.createReader();

        /**
         * @return ordinal if given row id is found; otherwise return -1
         * rowId must increase
         */
        @Override
        public int getOrdinalForRowId(int rowId) throws IOException
        {
            long offset = rowToOrdinalOffset + (long) rowId * 8;
            if (offset >= segmentEnd - 8)
                return -1;

            reader.seek(offset);
            int foundRowId = reader.readInt();
            assert foundRowId == rowId : "foundRowId=" + foundRowId + " instead of rowId=" + rowId;

            return reader.readInt();
        }

        @Override
        public void forEachOrdinalInRange(int startRowId, int endRowId, OrdinalConsumer consumer) throws IOException
        {
            long startOffset = max(rowToOrdinalOffset, rowToOrdinalOffset + (long) startRowId * 8);
            if (startOffset >= segmentEnd - 8)
                return;  // start rowid is larger than any rowId that has an associated vector ordinal

            reader.seek(startOffset);

            while (reader.getFilePointer() < segmentEnd - 8)
            {
                int rowId = reader.readInt();
                int ordinal = reader.readInt();

                if (rowId > endRowId)
                    break;

                if (ordinal != -1)
                    consumer.accept(rowId, ordinal);
            }
        }

        @Override
        public void close()
        {
            reader.close();
        }
    }

    public void close()
    {
        fh.close();
    }

    private class OneToManyRowIdsView implements RowIdsView
    {
        @Override
        public PrimitiveIterator.OfInt getSegmentRowIdsMatching(int ordinal)
        {
            Preconditions.checkArgument(ordinal <= maxOrdinal, "vectorOrdinal %s is out of bounds %s", ordinal, maxOrdinal);

            int[] rowIds = extraRowsByOrdinal.get(ordinal);
            // no entry means there is just one rowid matching the ordinal
            if (rowIds == null)
                return new SingletonIntIterator(ordinal);
            // zero-length entry means it's a hole
            if (rowIds.length == 0)
                return IntStream.empty().iterator();
            // otherwise return the rowIds
            return Arrays.stream(rowIds).iterator();
        }

        @Override
        public void close()
        {
            // no-op
        }
    }

    private class OneToManyOrdinalsView implements OrdinalsView {
        @Override
        public int getOrdinalForRowId(int rowId)
        {
            assert rowId >= 0 : rowId;
            if (rowId > maxRowId) {
                return -1;
            }

            int index = Arrays.binarySearch(extraRowIds, rowId);
            if (index >= 0) {
                // Found in extra rows
                return extraOrdinals[index];
            }

            // If it's not an "extra" row then the ordinal is the same as the rowId
            return rowId;
        }

        @Override
        public void forEachOrdinalInRange(int startRowId, int endRowId, OrdinalConsumer consumer) throws IOException
        {
            int rawIndex = Arrays.binarySearch(extraRowIds, startRowId);
            int extraIndex = rawIndex >= 0 ? rawIndex : ~rawIndex;
            for (int rowId = max(0, startRowId); rowId <= min(endRowId, maxRowId); rowId++)
            {
                if (extraIndex < extraRowIds.length && extraRowIds[extraIndex] == rowId)
                {
                    consumer.accept(extraRowIds[extraIndex], extraOrdinals[extraIndex]);
                    extraIndex++;
                }
                else
                {
                    consumer.accept(rowId, rowId);
                }
            }
        }

        @Override
        public void close() {
            // no-op
        }
    }

    @Override
    public long cachedBytesUsed()
    {
        if (structure != V5VectorPostingsWriter.Structure.ONE_TO_MANY) {
            return 0;
        }

        long bytes = 0;
        if (extraRowIds != null) {
            bytes += extraRowIds.length * 4L;
        }
        if (extraOrdinals != null) {
            bytes += extraOrdinals.length * 4L;
        }
        if (extraRowsByOrdinal != null) {
            for (int[] rowIds : extraRowsByOrdinal.values()) {
                bytes += rowIds.length * 4L;
            }
        }
        return bytes;
    }
}
