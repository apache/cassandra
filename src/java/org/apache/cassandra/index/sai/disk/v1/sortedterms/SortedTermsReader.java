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

package org.apache.cassandra.index.sai.disk.v1.sortedterms;

import java.io.IOException;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.index.sai.disk.io.IndexInputReader;
import org.apache.cassandra.index.sai.disk.v1.LongArray;
import org.apache.cassandra.index.sai.disk.v1.SAICodecUtils;
import org.apache.cassandra.index.sai.disk.v1.bitpack.MonotonicBlockPackedReader;
import org.apache.cassandra.index.sai.disk.v1.bitpack.NumericValuesMeta;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.lucene.util.BytesRef;

import static org.apache.cassandra.index.sai.disk.v1.sortedterms.SortedTermsWriter.TERMS_DICT_BLOCK_MASK;
import static org.apache.cassandra.index.sai.disk.v1.sortedterms.SortedTermsWriter.TERMS_DICT_BLOCK_SHIFT;

/**
 * Provides read access to a sorted on-disk sequence of terms written by {@link SortedTermsWriter}.
 * <p>
 * Allows constant-time look up of the term at a given point id.
 * <p>
 * Care has been taken to make this structure as efficient as possible.
 * Reading terms does not require allocating data heap buffers per each read operation.
 * Only one term at a time is loaded to memory.
 * Low complexity algorithms are used – a lookup of the term by point id is constant time,
 * and a lookup of the point id by the term is logarithmic.
 * <p>
 * Because the blocks are prefix compressed, random access applies only to the locating the whole block.
 * In order to jump to a concrete term inside the block, the block terms are iterated from the block beginning.
 * Expect random access by {@link Cursor#seekToPointId(long)} to be slower
 * than just moving to the next term with {@link Cursor#advance()}.
 * <p>
 * For documentation of the underlying on-disk data structures, see the package documentation.
 *
 * @see SortedTermsWriter
 * @see org.apache.cassandra.index.sai.disk.v1.sortedterms
 */
@NotThreadSafe
public class SortedTermsReader
{
    private static final long BEFORE_START = -1;

    private final FileHandle termsData;
    private final SortedTermsMeta meta;
    private final LongArray.Factory blockOffsetsFactory;

    /**
     * Creates a new reader based on its data components.
     * <p>
     * It does not own the components, so you must close them separately after you're done with the reader.
     * @param termsDataFileHandle handle to the file with a sequence of prefix-compressed blocks
     *                  each storing a fixed number of terms
     * @param termsDataBlockOffsets handle to the file containing an encoded sequence of the file offsets pointing to the blocks
     * @param meta metadata object created earlier by the writer
     * @param blockOffsetsMeta metadata object for the block offsets
     */
    public SortedTermsReader(@Nonnull FileHandle termsDataFileHandle,
                             @Nonnull FileHandle termsDataBlockOffsets,
                             @Nonnull SortedTermsMeta meta,
                             @Nonnull NumericValuesMeta blockOffsetsMeta) throws IOException
    {
        this.termsData = termsDataFileHandle;
        this.meta = meta;
        this.blockOffsetsFactory = new MonotonicBlockPackedReader(termsDataBlockOffsets, blockOffsetsMeta);
    }

    /**
     * Opens a cursor over the terms stored in the terms file.
     * <p>
     * This does not read any data yet.
     * The cursor is initially positioned before the first item.
     * <p>
     * The cursor is to be used in a single thread.
     * The cursor is valid as long this object hasn't been closed.
     * You must close the cursor when you no longer need it.
     */
    public @Nonnull Cursor openCursor() throws IOException
    {
        return new Cursor(termsData, blockOffsetsFactory);
    }

    /**
     * Allows reading the terms from the terms file.
     * Can quickly seek to a random term by point id.
     * <p>
     * This object is stateful and not thread safe.
     * It maintains a position to the current term as well as a buffer that can hold one term.
     */
    @NotThreadSafe
    public class Cursor implements AutoCloseable
    {
        private final IndexInputReader termsInput;
        private final long termsDataFp;
        private final LongArray blockOffsets;

        // The term the cursor currently points to. Initially empty.
        private final BytesRef currentTerm;

        // The point id the cursor currently points to. BEFORE_START means before the first item.
        private long pointId = BEFORE_START;

        Cursor(FileHandle termsFile, LongArray.Factory blockOffsetsFactory) throws IOException
        {
            this.termsInput = IndexInputReader.create(termsFile);
            SAICodecUtils.validate(this.termsInput);
            this.termsDataFp = this.termsInput.getFilePointer();
            this.blockOffsets = new LongArray.DeferredLongArray(blockOffsetsFactory::open);
            this.currentTerm = new BytesRef(meta.maxTermLength);
        }

        /**
         * Returns the current position of the cursor.
         * Initially, before the first call to {@link #advance}, the cursor is positioned at -1.
         * After reading all the items, the cursor is positioned at index one
         * greater than the position of the last item.
         */
        public long pointId()
        {
            return pointId;
        }

        /**
         * Returns the current term data as {@link ByteComparable} referencing the internal term buffer.
         * The term data stored behind that reference is valid only until the next call to
         * {@link #advance} or {@link #seekToPointId(long)}.
         */
        public @Nonnull ByteComparable term()
        {
            return ByteComparable.fixedLength(currentTerm.bytes, currentTerm.offset, currentTerm.length);
        }

        /**
         * Positions the cursor on the target point id and reads the term at target to the current term buffer.
         * <p>
         * It is allowed to position the cursor before the first item or after the last item;
         * in these cases the internal buffer is cleared.
         * <p>
         * This method has constant complexity.
         *
         * @param pointId point id to lookup
         * @throws IOException if a seek and read from the terms file fails
         * @throws IndexOutOfBoundsException if the target point id is less than -1 or greater than the number of terms
         */
        public void seekToPointId(long pointId) throws IOException
        {
            if (pointId < 0 || pointId > meta.termCount)
                throw new IndexOutOfBoundsException(String.format("The target point id [%s] cannot be less than 0 or " +
                                                                  "greater than the term count [%s]", pointId, meta.termCount));
            long blockIndex = pointId >>> TERMS_DICT_BLOCK_SHIFT;
            long blockAddress = blockOffsets.get(blockIndex);
            termsInput.seek(blockAddress + termsDataFp);
            this.pointId = (blockIndex << TERMS_DICT_BLOCK_SHIFT) - 1;
            while (this.pointId < pointId && advance());
        }

        @Override
        public void close()
        {
            termsInput.close();
        }

        /**
         * Advances the cursor to the next term and reads it into the current term buffer.
         * <p>
         * If there are no more available terms, clears the term buffer and the cursor's position will point to the
         * one behind the last item.
         * <p>
         * This method has constant time complexity.
         *
         * @return true if the cursor was advanced successfully, false if the end of file was reached
         * @throws IOException if a read from the terms file fails
         */
        @VisibleForTesting
        protected boolean advance() throws IOException
        {
            if (pointId >= meta.termCount || ++pointId >= meta.termCount)
            {
                currentTerm.length = 0;
                return false;
            }

            int prefixLength;
            int suffixLength;
            if ((pointId & TERMS_DICT_BLOCK_MASK) == 0L)
            {
                prefixLength = 0;
                suffixLength = termsInput.readVInt();
            }
            else
            {
                // Read the prefix and suffix lengths following the compression mechanism described
                // in the SortedTermsWriter. If the lengths contained in the starting byte are less
                // than the 4 bit maximum then nothing further is read. Otherwise, the lengths in the
                // following vints are added.
                int compressedLengths = Byte.toUnsignedInt(termsInput.readByte());
                prefixLength = compressedLengths & 0x0F;
                suffixLength = 1 + (compressedLengths >>> 4);
                if (prefixLength == 15)
                    prefixLength += termsInput.readVInt();
                if (suffixLength == 16)
                    suffixLength += termsInput.readVInt();
            }

            assert prefixLength + suffixLength <= meta.maxTermLength;
            currentTerm.length = prefixLength + suffixLength;
            // The currentTerm is appended to as the suffix for the current term is
            // added to the existing prefix.
            termsInput.readBytes(currentTerm.bytes, prefixLength, suffixLength);
            return true;
        }
    }
}
