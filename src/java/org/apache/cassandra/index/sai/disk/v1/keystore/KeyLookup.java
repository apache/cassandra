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

package org.apache.cassandra.index.sai.disk.v1.keystore;

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
import org.apache.cassandra.utils.FastByteOperations;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;

/**
 * Provides read access to an on-disk sequence of terms written by {@link KeyStoreWriter}.
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
 * <p>
 * For documentation of the underlying on-disk data structures, see the package documentation.
 *
 * @see KeyStoreWriter
 * @see org.apache.cassandra.index.sai.disk.v1.keystore
 */
@NotThreadSafe
public class KeyLookup
{
    public static final String INDEX_OUT_OF_BOUNDS = "The target point id [%d] cannot be less than 0 or greater than or equal to the term count [%d]";

    private final FileHandle termsData;
    private final KeyLookupMeta meta;
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
    public KeyLookup(@Nonnull FileHandle termsDataFileHandle,
                     @Nonnull FileHandle termsDataBlockOffsets,
                     @Nonnull KeyLookupMeta meta,
                     @Nonnull NumericValuesMeta blockOffsetsMeta) throws IOException
    {
        this.termsData = termsDataFileHandle;
        this.meta = meta;
        this.blockOffsetsFactory = new MonotonicBlockPackedReader(termsDataBlockOffsets, blockOffsetsMeta);
    }

    /**
     * Opens a cursor over the terms stored in the terms file.
     * <p>
     * This will read the first term into the term buffer and point to the first point in the terms file.
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
        private final int blockShift;
        private final int blockMask;
        private final boolean partitioned;
        private final long termsDataFp;
        private final LongArray blockOffsets;

        // The term the cursor currently points to. Initially empty.
        private final BytesRef currentTerm;

        private final BytesRef nextBlockTerm;

        // The point id the cursor currently points to.
        private long currentPointId;
        private long currentBlockIndex;

        Cursor(FileHandle termsFile, LongArray.Factory blockOffsetsFactory) throws IOException
        {
            this.termsInput = IndexInputReader.create(termsFile);
            SAICodecUtils.validate(this.termsInput);
            this.blockShift = this.termsInput.readVInt();
            this.blockMask = (1 << this.blockShift) - 1;
            this.partitioned = this.termsInput.readByte() == 1;
            this.termsDataFp = this.termsInput.getFilePointer();
            this.blockOffsets = new LongArray.DeferredLongArray(blockOffsetsFactory::open);
            this.currentTerm = new BytesRef(meta.maxTermLength);
            this.nextBlockTerm = new BytesRef(meta.maxTermLength);
            termsInput.seek(termsDataFp);
            readTerm(currentPointId, currentTerm);
        }

        /**
         * Positions the cursor on the target point id and reads the term at target to the current term buffer.
         * <p>
         * It is allowed to position the cursor before the first item or after the last item;
         * in these cases the internal buffer is cleared.
         *
         * @param pointId point id to lookup
         * @return The {@link ByteComparable} containing the term
         * @throws IndexOutOfBoundsException if the target point id is less than -1 or greater than the number of terms
         */
        public @Nonnull ByteComparable seekForwardToPointId(long pointId)
        {
            if (pointId < 0 || pointId >= meta.termCount)
                throw new IndexOutOfBoundsException(String.format(INDEX_OUT_OF_BOUNDS, pointId, meta.termCount));

            assert pointId >= currentPointId : "Attempt to seek backwards in seekForwardsToPointId. Next pointId was "
                                               + pointId + " while current pointId is " + currentPointId;
            if (pointId != currentPointId)
            {
                long blockIndex = pointId >>> blockShift;
                if (blockIndex != currentBlockIndex)
                {
                    currentBlockIndex = blockIndex;
                    resetToCurrentBlock();
                }
            }
            while (currentPointId < pointId)
            {
                currentPointId++;
                readCurrentTerm();
                updateCurrentBlockIndex(currentPointId);
            }

            return ByteComparable.fixedLength(currentTerm.bytes, currentTerm.offset, currentTerm.length);
        }

        /**
         * Finds the pointId for a term within a range of pointIds. The start and end of the range must not
         * exceed the number of terms available. The terms within the range are expected to be in lexographical order.
         * <p>
         * If the term is not in the block containing the start of the range a binary search is done to find
         * the block containing the search. That block is then searched to return the pointId that corresponds
         * to the term that either equal to or next highest to the term.
         *
         * @param term The term to seek for with the partition
         * @param startingPointId the inclusive starting point for the partition
         * @param endingPointId the exclusive ending point for the partition.
         *                      Note: this can be equal to the number of terms if this is the last partition
         * @return a {@code long} representing the pointId of the term that is >= to the term passed to the method, or
         *         -1 if the term passed is > all the terms.
         */
        public long partitionedSeekToTerm(ByteComparable term, long startingPointId, long endingPointId)
        {
            assert partitioned : "Cannot do a partitioned seek to term on non-partitioned terms";

            BytesRef skipTerm = asBytesRef(term);

            updateCurrentBlockIndex(startingPointId);
            resetToCurrentBlock();

            if (compareTerms(currentTerm, skipTerm) == 0)
                return startingPointId;

            if (notInCurrentBlock(startingPointId, skipTerm))
            {
                long split = (endingPointId - startingPointId) >>> blockShift;
                long splitPointId = startingPointId;
                while (split > 0)
                {
                    updateCurrentBlockIndex(Math.min((splitPointId >>> blockShift) + split, blockOffsets.length() - 1));
                    resetToCurrentBlock();

                    if (currentPointId >= endingPointId)
                    {
                        updateCurrentBlockIndex((endingPointId - 1));
                        resetToCurrentBlock();
                    }

                    int cmp = compareTerms(currentTerm, skipTerm);

                    if (cmp == 0)
                        return currentPointId;

                    if (cmp < 0)
                        splitPointId = currentPointId;

                    split /= 2;
                }
                // After we finish the binary search we need to move the block back till we hit a block that has
                // a starting term that is less than or equal to the skip term
                while (currentBlockIndex > 0 && compareTerms(currentTerm, skipTerm) > 0)
                {
                    currentBlockIndex--;
                    resetToCurrentBlock();
                }
            }

            // Depending on where we are in the block we may need to move forwards to the starting point ID
            while (currentPointId < startingPointId)
            {
                currentPointId++;
                readCurrentTerm();
                updateCurrentBlockIndex(currentPointId);
            }

            // Move forward to the ending point ID, returning the point ID if we find our term
            while (currentPointId < endingPointId)
            {
                if (compareTerms(currentTerm, skipTerm) >= 0)
                    return currentPointId;
                currentPointId++;
                if (currentPointId == meta.termCount)
                    return -1;
                readCurrentTerm();
                updateCurrentBlockIndex(currentPointId);
            }
            return endingPointId < meta.termCount ? endingPointId : -1;
        }

        @VisibleForTesting
        public void reset() throws IOException
        {
            currentPointId = 0;
            currentBlockIndex = 0;
            termsInput.seek(termsDataFp);
            readCurrentTerm();
        }


        @Override
        public void close()
        {
            termsInput.close();
        }

        private void updateCurrentBlockIndex(long pointId)
        {
            currentBlockIndex = pointId >>> blockShift;
        }

        private boolean notInCurrentBlock(long pointId, BytesRef term)
        {
            if (inLastBlock(pointId))
                return false;

            // Load the starting value of the next block into nextBlockTerm.
            long blockIndex = (pointId >>> blockShift) + 1;
            long currentFp = termsInput.getFilePointer();
            termsInput.seek(blockOffsets.get(blockIndex) + termsDataFp);
            readTerm(blockIndex << blockShift, nextBlockTerm);
            termsInput.seek(currentFp);

            return compareTerms(term, nextBlockTerm) >= 0;
        }

        private boolean inLastBlock(long pointId)
        {
            return pointId >>> blockShift == blockOffsets.length() - 1;
        }

        // Reset currentPointId and currentTerm to be at the start of the block
        // pointed to by currentBlockIndex.
        private void resetToCurrentBlock()
        {
            termsInput.seek(blockOffsets.get(currentBlockIndex) + termsDataFp);
            currentPointId = currentBlockIndex << blockShift;
            readCurrentTerm();
        }

        private void readCurrentTerm()
        {
            readTerm(currentPointId, currentTerm);
        }

        // Read the next term indicated by pointId.
        //
        // Note: pointId is only used to determine whether we are at the start of a block. It is
        // important that resetPosition is called prior to multiple calls to readTerm. It is
        // easy to get out of position.
        private void readTerm(long pointId, BytesRef term)
        {
            try
            {
                int prefixLength;
                int suffixLength;
                if ((pointId & blockMask) == 0L)
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
                    suffixLength = compressedLengths >>> 4;
                    if (prefixLength == 15)
                        prefixLength += termsInput.readVInt();
                    if (suffixLength == 15)
                        suffixLength += termsInput.readVInt();
                }

                assert prefixLength + suffixLength <= meta.maxTermLength;
                if (prefixLength + suffixLength > 0)
                {
                    term.length = prefixLength + suffixLength;
                    // The currentTerm is appended to as the suffix for the current term is
                    // added to the existing prefix.
                    termsInput.readBytes(term.bytes, prefixLength, suffixLength);
                }
            }
            catch (IOException e)
            {
                throw Throwables.cleaned(e);
            }
        }

        private int compareTerms(BytesRef left, BytesRef right)
        {
            return FastByteOperations.compareUnsigned(left.bytes, left.offset, left.offset + left.length,
                                                      right.bytes, right.offset, right.offset + right.length);
        }

        private BytesRef asBytesRef(ByteComparable source)
        {
            BytesRefBuilder builder = new BytesRefBuilder();

            ByteSource byteSource = source.asComparableBytes(ByteComparable.Version.OSS50);
            int val;
            while ((val = byteSource.next()) != ByteSource.END_OF_STREAM)
                builder.append((byte) val);
            return builder.get();
        }
    }
}
