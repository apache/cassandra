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

package org.apache.cassandra.index.sai.disk.v9.keystore;

import java.io.IOException;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.cassandra.index.sai.disk.io.IndexInputReader;
import org.apache.cassandra.index.sai.disk.v1.LongArray;
import org.apache.cassandra.index.sai.disk.v1.bitpack.MonotonicBlockPackedReader;
import org.apache.cassandra.index.sai.disk.v1.bitpack.NumericValuesMeta;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.utils.FastByteOperations;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;

/**
 * Provides read access to an on-disk sequence of partition or clustering keys written by {@link KeyStoreWriter}.
 * <p>
 * It's important that the implementation is performant.
 * <p>
 * Because the blocks are prefix compressed, random access applies only to the locating the whole block.
 * In order to jump to a concrete key inside the block, the block keys are iterated from the block beginning.
 *
 * @see KeyStoreWriter
 */
@NotThreadSafe
public class KeyLookup
{
    public static final String INDEX_OUT_OF_BOUNDS = "The target point id [%d] cannot be less than 0 or greater than or equal to the key count [%d]";
    private final FileHandle keysFileHandle;
    private final KeyLookupMeta keyLookupMeta;
    private final LongArray.Factory keyBlockOffsetsFactory;

    /**
     * Creates a new reader based on its data components.
     * <p>
     * It does not own the components, so you must close them separately after you're done with the reader.
     *
     * @param keysFileHandle      handle to the file with a sequence of prefix-compressed blocks,
     *                            each storing a fixed number of keys
     * @param keysBlockOffsets    handle to the file containing an encoded sequence of the file offsets pointing to the blocks
     * @param keyLookupMeta       metadata object created earlier by the writer
     * @param keyBlockOffsetsMeta metadata object for the block offsets
     */
    public KeyLookup(@Nonnull FileHandle keysFileHandle,
                     @Nonnull FileHandle keysBlockOffsets,
                     @Nonnull KeyLookupMeta keyLookupMeta,
                     @Nonnull NumericValuesMeta keyBlockOffsetsMeta) throws IOException
    {
        this.keysFileHandle = keysFileHandle;
        this.keyLookupMeta = keyLookupMeta;
        this.keyBlockOffsetsFactory = new MonotonicBlockPackedReader(keysBlockOffsets, keyBlockOffsetsMeta);
    }

    /**
     * Opens a cursor over the keys stored in the keys file.
     * <p>
     * This will read the first key into the key buffer and point to the first point in the keys file.
     * <p>
     * The cursor is to be used in a single thread.
     * The cursor is valid as long as this object hasn't been closed.
     * You must close the cursor when you no longer need it.
     */
    public @Nonnull Cursor openCursor()
    {
        if (keyLookupMeta.keyCount == 0)
            return new EmptyCursor();
        return new IndexInputCursor(keysFileHandle, keyBlockOffsetsFactory);
    }

    /**
     * This interface is introduced a workaround, when a cursor is open for partition with no data.
     * For this an Empty Cursor is implemented
     * <p>
     * Otherwise, the main goal is to allow reading the keys from a keys file
     * and quickly seek to a random key by point id.
     * <p>
     * Its instances can be stateful and not thread-safe and
     * maintain a position to the current key as well as a buffer that can hold one key.
     */
    @NotThreadSafe
    public interface Cursor extends AutoCloseable
    {
        /**
         * Finds a pointId for the clustering key in a partition defined by the range of point ids.
         * The start and end of the range must not exceed the number of keys available.
         * The keys within the range are expected to be in lexicographical order.
         * <p>
         * Should not be used without clustering.
         *
         * @param key             the key to seek for within the partition
         * @param startingPointId the inclusive starting point for the partition
         * @param endingPointId   the exclusive ending point for the partition
         *                        Note: this can be equal to the number of keys if this is the last partition
         * @return a {@code long} representing the pointId of the closest key from the partition
         * that is >= to the key passed to the method, or -1 if the key passed is > all the keys.
         */
        long clusteredSeekToKey(ByteComparable key, long startingPointId, long endingPointId);

        /**
         * Positions the cursor on the target point id and reads the key at the target to the current key buffer.
         * <p>
         * It is allowed to position the cursor before the first item or after the last item;
         * in these cases the internal buffer is cleared.
         *
         * @param target point id to lookup
         * @return The {@link ByteComparable} containing the key
         * @throws IndexOutOfBoundsException if the target point id is less than -1 or greater than the number of keys
         */
        @Nonnull
        ByteComparable seekToPointId(long target);

        void close() throws IOException;
    }

    /**
     * An empty cursor implementation that is returned when keyCount is 0.
     * This cursor has no keys, and all operations either throw exceptions or return sentinel values.
     */
    private static class EmptyCursor implements Cursor
    {
        @Override
        public long clusteredSeekToKey(ByteComparable key, long startingPointId, long endingPointId)
        {
            return -1;
        }

        @Override
        public @Nonnull ByteComparable seekToPointId(long target)
        {
            throw new IndexOutOfBoundsException(String.format(INDEX_OUT_OF_BOUNDS, target, 0));
        }


        @Override
        public void close()
        {
            // No-op for empty cursor
        }
    }

    /**
     * Allows reading the keys from the keys file.
     * Can quickly seek to a random key by point id.
     * <p>
     * This object is stateful and not thread-safe.
     * It maintains a position to the current key as well as a buffer that can hold one key.
     */
    @NotThreadSafe
    public class IndexInputCursor implements Cursor
    {
        private final IndexInputReader keysInput;
        private final int blockShift;
        private final int blockMask;
        private final boolean clustering;
        private final long keysFilePointer;
        private final LongArray blockOffsets;

        // The key the cursor currently points to. Initially empty.
        private final BytesRef currentKey;

        // A temporary buffer used to hold the key at the start of the next block.
        private final BytesRef nextBlockKey;

        // The point id the cursor currently points to.
        private long currentPointId;
        private long currentBlockIndex;

        IndexInputCursor(FileHandle keysFileHandle, LongArray.Factory blockOffsetsFactory)
        {
            try
            {
                this.keysInput = IndexInputReader.create(keysFileHandle);
                SAICodecUtils.validate(this.keysInput);

                this.blockShift = this.keysInput.readVInt();
                this.blockMask = (1 << this.blockShift) - 1;
                this.clustering = this.keysInput.readByte() == 1;
            }
            catch (IOException e)
            {
                throw Throwables.unchecked(Throwables.close(e, keysFileHandle));
            }

            this.keysFilePointer = this.keysInput.getFilePointer();
            this.blockOffsets = new LongArray.DeferredLongArray(blockOffsetsFactory::open);
            this.currentKey = new BytesRef(keyLookupMeta.maxKeyLength);
            this.nextBlockKey = new BytesRef(keyLookupMeta.maxKeyLength);

            keysInput.seek(keysFilePointer);
            readKey(currentPointId, currentKey);
        }

        /**
         * Finds a pointId for the clustering key within a partition.
         * <p>
         * It assumes the keys within the partition are sorted, as it uses binary search.
         * <p>
         * If the key is not in the block containing the start of the range, a binary search is done to find
         * the block containing the search key. That block is then searched to return the pointId that corresponds
         * to the key that is either equal to or next highest to the search key.
         */
        @Override
        public long clusteredSeekToKey(ByteComparable key, long startingPointId, long endingPointId)
        {
            assert clustering : "Requires clustering so the clustering keys are sorted";

            BytesRef searchKey = asBytesRef(key);

            positionAtPointId(startingPointId);

            // We can return immediately if the currentPointId is within the requested partition range and the keys match
            if (currentPointId >= startingPointId && currentPointId < endingPointId && compareKeys(currentKey, searchKey) == 0)
                return currentPointId;

            binarySearchKeyInRange(startingPointId, endingPointId, searchKey);

            // Depending on where we are in the block, we may need to move forwards to the starting point ID
            while (currentPointId < startingPointId)
                advanceToNextKey();

            // Move forward to the ending point ID, returning the point ID if we find our key
            while (currentPointId < endingPointId)
            {
                if (compareKeys(currentKey, searchKey) >= 0)
                    return currentPointId;

                if (!advanceToNextKey())
                    return -1;
            }
            return endingPointId < keyLookupMeta.keyCount ? endingPointId : -1;
        }

        private void binarySearchKeyInRange(long startingPointId, long endingPointId, BytesRef searchKey)
        {
            long lowSearchId = startingPointId;
            long highSearchId = endingPointId;

            // We will keep going with the binary shift while the search consists of at least one block
            while ((highSearchId - lowSearchId) >>> blockShift > 0)
            {
                long midSearchId = lowSearchId + (highSearchId - lowSearchId) / 2;

                // See if searchKey exists in the block containing the midSearchId or is above or below it
                int position = moveToBlockAndCompareTo(midSearchId, searchKey);

                if (position == 0)
                {
                    lowSearchId = currentPointId;
                    break;
                }

                if (position < 0)
                    highSearchId = midSearchId;
                else
                    lowSearchId = midSearchId;
            }

            positionAtPointId(lowSearchId);
        }

        /**
         * Positions the cursor at the specified point ID by moving to the appropriate block
         * and reading the key at that position.
         */
        private void positionAtPointId(long pointId)
        {
            updateCurrentBlockIndex(pointId);
            resetToCurrentBlock();
        }

        /**
         * Advances to the next key in the sequence.
         *
         * @return true if successfully advanced, false if reached the end of keys
         */
        private boolean advanceToNextKey()
        {
            currentPointId++;
            if (currentPointId == keyLookupMeta.keyCount)
                return false;

            readCurrentKey();
            updateCurrentBlockIndex(currentPointId);
            return true;
        }

        /**
         * Moves to a block and determines if the key is in the block.
         *
         * @return -1 if the key is before the block, 0 if the key is in the block, 1 if the key is after the block
         */
        private int moveToBlockAndCompareTo(long pointId, BytesRef key)
        {
            positionAtPointId(pointId);

            if (compareKeys(key, currentKey) < 0)
                return -1;

            // If we are in the last block, we will assume for now that the key is in the last block and defer
            // the final decision to later (if we can't find it).
            if (currentBlockIndex == blockOffsets.length() - 1)
                return 0;

            // Finish by getting the starting key of the next block and comparing that with the key.
            keysInput.seek(blockOffsets.get(currentBlockIndex + 1) + keysFilePointer);
            readKey((currentBlockIndex + 1) << blockShift, nextBlockKey);
            return compareKeys(key, nextBlockKey) < 0 ? 0 : 1;
        }

        private void updateCurrentBlockIndex(long pointId)
        {
            currentBlockIndex = pointId >>> blockShift;
        }

        // Reset currentPointId and currentKey to be at the start of the block pointed to by currentBlockIndex.
        private void resetToCurrentBlock()
        {
            keysInput.seek(blockOffsets.get(currentBlockIndex) + keysFilePointer);
            currentPointId = currentBlockIndex << blockShift;
            readCurrentKey();
        }

        private void readCurrentKey()
        {
            readKey(currentPointId, currentKey);
        }

        // Read the next key indicated by pointId.
        //
        // Note: pointId is only used to determine whether we are at the start of a block. It is
        // important that resetPosition is called prior to multiple calls to readKey. It is
        // easy to get out of position.
        private void readKey(long pointId, BytesRef key)
        {
            try
            {
                int prefixLength;
                int suffixLength;
                if ((pointId & blockMask) == 0L)
                {
                    prefixLength = 0;
                    suffixLength = keysInput.readVInt();
                }
                else
                {
                    // Read the prefix and suffix lengths following the compression mechanism described
                    // in the KeyStoreWriterWriter. If the lengths contained in the starting byte are less
                    // than the 4-bit maximum, then nothing further is read. Otherwise, the lengths in the
                    // following vints are added.
                    int compressedLengths = Byte.toUnsignedInt(keysInput.readByte());
                    prefixLength = compressedLengths & 0x0F;
                    suffixLength = compressedLengths >>> 4;
                    if (prefixLength == 15)
                        prefixLength += keysInput.readVInt();
                    if (suffixLength == 15)
                        suffixLength += keysInput.readVInt();
                }

                assert prefixLength + suffixLength <= keyLookupMeta.maxKeyLength;
                if (prefixLength + suffixLength > 0)
                {
                    key.length = prefixLength + suffixLength;
                    // The currentKey is appended to as the suffix for the current key is
                    // added to the existing prefix.
                    keysInput.readBytes(key.bytes, prefixLength, suffixLength);
                }
            }
            catch (IOException e)
            {
                throw Throwables.cleaned(e);
            }
        }

        private int compareKeys(BytesRef left, BytesRef right)
        {
            return FastByteOperations.compareUnsigned(left.bytes, left.offset, left.offset + left.length,
                                                      right.bytes, right.offset, right.offset + right.length);
        }

        private BytesRef asBytesRef(ByteComparable source)
        {
            BytesRefBuilder builder = new BytesRefBuilder();

            ByteSource byteSource = source.asComparableBytes(TypeUtil.BYTE_COMPARABLE_VERSION);
            int val;
            while ((val = byteSource.next()) != ByteSource.END_OF_STREAM)
                builder.append((byte) val);
            return builder.get();
        }

        /**
         * Positions the cursor on the target point id and reads the key at the target to the current key buffer.
         * <p>
         * It is allowed to position the cursor before the first item or after the last item;
         * in these cases the internal buffer is cleared.
         *
         * @param target point id to lookup
         * @return The {@link ByteComparable} containing the key
         * @throws IndexOutOfBoundsException if the target point id is less than -1 or greater than the number of keys
         */
        @Override
        public @Nonnull ByteComparable seekToPointId(long target)
        {
            if (target <= -1 || target >= keyLookupMeta.keyCount)
                throw new IndexOutOfBoundsException(String.format(INDEX_OUT_OF_BOUNDS, target, keyLookupMeta.keyCount));

            if (target != currentPointId)
            {
                long blockIndex = target >>> blockShift;
                // We need to reset the block if the block index has changed or the target < currentPointId.
                // We can read forward in the same block without a reset, but we can't read backwards, and token
                // collision can result in us moving backwards.
                if (blockIndex != currentBlockIndex || target < currentPointId)
                {
                    currentBlockIndex = blockIndex;
                    resetToCurrentBlock();
                }

                // Advance forward to the target position
                while (currentPointId < target)
                    advanceToNextKey();
            }

            return ByteComparable.preencoded(TypeUtil.BYTE_COMPARABLE_VERSION, currentKey.bytes, currentKey.offset, currentKey.length);
        }

        @Override
        public void close() throws IOException
        {
            blockOffsets.close();
            keysInput.close();
        }
    }
}
