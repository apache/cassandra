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
package org.apache.cassandra.io.sstable.format.bti;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.io.tries.IncrementalTrieWriter;
import org.apache.cassandra.io.tries.Walker;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

/**
 * Partition index builder: stores index or data positions in an incrementally built, page aware on-disk trie.
 * <p>
 * The files created by this builder are read by {@link PartitionIndex}.
 */
class PartitionIndexBuilder implements AutoCloseable
{
    private final SequentialWriter writer;
    private final IncrementalTrieWriter<PartitionIndex.Payload> trieWriter;
    private final FileHandle.Builder fhBuilder;

    // the last synced data file position
    private long dataSyncPosition;
    // the last synced row index file position
    private long rowIndexSyncPosition;
    // the last synced partition index file position
    private long partitionIndexSyncPosition;

    // Partial index can only be used after all three files have been synced to the required positions.
    private long partialIndexDataEnd;
    private long partialIndexRowEnd;
    private long partialIndexPartitionEnd;
    private IncrementalTrieWriter.PartialTail partialIndexTail;
    private Consumer<PartitionIndex> partialIndexConsumer;
    private DecoratedKey partialIndexLastKey;

    private int lastDiffPoint;
    private DecoratedKey firstKey;
    private DecoratedKey lastKey;
    private DecoratedKey lastWrittenKey;
    private PartitionIndex.Payload lastPayload;

    // Encode each key to its comparable bytes once and keep them in a scratch buffer, instead of encoding the same
    // key again for the trie and for diffPoint.  The trie keeps the previous key and reads it again when the next key
    // is added, and diffPoint needs the current key's bytes while the previous key is still in use, so three keys are
    // in use at the same time.  A ring of SCRATCH_SLOTS buffers, advanced one slot per key, keeps all three unchanged
    // while they are needed; two buffers would overwrite the previous key too soon.  SCRATCH_SLOTS is 3 because three
    // keys are in use at once.
    private static final int SCRATCH_SLOTS = 3;
    private static final int INITIAL_SCRATCH = 64;
    private final byte[][] keyScratch = new byte[SCRATCH_SLOTS][];
    private int slot = 0;           // ring slot for the next key
    private byte[] lastKeyBytes;    // scratch buffer holding lastKey's bytes
    private int lastKeyLen;
    private byte[] triePrev;        // scratch buffer last given to the trie; the trie reads it again when the next key is added

    public PartitionIndexBuilder(SequentialWriter writer, FileHandle.Builder fhBuilder)
    {
        this(writer, fhBuilder, IncrementalTrieWriter.open(PartitionIndex.TRIE_SERIALIZER, writer));
    }

    @VisibleForTesting
    PartitionIndexBuilder(SequentialWriter writer, FileHandle.Builder fhBuilder, IncrementalTrieWriter<PartitionIndex.Payload> trieWriter)
    {
        this.writer = writer;
        this.trieWriter = trieWriter;
        this.fhBuilder = fhBuilder;
    }

    /*
     * Called when partition index has been flushed to the given position.
     * If this makes all required positions for a partial view flushed, this will call the partialIndexConsumer.
     */
    public void markPartitionIndexSynced(long upToPosition)
    {
        partitionIndexSyncPosition = upToPosition;
        refreshReadableBoundary();
    }

    /*
     * Called when row index has been flushed to the given position.
     * If this makes all required positions for a partial view flushed, this will call the partialIndexConsumer.
     */
    public void markRowIndexSynced(long upToPosition)
    {
        rowIndexSyncPosition = upToPosition;
        refreshReadableBoundary();
    }

    /*
     * Called when data file has been flushed to the given position.
     * If this makes all required positions for a partial view flushed, this will call the partialIndexConsumer.
     */
    public void markDataSynced(long upToPosition)
    {
        dataSyncPosition = upToPosition;
        refreshReadableBoundary();
    }

    private void refreshReadableBoundary()
    {
        if (partialIndexConsumer == null)
            return;
        if (dataSyncPosition < partialIndexDataEnd)
            return;
        if (rowIndexSyncPosition < partialIndexRowEnd)
            return;
        if (partitionIndexSyncPosition < partialIndexPartitionEnd)
            return;

        try (FileHandle fh = fhBuilder.withLengthOverride(writer.getLastFlushOffset()).complete())
        {
            PartitionIndex pi = new PartitionIndexEarly(fh, partialIndexTail.root(), partialIndexTail.count(), firstKey, partialIndexLastKey, partialIndexTail.cutoff(), partialIndexTail.tail());
            partialIndexConsumer.accept(pi);
            partialIndexConsumer = null;
        }
        finally
        {
            fhBuilder.withLengthOverride(FileHandle.Builder.NO_LENGTH_OVERRIDE);
        }

    }

    /**
    * @param decoratedKey the key for this record
    * @param position the position to write with the record:
    *    - positive if position points to an index entry in the index file
    *    - negative if ~position points directly to the key in the data file
    */
    public void addEntry(DecoratedKey decoratedKey, long position) throws IOException
    {
        // The slot we are about to overwrite must not still hold the last key, or the previous key that the trie reads again.
        assert keyScratch[slot] == null || (keyScratch[slot] != lastKeyBytes && keyScratch[slot] != triePrev)
            : "scratch ring slot " + slot + " is still live";
        int curLen = materialize(slot, decoratedKey);
        byte[] curBytes = keyScratch[slot];

        if (lastKey == null)
        {
            firstKey = decoratedKey;
            lastDiffPoint = 0;
        }
        else
        {
            int diffPoint = diffPoint(lastKeyBytes, lastKeyLen, curBytes, curLen);
            // Limit to lastKeyLen: the old ByteComparable.cut stopped at the end of the key's bytes, but fixedLength
            // does not, so limit it here to build the exact same prefix.
            int m = Math.min(Math.max(diffPoint, lastDiffPoint), lastKeyLen);
            trieWriter.add(ByteComparable.fixedLength(lastKeyBytes, 0, m), lastPayload);
            triePrev = lastKeyBytes;
            lastWrittenKey = lastKey;
            lastDiffPoint = diffPoint;
        }
        lastKey = decoratedKey;
        lastPayload = new PartitionIndex.Payload(position, decoratedKey.filterHashLowerBits());
        lastKeyBytes = curBytes;
        lastKeyLen = curLen;
        slot = slot + 1 == SCRATCH_SLOTS ? 0 : slot + 1;
    }

    /**
     * Encodes the key's comparable bytes into the given scratch slot, growing it as needed, and returns the encoded
     * length.  The caller reads the backing array from {@code keyScratch[slot]}.  That array must stay unchanged until
     * the trie is done reading it as the previous key, which the ring guarantees.
     */
    private int materialize(int slot, DecoratedKey key)
    {
        byte[] buf = keyScratch[slot];
        if (buf == null)
            buf = keyScratch[slot] = new byte[INITIAL_SCRATCH];
        ByteSource src = key.asComparableBytes(Walker.BYTE_COMPARABLE_VERSION);
        int len = 0;
        for (int b = src.next(); b != ByteSource.END_OF_STREAM; b = src.next())
        {
            if (len == buf.length)
                buf = keyScratch[slot] = Arrays.copyOf(buf, buf.length * 2);
            buf[len++] = (byte) b;
        }
        return len;
    }

    /**
     * Returns the number of equal leading bytes plus one, matching {@link ByteComparable#diffPoint}.  {@link
     * Arrays#mismatch} returns the first differing index over the two ranges, or -1 when the ranges are equal; when one
     * range is a prefix of the other it returns the shorter length.  Both no-mismatch cases mean the shared prefix runs
     * to the shorter length, so we map them to {@code lim} and add one to match the diffPoint convention.  Comparing
     * the bytes directly is enough: equal bytes stay equal whether read as signed or unsigned, so there is no need to
     * mask them.
     */
    private static int diffPoint(byte[] a, int la, byte[] b, int lb)
    {
        int lim = Math.min(la, lb);
        int m = Arrays.mismatch(a, 0, la, b, 0, lb);
        return (m < 0 ? lim : m) + 1;
    }

    public long complete() throws IOException
    {
        // Do not trigger pending partial builds.
        partialIndexConsumer = null;

        if (lastKey != lastWrittenKey)
        {
            // Limit to lastKeyLen for the same reason as in addEntry: fixedLength does not stop at the end of the
            // key's bytes on its own.
            int m = Math.min(lastDiffPoint, lastKeyLen);
            trieWriter.add(ByteComparable.fixedLength(lastKeyBytes, 0, m), lastPayload);
        }

        long root = trieWriter.complete();
        long count = trieWriter.count();
        long firstKeyPos = writer.position();
        if (firstKey != null)
        {
            ByteBufferUtil.writeWithShortLength(firstKey.getKey(), writer);
            ByteBufferUtil.writeWithShortLength(lastKey.getKey(), writer);
        }
        else
        {
            assert lastKey == null;
            writer.writeShort(0);
            writer.writeShort(0);
        }

        writer.writeLong(firstKeyPos);
        writer.writeLong(count);
        writer.writeLong(root);

        writer.sync();
        fhBuilder.withLengthOverride(writer.getLastFlushOffset());

        return root;
    }

    /**
     * Builds a PartitionIndex representing the records written until this point without interrupting writes. Because
     * data in buffered writers does not get immediately flushed to the file system, and we do not want to force flushing
     * of the relevant files (which e.g. could cause a problem for compressed data files), this call cannot return
     * immediately. Instead, it will take an index snapshot but wait with making it active (by calling the provided
     * callback) until it registers that all relevant files (data, row index and partition index) have been flushed at
     * least as far as the required positions.
     *
     * @param callWhenReady callback that is given the prepared partial index when all relevant data has been flushed
     * @param rowIndexEnd the position in the row index file we need to be able to read to (exclusive) to read all
     *                    records written so far
     * @param dataEnd the position in the data file we need to be able to read to (exclusive) to read all records
     *                    written so far
     * @return true if the request was accepted, false if there's no point to do this at this time (e.g. another
     *         partial representation is prepared but still isn't usable).
     */
    public boolean buildPartial(Consumer<PartitionIndex> callWhenReady, long rowIndexEnd, long dataEnd)
    {
        // If we haven't advanced since the last time we prepared, there's nothing to do.
        if (lastWrittenKey == partialIndexLastKey)
            return false;

        // Don't waste time if an index was already prepared but hasn't reached usability yet.
        if (partialIndexConsumer != null)
            return false;

        try
        {
            partialIndexTail = trieWriter.makePartialRoot();
            partialIndexDataEnd = dataEnd;
            partialIndexRowEnd = rowIndexEnd;
            partialIndexPartitionEnd = writer.position();
            partialIndexLastKey = lastWrittenKey;
            partialIndexConsumer = callWhenReady;
            return true;
        }
        catch (IOException e)
        {
            // As writes happen on in-memory buffers, failure here is not expected.
            throw new AssertionError(e);
        }
    }

    // close the builder and release any associated memory
    public void close()
    {
        trieWriter.close();
    }
}
