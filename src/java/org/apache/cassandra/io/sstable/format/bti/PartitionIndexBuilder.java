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
import java.util.function.Consumer;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.io.tries.IncrementalTrieWriter;
import org.apache.cassandra.io.tries.Walker;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

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

    public PartitionIndexBuilder(SequentialWriter writer, FileHandle.Builder fhBuilder)
    {
        this.writer = writer;
        this.trieWriter = IncrementalTrieWriter.open(PartitionIndex.TRIE_SERIALIZER, writer);
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
            fhBuilder.withLengthOverride(-1);
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
        if (lastKey == null)
        {
            firstKey = decoratedKey;
            lastDiffPoint = 0;
        }
        else
        {
            int diffPoint = ByteComparable.diffPoint(lastKey, decoratedKey, Walker.BYTE_COMPARABLE_VERSION);
            ByteComparable prevPrefix = ByteComparable.cut(lastKey, Math.max(diffPoint, lastDiffPoint));
            trieWriter.add(prevPrefix, lastPayload);
            lastWrittenKey = lastKey;
            lastDiffPoint = diffPoint;
        }
        lastKey = decoratedKey;
        lastPayload = new PartitionIndex.Payload(position, decoratedKey.filterHashLowerBits());
    }

    public long complete() throws IOException
    {
        // Do not trigger pending partial builds.
        partialIndexConsumer = null;

        if (lastKey != lastWrittenKey)
        {
            ByteComparable prevPrefix = ByteComparable.cut(lastKey, lastDiffPoint);
            trieWriter.add(prevPrefix, lastPayload);
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
