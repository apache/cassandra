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
package org.apache.cassandra.io.sstable.format.big;

import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.compaction.CompactionInterruptedException;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.io.sstable.IScrubber;
import org.apache.cassandra.io.sstable.SSTableRewriter;
import org.apache.cassandra.io.sstable.format.SortedTableScrubber;
import org.apache.cassandra.io.sstable.format.big.BigFormat.Components;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.OutputHandler;

public class BigTableScrubber extends SortedTableScrubber<BigTableReader> implements IScrubber
{
    private final boolean isIndex;

    private final RandomAccessReader indexFile;
    private final RowIndexEntry.IndexSerializer rowIndexEntrySerializer;

    private ByteBuffer currentIndexKey;
    private ByteBuffer nextIndexKey;
    private long currentPartitionPositionFromIndex;
    private long nextPartitionPositionFromIndex;

    public BigTableScrubber(ColumnFamilyStore cfs,
                            LifecycleTransaction transaction,
                            OutputHandler outputHandler,
                            Options options)
    {
        super(cfs, transaction, outputHandler, options);

        this.rowIndexEntrySerializer = new RowIndexEntry.Serializer(sstable.descriptor.version, sstable.header, cfs.getMetrics());

        boolean hasIndexFile = sstable.descriptor.fileFor(Components.PRIMARY_INDEX).exists();
        this.isIndex = cfs.isIndex();
        if (!hasIndexFile)
        {
            // if there's any corruption in the -Data.db then partitions can't be skipped over. but it's worth a shot.
            outputHandler.warn("Missing component: %s", sstable.descriptor.fileFor(Components.PRIMARY_INDEX));
        }

        this.indexFile = hasIndexFile
                         ? RandomAccessReader.open(sstable.descriptor.fileFor(Components.PRIMARY_INDEX))
                         : null;

        this.currentPartitionPositionFromIndex = 0;
        this.nextPartitionPositionFromIndex = 0;
    }

    @Override
    protected UnfilteredRowIterator withValidation(UnfilteredRowIterator iter, String filename)
    {
        return options.checkData && !isIndex ? UnfilteredRowIterators.withValidation(iter, filename) : iter;
    }

    @Override
    protected void scrubInternal(SSTableRewriter writer) throws IOException
    {
        try
        {
            nextIndexKey = indexAvailable() ? ByteBufferUtil.readWithShortLength(indexFile) : null;
            if (indexAvailable())
            {
                long firstRowPositionFromIndex = rowIndexEntrySerializer.deserializePositionAndSkip(indexFile);
                // Normally 0. A Data.db assembled by copying cell-aligned byte ranges out of a larger one -- a
                // zero-copy split child, or a slice received by partial zero-copy streaming -- opens with a DEAD
                // PREFIX instead: the head of its first cell, holding the tail of a partition that starts before the
                // copied range. Only a position that could be such a prefix is taken as one (see deadPrefixLimit).
                if (firstRowPositionFromIndex != 0 && firstRowPositionFromIndex < deadPrefixLimit())
                {
                    nextPartitionPositionFromIndex = firstRowPositionFromIndex;
                    dataFile.seek(firstRowPositionFromIndex);
                }
                else
                {
                    // Any other non-zero position means Index.db is wrong, and this handles it as it always has: the
                    // assertion is thrown out of the enclosing try, whose catch clause drops the index and scrubs
                    // Data.db unaided (throwIfFatal deliberately lets an AssertionError through). Seeking to the
                    // position instead would land the key read mid-partition, fail the comparison against the index
                    // key, and send the "Retrying from partition index" path to the very same bad position -- and
                    // since scrub obsoletes the original, whatever it appends there is what the partition becomes.
                    assert firstRowPositionFromIndex == 0 : firstRowPositionFromIndex;
                }
            }
        }
        catch (Throwable ex)
        {
            throwIfFatal(ex);
            nextIndexKey = null;
            nextPartitionPositionFromIndex = dataFile.length();
            if (indexFile != null)
                indexFile.seek(indexFile.length());
        }

        DecoratedKey prevKey = null;

        // Only an sstable that DECLARES unindexed regions may have bytes no index entry describes BETWEEN
        // partitions, and only for those may the index be preferred to the data pointer below.
        boolean hasUnindexedRegions = sstable.hasUnindexedRegions();

        TableMetadata tableMetadata = cfs.metadata.getLocal();
        while (!dataFile.isEOF())
        {
            if (scrubInfo.isStopRequested())
                throw new CompactionInterruptedException(scrubInfo.getCompactionInfo());

            // A partial zero-copy stream can leave unindexed bytes BETWEEN partitions, not just before the first:
            // the tail of a boundary compression chunk, holding partitions the receiver did not ask for. Reading
            // one as a partition is recoverable but costs an alarming warning per gap on an sstable that is not
            // corrupt, so for those sstables - and ONLY those - skip to where the index says the next partition is.
            // For every other sstable this condition can only be true because Index.db is wrong, and then the data
            // file is the more trustworthy of the two: walking on keeps the pointer at the true partition start, so
            // the key matches its index entry and the disagreement is merely warned about below, whereas seeking
            // would put the key read mid-partition and send "Retrying from partition index" to the same bad
            // position. A non-null nextIndexKey is what tells a real position (next* describe the partition about to
            // be read; updateIndexKey below shifts them to current) from the dataFile.length() sentinel the index
            // sets once exhausted. The length() bound is not optional: the position comes straight out of Index.db,
            // seek() throws past length(), and scrub() catches only IOException, so one corrupt entry would abort
            // the whole scrub and recover nothing rather than fall through to the key-mismatch path that handles it.
            if (hasUnindexedRegions
                && nextIndexKey != null
                && nextPartitionPositionFromIndex > dataFile.getFilePointer()
                && nextPartitionPositionFromIndex < dataFile.length())
            {
                dataFile.seek(nextPartitionPositionFromIndex);
            }

            long partitionStart = dataFile.getFilePointer();
            outputHandler.debug("Reading row at %d", partitionStart);

            DecoratedKey key = null;
            try
            {
                ByteBuffer raw = ByteBufferUtil.readWithShortLength(dataFile);
                if (!tableMetadata.isIndex())
                    tableMetadata.partitionKeyType.validate(raw);
                key = sstable.decorateKey(raw);
            }
            catch (Throwable th)
            {
                throwIfFatal(th);
                // check for null key below
            }

            long dataStartFromIndex = -1;
            long dataSizeFromIndex = -1;
            updateIndexKey();

            if (indexAvailable())
            {
                if (currentIndexKey != null)
                {
                    dataStartFromIndex = currentPartitionPositionFromIndex + 2 + currentIndexKey.remaining();
                    dataSizeFromIndex = nextPartitionPositionFromIndex - dataStartFromIndex;
                }
            }

            long dataStart = dataFile.getFilePointer();

            String keyName = key == null ? "(unreadable key)" : keyString(key);
            outputHandler.debug("partition %s is %s", keyName, FBUtilities.prettyPrintMemory(dataSizeFromIndex));
            assert currentIndexKey != null || !indexAvailable();

            try
            {
                if (key == null)
                    throw new IOError(new IOException("Unable to read partition key from data file"));

                if (currentIndexKey != null && !key.getKey().equals(currentIndexKey))
                {
                    throw new IOError(new IOException(String.format("Key from data file (%s) does not match key from index file (%s)",
                                                                    //ByteBufferUtil.bytesToHex(key.getKey()), ByteBufferUtil.bytesToHex(currentIndexKey))));
                                                                    "_too big_", ByteBufferUtil.bytesToHex(currentIndexKey))));
                }

                if (indexFile != null && dataSizeFromIndex > dataFile.length())
                    throw new IOError(new IOException("Impossible partition size (greater than file length): " + dataSizeFromIndex));

                if (indexFile != null && dataStart != dataStartFromIndex)
                    outputHandler.warn("Data file partition position %d differs from index file row position %d", dataStart, dataStartFromIndex);

                if (tryAppend(prevKey, key, writer))
                    prevKey = key;
            }
            catch (Throwable th)
            {
                throwIfFatal(th);
                outputHandler.warn(th, "Error reading partition %s (stacktrace follows):", keyName);

                if (currentIndexKey != null
                    && (key == null || !key.getKey().equals(currentIndexKey) || dataStart != dataStartFromIndex))
                {

                    outputHandler.output("Retrying from partition index; data is %s bytes starting at %s",
                                         dataSizeFromIndex, dataStartFromIndex);
                    key = sstable.decorateKey(currentIndexKey);
                    try
                    {
                        if (!tableMetadata.isIndex())
                            tableMetadata.partitionKeyType.validate(key.getKey());
                        dataFile.seek(dataStartFromIndex);

                        if (tryAppend(prevKey, key, writer))
                            prevKey = key;
                    }
                    catch (Throwable th2)
                    {
                        throwIfFatal(th2);
                        throwIfCannotContinue(key, th2);

                        outputHandler.warn(th2, "Retry failed too. Skipping to next partition (retry's stacktrace follows)");
                        badPartitions++;
                        if (!seekToNextPartition())
                            break;
                    }
                }
                else
                {
                    throwIfCannotContinue(key, th);

                    outputHandler.warn("Partition starting at position %d is unreadable; skipping to next", dataStart);
                    badPartitions++;
                    if (currentIndexKey != null)
                        if (!seekToNextPartition())
                            break;
                }
            }
        }
    }

    /**
     * Exclusive bound on a non-zero first partition position this scrubber will accept as a DEAD PREFIX rather than
     * treat as a wrong index entry.
     * <p>
     * An sstable whose Data.db was assembled from verbatim cell-aligned byte ranges of a larger one carries the bytes
     * of its first cell that precede the copied range: {@code lo mod cellLength} of them, so a legitimate prefix is
     * always inside the FIRST cell. The cell is the compression chunk length (uncompressed positions are pinned to
     * multiples of it), or CRC.db's chunk size for an uncompressed sstable. Capped by the data length so the caller's
     * seek cannot go out of bounds, and 0 - no tolerance at all, i.e. the "first partition is at 0" this used to
     * assert - when the grid cannot be read, since then nothing bounds the prefix.
     * <p>
     * {@code StatsMetadata#hasUnindexedRegions} is deliberately not the test here: it marks only INTERIOR unindexed
     * regions, and is left unset for an sstable that has nothing but a prefix precisely so those can still be read by
     * the linear scanner. Only called for a non-zero position, so an ordinary scrub never reaches it.
     */
    private long deadPrefixLimit()
    {
        long cellLength;
        if (sstable.compression)
        {
            cellLength = sstable.getCompressionMetadata().chunkLength();
        }
        else
        {
            File crc = sstable.descriptor.fileFor(Components.CRC);
            if (!crc.exists())
                return 0;
            try (RandomAccessReader in = RandomAccessReader.open(crc))
            {
                cellLength = in.readInt();
            }
            catch (IOException | RuntimeException e)
            {
                return 0;
            }
        }
        return cellLength <= 0 ? 0 : Math.min(cellLength, dataFile.length());
    }

    private void updateIndexKey()
    {
        currentIndexKey = nextIndexKey;
        currentPartitionPositionFromIndex = nextPartitionPositionFromIndex;
        try
        {
            nextIndexKey = !indexAvailable() ? null : ByteBufferUtil.readWithShortLength(indexFile);

            nextPartitionPositionFromIndex = !indexAvailable()
                                             ? dataFile.length()
                                             : rowIndexEntrySerializer.deserializePositionAndSkip(indexFile);
        }
        catch (Throwable th)
        {
            JVMStabilityInspector.inspectThrowable(th);
            outputHandler.warn(th, "Error reading index file");
            nextIndexKey = null;
            nextPartitionPositionFromIndex = dataFile.length();
        }
    }

    private boolean indexAvailable()
    {
        return indexFile != null && !indexFile.isEOF();
    }

    private boolean seekToNextPartition()
    {
        while (nextPartitionPositionFromIndex < dataFile.length())
        {
            try
            {
                dataFile.seek(nextPartitionPositionFromIndex);
                return true;
            }
            catch (Throwable th)
            {
                throwIfFatal(th);
                outputHandler.warn(th, "Failed to seek to next partition position %d", nextPartitionPositionFromIndex);
                badPartitions++;
            }

            updateIndexKey();
        }

        return false;
    }

    @Override
    protected void throwIfCannotContinue(DecoratedKey key, Throwable th)
    {
        if (isIndex)
        {
            outputHandler.warn("An error occurred while scrubbing the partition with key '%s' for an index table. " +
                               "Scrubbing will abort for this table and the index will be rebuilt.", keyString(key));
            throw new IOError(th);
        }

        super.throwIfCannotContinue(key, th);
    }

    @Override
    public void close()
    {
        fileAccessLock.writeLock().lock();
        try
        {
            FileUtils.closeQuietly(dataFile);
            FileUtils.closeQuietly(indexFile);
        }
        finally
        {
            fileAccessLock.writeLock().unlock();
        }
    }
}
