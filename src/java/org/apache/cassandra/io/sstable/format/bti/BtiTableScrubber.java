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

import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.compaction.CompactionInterruptedException;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.io.sstable.IScrubber;
import org.apache.cassandra.io.sstable.SSTableRewriter;
import org.apache.cassandra.io.sstable.format.SortedTableScrubber;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat.Components;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.cassandra.utils.Throwables;

public class BtiTableScrubber extends SortedTableScrubber<BtiTableReader> implements IScrubber
{
    private final boolean isIndex;
    private final AbstractType<?> partitionKeyType;
    private ScrubPartitionIterator indexIterator;

    public BtiTableScrubber(ColumnFamilyStore cfs,
                            LifecycleTransaction transaction,
                            OutputHandler outputHandler,
                            IScrubber.Options options)
    {
        super(cfs, transaction, outputHandler, options);

        boolean hasIndexFile = sstable.getComponents().contains(Components.PARTITION_INDEX);
        this.isIndex = cfs.isIndex();
        this.partitionKeyType = cfs.metadata.get().partitionKeyType;
        if (!hasIndexFile)
        {
            // if there's any corruption in the -Data.db then partitions can't be skipped over. but it's worth a shot.
            outputHandler.warn("Missing index component");
        }

        try
        {
            this.indexIterator = hasIndexFile
                                 ? openIndexIterator()
                                 : null;
        }
        catch (RuntimeException ex)
        {
            outputHandler.warn("Detected corruption in the index file - cannot open index iterator", ex);
        }
    }

    private ScrubPartitionIterator openIndexIterator()
    {
        try
        {
            return sstable.scrubPartitionsIterator();
        }
        catch (Throwable t)
        {
            outputHandler.warn(t, "Index is unreadable, scrubbing will continue without index.");
        }
        return null;
    }

    @Override
    protected UnfilteredRowIterator withValidation(UnfilteredRowIterator iter, String filename)
    {
        return options.checkData && !isIndex ? UnfilteredRowIterators.withValidation(iter, filename) : iter;
    }

    @Override
    public void scrubInternal(SSTableRewriter writer)
    {
        if (indexAvailable() && indexIterator.dataPosition() != 0)
        {
            long firstPositionFromIndex = indexIterator.dataPosition();
            // A Data.db assembled from verbatim cell-aligned byte ranges of a larger one - a zero-copy split child,
            // or a slice received by partial zero-copy streaming - opens with a DEAD PREFIX: the head of its first
            // cell, holding the tail of a partition that starts before the copied range. Start the walk at the first
            // indexed partition and KEEP the index. Discarding it, which is what this used to do, left BTI with no
            // way to skip to the next partition: the prefix was read as a partition key, nothing could resync, and
            // SortedTableScrubber.outputSummary reported "No valid partitions found" and marked a healthy sstable
            // for deletion. A position too large to be a prefix (see deadPrefixLimit) still means Index.db is wrong,
            // and there the old refusal is right - the data file is the more trustworthy of the two.
            if (firstPositionFromIndex < deadPrefixLimit())
            {
                dataFile.seek(firstPositionFromIndex);
            }
            else
            {
                outputHandler.warn("First position reported by index should be 0, was " +
                                   firstPositionFromIndex +
                                   ", continuing without index.");
                indexIterator.close();
                indexIterator = null;
            }
        }

        DecoratedKey prevKey = null;

        // Only an sstable that DECLARES unindexed regions may have bytes no index entry describes BETWEEN
        // partitions, and only for those may the index be preferred to the data pointer below.
        boolean hasUnindexedRegions = sstable.hasUnindexedRegions();

        while (!dataFile.isEOF())
        {
            if (scrubInfo.isStopRequested())
                throw new CompactionInterruptedException(scrubInfo.getCompactionInfo());

            // A partial zero-copy stream can leave unindexed bytes BETWEEN partitions, not just before the first: the
            // tail of a boundary compression chunk, holding partitions the receiver did not ask for. Reading one as a
            // partition is recoverable through the retry path below but costs an alarming warning per gap on an
            // sstable that is not corrupt, so for those sstables - and ONLY those - skip to where the index says the
            // next partition is. For every other sstable this condition can only be true because the index is wrong,
            // and then the data file is the more trustworthy of the two: walking on keeps the pointer at the true
            // partition start, so the key matches its index entry and the disagreement is merely warned about below.
            // The length() bound keeps a corrupt index entry out of seek(), which throws past length() and would
            // abort the whole scrub rather than fall through to the key-mismatch path that handles it.
            if (hasUnindexedRegions
                && indexAvailable()
                && indexIterator.dataPosition() > dataFile.getFilePointer()
                && indexIterator.dataPosition() < dataFile.length())
            {
                dataFile.seek(indexIterator.dataPosition());
            }

            // position in a data file where the partition starts
            long dataStart = dataFile.getFilePointer();
            outputHandler.debug("Reading row at %d", dataStart);

            DecoratedKey key = null;
            Throwable keyReadError = null;
            try
            {
                ByteBuffer raw = ByteBufferUtil.readWithShortLength(dataFile);
                if (!isIndex)
                    partitionKeyType.validate(raw);
                key = sstable.decorateKey(raw);
            }
            catch (Throwable th)
            {
                keyReadError = th;
                throwIfFatal(th);
                // check for null key below
            }

            // position of the partition in a data file, it points to the beginning of the partition key
            long dataStartFromIndex = -1;
            // size of the partition (including partition key)
            long dataSizeFromIndex = -1;
            ByteBuffer currentIndexKey = null;
            if (indexAvailable())
            {
                currentIndexKey = indexIterator.key();
                dataStartFromIndex = indexIterator.dataPosition();
                if (!indexIterator.isExhausted())
                {
                    try
                    {
                        indexIterator.advance();
                        if (!indexIterator.isExhausted())
                            dataSizeFromIndex = indexIterator.dataPosition() - dataStartFromIndex;
                    }
                    catch (Throwable th)
                    {
                        throwIfFatal(th);
                        outputHandler.warn(th,
                                           "Failed to advance to the next index position. Index is corrupted. " +
                                           "Continuing without the index. Last position read is %d.",
                                           indexIterator.dataPosition());
                        indexIterator.close();
                        indexIterator = null;
                        currentIndexKey = null;
                        dataStartFromIndex = -1;
                        dataSizeFromIndex = -1;
                    }
                }
            }

            String keyName = key == null ? "(unreadable key)" : keyString(key);
            outputHandler.debug("partition %s is %s", keyName, FBUtilities.prettyPrintMemory(dataSizeFromIndex));

            try
            {
                if (key == null)
                    throw new IOError(new IOException("Unable to read partition key from data file", keyReadError));

                if (currentIndexKey != null && !key.getKey().equals(currentIndexKey))
                {
                    throw new IOError(new IOException(String.format("Key from data file (%s) does not match key from index file (%s)",
                                                                    ByteBufferUtil.bytesToHex(key.getKey()), ByteBufferUtil.bytesToHex(currentIndexKey))));
                }

                if (indexIterator != null && dataSizeFromIndex > dataFile.length())
                    throw new IOError(new IOException("Impossible partition size (greater than file length): " + dataSizeFromIndex));

                if (indexIterator != null && dataStart != dataStartFromIndex)
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

                    // position where the row should start in a data file (right after the partition key)
                    long rowStartFromIndex = dataStartFromIndex + TypeSizes.SHORT_SIZE + currentIndexKey.remaining();
                    outputHandler.output("Retrying from partition index; data is %s bytes starting at %s",
                                         dataSizeFromIndex, rowStartFromIndex);
                    key = sstable.decorateKey(currentIndexKey);
                    try
                    {
                        if (!isIndex)
                            partitionKeyType.validate(key.getKey());
                        dataFile.seek(rowStartFromIndex);

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

                    badPartitions++;
                    if (indexIterator != null)
                    {
                        outputHandler.warn("Partition starting at position %d is unreadable; skipping to next", dataStart);
                        if (!seekToNextPartition())
                            break;
                    }
                    else
                    {
                        outputHandler.warn("Unrecoverable error while scrubbing %s." +
                                           "Scrubbing cannot continue. The sstable will be marked for deletion. " +
                                           "You can attempt manual recovery from the pre-scrub snapshot. " +
                                           "You can also run nodetool repair to transfer the data from a healthy replica, if any.",
                                           sstable);
                        // There's no way to resync and continue. Give up.
                        break;
                    }
                }
            }
        }
    }


    private boolean indexAvailable()
    {
        return indexIterator != null && !indexIterator.isExhausted();
    }

    /**
     * Exclusive bound on a non-zero first partition position this scrubber will accept as a DEAD PREFIX rather than
     * treat as a wrong index.
     * <p>
     * An sstable whose Data.db was assembled from verbatim cell-aligned byte ranges of a larger one carries the bytes
     * of its first cell that precede the copied range: {@code lo mod cellLength} of them, so a legitimate prefix is
     * always inside the FIRST cell. The cell is the compression chunk length (uncompressed positions are pinned to
     * multiples of it), or CRC.db's chunk size for an uncompressed sstable. Capped by the data length so the caller's
     * seek cannot go out of bounds, and 0 - no tolerance at all, i.e. the "must be 0" this used to require - when the
     * grid cannot be read, since then nothing bounds the prefix.
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

    private boolean seekToNextPartition()
    {
        while (indexAvailable())
        {
            long nextRowPositionFromIndex = indexIterator.dataPosition();

            try
            {
                dataFile.seek(nextRowPositionFromIndex);
                return true;
            }
            catch (Throwable th)
            {
                throwIfFatal(th);
                outputHandler.warn(th, "Failed to seek to next row position %d", nextRowPositionFromIndex);
                badPartitions++;
            }

            try
            {
                indexIterator.advance();
            }
            catch (Throwable th)
            {
                outputHandler.warn(th, "Failed to go to the next entry in index");
                throw Throwables.cleaned(th);
            }
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
            FileUtils.closeQuietly(indexIterator);
        }
        finally
        {
            fileAccessLock.writeLock().unlock();
        }
    }
}
