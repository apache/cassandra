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
import org.apache.cassandra.io.util.FileUtils;
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
            outputHandler.warn("First position reported by index should be 0, was " +
                               indexIterator.dataPosition() +
                               ", continuing without index.");
            indexIterator.close();
            indexIterator = null;
        }

        DecoratedKey prevKey = null;

        while (!dataFile.isEOF())
        {
            if (scrubInfo.isStopRequested())
                throw new CompactionInterruptedException(scrubInfo.getCompactionInfo());

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
