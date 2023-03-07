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
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
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
                // throw away variable, so we don't have a side effect in the assertion
                long firstRowPositionFromIndex = rowIndexEntrySerializer.deserializePositionAndSkip(indexFile);
                assert firstRowPositionFromIndex == 0 : firstRowPositionFromIndex;
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

        while (!dataFile.isEOF())
        {
            if (scrubInfo.isStopRequested())
                throw new CompactionInterruptedException(scrubInfo.getCompactionInfo());

            long partitionStart = dataFile.getFilePointer();
            outputHandler.debug("Reading row at %d", partitionStart);

            DecoratedKey key = null;
            try
            {
                ByteBuffer raw = ByteBufferUtil.readWithShortLength(dataFile);
                if (!cfs.metadata.getLocal().isIndex())
                    cfs.metadata.getLocal().partitionKeyType.validate(raw);
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
                        if (!cfs.metadata.getLocal().isIndex())
                            cfs.metadata.getLocal().partitionKeyType.validate(key.getKey());
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
