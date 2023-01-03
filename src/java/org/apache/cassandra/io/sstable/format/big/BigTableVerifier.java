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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.NoSuchFileException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Throwables;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.compaction.CompactionInterruptedException;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.IVerifier;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SortedTableVerifier;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.OutputHandler;

public class BigTableVerifier extends SortedTableVerifier<BigTableReader> implements IVerifier
{
    private final RandomAccessReader indexFile;
    private final RowIndexEntry.IndexSerializer rowIndexEntrySerializer;

    public BigTableVerifier(ColumnFamilyStore cfs, BigTableReader sstable, OutputHandler outputHandler, boolean isOffline, Options options)
    {
        super(cfs, sstable, outputHandler, isOffline, options);

        this.rowIndexEntrySerializer = new RowIndexEntry.Serializer(sstable.descriptor.version, sstable.header);
        this.indexFile = RandomAccessReader.open(new File(sstable.descriptor.filenameFor(Component.PRIMARY_INDEX)));
    }

    @Override
    public void verify()
    {
        verifySSTableVersion();

        verifySSTableMetadata();

        verifyIndex();

        verifyIndexSummary();

        verifyBloomFilter();

        if (options.checkOwnsTokens && !isOffline && !(cfs.getPartitioner() instanceof LocalPartitioner))
        {
            if (verifyOwnedRanges() == 0)
                return;
        }

        if (options.quick)
            return;

        if (verifyDigest() && !options.extendedVerification)
            return;

        verifySSTable();

        outputHandler.output("Verify of %s succeeded. All %d rows read successfully", sstable, goodRows);
    }

    private void verifySSTable()
    {
        long rowStart;
        outputHandler.output("Extended Verify requested, proceeding to inspect values");

        try
        {
            ByteBuffer nextIndexKey = ByteBufferUtil.readWithShortLength(indexFile);
            {
                long firstRowPositionFromIndex = rowIndexEntrySerializer.deserializePositionAndSkip(indexFile);
                if (firstRowPositionFromIndex != 0)
                    markAndThrow(new RuntimeException("firstRowPositionFromIndex != 0: " + firstRowPositionFromIndex));
            }

            List<Range<Token>> ownedRanges = isOffline ? Collections.emptyList() : Range.normalize(tokenLookup.apply(cfs.metadata().keyspace));
            RangeOwnHelper rangeOwnHelper = new RangeOwnHelper(ownedRanges);
            DecoratedKey prevKey = null;

            while (!dataFile.isEOF())
            {

                if (verifyInfo.isStopRequested())
                    throw new CompactionInterruptedException(verifyInfo.getCompactionInfo());

                rowStart = dataFile.getFilePointer();
                outputHandler.debug("Reading row at %d", rowStart);

                DecoratedKey key = null;
                try
                {
                    key = sstable.decorateKey(ByteBufferUtil.readWithShortLength(dataFile));
                }
                catch (Throwable th)
                {
                    throwIfFatal(th);
                    // check for null key below
                }

                if (options.checkOwnsTokens && ownedRanges.size() > 0 && !(cfs.getPartitioner() instanceof LocalPartitioner))
                {
                    try
                    {
                        rangeOwnHelper.validate(key);
                    }
                    catch (Throwable t)
                    {
                        outputHandler.warn(t, "Key %s in sstable %s not owned by local ranges %s", key, sstable, ownedRanges);
                        markAndThrow(t);
                    }
                }

                ByteBuffer currentIndexKey = nextIndexKey;
                long nextRowPositionFromIndex = 0;
                try
                {
                    nextIndexKey = indexFile.isEOF() ? null : ByteBufferUtil.readWithShortLength(indexFile);
                    nextRowPositionFromIndex = indexFile.isEOF()
                                               ? dataFile.length()
                                               : rowIndexEntrySerializer.deserializePositionAndSkip(indexFile);
                }
                catch (Throwable th)
                {
                    markAndThrow(th);
                }

                long dataStart = dataFile.getFilePointer();
                long dataStartFromIndex = currentIndexKey == null
                                          ? -1
                                          : rowStart + 2 + currentIndexKey.remaining();

                long dataSize = nextRowPositionFromIndex - dataStartFromIndex;
                // avoid an NPE if key is null
                String keyName = key == null ? "(unreadable key)" : ByteBufferUtil.bytesToHex(key.getKey());
                outputHandler.debug("row %s is %s", keyName, FBUtilities.prettyPrintMemory(dataSize));

                assert currentIndexKey != null || indexFile.isEOF();

                try
                {
                    if (key == null || dataSize > dataFile.length())
                        markAndThrow(new RuntimeException(String.format("key = %s, dataSize=%d, dataFile.length() = %d", key, dataSize, dataFile.length())));

                    try (UnfilteredRowIterator iterator = SSTableIdentityIterator.create(sstable, dataFile, key))
                    {
                        Row first = null;
                        int duplicateRows = 0;
                        long minTimestamp = Long.MAX_VALUE;
                        long maxTimestamp = Long.MIN_VALUE;
                        while (iterator.hasNext())
                        {
                            Unfiltered uf = iterator.next();
                            if (uf.isRow())
                            {
                                Row row = (Row) uf;
                                if (first != null && first.clustering().equals(row.clustering()))
                                {
                                    duplicateRows++;
                                    for (Cell cell : row.cells())
                                    {
                                        maxTimestamp = Math.max(cell.timestamp(), maxTimestamp);
                                        minTimestamp = Math.min(cell.timestamp(), minTimestamp);
                                    }
                                }
                                else
                                {
                                    if (duplicateRows > 0)
                                        logDuplicates(key, first, duplicateRows, minTimestamp, maxTimestamp);
                                    duplicateRows = 0;
                                    first = row;
                                    maxTimestamp = Long.MIN_VALUE;
                                    minTimestamp = Long.MAX_VALUE;
                                }
                            }
                        }
                        if (duplicateRows > 0)
                            logDuplicates(key, first, duplicateRows, minTimestamp, maxTimestamp);
                    }

                    if ((prevKey != null && prevKey.compareTo(key) > 0) || !key.getKey().equals(currentIndexKey) || dataStart != dataStartFromIndex)
                        markAndThrow(new RuntimeException("Key out of order: previous = " + prevKey + " : current = " + key));
                    goodRows++;
                    prevKey = key;


                    outputHandler.debug("Row %s at %s valid, moving to next row at %s ", goodRows, rowStart, nextRowPositionFromIndex);
                    dataFile.seek(nextRowPositionFromIndex);
                }
                catch (Throwable th)
                {
                    markAndThrow(th);
                }
            }
        }
        catch (Throwable t)
        {
            Throwables.throwIfUnchecked(t);
            throw new RuntimeException(t);
        }
        finally
        {
            controller.close();
        }
    }

    private void verifyIndexSummary()
    {
        try
        {
            outputHandler.debug("Deserializing index summary for %s", sstable);
            deserializeIndexSummary(sstable);
        }
        catch (Throwable t)
        {
            outputHandler.output("Index summary is corrupt - if it is removed it will get rebuilt on startup %s", sstable.descriptor.filenameFor(Component.SUMMARY));
            outputHandler.warn(t);
            markAndThrow(t, false);
        }
    }

    private void verifyIndex()
    {
        try
        {
            outputHandler.debug("Deserializing index for %s", sstable);
            deserializeIndex(sstable);
        }
        catch (Throwable t)
        {
            outputHandler.warn(t);
            markAndThrow(t);
        }
    }

    private void logDuplicates(DecoratedKey key, Row first, int duplicateRows, long minTimestamp, long maxTimestamp)
    {
        String keyString = sstable.metadata().partitionKeyType.getString(key.getKey());
        long firstMaxTs = Long.MIN_VALUE;
        long firstMinTs = Long.MAX_VALUE;
        for (Cell cell : first.cells())
        {
            firstMaxTs = Math.max(firstMaxTs, cell.timestamp());
            firstMinTs = Math.min(firstMinTs, cell.timestamp());
        }
        outputHandler.output("%d duplicate rows found for [%s %s] in %s.%s (%s), timestamps: [first row (%s, %s)], [duplicates (%s, %s, eq:%b)]",
                             duplicateRows,
                             keyString, first.clustering().toString(sstable.metadata()),
                             sstable.metadata().keyspace,
                             sstable.metadata().name,
                             sstable,
                             dateString(firstMinTs), dateString(firstMaxTs),
                             dateString(minTimestamp), dateString(maxTimestamp), minTimestamp == maxTimestamp);
    }

    private String dateString(long time)
    {
        return Instant.ofEpochMilli(TimeUnit.MICROSECONDS.toMillis(time)).toString();
    }


    private void deserializeIndex(SSTableReader sstable) throws IOException
    {
        try (RandomAccessReader primaryIndex = RandomAccessReader.open(new File(sstable.descriptor.filenameFor(Component.PRIMARY_INDEX))))
        {
            long indexSize = primaryIndex.length();

            while ((primaryIndex.getFilePointer()) != indexSize)
            {
                ByteBuffer key = ByteBufferUtil.readWithShortLength(primaryIndex);
                RowIndexEntry.Serializer.skip(primaryIndex, sstable.descriptor.version);
            }
        }
    }

    private void deserializeIndexSummary(SSTableReader sstable) throws IOException
    {
        IndexSummaryComponent summaryComponent = IndexSummaryComponent.load(sstable.descriptor, cfs.metadata());
        if (summaryComponent == null)
            throw new NoSuchFileException("Index summary component of sstable " + sstable.descriptor.baseFilename() + " is missing");
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