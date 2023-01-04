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
package org.apache.cassandra.io.sstable.format.trieindex;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import com.google.common.base.Throwables;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.compaction.CompactionInterruptedException;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.IVerifier;
import org.apache.cassandra.io.sstable.KeyReader;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SortedTableVerifier;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.OutputHandler;

public class BtiTableVerifier extends SortedTableVerifier<BtiTableReader> implements IVerifier
{
    public BtiTableVerifier(ColumnFamilyStore cfs, BtiTableReader sstable, boolean isOffline, Options options)
    {
        this(cfs, sstable, new OutputHandler.LogOutput(), isOffline, options);
    }

    public BtiTableVerifier(ColumnFamilyStore cfs, BtiTableReader sstable, OutputHandler outputHandler, boolean isOffline, Options options)
    {
        super(cfs, sstable, outputHandler, isOffline, options);
    }

    public void verify()
    {
        boolean extended = options.extendedVerification;
        long rowStart;

        verifySSTableVersion();

        verifySSTableMetadata();

        verifyIndex();

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

        try (KeyReader indexIterator = sstable.keyReader())
        {
            if (indexIterator.dataPosition() != 0)
                markAndThrow(new RuntimeException("First row position from index != 0: " + indexIterator.dataPosition()));

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

                ByteBuffer currentIndexKey = indexIterator.key();
                long nextRowPositionFromIndex = 0;
                try
                {
                    nextRowPositionFromIndex = indexIterator.advance()
                                               ? indexIterator.dataPosition()
                                               : dataFile.length();
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

                try
                {
                    if (key == null || dataSize > dataFile.length())
                        markAndThrow(new RuntimeException(String.format("key = %s, dataSize=%d, dataFile.length() = %d", key, dataSize, dataFile.length())));

                    //mimic the scrub read path, intentionally unused
                    UnfilteredRowIterator iterator = SSTableIdentityIterator.create(sstable, dataFile, key);
                    iterator.close();

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
            throw Throwables.propagate(t);
        }
        finally
        {
            controller.close();
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

    private void deserializeIndex(SSTableReader sstable) throws IOException
    {
        try (KeyReader it = sstable.keyReader())
        {
            ByteBuffer last = it.key();
            while (it.advance()) last = it.key(); // no-op, just check if index is readable
            if (!Objects.equals(last, sstable.last.getKey()))
                throw new CorruptSSTableException(new IOException("Failed to read partition index"), it.toString());
        }
    }

    public void close()
    {
        fileAccessLock.writeLock().lock();
        try
        {
            FileUtils.closeQuietly(dataFile);
        }
        finally
        {
            fileAccessLock.writeLock().unlock();
        }
    }
}
