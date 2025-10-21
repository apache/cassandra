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
package org.apache.cassandra.io.sstable.format;

import java.io.IOError;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.schema.TableMetadata;

import static org.apache.cassandra.io.sstable.format.SSTableReader.PartitionPositionBounds;

/// Simple SSTable scanner that reads sequentially through an SSTable without using the index.
///
/// This is a significant improvement for the performance of compaction over using the full-blown DataRange-capable
/// [SSTableScanner] and enables correct calculation of data sizes to process.
public class SSTableSimpleScanner
implements ISSTableScanner
{
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final RandomAccessReader dfile;
    private final SSTableReader sstable;

    private final Iterator<PartitionPositionBounds> rangeIterator;

    private long bytesScannedInPreviousRanges;

    private final long sizeInBytes;
    private final long compressedSizeInBytes;

    private long currentEndPosition;
    private long currentStartPosition;

    private SSTableIdentityIterator currentIterator;
    private DecoratedKey lastKey;

    /// Create a new simple scanner over the given sstables and the given ranges of uncompressed positions.
    /// Each range must start and end on a partition boundary, and, to satisfy the contract of [ISSTableScanner], the
    /// ranges must be non-overlapping and in ascending order. This scanner will throw an [IllegalArgumentException] if
    /// the latter is not true.
    ///
    /// The ranges can be constructed by [SSTableReader#getPositionsForRanges] and similar methods as done by the
    /// various [SSTableReader#getScanner] variations.
    public SSTableSimpleScanner(SSTableReader sstable,
                                Collection<PartitionPositionBounds> boundsList)
    {
        assert sstable != null;

        this.dfile = sstable.openDataReaderForScan();
        this.sstable = sstable;
        this.sizeInBytes = boundsList.stream().mapToLong(ppb -> ppb.upperPosition - ppb.lowerPosition).sum();
        this.compressedSizeInBytes = sstable.compression ? sstable.onDiskSizeForPartitionPositions(boundsList) : sizeInBytes;
        this.rangeIterator = boundsList.iterator();
        this.currentEndPosition = 0;
        this.currentStartPosition = 0;
        this.bytesScannedInPreviousRanges = 0;
        this.currentIterator = null;
        this.lastKey = null;
    }

    public void close()
    {
        if (isClosed.compareAndSet(false, true))
        {
            // ensure we report what we have actually processed
            bytesScannedInPreviousRanges += dfile.getFilePointer() - currentStartPosition;
            dfile.close();
            // close() may change the file pointer, update so that the difference is 0 when reported by getBytesScanned()
            currentStartPosition = dfile.getFilePointer();
        }
    }

    @Override
    public long getLengthInBytes()
    {
        return sizeInBytes;
    }


    public long getCompressedLengthInBytes()
    {
        return compressedSizeInBytes;
    }

    @Override
    public long getCurrentPosition()
    {
        return dfile.getFilePointer();
    }

    public long getBytesScanned()
    {
        return bytesScannedInPreviousRanges + dfile.getFilePointer() - currentStartPosition;
    }

    @Override
    public Set<SSTableReader> getBackingSSTables()
    {
        return ImmutableSet.of(sstable);
    }

    public TableMetadata metadata()
    {
        return sstable.metadata();
    }

    public boolean hasNext()
    {
        if (currentIterator != null)
        {
            currentIterator.close(); // Ensure that the iterator cannot be used further. No op if already closed.

            // Row iterator must be exhausted to advance to next partition
            currentIterator.exhaust();
            currentIterator = null;
        }

        if (dfile.getFilePointer() < currentEndPosition)
            return true;

        return advanceRange();
    }

    boolean advanceRange()
    {
        try
        {
            if (!rangeIterator.hasNext())
                return false;

            bytesScannedInPreviousRanges += currentEndPosition - currentStartPosition;

            PartitionPositionBounds nextRange = rangeIterator.next();
            if (currentEndPosition > nextRange.lowerPosition)
                throw new IllegalArgumentException("Ranges supplied to SSTableSimpleScanner must be non-overlapping and in ascending order.");

            currentEndPosition = nextRange.upperPosition;
            currentStartPosition = nextRange.lowerPosition;
            dfile.seek(currentStartPosition);
            return true;
        }
        catch (CorruptSSTableException e)
        {
            sstable.markSuspect();
            throw e;
        }
        catch (IOError e)
        {
            if (e.getCause() instanceof IOException)
            {
                sstable.markSuspect();
                throw new CorruptSSTableException((Exception)e.getCause(), sstable.getFilename());
            }
            else
            {
                throw e;
            }
        }
    }

    public UnfilteredRowIterator next()
    {
        if (!hasNext())
            throw new NoSuchElementException();

        currentIterator = SSTableIdentityIterator.create(sstable, dfile, false);
        DecoratedKey currentKey = currentIterator.partitionKey();
        if (lastKey != null && lastKey.compareTo(currentKey) >= 0)
        {
            sstable.markSuspect();
            throw new CorruptSSTableException(new IllegalStateException(String.format("Invalid key order: current %s <= previous %s",
                                                                                      currentKey,
                                                                                      lastKey)),
                                              sstable.getFilename());
        }
        lastKey = currentKey;
        return currentIterator;
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString()
    {
        return String.format("%s(sstable=%s)", getClass().getSimpleName(), sstable);
    }
}
