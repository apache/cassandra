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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.LazilyInitializedUnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.AbstractBounds.Boundary;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.AbstractRowIndexEntry;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.AbstractIterator;

import static org.apache.cassandra.dht.AbstractBounds.isEmpty;
import static org.apache.cassandra.dht.AbstractBounds.maxLeft;
import static org.apache.cassandra.dht.AbstractBounds.minRight;

public abstract class SSTableScanner<S extends SSTableReader,
                                     E extends AbstractRowIndexEntry,
                                     I extends SSTableScanner<S, E, I>.BaseKeyScanningIterator>
implements ISSTableScanner
{
    protected final AtomicBoolean isClosed = new AtomicBoolean(false);
    protected final RandomAccessReader dfile;
    protected final S sstable;

    protected final Iterator<AbstractBounds<PartitionPosition>> rangeIterator;

    protected final ColumnFilter columns;
    protected final DataRange dataRange;
    private final SSTableReadsListener listener;

    protected I iterator;

    protected long startScan = -1;
    protected long bytesScanned = 0;

    protected SSTableScanner(S sstable,
                             ColumnFilter columns,
                             DataRange dataRange,
                             Iterator<AbstractBounds<PartitionPosition>> rangeIterator,
                             SSTableReadsListener listener)
    {
        assert sstable != null;

        this.dfile = sstable.openDataReader();
        this.sstable = sstable;
        this.columns = columns;
        this.dataRange = dataRange;
        this.rangeIterator = rangeIterator;
        this.listener = listener;
    }

    protected static List<AbstractBounds<PartitionPosition>> makeBounds(SSTableReader sstable, Collection<Range<Token>> tokenRanges)
    {
        List<AbstractBounds<PartitionPosition>> boundsList = new ArrayList<>(tokenRanges.size());
        for (Range<Token> range : Range.normalize(tokenRanges))
            addRange(sstable, Range.makeRowRange(range), boundsList);
        return boundsList;
    }

    protected static List<AbstractBounds<PartitionPosition>> makeBounds(SSTableReader sstable, DataRange dataRange)
    {
        List<AbstractBounds<PartitionPosition>> boundsList = new ArrayList<>(2);
        addRange(sstable, dataRange.keyRange(), boundsList);
        return boundsList;
    }

    protected static AbstractBounds<PartitionPosition> fullRange(SSTableReader sstable)
    {
        return new Bounds<>(sstable.getFirst(), sstable.getLast());
    }

    private static void addRange(SSTableReader sstable, AbstractBounds<PartitionPosition> requested, List<AbstractBounds<PartitionPosition>> boundsList)
    {
        if (requested instanceof Range && ((Range<?>) requested).isWrapAround())
        {
            if (requested.right.compareTo(sstable.getFirst()) >= 0)
            {
                // since we wrap, we must contain the whole sstable prior to stopKey()
                Boundary<PartitionPosition> left = new Boundary<>(sstable.getFirst(), true);
                Boundary<PartitionPosition> right;
                right = requested.rightBoundary();
                right = minRight(right, sstable.getLast(), true);
                if (!isEmpty(left, right))
                    boundsList.add(AbstractBounds.bounds(left, right));
            }
            if (requested.left.compareTo(sstable.getLast()) <= 0)
            {
                // since we wrap, we must contain the whole sstable after dataRange.startKey()
                Boundary<PartitionPosition> right = new Boundary<>(sstable.getLast(), true);
                Boundary<PartitionPosition> left;
                left = requested.leftBoundary();
                left = maxLeft(left, sstable.getFirst(), true);
                if (!isEmpty(left, right))
                    boundsList.add(AbstractBounds.bounds(left, right));
            }
        }
        else
        {
            assert !AbstractBounds.strictlyWrapsAround(requested.left, requested.right);
            Boundary<PartitionPosition> left, right;
            left = requested.leftBoundary();
            right = requested.rightBoundary();
            left = maxLeft(left, sstable.getFirst(), true);
            // apparently isWrapAround() doesn't count Bounds that extend to the limit (min) as wrapping
            right = requested.right.isMinimum() ? new Boundary<>(sstable.getLast(), true)
                                                : minRight(right, sstable.getLast(), true);
            if (!isEmpty(left, right))
                boundsList.add(AbstractBounds.bounds(left, right));
        }
    }

    public void close()
    {
        try
        {
            if (isClosed.compareAndSet(false, true))
            {
                markScanned();
                doClose();
            }
        }
        catch (IOException e)
        {
            sstable.markSuspect();
            throw new CorruptSSTableException(e, sstable.getFilename());
        }
    }

    protected abstract void doClose() throws IOException;

    @Override
    public long getLengthInBytes()
    {
        return sstable.uncompressedLength();
    }


    public long getCompressedLengthInBytes()
    {
        return sstable.onDiskLength();
    }

    @Override
    public long getCurrentPosition()
    {
        return dfile.getFilePointer();
    }

    public long getBytesScanned()
    {
        return bytesScanned;
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
        if (iterator == null)
            iterator = createIterator();
        return iterator.hasNext();
    }

    public UnfilteredRowIterator next()
    {
        if (iterator == null)
            iterator = createIterator();
        return iterator.next();
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    private I createIterator()
    {
        this.listener.onScanningStarted(sstable);
        return doCreateIterator();
    }

    protected abstract I doCreateIterator();

    private void markScanned()
    {
        if (startScan != -1)
        {
            bytesScanned += dfile.getFilePointer() - startScan;
            startScan = -1;
        }
    }

    @Override
    public String toString()
    {
        return String.format("%s(dfile=%s sstable=%s)", getClass().getSimpleName(), dfile, sstable);
    }

    public abstract class BaseKeyScanningIterator extends AbstractIterator<UnfilteredRowIterator>
    {
        protected DecoratedKey currentKey;
        protected E currentEntry;
        private LazilyInitializedUnfilteredRowIterator currentRowIterator;

        protected abstract boolean prepareToIterateRow() throws IOException;

        protected abstract UnfilteredRowIterator getRowIterator(E indexEntry, DecoratedKey key) throws IOException;

        protected UnfilteredRowIterator computeNext()
        {
            if (currentRowIterator != null && currentRowIterator.isOpen() && currentRowIterator.hasNext())
                throw new IllegalStateException("The UnfilteredRowIterator returned by the last call to next() was initialized: " +
                                                "it must be closed before calling hasNext() or next() again.");

            try
            {
                markScanned();

                if (!prepareToIterateRow())
                    return endOfData();

                /*
                 * For a given partition key, we want to avoid hitting the data file unless we're explicitly asked.
                 * This is important for PartitionRangeReadCommand#checkCacheFilter.
                 */
                return currentRowIterator = new LazilyInitializedUnfilteredRowIterator(currentKey)
                {
                    // Store currentEntry reference during object instantiation as later (during initialize) the
                    // reference may point to a different entry.
                    private final E rowIndexEntry = currentEntry;

                    protected UnfilteredRowIterator initializeIterator()
                    {
                        try
                        {
                            startScan = rowIndexEntry.position;
                            return getRowIterator(rowIndexEntry, partitionKey());
                        }
                        catch (CorruptSSTableException | IOException e)
                        {
                            sstable.markSuspect();
                            throw new CorruptSSTableException(e, sstable.getFilename());
                        }
                    }
                };
            }
            catch (CorruptSSTableException | IOException e)
            {
                sstable.markSuspect();
                throw new CorruptSSTableException(e, sstable.getFilename());
            }
        }
    }
}
