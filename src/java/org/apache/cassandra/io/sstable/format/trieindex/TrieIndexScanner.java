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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;

import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.AbstractUnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.LazilyInitializedUnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.AbstractBounds.Boundary;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.RowIndexEntry;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReader.PartitionPositionBounds;
import org.apache.cassandra.io.sstable.format.SSTableReadsListener;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.CloseableIterator;

import static org.apache.cassandra.dht.AbstractBounds.isEmpty;
import static org.apache.cassandra.dht.AbstractBounds.maxLeft;
import static org.apache.cassandra.dht.AbstractBounds.minRight;

// TODO STAR-247: implement unit test
public class TrieIndexScanner implements ISSTableScanner
{
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    protected final RandomAccessReader dfile;
    public final TrieIndexSSTableReader sstable;

    private final Iterator<AbstractBounds<PartitionPosition>> rangeIterator;

    private final ColumnFilter columns;
    private final DataRange dataRange;
    private final SSTableReadsListener listener;
    private long startScan = -1;
    private long bytesScanned = 0;

    protected CloseableIterator<UnfilteredRowIterator> iterator;

    // Full scan of the sstables
    public static ISSTableScanner getScanner(TrieIndexSSTableReader sstable)
    {
        return getScanner(sstable, Iterators.singletonIterator(fullRange(sstable)));
    }

    public static ISSTableScanner getScanner(TrieIndexSSTableReader sstable,
                                             ColumnFilter columns,
                                             DataRange dataRange,
                                             SSTableReadsListener listener)
    {
        return new TrieIndexScanner(sstable, columns, dataRange, makeBounds(sstable, dataRange).iterator(), listener);
    }

    public static ISSTableScanner getScanner(TrieIndexSSTableReader sstable, Collection<Range<Token>> tokenRanges)
    {
        // We want to avoid allocating a SSTableScanner if the range don't overlap the sstable (#5249)
        List<PartitionPositionBounds> positions = sstable.getPositionsForRanges(tokenRanges);
        if (positions.isEmpty())
            return new EmptySSTableScanner(sstable);

        return getScanner(sstable, makeBounds(sstable, tokenRanges).iterator());
    }

    public static ISSTableScanner getScanner(TrieIndexSSTableReader sstable, Iterator<AbstractBounds<PartitionPosition>> rangeIterator)
    {
        return new TrieIndexScanner(sstable, ColumnFilter.all(sstable.metadata()), null, rangeIterator, SSTableReadsListener.NOOP_LISTENER);
    }

    private TrieIndexScanner(TrieIndexSSTableReader sstable,
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

    public static List<AbstractBounds<PartitionPosition>> makeBounds(SSTableReader sstable, Collection<Range<Token>> tokenRanges)
    {
        List<AbstractBounds<PartitionPosition>> boundsList = new ArrayList<>(tokenRanges.size());
        for (Range<Token> range : Range.normalize(tokenRanges))
            addRange(sstable, Range.makeRowRange(range), boundsList);
        return boundsList;
    }

    static List<AbstractBounds<PartitionPosition>> makeBounds(SSTableReader sstable, DataRange dataRange)
    {
        List<AbstractBounds<PartitionPosition>> boundsList = new ArrayList<>(2);
        addRange(sstable, dataRange.keyRange(), boundsList);
        return boundsList;
    }

    static AbstractBounds<PartitionPosition> fullRange(SSTableReader sstable)
    {
        return new Bounds<>(sstable.first, sstable.last);
    }

    private static void addRange(SSTableReader sstable, AbstractBounds<PartitionPosition> requested, List<AbstractBounds<PartitionPosition>> boundsList)
    {
        if (requested instanceof Range && ((Range<?>) requested).isWrapAround())
        {
            if (requested.right.compareTo(sstable.first) >= 0)
            {
                // since we wrap, we must contain the whole sstable prior to stopKey()
                Boundary<PartitionPosition> left = new Boundary<>(sstable.first, true);
                Boundary<PartitionPosition> right;
                right = requested.rightBoundary();
                right = minRight(right, sstable.last, true);
                if (!isEmpty(left, right))
                    boundsList.add(AbstractBounds.bounds(left, right));
            }
            if (requested.left.compareTo(sstable.last) <= 0)
            {
                // since we wrap, we must contain the whole sstable after dataRange.startKey()
                Boundary<PartitionPosition> right = new Boundary<>(sstable.last, true);
                Boundary<PartitionPosition> left;
                left = requested.leftBoundary();
                left = maxLeft(left, sstable.first, true);
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
            left = maxLeft(left, sstable.first, true);
            // apparently isWrapAround() doesn't count Bounds that extend to the limit (min) as wrapping
            right = requested.right.isMinimum() ? new Boundary<>(sstable.last, true)
                                                    : minRight(right, sstable.last, true);
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
                FileUtils.close(dfile);
                if (iterator != null)
                    iterator.close();
            }
        }
        catch (IOException e)
        {
            sstable.markSuspect();
            throw new CorruptSSTableException(e, sstable.getFilename());
        }
    }

    public long getBytesScanned()
    {
        return bytesScanned;
    }

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

    @Override
    public Set<SSTableReader> getBackingSSTables()
    {
        return ImmutableSet.of(sstable);
    }

    public int level()
    {
        return sstable.getSSTableLevel();
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

    private CloseableIterator<UnfilteredRowIterator> createIterator()
    {
        this.listener.onScanningStarted(sstable);
        return new KeyScanningIterator();
    }

    protected class KeyScanningIterator extends AbstractIterator<UnfilteredRowIterator> implements CloseableIterator<UnfilteredRowIterator>
    {
        private DecoratedKey currentKey;
        private RowIndexEntry currentEntry;
        private PartitionIterator iterator;
        private LazilyInitializedUnfilteredRowIterator currentRowIterator;

        protected UnfilteredRowIterator computeNext()
        {
            if (currentRowIterator != null && currentRowIterator.initialized() && !currentRowIterator.isClosed() && currentRowIterator.hasNext())
                throw new IllegalStateException("The UnfilteredRowIterator returned by the last call to next() was initialized: " +
                                                "it should be either exhausted or closed before calling hasNext() or next() again.");

            try
            {
                while (true)
                {
                    if (startScan != -1)
                        bytesScanned += getCurrentPosition() - startScan;

                    if (iterator != null)
                    {
                        currentEntry = iterator.entry();
                        currentKey = iterator.decoratedKey();
                        if (currentEntry != null)
                        {
                            iterator.advance();
                            break;
                        }
                        iterator.close();
                        iterator = null;
                    }

                    // try next range
                    if (!rangeIterator.hasNext())
                        return endOfData();
                    iterator = sstable.coveredKeysIterator(rangeIterator.next());
                }
                startScan = -1;

                /*
                 * For a given partition key, we want to avoid hitting the data
                 * file unless we're explicitly asked to. This is important
                 * for PartitionRangeReadCommand#checkCacheFilter.
                 */
                currentRowIterator = new LazilyInitializedUnfilteredRowIterator(currentKey)
                {
                    // Store currentEntry referency during object instantination as later (during initialize) the
                    // reference may point to a different entry.
                    private final RowIndexEntry rowIndexEntry = currentEntry;

                    protected UnfilteredRowIterator initializeIterator()
                    {
                        try
                        {
                            if (startScan != -1)
                                bytesScanned += getCurrentPosition() - startScan;

                            startScan = rowIndexEntry.position;
                            if (dataRange == null)
                            {
                                return sstable.simpleIterator(dfile, partitionKey(), rowIndexEntry, false);
                            }
                            else
                            {
                                ClusteringIndexFilter filter = dataRange.clusteringIndexFilter(partitionKey());
                                return sstable.iterator(dfile, partitionKey(), rowIndexEntry, filter.getSlices(TrieIndexScanner.this.metadata()), columns, filter.isReversed());
                            }
                        }
                        catch (CorruptSSTableException e)
                        {
                            sstable.markSuspect();
                            throw new CorruptSSTableException(e, sstable.getFilename());
                        }
                    }
                };
                return currentRowIterator;
            }
            catch (CorruptSSTableException | IOException e)
            {
                sstable.markSuspect();
                throw new CorruptSSTableException(e, sstable.getFilename());
            }
        }

        @Override
        public void close()
        {
            if (iterator != null)
                iterator.close();
        }
    }

    @Override
    public String toString()
    {
        return String.format("%s(dfile=%s sstable=%s)", getClass().getSimpleName(), dfile, sstable);
    }

    public static class EmptySSTableScanner extends AbstractUnfilteredPartitionIterator implements ISSTableScanner
    {
        private final SSTableReader sstable;

        public EmptySSTableScanner(SSTableReader sstable)
        {
            this.sstable = sstable;
        }

        public long getFilePointer()
        {
            return 0;
        }

        public long getBytesScanned()
        {
            return 0;
        }

        @Override
        public long getLengthInBytes()
        {
            return 0;
        }

        public long getCompressedLengthInBytes()
        {
            return 0;
        }

        @Override
        public long getCurrentPosition()
        {
            return 0;
        }

        public int level()
        {
            return sstable.getSSTableLevel();
        }

        @Override
        public Set<SSTableReader> getBackingSSTables()
        {
            return Collections.emptySet();
        }

        public TableMetadata metadata()
        {
            return sstable.metadata();
        }

        public void close()
        {

        }

        public boolean hasNext()
        {
            return false;
        }

        public UnfilteredRowIterator next()
        {
            throw new NoSuchElementException();
        }
    }
}
