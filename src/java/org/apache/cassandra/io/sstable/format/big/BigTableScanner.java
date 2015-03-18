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
import java.util.*;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.RateLimiter;

import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.db.columniterator.IColumnIteratorFactory;
import org.apache.cassandra.db.columniterator.LazyColumnIterator;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.AbstractBounds.Boundary;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.dht.AbstractBounds.isEmpty;
import static org.apache.cassandra.dht.AbstractBounds.maxLeft;
import static org.apache.cassandra.dht.AbstractBounds.minRight;

public class BigTableScanner implements ISSTableScanner
{
    protected final RandomAccessReader dfile;
    protected final RandomAccessReader ifile;
    public final SSTableReader sstable;

    private final Iterator<AbstractBounds<RowPosition>> rangeIterator;
    private AbstractBounds<RowPosition> currentRange;

    private final DataRange dataRange;
    private final RowIndexEntry.IndexSerializer rowIndexEntrySerializer;

    protected Iterator<OnDiskAtomIterator> iterator;

    public static ISSTableScanner getScanner(SSTableReader sstable, DataRange dataRange, RateLimiter limiter)
    {
        return new BigTableScanner(sstable, dataRange, limiter);
    }
    public static ISSTableScanner getScanner(SSTableReader sstable, Collection<Range<Token>> tokenRanges, RateLimiter limiter)
    {
        // We want to avoid allocating a SSTableScanner if the range don't overlap the sstable (#5249)
        List<Pair<Long, Long>> positions = sstable.getPositionsForRanges(tokenRanges);
        if (positions.isEmpty())
            return new EmptySSTableScanner(sstable.getFilename());

        return new BigTableScanner(sstable, tokenRanges, limiter);
    }

    /**
     * @param sstable SSTable to scan; must not be null
     * @param dataRange a single range to scan; must not be null
     * @param limiter background i/o RateLimiter; may be null
     */
    private BigTableScanner(SSTableReader sstable, DataRange dataRange, RateLimiter limiter)
    {
        assert sstable != null;

        this.dfile = limiter == null ? sstable.openDataReader() : sstable.openDataReader(limiter);
        this.ifile = sstable.openIndexReader();
        this.sstable = sstable;
        this.dataRange = dataRange;
        this.rowIndexEntrySerializer = sstable.descriptor.version.getSSTableFormat().getIndexSerializer(sstable.metadata);

        List<AbstractBounds<RowPosition>> boundsList = new ArrayList<>(2);
        // we enforce the first/last keys of the sstablereader
        if (dataRange.isWrapAround())
        {
            if (dataRange.stopKey().compareTo(sstable.first) >= 0)
            {
                // since we wrap, we must contain the whole sstable prior to stopKey()
                Boundary<RowPosition> left = new Boundary<RowPosition>(sstable.first, true);
                Boundary<RowPosition> right;
                right = dataRange.keyRange().rightBoundary();
                right = minRight(right, sstable.last, true);
                if (!isEmpty(left, right))
                    boundsList.add(AbstractBounds.bounds(left, right));
            }
            if (dataRange.startKey().compareTo(sstable.last) <= 0)
            {
                // since we wrap, we must contain the whole sstable after dataRange.startKey()
                Boundary<RowPosition> right = new Boundary<RowPosition>(sstable.last, true);
                Boundary<RowPosition> left;
                left = dataRange.keyRange().leftBoundary();
                left = maxLeft(left, sstable.first, true);
                if (!isEmpty(left, right))
                    boundsList.add(AbstractBounds.bounds(left, right));
            }
        }
        else
        {
            assert dataRange.startKey().compareTo(dataRange.stopKey()) <= 0 || dataRange.stopKey().isMinimum();
            Boundary<RowPosition> left, right;
            left = dataRange.keyRange().leftBoundary();
            right = dataRange.keyRange().rightBoundary();
            left = maxLeft(left, sstable.first, true);
            // apparently isWrapAround() doesn't count Bounds that extend to the limit (min) as wrapping
            right = dataRange.stopKey().isMinimum() ? new Boundary<RowPosition>(sstable.last, true)
                                                    : minRight(right, sstable.last, true);
            if (!isEmpty(left, right))
                boundsList.add(AbstractBounds.bounds(left, right));
        }
        this.rangeIterator = boundsList.iterator();
    }

    /**
     * @param sstable SSTable to scan; must not be null
     * @param tokenRanges A set of token ranges to scan
     * @param limiter background i/o RateLimiter; may be null
     */
    private BigTableScanner(SSTableReader sstable, Collection<Range<Token>> tokenRanges, RateLimiter limiter)
    {
        assert sstable != null;

        this.dfile = limiter == null ? sstable.openDataReader() : sstable.openDataReader(limiter);
        this.ifile = sstable.openIndexReader();
        this.sstable = sstable;
        this.dataRange = null;
        this.rowIndexEntrySerializer = sstable.descriptor.version.getSSTableFormat().getIndexSerializer(sstable.metadata);

        List<Range<Token>> normalized = Range.normalize(tokenRanges);
        List<AbstractBounds<RowPosition>> boundsList = new ArrayList<>(normalized.size());
        // we enforce the first/last keys of the sstablereader
        for (Range<Token> range : normalized)
        {
            // cap our ranges by the start/end of the sstable
            RowPosition right = range.right.maxKeyBound();
            if (right.compareTo(sstable.last) > 0)
                right = sstable.last;

            RowPosition left = range.left.maxKeyBound();
            if (left.compareTo(sstable.first) < 0)
            {
                if (sstable.first.compareTo(right) <= 0)
                    boundsList.add(new Bounds<>(sstable.first, right));
            }
            else if (left.compareTo(right) < 0)
                boundsList.add(new Range<>(left, right));
        }

        this.rangeIterator = boundsList.iterator();
    }

    private void seekToCurrentRangeStart()
    {
        long indexPosition = sstable.getIndexScanPosition(currentRange.left);
        ifile.seek(indexPosition);
        try
        {

            while (!ifile.isEOF())
            {
                indexPosition = ifile.getFilePointer();
                DecoratedKey indexDecoratedKey = sstable.partitioner.decorateKey(ByteBufferUtil.readWithShortLength(ifile));
                if (indexDecoratedKey.compareTo(currentRange.left) > 0 || currentRange.contains(indexDecoratedKey))
                {
                    // Found, just read the dataPosition and seek into index and data files
                    long dataPosition = ifile.readLong();
                    ifile.seek(indexPosition);
                    dfile.seek(dataPosition);
                    break;
                }
                else
                {
                    RowIndexEntry.Serializer.skip(ifile);
                }
            }
        }
        catch (IOException e)
        {
            sstable.markSuspect();
            throw new CorruptSSTableException(e, sstable.getFilename());
        }
    }

    public void close() throws IOException
    {
        FileUtils.close(dfile, ifile);
    }

    public long getLengthInBytes()
    {
        return dfile.length();
    }

    public long getCurrentPosition()
    {
        return dfile.getFilePointer();
    }

    public String getBackingFiles()
    {
        return sstable.toString();
    }

    public boolean hasNext()
    {
        if (iterator == null)
            iterator = createIterator();
        return iterator.hasNext();
    }

    public OnDiskAtomIterator next()
    {
        if (iterator == null)
            iterator = createIterator();
        return iterator.next();
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    private Iterator<OnDiskAtomIterator> createIterator()
    {
        return new KeyScanningIterator();
    }

    protected class KeyScanningIterator extends AbstractIterator<OnDiskAtomIterator>
    {
        private DecoratedKey nextKey;
        private RowIndexEntry nextEntry;
        private DecoratedKey currentKey;
        private RowIndexEntry currentEntry;

        protected OnDiskAtomIterator computeNext()
        {
            try
            {
                if (nextEntry == null)
                {
                    do
                    {
                        // we're starting the first range or we just passed the end of the previous range
                        if (!rangeIterator.hasNext())
                            return endOfData();

                        currentRange = rangeIterator.next();
                        seekToCurrentRangeStart();

                        if (ifile.isEOF())
                            return endOfData();

                        currentKey = sstable.partitioner.decorateKey(ByteBufferUtil.readWithShortLength(ifile));
                        currentEntry = rowIndexEntrySerializer.deserialize(ifile, sstable.descriptor.version);
                    } while (!currentRange.contains(currentKey));
                }
                else
                {
                    // we're in the middle of a range
                    currentKey = nextKey;
                    currentEntry = nextEntry;
                }

                if (ifile.isEOF())
                {
                    nextEntry = null;
                    nextKey = null;
                }
                else
                {
                    // we need the position of the start of the next key, regardless of whether it falls in the current range
                    nextKey = sstable.partitioner.decorateKey(ByteBufferUtil.readWithShortLength(ifile));
                    nextEntry = rowIndexEntrySerializer.deserialize(ifile, sstable.descriptor.version);

                    if (!currentRange.contains(nextKey))
                    {
                        nextKey = null;
                        nextEntry = null;
                    }
                }

                if (dataRange == null || dataRange.selectsFullRowFor(currentKey.getKey()))
                {
                    dfile.seek(currentEntry.position + currentEntry.headerOffset());
                    ByteBufferUtil.readWithShortLength(dfile); // key
                    return new SSTableIdentityIterator(sstable, dfile, currentKey);
                }

                return new LazyColumnIterator(currentKey, new IColumnIteratorFactory()
                {
                    public OnDiskAtomIterator create()
                    {
                        return dataRange.columnFilter(currentKey.getKey()).getSSTableColumnIterator(sstable, dfile, currentKey, currentEntry);
                    }
                });

            }
            catch (IOException e)
            {
                sstable.markSuspect();
                throw new CorruptSSTableException(e, sstable.getFilename());
            }
        }
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "(" +
               "dfile=" + dfile +
               " ifile=" + ifile +
               " sstable=" + sstable +
               ")";
    }

    public static class EmptySSTableScanner implements ISSTableScanner
    {
        private final String filename;

        public EmptySSTableScanner(String filename)
        {
            this.filename = filename;
        }

        public long getLengthInBytes()
        {
            return 0;
        }

        public long getCurrentPosition()
        {
            return 0;
        }

        public String getBackingFiles()
        {
            return filename;
        }

        public boolean hasNext()
        {
            return false;
        }

        public OnDiskAtomIterator next()
        {
            return null;
        }

        public void close() throws IOException { }

        public void remove() { }
    }


}
