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
package org.apache.cassandra.io.sstable;

import java.io.IOException;
import java.util.Iterator;

import com.google.common.util.concurrent.RateLimiter;

import com.google.common.collect.AbstractIterator;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.db.columniterator.IColumnIteratorFactory;
import org.apache.cassandra.db.columniterator.LazyColumnIterator;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.db.compaction.ICompactionScanner;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.ByteBufferUtil;

public class SSTableScanner implements ICompactionScanner
{
    protected final RandomAccessReader dfile;
    protected final RandomAccessReader ifile;
    public final SSTableReader sstable;
    protected Iterator<OnDiskAtomIterator> iterator;
    private final QueryFilter filter;
    private long stopAt;

    /**
     * @param sstable SSTable to scan; must not be null
     * @param filter filter to use when scanning the columns; may be null
     * @param limiter background i/o RateLimiter; may be null
     */
    SSTableScanner(SSTableReader sstable, QueryFilter filter, RateLimiter limiter)
    {
        assert sstable != null;

        this.dfile = limiter == null ? sstable.openDataReader() : sstable.openDataReader(limiter);
        this.ifile = sstable.openIndexReader();
        this.sstable = sstable;
        this.filter = filter;
        stopAt = dfile.length();
    }

    public SSTableScanner(SSTableReader sstable, QueryFilter filter, RowPosition startWith, RateLimiter limiter)
    {
        this(sstable, filter, limiter);

        long indexPosition = sstable.getIndexScanPosition(startWith);
        // -1 means the key is before everything in the sstable. So just start from the beginning.
        if (indexPosition == -1)
            indexPosition = 0;
        ifile.seek(indexPosition);

        try
        {
            while (!ifile.isEOF())
            {
                indexPosition = ifile.getFilePointer();
                DecoratedKey indexDecoratedKey = sstable.partitioner.decorateKey(ByteBufferUtil.readWithShortLength(ifile));
                int comparison = indexDecoratedKey.compareTo(startWith);
                if (comparison >= 0)
                {
                    // Found, just read the dataPosition and seek into index and data files
                    long dataPosition = ifile.readLong();
                    ifile.seek(indexPosition);
                    dfile.seek(dataPosition);
                    break;
                }
                else
                {
                    RowIndexEntry.serializer.skip(ifile);
                }
            }
        }
        catch (IOException e)
        {
            sstable.markSuspect();
            throw new CorruptSSTableException(e, sstable.getFilename());
        }

    }

    public SSTableScanner(SSTableReader sstable, QueryFilter filter, Range<Token> range, RateLimiter limiter)
    {
        this(sstable, filter, range.toRowBounds().left, limiter);

        if (range.isWrapAround())
        {
            stopAt = dfile.length();
        }
        else
        {
            RowIndexEntry position = sstable.getPosition(range.toRowBounds().right, SSTableReader.Operator.GT);
            stopAt = position == null ? dfile.length() : position.position;
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
                if (ifile.isEOF() && nextKey == null)
                    return endOfData();

                if (currentKey == null)
                {
                    currentKey = sstable.partitioner.decorateKey(ByteBufferUtil.readWithShortLength(ifile));
                    currentEntry = RowIndexEntry.serializer.deserialize(ifile, sstable.descriptor.version);
                }
                else
                {
                    currentKey = nextKey;
                    currentEntry = nextEntry;
                }

                assert currentEntry.position <= stopAt;
                if (currentEntry.position == stopAt)
                    return endOfData();

                if (ifile.isEOF())
                {
                    nextKey = null;
                    nextEntry = null;
                }
                else
                {
                    nextKey = sstable.partitioner.decorateKey(ByteBufferUtil.readWithShortLength(ifile));
                    nextEntry = RowIndexEntry.serializer.deserialize(ifile, sstable.descriptor.version);
                }

                assert !dfile.isEOF();

                if (filter == null)
                {
                    dfile.seek(currentEntry.position);
                    ByteBufferUtil.readWithShortLength(dfile); // key
                    if (sstable.descriptor.version.hasRowSizeAndColumnCount)
                        dfile.readLong();
                    long dataSize = (nextEntry == null ? dfile.length() : nextEntry.position) - dfile.getFilePointer();
                    return new SSTableIdentityIterator(sstable, dfile, currentKey, dataSize);
                }

                return new LazyColumnIterator(currentKey, new IColumnIteratorFactory()
                {
                    public OnDiskAtomIterator create()
                    {
                        return filter.getSSTableColumnIterator(sstable, dfile, currentKey, currentEntry);
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
}
