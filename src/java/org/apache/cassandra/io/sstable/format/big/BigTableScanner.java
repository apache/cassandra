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
import java.util.Collection;
import java.util.Iterator;

import com.google.common.collect.Iterators;

import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.io.sstable.format.SSTableScanner;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.ByteBufferUtil;

public class BigTableScanner extends SSTableScanner<BigTableReader, RowIndexEntry, BigTableScanner.BigScanningIterator>
{
    protected final RandomAccessReader ifile;

    private AbstractBounds<PartitionPosition> currentRange;

    private final RowIndexEntry.IndexSerializer rowIndexEntrySerializer;

    // Full scan of the sstables
    public static ISSTableScanner getScanner(BigTableReader sstable)
    {
        return getScanner(sstable, Iterators.singletonIterator(fullRange(sstable)));
    }

    public static ISSTableScanner getScanner(BigTableReader sstable,
                                             ColumnFilter columns,
                                             DataRange dataRange,
                                             SSTableReadsListener listener)
    {
        return new BigTableScanner(sstable, columns, dataRange, makeBounds(sstable, dataRange).iterator(), listener);
    }

    public static ISSTableScanner getScanner(BigTableReader sstable, Collection<Range<Token>> tokenRanges)
    {
        return getScanner(sstable, makeBounds(sstable, tokenRanges).iterator());
    }

    public static ISSTableScanner getScanner(BigTableReader sstable, Iterator<AbstractBounds<PartitionPosition>> rangeIterator)
    {
        return new BigTableScanner(sstable, ColumnFilter.all(sstable.metadata()), null, rangeIterator, SSTableReadsListener.NOOP_LISTENER);
    }

    private BigTableScanner(BigTableReader sstable,
                            ColumnFilter columns,
                            DataRange dataRange,
                            Iterator<AbstractBounds<PartitionPosition>> rangeIterator,
                            SSTableReadsListener listener)
    {
        super(sstable, columns, dataRange, rangeIterator, listener);
        this.ifile = sstable.openIndexReader();
        this.rowIndexEntrySerializer = new RowIndexEntry.Serializer(sstable.descriptor.version, sstable.header, sstable.owner().map(SSTable.Owner::getMetrics).orElse(null));
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
                DecoratedKey indexDecoratedKey = sstable.decorateKey(ByteBufferUtil.readWithShortLength(ifile));
                if (indexDecoratedKey.compareTo(currentRange.left) > 0 || currentRange.contains(indexDecoratedKey))
                {
                    // Found, just read the dataPosition and seek into index and data files
                    long dataPosition = RowIndexEntry.Serializer.readPosition(ifile);
                    ifile.seek(indexPosition);
                    dfile.seek(dataPosition);
                    break;
                }
                else
                {
                    RowIndexEntry.Serializer.skip(ifile, sstable.descriptor.version);
                }
            }
        }
        catch (IOException e)
        {
            sstable.markSuspect();
            throw new CorruptSSTableException(e, sstable.getFilename());
        }
    }

    protected void doClose() throws IOException
    {
        FileUtils.close(dfile, ifile);
    }

    protected BigScanningIterator doCreateIterator()
    {
        return new BigScanningIterator();
    }

    protected class BigScanningIterator extends SSTableScanner<BigTableReader, RowIndexEntry, BigTableScanner.BigScanningIterator>.BaseKeyScanningIterator
    {
        private DecoratedKey nextKey;
        private RowIndexEntry nextEntry;

        protected boolean prepareToIterateRow() throws IOException
        {
            if (nextEntry == null)
            {
                do
                {
                    if (startScan != -1)
                        bytesScanned += dfile.getFilePointer() - startScan;

                    // we're starting the first range or we just passed the end of the previous range
                    if (!rangeIterator.hasNext())
                        return false;

                    currentRange = rangeIterator.next();
                    seekToCurrentRangeStart();
                    startScan = dfile.getFilePointer();

                    if (ifile.isEOF())
                        return false;

                    currentKey = sstable.decorateKey(ByteBufferUtil.readWithShortLength(ifile));
                    currentEntry = rowIndexEntrySerializer.deserialize(ifile);
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
                nextKey = sstable.decorateKey(ByteBufferUtil.readWithShortLength(ifile));
                nextEntry = rowIndexEntrySerializer.deserialize(ifile);

                if (!currentRange.contains(nextKey))
                {
                    nextKey = null;
                    nextEntry = null;
                }
            }
            return true;
        }

        protected UnfilteredRowIterator getRowIterator(RowIndexEntry rowIndexEntry, DecoratedKey key) throws IOException
        {
            if (dataRange == null)
            {
                dfile.seek(rowIndexEntry.position);
                startScan = dfile.getFilePointer();
                ByteBufferUtil.skipShortLength(dfile); // key
                return SSTableIdentityIterator.create(sstable, dfile, key);
            }
            else
            {
                startScan = dfile.getFilePointer();
            }

            ClusteringIndexFilter filter = dataRange.clusteringIndexFilter(key);
            return sstable.rowIterator(dfile, key, rowIndexEntry, filter.getSlices(BigTableScanner.this.metadata()), columns, filter.isReversed());
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
