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
import java.util.Iterator;

import javax.annotation.Nullable;

import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.AbstractBounds;
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

    public static ISSTableScanner getScanner(BigTableReader sstable,
                                             ColumnFilter columns,
                                             DataRange dataRange,
                                             SSTableReadsListener listener)
    {
        return new BigTableScanner(sstable, columns, dataRange, makeBounds(sstable, dataRange).iterator(), listener);
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

    // Helper method to seek to the index for the given position or range and optionally in the data file
    private long seekAndProcess(@Nullable PartitionPosition position,
                                @Nullable AbstractBounds<PartitionPosition> range,
                                boolean seekDataFile) throws CorruptSSTableException
    {
        long indexPosition = sstable.getIndexScanPosition(position);
        ifile.seek(indexPosition);
        try
        {

            while (!ifile.isEOF())
            {
                indexPosition = ifile.getFilePointer();
                DecoratedKey indexDecoratedKey = sstable.decorateKey(ByteBufferUtil.readWithShortLength(ifile));

                // Whether the position or range is present in the SSTable.
                boolean isFound = (range == null && indexDecoratedKey.compareTo(position) > 0)
                                  || (range != null && (indexDecoratedKey.compareTo(range.left) > 0 || range.contains(indexDecoratedKey)));
                if (isFound)
                {
                    // Found, just read the dataPosition and seek into index and data files
                    long dataPosition = RowIndexEntry.Serializer.readPosition(ifile);
                    // seek the index file position as we will presumably seek further when going to the next position.
                    ifile.seek(indexPosition);
                    if (seekDataFile)
                    {
                        dfile.seek(dataPosition);
                    }
                    return dataPosition;
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
        // If for whatever reason we don't find position in file, just return 0
        return 0L;
    }

    /**
     * Seeks to the start of the current range and updates both the index and data file positions.
     */
    private void seekToCurrentRangeStart() throws CorruptSSTableException
    {
        seekAndProcess(currentRange.left, currentRange, true);
    }

    /**
     * Gets the position in the data file, but does not seek to it. This does seek the index to find the data position
     * but does not actually seek the data file.
     * @param position position to find in data file.
     * @return offset in data file where position exists.
     * @throws CorruptSSTableException if SSTable was malformed
     */
    public long getDataPosition(PartitionPosition position) throws CorruptSSTableException
    {
        return seekAndProcess(position, null, false);
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
