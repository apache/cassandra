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
package org.apache.cassandra.io.sstable.format.bti;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

import org.apache.cassandra.config.Config.DiskAccessMode;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.io.sstable.format.SSTableScanner;
import org.apache.cassandra.io.util.FileUtils;

public class BtiTableScanner extends SSTableScanner<BtiTableReader, TrieIndexEntry, BtiTableScanner.BtiScanningIterator>
{
    public static BtiTableScanner getScanner(BtiTableReader sstable,
                                             ColumnFilter columns,
                                             DataRange dataRange,
                                             SSTableReadsListener listener)
    {
        return new BtiTableScanner(sstable, columns, dataRange, makeBounds(sstable, dataRange).iterator(), listener,
                                   coversFullRange(sstable, dataRange), null);
    }

    /**
     * An index-driven scanner over exactly {@code bounds}, returning whole partitions -- the same result
     * {@code SSTableSimpleScanner} produces for the same bounds, but reached by walking Partitions.db instead of
     * reading Data.db linearly. A null {@code DataRange} is what the rest of this class already understands as
     * "no clustering restriction", so the partitions come back unfiltered.
     * <p>
     * This exists for sstables that cannot be read linearly at all: one holding partitions its index does not
     * describe, which is what {@code StatsMetadata#hasUnindexedRegions} marks. See
     * {@code SSTableReader.indexDrivenScanner}.
     * <p>
     * The scanner is restricted to {@code bounds}, so it is never a full-range scanner: a caller that reacts to
     * {@link ISSTableScanner#isFullRange()} by reading Data.db itself -- as cursor compaction does -- must not be
     * told otherwise, whether or not the bounds happen to span the sstable.
     */
    public static ISSTableScanner getScanner(BtiTableReader sstable,
                                             Iterator<AbstractBounds<PartitionPosition>> bounds,
                                             DiskAccessMode diskAccessMode)
    {
        return new BtiTableScanner(sstable, ColumnFilter.all(sstable.metadata()), null,
                                   makeBounds(sstable, bounds).iterator(), SSTableReadsListener.NOOP_LISTENER,
                                   false, diskAccessMode);
    }

    private BtiTableScanner(BtiTableReader sstable,
                            ColumnFilter columns,
                            DataRange dataRange,
                            Iterator<AbstractBounds<PartitionPosition>> rangeIterator,
                            SSTableReadsListener listener,
                            boolean fullRange,
                            DiskAccessMode diskAccessMode)
    {
        super(sstable, columns, dataRange, rangeIterator, listener, fullRange, diskAccessMode);
    }

    protected void doClose() throws IOException
    {
        FileUtils.close(dfile, iterator);
    }

    @Override
    protected BtiScanningIterator doCreateIterator()
    {
        return new BtiScanningIterator();
    }

    protected class BtiScanningIterator extends SSTableScanner<BtiTableReader, TrieIndexEntry, BtiTableScanner.BtiScanningIterator>.BaseKeyScanningIterator implements Closeable
    {
        private PartitionIterator iterator;

        @Override
        protected boolean prepareToIterateRow() throws IOException
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
                        return true;
                    }
                    iterator.close();
                    iterator = null;
                }

                // try next range
                if (!rangeIterator.hasNext())
                    return false;
                iterator = sstable.coveredKeysIterator(rangeIterator.next());
            }
        }

        @Override
        protected UnfilteredRowIterator getRowIterator(TrieIndexEntry indexEntry, DecoratedKey key)
        {
            if (dataRange == null)
            {
                return sstable.simpleIterator(dfile, key, indexEntry.position, false);
            }
            else
            {
                ClusteringIndexFilter filter = dataRange.clusteringIndexFilter(key);
                return sstable.rowIterator(dfile, key, indexEntry, filter.getSlices(BtiTableScanner.this.metadata()), columns, filter.isReversed());
            }
        }

        @Override
        public void close()
        {
            super.close();  // can't throw
            if (iterator != null)
                iterator.close();
        }
    }
}
