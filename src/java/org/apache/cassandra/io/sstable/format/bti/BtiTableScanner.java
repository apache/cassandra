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
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.io.sstable.format.SSTableScanner;
import org.apache.cassandra.io.util.FileUtils;

public class BtiTableScanner extends SSTableScanner<BtiTableReader, TrieIndexEntry, BtiTableScanner.BtiScanningIterator>
{
    // Full scan of the sstables
    public static BtiTableScanner getScanner(BtiTableReader sstable)
    {
        return getScanner(sstable, Iterators.singletonIterator(fullRange(sstable)));
    }

    public static BtiTableScanner getScanner(BtiTableReader sstable,
                                             ColumnFilter columns,
                                             DataRange dataRange,
                                             SSTableReadsListener listener)
    {
        return new BtiTableScanner(sstable, columns, dataRange, makeBounds(sstable, dataRange).iterator(), listener);
    }

    public static BtiTableScanner getScanner(BtiTableReader sstable, Collection<Range<Token>> tokenRanges)
    {
        return getScanner(sstable, makeBounds(sstable, tokenRanges).iterator());
    }

    public static BtiTableScanner getScanner(BtiTableReader sstable, Iterator<AbstractBounds<PartitionPosition>> rangeIterator)
    {
        return new BtiTableScanner(sstable, ColumnFilter.all(sstable.metadata()), null, rangeIterator, SSTableReadsListener.NOOP_LISTENER);
    }

    private BtiTableScanner(BtiTableReader sstable,
                            ColumnFilter columns,
                            DataRange dataRange,
                            Iterator<AbstractBounds<PartitionPosition>> rangeIterator,
                            SSTableReadsListener listener)
    {
        super(sstable, columns, dataRange, rangeIterator, listener);
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
