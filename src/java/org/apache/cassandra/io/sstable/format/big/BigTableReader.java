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
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.io.sstable.format.SSTableReaderBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.SSTableIterator;
import org.apache.cassandra.db.columniterator.SSTableReversedIterator;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReadsListener;
import org.apache.cassandra.io.sstable.format.SSTableReadsListener.SelectionReason;
import org.apache.cassandra.io.sstable.format.SSTableReadsListener.SkippingReason;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * SSTableReaders are open()ed by Keyspace.onStart; after that they are created by SSTableWriter.renameAndOpen.
 * Do not re-call open() on existing SSTable files; use the references kept by ColumnFamilyStore post-start instead.
 */
public class BigTableReader extends SSTableReader
{
    private static final Logger logger = LoggerFactory.getLogger(BigTableReader.class);

    BigTableReader(SSTableReaderBuilder builder)
    {
        super(builder);
    }

    public UnfilteredRowIterator rowIterator(DecoratedKey key,
                                             Slices slices,
                                             ColumnFilter selectedColumns,
                                             boolean reversed,
                                             SSTableReadsListener listener)
    {
        RowIndexEntry rie = getPosition(key, SSTableReader.Operator.EQ, listener);
        return rowIterator(null, key, rie, slices, selectedColumns, reversed);
    }

    @SuppressWarnings("resource")
    public UnfilteredRowIterator rowIterator(FileDataInput file, DecoratedKey key, RowIndexEntry indexEntry, Slices slices, ColumnFilter selectedColumns, boolean reversed)
    {
        if (indexEntry == null)
            return UnfilteredRowIterators.noRowsIterator(metadata(), key, Rows.EMPTY_STATIC_ROW, DeletionTime.LIVE, reversed);
        return reversed
             ? new SSTableReversedIterator(this, file, key, indexEntry, slices, selectedColumns, ifile)
             : new SSTableIterator(this, file, key, indexEntry, slices, selectedColumns, ifile);
    }

    @Override
    public ISSTableScanner partitionIterator(ColumnFilter columns, DataRange dataRange, SSTableReadsListener listener)
    {
        return BigTableScanner.getScanner(this, columns, dataRange, listener);
    }

    /**
     * Direct I/O SSTableScanner over an iterator of bounds.
     *
     * @param boundsIterator the keys to cover
     * @return A Scanner for seeking over the rows of the SSTable.
     */
    public ISSTableScanner getScanner(Iterator<AbstractBounds<PartitionPosition>> boundsIterator)
    {
        return BigTableScanner.getScanner(this, boundsIterator);
    }

    /**
     * Direct I/O SSTableScanner over the full sstable.
     *
     * @return A Scanner for reading the full SSTable.
     */
    public ISSTableScanner getScanner()
    {
        return BigTableScanner.getScanner(this);
    }

    /**
     * Direct I/O SSTableScanner over a defined collection of ranges of tokens.
     *
     * @param ranges the range of keys to cover
     * @return A Scanner for seeking over the rows of the SSTable.
     */
    public ISSTableScanner getScanner(Collection<Range<Token>> ranges)
    {
        if (ranges != null)
            return BigTableScanner.getScanner(this, ranges);
        else
            return getScanner();
    }


    @SuppressWarnings("resource") // caller to close
    @Override
    public UnfilteredRowIterator simpleIterator(FileDataInput dfile, DecoratedKey key, RowIndexEntry position, boolean tombstoneOnly)
    {
        return SSTableIdentityIterator.create(this, dfile, position, key, tombstoneOnly);
    }

    /**
     * @param key The key to apply as the rhs to the given Operator. A 'fake' key is allowed to
     * allow key selection by token bounds but only if op != * EQ
     * @param op The Operator defining matching keys: the nearest key to the target matching the operator wins.
     * @param updateCacheAndStats true if updating stats and cache
     * @return The index entry corresponding to the key, or null if the key is not present
     */
    protected RowIndexEntry getPosition(PartitionPosition key,
                                        Operator op,
                                        boolean updateCacheAndStats,
                                        boolean permitMatchPastLast,
                                        SSTableReadsListener listener)
    {
        // Having no index file is impossible in a normal operation. The only way it might happen is running
        // Scrubber that does not really rely onto this method.
        if (ifile == null)
        {
            return null;
        }

        if (op == Operator.EQ)
        {
            assert key instanceof DecoratedKey; // EQ only make sense if the key is a valid row key
            if (!bf.isPresent((DecoratedKey)key))
            {
                listener.onSSTableSkipped(this, SkippingReason.BLOOM_FILTER);
                Tracing.trace("Bloom filter allows skipping sstable {}", descriptor.id);
                bloomFilterTracker.addTrueNegative();
                return null;
            }
        }

        // next, the key cache (only make sense for valid row key)
        if ((op == Operator.EQ || op == Operator.GE) && (key instanceof DecoratedKey))
        {
            DecoratedKey decoratedKey = (DecoratedKey) key;
            RowIndexEntry cachedPosition = getCachedPosition(decoratedKey, updateCacheAndStats);
            if (cachedPosition != null)
            {
                // we do not need to track "true positive" for Bloom Filter here because it has been already tracked
                // inside getCachedPosition method
                listener.onSSTableSelected(this, cachedPosition, SelectionReason.KEY_CACHE_HIT);
                Tracing.trace("Key cache hit for sstable {}", descriptor.id);
                return cachedPosition;
            }
        }

        // check the smallest and greatest keys in the sstable to see if it can't be present
        boolean skip = false;
        if (key.compareTo(first) < 0)
        {
            if (op == Operator.EQ)
                skip = true;
            else
                key = first;

            op = Operator.EQ;
        }
        else
        {
            int l = last.compareTo(key);
            // l <= 0  => we may be looking past the end of the file; we then narrow our behaviour to:
            //             1) skipping if strictly greater for GE and EQ;
            //             2) skipping if equal and searching GT, and we aren't permitting matching past last
            skip = l <= 0 && (l < 0 || (!permitMatchPastLast && op == Operator.GT));
        }
        if (skip)
        {
            if (op == Operator.EQ && updateCacheAndStats)
                bloomFilterTracker.addFalsePositive();
            listener.onSSTableSkipped(this, SkippingReason.MIN_MAX_KEYS);
            Tracing.trace("Check against min and max keys allows skipping sstable {}", descriptor.id);
            return null;
        }

        int binarySearchResult = indexSummary.binarySearch(key);
        long sampledPosition = getIndexScanPositionFromBinarySearchResult(binarySearchResult, indexSummary);
        int sampledIndex = getIndexSummaryIndexFromBinarySearchResult(binarySearchResult);

        int effectiveInterval = indexSummary.getEffectiveIndexIntervalAfterIndex(sampledIndex);

        // scan the on-disk index, starting at the nearest sampled position.
        // The check against IndexInterval is to be exit the loop in the EQ case when the key looked for is not present
        // (bloom filter false positive). But note that for non-EQ cases, we might need to check the first key of the
        // next index position because the searched key can be greater the last key of the index interval checked if it
        // is lesser than the first key of next interval (and in that case we must return the position of the first key
        // of the next interval).
        int i = 0;
        String path = null;
        try (FileDataInput in = ifile.createReader(sampledPosition))
        {
            path = in.getPath();
            while (!in.isEOF())
            {
                i++;

                ByteBuffer indexKey = ByteBufferUtil.readWithShortLength(in);

                boolean opSatisfied; // did we find an appropriate position for the op requested
                boolean exactMatch; // is the current position an exact match for the key, suitable for caching

                // Compare raw keys if possible for performance, otherwise compare decorated keys.
                if (op == Operator.EQ && i <= effectiveInterval)
                {
                    opSatisfied = exactMatch = indexKey.equals(((DecoratedKey) key).getKey());
                }
                else
                {
                    DecoratedKey indexDecoratedKey = decorateKey(indexKey);
                    int comparison = indexDecoratedKey.compareTo(key);
                    int v = op.apply(comparison);
                    opSatisfied = (v == 0);
                    exactMatch = (comparison == 0);
                    if (v < 0)
                    {
                        if (op == SSTableReader.Operator.EQ && updateCacheAndStats)
                            bloomFilterTracker.addFalsePositive();
                        listener.onSSTableSkipped(this, SkippingReason.PARTITION_INDEX_LOOKUP);
                        Tracing.trace("Partition index lookup allows skipping sstable {}", descriptor.id);
                        return null;
                    }
                }

                if (opSatisfied)
                {
                    // read data position from index entry
                    RowIndexEntry indexEntry = rowIndexEntrySerializer.deserialize(in);
                    if (exactMatch && updateCacheAndStats)
                    {
                        assert key instanceof DecoratedKey; // key can be == to the index key only if it's a true row key
                        DecoratedKey decoratedKey = (DecoratedKey)key;

                        if (logger.isTraceEnabled())
                        {
                            // expensive sanity check!  see CASSANDRA-4687
                            try (FileDataInput fdi = dfile.createReader(indexEntry.position))
                            {
                                DecoratedKey keyInDisk = decorateKey(ByteBufferUtil.readWithShortLength(fdi));
                                if (!keyInDisk.equals(key))
                                    throw new AssertionError(String.format("%s != %s in %s", keyInDisk, key, fdi.getPath()));
                            }
                        }

                        // store exact match for the key
                        cacheKey(decoratedKey, indexEntry);
                    }
                    if (op == Operator.EQ && updateCacheAndStats)
                        bloomFilterTracker.addTruePositive();
                    listener.onSSTableSelected(this, indexEntry, SelectionReason.INDEX_ENTRY_FOUND);
                    Tracing.trace("Partition index with {} entries found for sstable {}", indexEntry.columnsIndexCount(), descriptor.id);
                    return indexEntry;
                }

                RowIndexEntry.Serializer.skip(in, descriptor.version);
            }
        }
        catch (IOException e)
        {
            markSuspect();
            throw new CorruptSSTableException(e, path);
        }

        if (op == SSTableReader.Operator.EQ && updateCacheAndStats)
            bloomFilterTracker.addFalsePositive();
        listener.onSSTableSkipped(this, SkippingReason.INDEX_ENTRY_NOT_FOUND);
        Tracing.trace("Partition index lookup complete (bloom filter false positive) for sstable {}", descriptor.id);
        return null;
    }


}
