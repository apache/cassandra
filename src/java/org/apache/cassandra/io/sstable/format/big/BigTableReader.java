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

import com.google.common.base.Preconditions;

import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.format.AbstractRowIndexEntry;
import org.apache.cassandra.io.sstable.format.IScrubber;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReaderBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.*;
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
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.OutputHandler;

/**
 * SSTableReaders are open()ed by Keyspace.onStart; after that they are created by SSTableWriter.renameAndOpen.
 * Do not re-call open() on existing SSTable files; use the references kept by ColumnFamilyStore post-start instead.
 */
public class BigTableReader extends SSTableReader
{
    private static final Logger logger = LoggerFactory.getLogger(BigTableReader.class);

    private final RowIndexEntry.IndexSerializer rowIndexEntrySerializer;

    BigTableReader(SSTableReaderBuilder builder)
    {
        super(builder);

        this.rowIndexEntrySerializer = new RowIndexEntry.Serializer(descriptor.version, header);
    }

    public UnfilteredRowIterator rowIterator(DecoratedKey key,
                                             Slices slices,
                                             ColumnFilter selectedColumns,
                                             boolean reversed,
                                             SSTableReadsListener listener)
    {
        RowIndexEntry rie = getRowIndexEntry(key, SSTableReader.Operator.EQ, true, false, listener);
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

    /**
     * Retrieves the position while updating the key cache and the stats.
     * @param key The key to apply as the rhs to the given Operator. A 'fake' key is allowed to
     * allow key selection by token bounds but only if op != * EQ
     * @param op The Operator defining matching keys: the nearest key to the target matching the operator wins.
     */
    public final RowIndexEntry getRowIndexEntry(PartitionPosition key, Operator op)
    {
        return getRowIndexEntry(key, op, true, false, SSTableReadsListener.NOOP_LISTENER);
    }

    /**
     * @param key The key to apply as the rhs to the given Operator. A 'fake' key is allowed to
     * allow key selection by token bounds but only if op != * EQ
     * @param op The Operator defining matching keys: the nearest key to the target matching the operator wins.
     * @param updateCacheAndStats true if updating stats and cache
     * @return The index entry corresponding to the key, or null if the key is not present
     */
    @Override
    protected RowIndexEntry getRowIndexEntry(PartitionPosition key,
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
            AbstractRowIndexEntry cachedPosition = getCachedPosition(decoratedKey, updateCacheAndStats);
            if (cachedPosition != null && cachedPosition.getSSTableFormat().getType() == SSTableFormat.Type.BIG)
            {
                // we do not need to track "true positive" for Bloom Filter here because it has been already tracked
                // inside getCachedPosition method
                listener.onSSTableSelected(this, SelectionReason.KEY_CACHE_HIT);
                Tracing.trace("Key cache hit for sstable {}", descriptor.id);
                return (RowIndexEntry) cachedPosition;
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
                    listener.onSSTableSelected(this, SelectionReason.INDEX_ENTRY_FOUND);
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

    /**
     * @param key                 The key to apply as the rhs to the given Operator. A 'fake' key is allowed to
     *                            allow key selection by token bounds but only if op != * EQ
     * @param op                  The Operator defining matching keys: the nearest key to the target matching the operator wins.
     * @param updateCacheAndStats true if updating stats and cache
     * @return The index entry corresponding to the key, or null if the key is not present
     */
    @Override
    protected long getPosition(PartitionPosition key,
                               Operator op,
                               boolean updateCacheAndStats,
                               boolean permitMatchPastLast,
                               SSTableReadsListener listener)
    {
        RowIndexEntry rowIndexEntry = getRowIndexEntry(key, op, updateCacheAndStats, permitMatchPastLast, listener);
        return rowIndexEntry != null ? rowIndexEntry.position : -1;
    }

    @Override
    public DecoratedKey keyAt(long indexPosition) throws IOException
    {
        DecoratedKey key;
        try (FileDataInput in = ifile.createReader(indexPosition))
        {
            if (in.isEOF())
                return null;

            key = decorateKey(ByteBufferUtil.readWithShortLength(in));

            // hint read path about key location if caching is enabled
            // this saves index summary lookup and index file iteration which whould be pretty costly
            // especially in presence of promoted column indexes
            if (isKeyCacheEnabled())
                cacheKey(key, rowIndexEntrySerializer.deserialize(in));
        }

        return key;
    }

    @Override
    public RowIndexEntry deserializeKeyCacheValue(DataInputPlus input) throws IOException
    {
        return rowIndexEntrySerializer.deserializeForCache(input);
    }

    @Override
    public ClusteringPrefix<?> getLowerBoundPrefixFromCache(DecoratedKey partitionKey, ClusteringIndexFilter filter)
    {
        RowIndexEntry rowIndexEntry = (RowIndexEntry) getCachedPosition(partitionKey, false);
        if (rowIndexEntry == null || !rowIndexEntry.indexOnHeap())
            return null;

        try (RowIndexEntry.IndexInfoRetriever onHeapRetriever = rowIndexEntry.openWithIndex(null))
        {
            IndexInfo column = onHeapRetriever.columnsIndex(filter.isReversed() ? rowIndexEntry.columnsIndexCount() - 1 : 0);
            ClusteringPrefix<?> lowerBoundPrefix = filter.isReversed() ? column.lastName : column.firstName;
            assert lowerBoundPrefix.getRawValues().length <= metadata().comparator.size() :
            String.format("Unexpected number of clustering values %d, expected %d or fewer for %s",
                          lowerBoundPrefix.getRawValues().length,
                          metadata().comparator.size(),
                          getFilename());
            return lowerBoundPrefix;
        }
        catch (IOException e)
        {
            throw new AssertionError("Failed to deserialize row index entry", e);
        }

    }

    @Override
    public IScrubber getScrubber(LifecycleTransaction transaction, OutputHandler outputHandler, IScrubber.Options options)
    {
        ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(metadata());
        Preconditions.checkArgument(transaction.originals().contains(this));
        return new BigTableScrubber(cfs, transaction, outputHandler, options);
    }

    @Override
    public IVerifier getVerifier(OutputHandler outputHandler, boolean isOffline, IVerifier.Options options)
    {
        ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(metadata());
        return new BigTableVerifier(cfs, this, outputHandler, isOffline, options);
    }

}