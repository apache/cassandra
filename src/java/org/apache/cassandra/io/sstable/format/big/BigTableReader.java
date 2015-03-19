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

import com.google.common.util.concurrent.RateLimiter;
import org.apache.cassandra.cache.KeyCacheKey;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * SSTableReaders are open()ed by Keyspace.onStart; after that they are created by SSTableWriter.renameAndOpen.
 * Do not re-call open() on existing SSTable files; use the references kept by ColumnFamilyStore post-start instead.
 */
public class BigTableReader extends SSTableReader
{
    private static final Logger logger = LoggerFactory.getLogger(BigTableReader.class);

    BigTableReader(Descriptor desc, Set<Component> components, CFMetaData metadata, IPartitioner partitioner, Long maxDataAge, StatsMetadata sstableMetadata, OpenReason openReason)
    {
        super(desc, components, metadata, partitioner, maxDataAge, sstableMetadata, openReason);
    }

    public OnDiskAtomIterator iterator(DecoratedKey key, SortedSet<CellName> columns)
    {
        return new SSTableNamesIterator(this, key, columns);
    }

    public OnDiskAtomIterator iterator(FileDataInput input, DecoratedKey key, SortedSet<CellName> columns, RowIndexEntry indexEntry )
    {
        return new SSTableNamesIterator(this, input, key, columns, indexEntry);
    }

    public OnDiskAtomIterator iterator(DecoratedKey key, ColumnSlice[] slices, boolean reverse)
    {
        return new SSTableSliceIterator(this, key, slices, reverse);
    }

    public OnDiskAtomIterator iterator(FileDataInput input, DecoratedKey key, ColumnSlice[] slices, boolean reverse, RowIndexEntry indexEntry)
    {
        return new SSTableSliceIterator(this, input, key, slices, reverse, indexEntry);
    }
    /**
     *
     * @param dataRange filter to use when reading the columns
     * @return A Scanner for seeking over the rows of the SSTable.
     */
    public ISSTableScanner getScanner(DataRange dataRange, RateLimiter limiter)
    {
        return BigTableScanner.getScanner(this, dataRange, limiter);
    }


    /**
     * Direct I/O SSTableScanner over a defined collection of ranges of tokens.
     *
     * @param ranges the range of keys to cover
     * @return A Scanner for seeking over the rows of the SSTable.
     */
    public ISSTableScanner getScanner(Collection<Range<Token>> ranges, RateLimiter limiter)
    {
        return BigTableScanner.getScanner(this, ranges, limiter);
    }


    /**
     * @param key The key to apply as the rhs to the given Operator. A 'fake' key is allowed to
     * allow key selection by token bounds but only if op != * EQ
     * @param op The Operator defining matching keys: the nearest key to the target matching the operator wins.
     * @param updateCacheAndStats true if updating stats and cache
     * @return The index entry corresponding to the key, or null if the key is not present
     */
    protected RowIndexEntry getPosition(RowPosition key, Operator op, boolean updateCacheAndStats, boolean permitMatchPastLast)
    {
        if (op == Operator.EQ)
        {
            assert key instanceof DecoratedKey; // EQ only make sense if the key is a valid row key
            if (!bf.isPresent((DecoratedKey)key))
            {
                Tracing.trace("Bloom filter allows skipping sstable {}", descriptor.generation);
                return null;
            }
        }

        // next, the key cache (only make sense for valid row key)
        if ((op == Operator.EQ || op == Operator.GE) && (key instanceof DecoratedKey))
        {
            DecoratedKey decoratedKey = (DecoratedKey)key;
            KeyCacheKey cacheKey = new KeyCacheKey(metadata.cfId, descriptor, decoratedKey.getKey());
            RowIndexEntry cachedPosition = getCachedPosition(cacheKey, updateCacheAndStats);
            if (cachedPosition != null)
            {
                Tracing.trace("Key cache hit for sstable {}", descriptor.generation);
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
            Tracing.trace("Check against min and max keys allows skipping sstable {}", descriptor.generation);
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
        Iterator<FileDataInput> segments = ifile.iterator(sampledPosition);
        while (segments.hasNext())
        {
            FileDataInput in = segments.next();
            try
            {
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
                        DecoratedKey indexDecoratedKey = partitioner.decorateKey(indexKey);
                        int comparison = indexDecoratedKey.compareTo(key);
                        int v = op.apply(comparison);
                        opSatisfied = (v == 0);
                        exactMatch = (comparison == 0);
                        if (v < 0)
                        {
                            Tracing.trace("Partition index lookup allows skipping sstable {}", descriptor.generation);
                            return null;
                        }
                    }

                    if (opSatisfied)
                    {
                        // read data position from index entry
                        RowIndexEntry indexEntry = rowIndexEntrySerializer.deserialize(in, descriptor.version);
                        if (exactMatch && updateCacheAndStats)
                        {
                            assert key instanceof DecoratedKey; // key can be == to the index key only if it's a true row key
                            DecoratedKey decoratedKey = (DecoratedKey)key;

                            if (logger.isTraceEnabled())
                            {
                                // expensive sanity check!  see CASSANDRA-4687
                                FileDataInput fdi = dfile.getSegment(indexEntry.position);
                                DecoratedKey keyInDisk = partitioner.decorateKey(ByteBufferUtil.readWithShortLength(fdi));
                                if (!keyInDisk.equals(key))
                                    throw new AssertionError(String.format("%s != %s in %s", keyInDisk, key, fdi.getPath()));
                                fdi.close();
                            }

                            // store exact match for the key
                            cacheKey(decoratedKey, indexEntry);
                        }
                        if (op == Operator.EQ && updateCacheAndStats)
                            bloomFilterTracker.addTruePositive();
                        Tracing.trace("Partition index with {} entries found for sstable {}", indexEntry.columnsIndex().size(), descriptor.generation);
                        return indexEntry;
                    }

                    RowIndexEntry.Serializer.skip(in);
                }
            }
            catch (IOException e)
            {
                markSuspect();
                throw new CorruptSSTableException(e, in.getPath());
            }
            finally
            {
                FileUtils.closeQuietly(in);
            }
        }

        if (op == SSTableReader.Operator.EQ && updateCacheAndStats)
            bloomFilterTracker.addFalsePositive();
        Tracing.trace("Partition index lookup complete (bloom filter false positive) for sstable {}", descriptor.generation);
        return null;
    }


}
