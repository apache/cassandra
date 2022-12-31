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

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.Downsampling;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.IVerifier;
import org.apache.cassandra.io.sstable.format.IScrubber;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReadsListener;
import org.apache.cassandra.io.sstable.format.SSTableReadsListener.SelectionReason;
import org.apache.cassandra.io.sstable.format.SSTableReadsListener.SkippingReason;
import org.apache.cassandra.io.sstable.format.trieindex.PartitionIndex.IndexPosIterator;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.IFilter;
import org.apache.cassandra.utils.OutputHandler;

import static org.apache.cassandra.io.sstable.format.SSTableReader.Operator.EQ;
import static org.apache.cassandra.io.sstable.format.SSTableReader.Operator.GE;
import static org.apache.cassandra.io.sstable.format.SSTableReader.Operator.GT;
import static org.apache.cassandra.utils.concurrent.SharedCloseable.sharedCopyOrNull;

/**
 * SSTableReaders are open()ed by Keyspace.onStart; after that they are created by SSTableWriter.renameAndOpen.
 * Do not re-call open() on existing SSTable files; use the references kept by ColumnFamilyStore post-start instead.
 */
public class BtiTableReader extends SSTableReader
{
    private static final Logger logger = LoggerFactory.getLogger(BtiTableReader.class);

    private final FileHandle rowIndexFile;
    private final PartitionIndex partitionIndex;

    @VisibleForTesting
    public static final double fpChanceTolerance = Double.parseDouble(System.getProperty(Config.PROPERTY_PREFIX + "bloom_filter_fp_chance_tolerance", "0.000001"));

    public BtiTableReader(BtiTableReaderBuilder builder)
    {
        super(builder);
        this.rowIndexFile = builder.getRowIndexFile();
        this.partitionIndex = builder.getPartitionIndex();
    }

    private BtiTableReaderBuilder unbuild()
    {
        return super.unbuild(new BtiTableReaderBuilder(descriptor))
                    .setRowIndexFile(rowIndexFile)
                    .setPartitionIndex(partitionIndex);
    }

    /**
     * Clone this reader with the new start, open reason, bloom filter, and set the clone as replacement.
     *
     * @param first      new start for the replacement
     * @param openReason the {@code OpenReason} for the replacement.
     * @param bf         Bloom filter for the replacement
     * @return the cloned reader. That reader is set as a replacement by the method.
     */
    private BtiTableReader cloneInternal(DecoratedKey first, OpenReason openReason, IFilter bf)
    {
        return unbuild().setOpenReason(openReason)
                        .setFilter(bf)
                        .setFirst(first)
                        .setDataFile(sharedCopyOrNull(dfile))
                        .setRowIndexFile(sharedCopyOrNull(rowIndexFile))
                        .setPartitionIndex(sharedCopyOrNull(partitionIndex))
                        .build(true, true);
    }

    @Override
    protected List<AutoCloseable> setupInstance(boolean trackHotness)
    {
        ArrayList<AutoCloseable> closeables = Lists.newArrayList(bf, rowIndexFile, partitionIndex);
        closeables.addAll(super.setupInstance(trackHotness));
        return closeables;
    }

    protected boolean filterFirst()
    {
        return openReason == OpenReason.MOVED_START;
    }

    protected boolean filterLast()
    {
        return false;
    }

    public long estimatedKeys()
    {
        return partitionIndex == null ? 0 : partitionIndex.size();
    }

    @Override
    protected TrieIndexEntry getRowIndexEntry(PartitionPosition key, Operator op, boolean updateCacheAndStats, boolean permitMatchPastLast, SSTableReadsListener listener)
    {
        assert !permitMatchPastLast;

        PartitionPosition searchKey;
        Operator searchOp;

        if (op == EQ)
            return getExactPosition((DecoratedKey) key, listener, updateCacheAndStats);

        if (op == GT || op == GE)
        {
            if (filterLast() && last.compareTo(key) < 0)
                return null;
            boolean filteredLeft = (filterFirst() && first.compareTo(key) > 0);
            searchKey = filteredLeft ? first : key;
            searchOp = filteredLeft ? GE : op;

            try (PartitionIndex.Reader reader = partitionIndex.openReader())
            {
                return reader.ceiling(searchKey, (pos, assumeNoMatch, compareKey) -> retrieveEntryIfAcceptable(searchOp, compareKey, pos, assumeNoMatch));
            }
            catch (IOException e)
            {
                markSuspect();
                throw new CorruptSSTableException(e, rowIndexFile.path());
            }
        }

        throw new UnsupportedOperationException("Unsupported op: " + op);
    }

    /**
     * Called by getPosition above (via Reader.ceiling/floor) to check if the position satisfies the full key constraint.
     * This is called once if there is a prefix match (which can be in any relationship with the sought key, thus
     * assumeNoMatch: false), and if it returns null it is called again for the closest greater position
     * (with assumeNoMatch: true).
     * Returns the index entry at this position, or null if the search op rejects it.
     */
    private TrieIndexEntry retrieveEntryIfAcceptable(Operator searchOp, PartitionPosition searchKey, long pos, boolean assumeNoMatch) throws IOException
    {
        if (pos >= 0)
        {
            try (FileDataInput in = rowIndexFile.createReader(pos))
            {
                if (assumeNoMatch)
                    ByteBufferUtil.skipShortLength(in);
                else
                {
                    ByteBuffer indexKey = ByteBufferUtil.readWithShortLength(in);
                    DecoratedKey decorated = decorateKey(indexKey);
                    if (searchOp.apply(decorated.compareTo(searchKey)) != 0)
                        return null;
                }
                return TrieIndexEntry.deserialize(in, in.getFilePointer());
            }
        }
        else
        {
            pos = ~pos;
            if (!assumeNoMatch)
            {
                try (FileDataInput in = dfile.createReader(pos))
                {
                    ByteBuffer indexKey = ByteBufferUtil.readWithShortLength(in);
                    DecoratedKey decorated = decorateKey(indexKey);
                    if (searchOp.apply(decorated.compareTo(searchKey)) != 0)
                        return null;
                }
            }
            return new TrieIndexEntry(pos);
        }
    }

    @Override
    public DecoratedKey keyAtPositionFromSecondaryIndex(long keyPositionFromSecondaryIndex) throws IOException
    {
        try (RandomAccessReader reader = openDataReader())
        {
            reader.seek(keyPositionFromSecondaryIndex);
            if (reader.isEOF())
                return null;
            return decorateKey(ByteBufferUtil.readWithShortLength(reader));
        }
    }

    public TrieIndexEntry getExactPosition(DecoratedKey dk,
                                           SSTableReadsListener listener,
                                           boolean updateStats)
    {
        if (!bf.isPresent(dk))
        {
            listener.onSSTableSkipped(this, SkippingReason.BLOOM_FILTER);
            Tracing.trace("Bloom filter allows skipping sstable {}", descriptor.id);
            if (updateStats)
                bloomFilterTracker.addTrueNegative();
            return null;
        }

        if ((filterFirst() && first.compareTo(dk) > 0) || (filterLast() && last.compareTo(dk) < 0))
        {
            if (updateStats)
                bloomFilterTracker.addFalsePositive();
            listener.onSSTableSkipped(this, SkippingReason.MIN_MAX_KEYS);
            return null;
        }

        try (PartitionIndex.Reader reader = partitionIndex.openReader())
        {
            long indexPos = reader.exactCandidate(dk);
            if (indexPos == PartitionIndex.NOT_FOUND)
            {
                if (updateStats)
                    bloomFilterTracker.addFalsePositive();
                listener.onSSTableSkipped(this, SkippingReason.PARTITION_INDEX_LOOKUP);
                return null;
            }

            FileHandle fh;
            long seekPosition;
            if (indexPos >= 0)
            {
                fh = rowIndexFile;
                seekPosition = indexPos;
            }
            else
            {
                fh = dfile;
                seekPosition = ~indexPos;
            }

            try (FileDataInput in = fh.createReader(seekPosition))
            {
                return ByteBufferUtil.equalsWithShortLength(in, dk.getKey())
                       ? handleKeyFound(updateStats, listener, in, indexPos)
                       : handleKeyNotFound(updateStats, listener);
            }
        }
        catch (IOException e)
        {
            markSuspect();
            throw new CorruptSSTableException(e, rowIndexFile.path());
        }
    }

    private TrieIndexEntry handleKeyNotFound(boolean updateStats, SSTableReadsListener listener)
    {
        if (updateStats)
            bloomFilterTracker.addFalsePositive();
        listener.onSSTableSkipped(this, SkippingReason.INDEX_ENTRY_NOT_FOUND);
        return null;
    }

    private TrieIndexEntry handleKeyFound(boolean updateStats, SSTableReadsListener listener, FileDataInput in, long indexPos) throws IOException
    {
        if (updateStats)
            bloomFilterTracker.addTruePositive();
        TrieIndexEntry entry = indexPos >= 0 ? TrieIndexEntry.deserialize(in, in.getFilePointer())
                                             : new TrieIndexEntry(~indexPos);

        listener.onSSTableSelected(this, SelectionReason.INDEX_ENTRY_FOUND);
        return entry;
    }

    /**
     * @param bounds Must not be wrapped around ranges
     * @return PartitionIndexIterator within the given bounds
     */
    public PartitionIterator coveredKeysIterator(AbstractBounds<PartitionPosition> bounds) throws IOException
    {
        return new KeysRange(bounds).iterator();
    }

    public ScrubPartitionIterator scrubPartitionsIterator() throws IOException
    {
        return new ScrubIterator(partitionIndex, rowIndexFile);
    }

    private final class KeysRange
    {
        PartitionPosition left;
        boolean inclusiveLeft;
        PartitionPosition right;
        boolean inclusiveRight;

        KeysRange(AbstractBounds<PartitionPosition> bounds)
        {
            assert !AbstractBounds.strictlyWrapsAround(bounds.left, bounds.right) : String.format("[%s,%s]", bounds.left, bounds.right);

            left = bounds.left;
            inclusiveLeft = bounds.inclusiveLeft();
            if (filterFirst() && first.compareTo(left) > 0)
            {
                left = first;
                inclusiveLeft = true;
            }

            right = bounds.right;
            inclusiveRight = bounds.inclusiveRight();
            if (filterLast() && last.compareTo(right) < 0)
            {
                right = last;
                inclusiveRight = true;
            }
        }

        PartitionIterator iterator() throws IOException
        {
            return coveredKeysIterator(left, inclusiveLeft, right, inclusiveRight);
        }
    }

    public PartitionIterator coveredKeysIterator(PartitionPosition left, boolean inclusiveLeft, PartitionPosition right, boolean inclusiveRight) throws IOException
    {
        AbstractBounds<PartitionPosition> cover = Bounds.bounds(left, inclusiveLeft, right, inclusiveRight);
        boolean isLeftInSStableRange = !filterFirst() || first.compareTo(left) <= 0 && last.compareTo(left) >= 0;
        boolean isRightInSStableRange = !filterLast() || first.compareTo(right) <= 0 && last.compareTo(right) >= 0;
        if (isLeftInSStableRange || isRightInSStableRange || (cover.contains(first) && cover.contains(last)))
        {
            inclusiveLeft = isLeftInSStableRange ? inclusiveLeft : true;
            inclusiveRight = isRightInSStableRange ? inclusiveRight : true;
            return new PartitionIterator(partitionIndex,
                                         metadata().partitioner,
                                         rowIndexFile, dfile,
                                         isLeftInSStableRange ? left : first, inclusiveLeft ? -1 : 0,
                                         isRightInSStableRange ? right : last, inclusiveRight ? 0 : -1);
        }
        else
            return PartitionIterator.empty(partitionIndex);
    }

    @Override
    public PartitionIterator keyReader() throws IOException
    {
        return new PartitionIterator(partitionIndex, metadata().partitioner, rowIndexFile, dfile);
    }

    @Override
    public Iterable<DecoratedKey> getKeySamples(final Range<Token> range)
    {
        Iterator<IndexPosIterator> partitionKeyIterators = BtiTableScanner.makeBounds(this,
                                                                                      Collections.singleton(range))
                                                                          .stream()
                                                                          .map(this::indexPosIteratorForRange)
                                                                          .iterator();

        if (!partitionKeyIterators.hasNext())
            return Collections.emptyList();

        return () -> new AbstractIterator<DecoratedKey>()
        {
            IndexPosIterator currentItr = partitionKeyIterators.next();
            long count = -1;

            private long getNextPos() throws IOException
            {
                long pos;
                while ((pos = currentItr.nextIndexPos()) == PartitionIndex.NOT_FOUND
                       && partitionKeyIterators.hasNext())
                {
                    closeCurrentIt();
                    currentItr = partitionKeyIterators.next();
                }
                return pos;
            }

            private void closeCurrentIt()
            {
                if (currentItr != null)
                    currentItr.close();
                currentItr = null;
            }

            @Override
            protected DecoratedKey computeNext()
            {
                try
                {
                    while (true)
                    {
                        long pos = getNextPos();
                        count++;
                        if (pos == PartitionIndex.NOT_FOUND)
                            break;
                        if (count % Downsampling.BASE_SAMPLING_LEVEL == 0)
                        {
                            // handle exclusive start and exclusive end
                            DecoratedKey key = getKeyByPos(pos);
                            if (range.contains(key.getToken()))
                                return key;
                            count--;
                        }
                    }
                    closeCurrentIt();
                    return endOfData();
                }
                catch (IOException e)
                {
                    closeCurrentIt();
                    markSuspect();
                    throw new CorruptSSTableException(e, dfile.path());
                }
            }
        };
    }

    private DecoratedKey getKeyByPos(long pos) throws IOException
    {
        assert pos != PartitionIndex.NOT_FOUND;

        if (pos >= 0)
            try (FileDataInput in = rowIndexFile.createReader(pos))
            {
                return metadata().partitioner.decorateKey(ByteBufferUtil.readWithShortLength(in));
            }
        else
            try (FileDataInput in = dfile.createReader(~pos))
            {
                return metadata().partitioner.decorateKey(ByteBufferUtil.readWithShortLength(in));
            }
    }

    private IndexPosIterator indexPosIteratorForRange(AbstractBounds<PartitionPosition> bound)
    {
        return new IndexPosIterator(partitionIndex, bound.left, bound.right);
    }

    @Override
    public long estimatedKeysForRanges(Collection<Range<Token>> ranges)
    {
        // Estimate the number of partitions by calculating the bytes of the sstable that are covered by the specified
        // ranges and using the mean partition size to obtain a number of partitions from that.
        long selectedDataSize = 0;
        for (Range<Token> range : Range.normalize(ranges))
        {
            PartitionPosition left = range.left.minKeyBound();
            if (left.compareTo(first) <= 0)
                left = null;
            else if (left.compareTo(last) > 0)
                continue;   // no intersection

            PartitionPosition right = range.right.minKeyBound();
            if (range.right.isMinimum() || right.compareTo(last) >= 0)
                right = null;
            else if (right.compareTo(first) < 0)
                continue;   // no intersection

            if (left == null && right == null)
                return partitionIndex.size();   // sstable is fully covered, return full partition count to avoid rounding errors

            if (left == null && filterFirst())
                left = first;
            if (right == null && filterLast())
                right = last;

            long startPos = left != null ? getPosition(left, GE) : 0;
            long endPos = right != null ? getPosition(right, GE) : uncompressedLength();
            selectedDataSize += endPos - startPos;
        }
        return (long) (selectedDataSize / sstableMetadata.estimatedPartitionSize.rawMean());
    }


    @Override
    public UnfilteredRowIterator rowIterator(DecoratedKey key,
                                             Slices slices,
                                             ColumnFilter selectedColumns,
                                             boolean reversed,
                                             SSTableReadsListener listener)
    {
        return iterator(null, key, getExactPosition(key, listener, true), slices, selectedColumns, reversed);
    }

    public UnfilteredRowIterator iterator(FileDataInput dataFileInput,
                                          DecoratedKey key,
                                          TrieIndexEntry indexEntry,
                                          Slices slices,
                                          ColumnFilter selectedColumns,
                                          boolean reversed)
    {
        if (indexEntry == null)
            return UnfilteredRowIterators.noRowsIterator(metadata(), key, Rows.EMPTY_STATIC_ROW, DeletionTime.LIVE, reversed);

        return reversed
               ? new SSTableReversedIterator(this, dataFileInput, key, indexEntry, slices, selectedColumns, rowIndexFile)
               : new SSTableIterator(this, dataFileInput, key, indexEntry, slices, selectedColumns, rowIndexFile);
    }

    public interface PartitionReader extends Closeable
    {
        /**
         * Returns next item or null if exhausted.
         */
        Unfiltered next() throws IOException;
    }

    @Override
    public ISSTableScanner getScanner()
    {
        return BtiTableScanner.getScanner(this);
    }

    @Override
    public ISSTableScanner getScanner(Collection<Range<Token>> ranges)
    {
        if (ranges != null)
            return BtiTableScanner.getScanner(this, ranges);
        else
            return getScanner();
    }

    @Override
    public ISSTableScanner getScanner(Iterator<AbstractBounds<PartitionPosition>> rangeIterator)
    {
        return BtiTableScanner.getScanner(this, rangeIterator);
    }

    @Override
    public SSTableReader cloneAndReplace(IFilter newBloomFilter)
    {
        return cloneInternal(first, openReason, newBloomFilter);
    }

    @Override
    public SSTableReader cloneWithRestoredStart(DecoratedKey restoredStart)
    {
        return runWithLock(d -> cloneInternal(restoredStart, OpenReason.NORMAL, bf.sharedCopy()));
    }

    @Override
    public SSTableReader cloneWithNewStart(DecoratedKey newStart)
    {
        return runWithLock(d -> {
            assert openReason != OpenReason.EARLY;
            // TODO: merge with caller's firstKeyBeyond() work,to save time
            if (newStart.compareTo(first) > 0)
            {
                final long dataStart = getPosition(newStart, Operator.EQ);
                runOnClose(() -> dfile.dropPageCache(dataStart));
            }

            return cloneInternal(newStart, OpenReason.MOVED_START, bf.sharedCopy());
        });
    }

    @Override
    public DecoratedKey firstKeyBeyond(PartitionPosition token)
    {
        try
        {
            TrieIndexEntry pos = getRowIndexEntry(token, Operator.GT, true, false, SSTableReadsListener.NOOP_LISTENER);
            if (pos == null)
                return null;

            try (FileDataInput in = dfile.createReader(pos.position))
            {
                ByteBuffer indexKey = ByteBufferUtil.readWithShortLength(in);
                return decorateKey(indexKey);
            }
        }
        catch (IOException e)
        {
            markSuspect();
            throw new CorruptSSTableException(e, dfile.path());
        }
    }


    @Override
    public TrieIndexEntry deserializeKeyCacheValue(DataInputPlus input) throws IOException
    {
        return TrieIndexEntry.deserializeForCache(input);
    }

    @Override
    public ClusteringPrefix<?> getLowerBoundPrefixFromCache(DecoratedKey partitionKey, ClusteringIndexFilter filter)
    {
        return null;
    }

    @Override
    public void releaseComponents()
    {
        closeInternalComponent(partitionIndex);
    }

    @Override
    public int getEstimationSamples()
    {
        return 0;
    }

    @Override
    public UnfilteredPartitionIterator partitionIterator(ColumnFilter columnFilter, DataRange dataRange, SSTableReadsListener listener)
    {
        return BtiTableScanner.getScanner(this, columnFilter, dataRange, listener);
    }


    @Override
    public IScrubber getScrubber(LifecycleTransaction transaction, OutputHandler outputHandler, IScrubber.Options options)
    {
        ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(metadata());
        Preconditions.checkArgument(transaction.originals().contains(this));
        return new BtiTableScrubber(cfs, transaction, outputHandler, options);
    }

    @Override
    public IVerifier getVerifier(OutputHandler outputHandler, boolean isOffline, IVerifier.Options options)
    {
        ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(metadata());
        return new BtiTableVerifier(cfs, this, isOffline, options);
    }
}
