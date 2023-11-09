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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIteratorWithLowerBound;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.AbstractRowIndexEntry;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.Downsampling;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.IVerifier;
import org.apache.cassandra.io.sstable.IndexInfo;
import org.apache.cassandra.io.sstable.KeyReader;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.io.sstable.SSTableReadsListener.SelectionReason;
import org.apache.cassandra.io.sstable.SSTableReadsListener.SkippingReason;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReaderWithFilter;
import org.apache.cassandra.io.sstable.format.big.BigFormat.Components;
import org.apache.cassandra.io.sstable.indexsummary.IndexSummary;
import org.apache.cassandra.io.sstable.indexsummary.IndexSummaryBuilder;
import org.apache.cassandra.io.sstable.indexsummary.IndexSummarySupport;
import org.apache.cassandra.io.sstable.keycache.KeyCache;
import org.apache.cassandra.io.sstable.keycache.KeyCacheSupport;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.IFilter;
import org.apache.cassandra.utils.OutputHandler;

import static org.apache.cassandra.utils.concurrent.SharedCloseable.sharedCopyOrNull;

/**
 * SSTableReaders are open()ed by Keyspace.onStart; after that they are created by SSTableWriter.renameAndOpen.
 * Do not re-call open() on existing SSTable files; use the references kept by ColumnFamilyStore post-start instead.
 */
public class BigTableReader extends SSTableReaderWithFilter implements IndexSummarySupport<BigTableReader>,
                                                                       KeyCacheSupport<BigTableReader>
{
    private static final Logger logger = LoggerFactory.getLogger(BigTableReader.class);

    private final RowIndexEntry.IndexSerializer rowIndexEntrySerializer;
    private final IndexSummary indexSummary;
    private final FileHandle ifile;

    private final KeyCache keyCache;

    public BigTableReader(Builder builder, SSTable.Owner owner)
    {
        super(builder, owner);
        this.ifile = builder.getIndexFile();
        this.indexSummary = builder.getIndexSummary();
        this.rowIndexEntrySerializer = new RowIndexEntry.Serializer(descriptor.version, header, owner != null ? owner.getMetrics() : null);
        this.keyCache = Objects.requireNonNull(builder.getKeyCache());
    }

    @Override
    protected List<AutoCloseable> setupInstance(boolean trackHotness)
    {
        ArrayList<AutoCloseable> closeables = Lists.newArrayList(indexSummary, ifile);
        closeables.addAll(super.setupInstance(trackHotness));
        return closeables;
    }

    @Override
    public void releaseInMemoryComponents()
    {
        closeInternalComponent(indexSummary);
        assert indexSummary.isCleanedUp();
    }

    @Override
    public IndexSummary getIndexSummary()
    {
        return indexSummary;
    }

    public UnfilteredRowIterator rowIterator(DecoratedKey key,
                                             Slices slices,
                                             ColumnFilter selectedColumns,
                                             boolean reversed,
                                             SSTableReadsListener listener)
    {
        RowIndexEntry rie = getRowIndexEntry(key, SSTableReader.Operator.EQ, true, listener);
        return rowIterator(null, key, rie, slices, selectedColumns, reversed);
    }

    public UnfilteredRowIterator rowIterator(FileDataInput file, DecoratedKey key, RowIndexEntry indexEntry, Slices slices, ColumnFilter selectedColumns, boolean reversed)
    {
        if (indexEntry == null)
            return UnfilteredRowIterators.noRowsIterator(metadata(), key, Rows.EMPTY_STATIC_ROW, DeletionTime.LIVE, reversed);
        else if (reversed)
            return new SSTableReversedIterator(this, file, key, indexEntry, slices, selectedColumns, ifile);
        else
            return new SSTableIterator(this, file, key, indexEntry, slices, selectedColumns, ifile);
    }

    @Override
    public ISSTableScanner partitionIterator(ColumnFilter columns, DataRange dataRange, SSTableReadsListener listener)
    {
        return BigTableScanner.getScanner(this, columns, dataRange, listener);
    }

    @Override
    public KeyReader keyReader() throws IOException
    {
        return BigTableKeyReader.create(ifile, rowIndexEntrySerializer);
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
     * Finds and returns the first key beyond a given token in this SSTable or null if no such key exists.
     */
    @Override
    public DecoratedKey firstKeyBeyond(PartitionPosition token)
    {
        if (token.compareTo(getFirst()) < 0)
            return getFirst();

        long sampledPosition = getIndexScanPosition(token);

        if (ifile == null)
            return null;

        String path = null;
        try (FileDataInput in = ifile.createReader(sampledPosition))
        {
            path = in.getPath();
            while (!in.isEOF())
            {
                ByteBuffer indexKey = ByteBufferUtil.readWithShortLength(in);
                DecoratedKey indexDecoratedKey = decorateKey(indexKey);
                if (indexDecoratedKey.compareTo(token) > 0)
                    return indexDecoratedKey;

                RowIndexEntry.Serializer.skip(in, descriptor.version);
            }
        }
        catch (IOException e)
        {
            markSuspect();
            throw new CorruptSSTableException(e, path);
        }

        return null;
    }

    /**
     * Retrieves the position while updating the key cache and the stats.
     *
     * @param key The key to apply as the rhs to the given Operator. A 'fake' key is allowed to
     *            allow key selection by token bounds but only if op != * EQ
     * @param op  The Operator defining matching keys: the nearest key to the target matching the operator wins.
     */
    public final RowIndexEntry getRowIndexEntry(PartitionPosition key, Operator op)
    {
        return getRowIndexEntry(key, op, true, SSTableReadsListener.NOOP_LISTENER);
    }

    /**
     * @param key         The key to apply as the rhs to the given Operator. A 'fake' key is allowed to
     *                    allow key selection by token bounds but only if op != * EQ
     * @param operator    The Operator defining matching keys: the nearest key to the target matching the operator wins.
     * @param updateStats true if updating stats and cache
     * @return The index entry corresponding to the key, or null if the key is not present
     */
    @Override
    public RowIndexEntry getRowIndexEntry(PartitionPosition key,
                                          Operator operator,
                                          boolean updateStats,
                                          SSTableReadsListener listener)
    {
        // Having no index file is impossible in a normal operation. The only way it might happen is running
        // Scrubber that does not really rely on this method.
        if (ifile == null)
            return null;

        Operator searchOp = operator;

        // check the smallest and greatest keys in the sstable to see if it can't be present
        boolean skip = false;
        if (key.compareTo(getFirst()) < 0)
        {
            if (searchOp == Operator.EQ)
            {
                skip = true;
            }
            else
            {
                key = getFirst();
                searchOp = Operator.GE; // since op != EQ, bloom filter will be skipped; first key is included so no reason to check bloom filter
            }
        }
        else
        {
            int l = getLast().compareTo(key);
            skip = l < 0 // out of range, skip
                   || l == 0 && searchOp == Operator.GT; // search entry > key, but key is the last in range, so skip
            if (l == 0)
                searchOp = Operator.GE; // since op != EQ, bloom filter will be skipped, last key is included so no reason to check bloom filter
        }
        if (skip)
        {
            notifySkipped(SkippingReason.MIN_MAX_KEYS, listener, operator, updateStats);
            return null;
        }

        if (searchOp == Operator.EQ)
        {
            assert key instanceof DecoratedKey; // EQ only make sense if the key is a valid row key
            if (!isPresentInFilter((IFilter.FilterKey) key))
            {
                notifySkipped(SkippingReason.BLOOM_FILTER, listener, operator, updateStats);
                return null;
            }
        }

        // next, the key cache (only make sense for valid row key)
        if ((searchOp == Operator.EQ || searchOp == Operator.GE) && (key instanceof DecoratedKey))
        {
            DecoratedKey decoratedKey = (DecoratedKey) key;
            AbstractRowIndexEntry cachedPosition = getCachedPosition(decoratedKey, updateStats);
            if (cachedPosition != null && cachedPosition.getSSTableFormat() == descriptor.getFormat())
            {
                notifySelected(SelectionReason.KEY_CACHE_HIT, listener, operator, updateStats, cachedPosition);
                return (RowIndexEntry) cachedPosition;
            }
        }

        int binarySearchResult = indexSummary.binarySearch(key);
        long sampledPosition = indexSummary.getScanPositionFromBinarySearchResult(binarySearchResult);
        int sampledIndex = IndexSummary.getIndexFromBinarySearchResult(binarySearchResult);

        int effectiveInterval = indexSummary.getEffectiveIndexIntervalAfterIndex(sampledIndex);

        // scan the on-disk index, starting at the nearest sampled position.
        // The check against IndexInterval is to be exited the loop in the EQ case when the key looked for is not present
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
                if (searchOp == Operator.EQ && i <= effectiveInterval)
                {
                    opSatisfied = exactMatch = indexKey.equals(((DecoratedKey) key).getKey());
                }
                else
                {
                    DecoratedKey indexDecoratedKey = decorateKey(indexKey);
                    int comparison = indexDecoratedKey.compareTo(key);
                    int v = searchOp.apply(comparison);
                    opSatisfied = (v == 0);
                    exactMatch = (comparison == 0);
                    if (v < 0)
                    {
                        notifySkipped(SkippingReason.PARTITION_INDEX_LOOKUP, listener, operator, updateStats);
                        return null;
                    }
                }

                if (opSatisfied)
                {
                    // read data position from index entry
                    RowIndexEntry indexEntry = rowIndexEntrySerializer.deserialize(in);
                    if (exactMatch && updateStats)
                    {
                        assert key instanceof DecoratedKey; // key can be == to the index key only if it's a true row key
                        DecoratedKey decoratedKey = (DecoratedKey) key;

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
                    notifySelected(SelectionReason.INDEX_ENTRY_FOUND, listener, operator, updateStats, indexEntry);
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

        notifySkipped(SkippingReason.INDEX_ENTRY_NOT_FOUND, listener, operator, updateStats);
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
                               SSTableReadsListener listener)
    {
        RowIndexEntry rowIndexEntry = getRowIndexEntry(key, op, updateCacheAndStats, listener);
        return rowIndexEntry != null ? rowIndexEntry.position : -1;
    }

    @Override
    public DecoratedKey keyAtPositionFromSecondaryIndex(long keyPositionFromSecondaryIndex) throws IOException
    {
        DecoratedKey key;
        try (FileDataInput in = ifile.createReader(keyPositionFromSecondaryIndex))
        {
            if (in.isEOF())
                return null;

            key = decorateKey(ByteBufferUtil.readWithShortLength(in));

            // hint read path about key location if caching is enabled
            // this saves index summary lookup and index file iteration which whould be pretty costly
            // especially in presence of promoted column indexes
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
    public ClusteringBound<?> getLowerBoundPrefixFromCache(DecoratedKey partitionKey, boolean isReversed)
    {
        AbstractRowIndexEntry rie = getCachedPosition(partitionKey, false);
        if (!(rie instanceof RowIndexEntry))
            return null;

        RowIndexEntry rowIndexEntry = (RowIndexEntry) rie;
        if (!rowIndexEntry.indexOnHeap())
            return null;

        try (RowIndexEntry.IndexInfoRetriever onHeapRetriever = rowIndexEntry.openWithIndex(null))
        {
            IndexInfo columns = onHeapRetriever.columnsIndex(isReversed ? rowIndexEntry.blockCount() - 1 : 0);
            ClusteringBound<?> bound = isReversed ? columns.lastName.asEndBound() : columns.firstName.asStartBound();
            UnfilteredRowIteratorWithLowerBound.assertBoundSize(bound, this);
            return bound.artificialLowerBound(isReversed);
        }
        catch (IOException e)
        {
            throw new RuntimeException("should never occur", e);
        }
    }

    /**
     * @return An estimate of the number of keys in this SSTable based on the index summary.
     */
    @Override
    public long estimatedKeys()
    {
        return indexSummary.getEstimatedKeyCount();
    }

    /**
     * @return An estimate of the number of keys for given ranges in this SSTable.
     */
    @Override
    public long estimatedKeysForRanges(Collection<Range<Token>> ranges)
    {
        long sampleKeyCount = 0;
        List<IndexesBounds> sampleIndexes = indexSummary.getSampleIndexesForRanges(ranges);
        for (IndexesBounds sampleIndexRange : sampleIndexes)
            sampleKeyCount += (sampleIndexRange.upperPosition - sampleIndexRange.lowerPosition + 1);

        // adjust for the current sampling level: (BSL / SL) * index_interval_at_full_sampling
        long estimatedKeys = sampleKeyCount * ((long) Downsampling.BASE_SAMPLING_LEVEL * indexSummary.getMinIndexInterval()) / indexSummary.getSamplingLevel();
        return Math.max(1, estimatedKeys);
    }

    /**
     * Returns whether the number of entries in the IndexSummary > 2.  At full sampling, this is approximately
     * 1/INDEX_INTERVALth of the keys in this SSTable.
     */
    @Override
    public boolean isEstimationInformative()
    {
        return indexSummary.size() > 2;
    }

    @Override
    public Iterable<DecoratedKey> getKeySamples(final Range<Token> range)
    {
        return Iterables.transform(indexSummary.getKeySamples(range), bytes -> decorateKey(ByteBuffer.wrap(bytes)));
    }

    public RandomAccessReader openIndexReader()
    {
        if (ifile != null)
            return ifile.createReader();
        return null;
    }

    public FileHandle getIndexFile()
    {
        return ifile;
    }

    @Override
    public IVerifier getVerifier(ColumnFamilyStore cfs, OutputHandler outputHandler, boolean isOffline, IVerifier.Options options)
    {
        Preconditions.checkArgument(cfs.metadata().equals(metadata()));
        return new BigTableVerifier(cfs, this, outputHandler, isOffline, options);
    }

    /**
     * Gets the position with the index file to start scanning to find the given key (at most indexInterval keys away,
     * modulo downsampling of the index summary). Always returns a {@code value >= 0}
     */
    long getIndexScanPosition(PartitionPosition key)
    {
        if (openReason == OpenReason.MOVED_START && key.compareTo(getFirst()) < 0)
            key = getFirst();

        return indexSummary.getScanPosition(key);
    }

    protected final Builder unbuildTo(Builder builder, boolean sharedCopy)
    {
        Builder b = super.unbuildTo(builder, sharedCopy);
        if (builder.getIndexFile() == null)
            b.setIndexFile(sharedCopy ? sharedCopyOrNull(ifile) : ifile);
        if (builder.getIndexSummary() == null)
            b.setIndexSummary(sharedCopy ? sharedCopyOrNull(indexSummary) : indexSummary);

        b.setKeyCache(keyCache);

        return b;
    }

    @VisibleForTesting
    @Override
    public SSTableReaderWithFilter cloneAndReplace(IFilter filter)
    {
        return unbuildTo(new Builder(descriptor).setFilter(filter), true).build(owner().orElse(null), true, true);
    }

    /**
     * Clone this reader with the provided start and open reason, and set the clone as replacement.
     *
     * @param newFirst the first key for the replacement (which can be different from the original due to the pre-emptive
     *                 opening of compaction results).
     * @param reason   the {@code OpenReason} for the replacement.
     * @return the cloned reader. That reader is set as a replacement by the method.
     */
    private SSTableReader cloneAndReplace(DecoratedKey newFirst, OpenReason reason)
    {
        return unbuildTo(new Builder(descriptor), true)
               .setFirst(newFirst)
               .setOpenReason(reason)
               .build(owner().orElse(null), true, true);
    }

    /**
     * Clone this reader with the new values and set the clone as replacement.
     *
     * @param newFirst   the first key for the replacement (which can be different from the original due to the pre-emptive
     *                   opening of compaction results).
     * @param reason     the {@code OpenReason} for the replacement.
     * @param newSummary the index summary for the replacement.
     * @return the cloned reader. That reader is set as a replacement by the method.
     */
    private BigTableReader cloneAndReplace(DecoratedKey newFirst, OpenReason reason, IndexSummary newSummary)
    {
        return unbuildTo(new Builder(descriptor).setIndexSummary(newSummary), true)
                    .setIndexSummary(newSummary)
                    .setFirst(newFirst)
                    .setOpenReason(reason)
                    .build(owner().orElse(null), true, true);
    }

    public SSTableReader cloneWithRestoredStart(DecoratedKey restoredStart)
    {
        return runWithLock(ignored -> cloneAndReplace(restoredStart, OpenReason.NORMAL));
    }

    public SSTableReader cloneWithNewStart(DecoratedKey newStart)
    {
        return runWithLock(ignored -> {
            assert openReason != OpenReason.EARLY;
            // TODO: merge with caller's firstKeyBeyond() work,to save time
            if (newStart.compareTo(getFirst()) > 0)
            {
                Map<FileHandle, Long> handleAndPositions = new LinkedHashMap<>(2);
                if (dfile != null)
                    handleAndPositions.put(dfile, getPosition(newStart, Operator.EQ));
                if (ifile != null)
                    handleAndPositions.put(ifile, getIndexScanPosition(newStart));
                runOnClose(() -> handleAndPositions.forEach(FileHandle::dropPageCache));
            }

            return cloneAndReplace(newStart, OpenReason.MOVED_START);
        });
    }

    /**
     * Returns a new SSTableReader with the same properties as this SSTableReader except that a new IndexSummary will
     * be built at the target samplingLevel.  This (original) SSTableReader instance will be marked as replaced, have
     * its DeletingTask removed, and have its periodic read-meter sync task cancelled.
     *
     * @param samplingLevel the desired sampling level for the index summary on the new SSTableReader
     * @return a new SSTableReader
     */
    public BigTableReader cloneWithNewSummarySamplingLevel(ColumnFamilyStore parent, int samplingLevel) throws IOException
    {
        assert openReason != OpenReason.EARLY;

        int minIndexInterval = metadata().params.minIndexInterval;
        int maxIndexInterval = metadata().params.maxIndexInterval;
        double effectiveInterval = indexSummary.getEffectiveIndexInterval();

        IndexSummary newSummary;

        // We have to rebuild the summary from the on-disk primary index in three cases:
        // 1. The sampling level went up, so we need to read more entries off disk
        // 2. The min_index_interval changed (in either direction); this changes what entries would be in the summary
        //    at full sampling (and consequently at any other sampling level)
        // 3. The max_index_interval was lowered, forcing us to raise the sampling level
        if (samplingLevel > indexSummary.getSamplingLevel() || indexSummary.getMinIndexInterval() != minIndexInterval || effectiveInterval > maxIndexInterval)
        {
            newSummary = buildSummaryAtLevel(samplingLevel);
        }
        else if (samplingLevel < indexSummary.getSamplingLevel())
        {
            // we can use the existing index summary to make a smaller one
            newSummary = IndexSummaryBuilder.downsample(indexSummary, samplingLevel, minIndexInterval, getPartitioner());
        }
        else
        {
            throw new AssertionError("Attempted to clone SSTableReader with the same index summary sampling level and " +
                                     "no adjustments to min/max_index_interval");
        }

        // Always save the resampled index with lock to avoid racing with entire-sstable streaming
        return runWithLock(ignored -> {
            new IndexSummaryComponent(newSummary, getFirst(), getLast()).save(descriptor.fileFor(Components.SUMMARY), true);
            return cloneAndReplace(getFirst(), OpenReason.METADATA_CHANGE, newSummary);
        });
    }

    private IndexSummary buildSummaryAtLevel(int newSamplingLevel) throws IOException
    {
        // we read the positions in a BRAF, so we don't have to worry about an entry spanning a mmap boundary.
        RandomAccessReader primaryIndex = RandomAccessReader.open(descriptor.fileFor(Components.PRIMARY_INDEX));
        try
        {
            long indexSize = primaryIndex.length();
            try (IndexSummaryBuilder summaryBuilder = new IndexSummaryBuilder(estimatedKeys(), metadata().params.minIndexInterval, newSamplingLevel))
            {
                long indexPosition;
                while ((indexPosition = primaryIndex.getFilePointer()) != indexSize)
                {
                    summaryBuilder.maybeAddEntry(decorateKey(ByteBufferUtil.readWithShortLength(primaryIndex)), indexPosition);
                    RowIndexEntry.Serializer.skip(primaryIndex, descriptor.version);
                }

                return summaryBuilder.build(getPartitioner());
            }
        }
        finally
        {
            FileUtils.closeQuietly(primaryIndex);
        }
    }

    @Override
    public KeyCache getKeyCache()
    {
        return this.keyCache;
    }

    public static class Builder extends SSTableReaderWithFilter.Builder<BigTableReader, Builder>
    {
        private static final Logger logger = LoggerFactory.getLogger(Builder.class);

        private IndexSummary indexSummary;
        private FileHandle indexFile;
        private KeyCache keyCache = KeyCache.NO_CACHE;

        public Builder(Descriptor descriptor)
        {
            super(descriptor);
        }

        public Builder setIndexFile(FileHandle indexFile)
        {
            this.indexFile = indexFile;
            return this;
        }

        public Builder setIndexSummary(IndexSummary indexSummary)
        {
            this.indexSummary = indexSummary;
            return this;
        }

        public Builder setKeyCache(KeyCache keyCache)
        {
            this.keyCache = keyCache;
            return this;
        }

        public IndexSummary getIndexSummary()
        {
            return indexSummary;
        }

        public FileHandle getIndexFile()
        {
            return indexFile;
        }

        public KeyCache getKeyCache()
        {
            return keyCache;
        }

        @Override
        protected BigTableReader buildInternal(Owner owner)
        {
            return new BigTableReader(this, owner);
        }
    }
}
