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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

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
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.IFilter;
import org.apache.cassandra.utils.OutputHandler;

/**
 * SSTableReaders are open()ed by Keyspace.onStart; after that they are created by SSTableWriter.renameAndOpen.
 * Do not re-call open() on existing SSTable files; use the references kept by ColumnFamilyStore post-start instead.
 */
public class BigTableReader extends SSTableReader implements IndexSummarySupport<BigTableReader>
{
    private static final Logger logger = LoggerFactory.getLogger(BigTableReader.class);

    private final RowIndexEntry.IndexSerializer rowIndexEntrySerializer;
    private final IndexSummary indexSummary;
    private final FileHandle ifile;

    BigTableReader(SSTableReaderBuilder builder)
    {
        super(builder);
        this.ifile = builder.ifile;
        this.indexSummary = builder.summary;
        this.rowIndexEntrySerializer = new RowIndexEntry.Serializer(descriptor.version, header);
    }

    @Override
    protected List<AutoCloseable> setupInstance(boolean trackHotness)
    {
        ArrayList<AutoCloseable> closeables = Lists.newArrayList(bf, indexSummary, ifile);
        closeables.addAll(super.setupInstance(trackHotness));
        return closeables;
    }

    @Override
    public void releaseComponents()
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
        if (token.compareTo(first) < 0)
            return first;

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
        long sampledPosition = indexSummary.getScanPositionFromBinarySearchResult(binarySearchResult);
        int sampledIndex = IndexSummary.getIndexFromBinarySearchResult(binarySearchResult);

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

    /**
     * @return An estimate of the number of keys in this SSTable based on the index summary.
     */
    @Override
    public long estimatedKeys()
    {
        return indexSummary.getEstimatedKeyCount();
    }

    /**
     * @param ranges
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

    @Override
    public int getEstimationSamples()
    {
        return indexSummary.size();
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

    /**
     * Gets the position in the index file to start scanning to find the given key (at most indexInterval keys away,
     * modulo downsampling of the index summary). Always returns a {@code value >= 0}
     */
    long getIndexScanPosition(PartitionPosition key)
    {
        if (openReason == OpenReason.MOVED_START && key.compareTo(first) < 0)
            key = first;

        return indexSummary.getScanPositionFromBinarySearch(key);
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
        return cloneAndReplace(newFirst, reason, indexSummary.sharedCopy());
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
        BigTableReader replacement = internalOpen(descriptor,
                                                  components,
                                                  metadata,
                                                  ifile != null ? ifile.sharedCopy() : null,
                                                  dfile.sharedCopy(),
                                                  newSummary,
                                                  bf.sharedCopy(),
                                                  maxDataAge,
                                                  sstableMetadata,
                                                  reason,
                                                  header);

        replacement.first = newFirst;
        replacement.last = last;
        replacement.isSuspect.set(isSuspect.get());
        return replacement;
    }

    /**
     * Clone this reader with the new values and set the clone as replacement.
     *
     * @param newBloomFilter for the replacement
     * @return the cloned reader. That reader is set as a replacement by the method.
     */
    @VisibleForTesting
    public SSTableReader cloneAndReplace(IFilter newBloomFilter)
    {
        SSTableReader replacement = internalOpen(descriptor,
                                                 components,
                                                 metadata,
                                                 ifile.sharedCopy(),
                                                 dfile.sharedCopy(),
                                                 indexSummary,
                                                 newBloomFilter,
                                                 maxDataAge,
                                                 sstableMetadata,
                                                 openReason,
                                                 header);

        replacement.first = first;
        replacement.last = last;
        replacement.isSuspect.set(isSuspect.get());
        return replacement;
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
            if (newStart.compareTo(first) > 0)
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
     * @throws IOException
     */
    @SuppressWarnings("resource")
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
            SSTableReaderBuilder.saveSummary(descriptor, first, last, newSummary);
            return cloneAndReplace(first, OpenReason.METADATA_CHANGE, newSummary);
        });
    }

    private IndexSummary buildSummaryAtLevel(int newSamplingLevel) throws IOException
    {
        // we read the positions in a BRAF so we don't have to worry about an entry spanning a mmap boundary.
        RandomAccessReader primaryIndex = RandomAccessReader.open(new File(descriptor.filenameFor(Component.PRIMARY_INDEX)));
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

    /**
     * Open a RowIndexedReader which already has its state initialized (by SSTableWriter).
     */
    public static BigTableReader internalOpen(Descriptor desc,
                                              Set<Component> components,
                                              TableMetadataRef metadata,
                                              FileHandle ifile,
                                              FileHandle dfile,
                                              IndexSummary summary,
                                              IFilter bf,
                                              long maxDataAge,
                                              StatsMetadata sstableMetadata,
                                              OpenReason openReason,
                                              SerializationHeader header)
    {
        assert desc != null && ifile != null && dfile != null && summary != null && bf != null && sstableMetadata != null;

        return (BigTableReader) new SSTableReaderBuilder.ForWriter(desc, metadata, maxDataAge, components, sstableMetadata, openReason, header)
                                .bf(bf).ifile(ifile).dfile(dfile).summary(summary).build();
    }
}