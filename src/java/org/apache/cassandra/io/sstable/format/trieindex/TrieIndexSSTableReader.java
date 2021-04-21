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

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.InstrumentingCache;
import org.apache.cassandra.cache.KeyCacheKey;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.Downsampling;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.sstable.format.RowIndexEntry;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReadsListener;
import org.apache.cassandra.io.sstable.format.SSTableReadsListener.SelectionReason;
import org.apache.cassandra.io.sstable.format.SSTableReadsListener.SkippingReason;
import org.apache.cassandra.io.sstable.format.ScrubPartitionIterator;
import org.apache.cassandra.io.sstable.format.big.BigTableRowIndexEntry;
import org.apache.cassandra.io.sstable.format.trieindex.PartitionIndex.IndexPosIterator;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.sstable.metadata.ValidationMetadata;
import org.apache.cassandra.io.util.BufferedDataOutputStreamPlus;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.FilterFactory;
import org.apache.cassandra.utils.IFilter;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.SyncUtil;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.Ref;

import static org.apache.cassandra.io.sstable.format.SSTableReader.Operator.EQ;
import static org.apache.cassandra.io.sstable.format.SSTableReader.Operator.GE;
import static org.apache.cassandra.io.sstable.format.SSTableReader.Operator.GT;
import static org.apache.cassandra.io.sstable.format.SSTableReaderBuilder.defaultDataHandleBuilder;
import static org.apache.cassandra.io.sstable.format.SSTableReaderBuilder.defaultIndexHandleBuilder;

/**
 * SSTableReaders are open()ed by Keyspace.onStart; after that they are created by SSTableWriter.renameAndOpen.
 * Do not re-call open() on existing SSTable files; use the references kept by ColumnFamilyStore post-start instead.
 */
public class TrieIndexSSTableReader extends SSTableReader
{
    private static final Logger logger = LoggerFactory.getLogger(TrieIndexSSTableReader.class);

    protected FileHandle rowIndexFile;
    protected PartitionIndex partitionIndex;

    @VisibleForTesting
    public static final double fpChanceTolerance = Double.parseDouble(System.getProperty(Config.PROPERTY_PREFIX + "bloom_filter_fp_chance_tolerance", "0.000001"));

    TrieIndexSSTableReader(Descriptor desc, Set<Component> components, TableMetadataRef metadata, Long maxDataAge, StatsMetadata sstableMetadata, OpenReason openReason, SerializationHeader header, FileHandle dfile, FileHandle rowIndexFile, PartitionIndex partitionIndex, IFilter bf)
    {
        super(desc, components, metadata, maxDataAge, sstableMetadata, openReason, header, null, dfile, null, bf);
        this.rowIndexFile = rowIndexFile;
        this.partitionIndex = partitionIndex;
    }

    /**
     * Clone this reader with the new start, open reason, bloom filter, and set the clone as replacement.
     *
     * @param first      new start for the replacement
     * @param openReason the {@code OpenReason} for the replacement.
     * @param bf         Bloom filter for the replacement
     * @return the cloned reader. That reader is set as a replacement by the method.
     */
    private TrieIndexSSTableReader cloneInternal(DecoratedKey first, OpenReason openReason, IFilter bf)
    {
        TrieIndexSSTableReader clone = new TrieIndexSSTableReader(descriptor,
                                                                  components,
                                                                  metadata,
                                                                  maxDataAge,
                                                                  sstableMetadata,
                                                                  openReason,
                                                                  header,
                                                                  dfile.sharedCopy(),
                                                                  rowIndexFile.sharedCopy(),
                                                                  partitionIndex.sharedCopy(),
                                                                  bf);
        clone.first = first;
        clone.last = last;
        clone.isSuspect.set(isSuspect.get());
        clone.setup(true);

        return clone;
    }

    /**
     * Open a RowIndexedReader which already has its state initialized (by SSTableWriter).
     */
    static TrieIndexSSTableReader internalOpen(Descriptor desc,
                                               Set<Component> components,
                                               TableMetadataRef metadata,
                                               FileHandle rowIndexFile,
                                               FileHandle dfile,
                                               PartitionIndex partitionIndex,
                                               IFilter bf,
                                               long maxDataAge,
                                               StatsMetadata sstableMetadata,
                                               OpenReason openReason,
                                               SerializationHeader header)
    {
        assert desc != null && rowIndexFile != null && dfile != null && partitionIndex != null && bf != null && sstableMetadata != null;

        // Make sure the SSTableReader internalOpen part does the same.
        assert desc.getFormat() == TrieIndexFormat.instance;
        TrieIndexSSTableReader reader = new TrieIndexSSTableReader(desc, components, metadata, maxDataAge, sstableMetadata, openReason, header, dfile, rowIndexFile, partitionIndex, bf);
        reader.first = partitionIndex.firstKey();
        reader.last = partitionIndex.lastKey();

        return reader;
    }

    static TrieIndexSSTableReader internalOpen(Descriptor desc,
                                               Set<Component> components,
                                               TableMetadataRef metadata,
                                               FileHandle dfile,
                                               IFilter bf,
                                               long maxDataAge,
                                               StatsMetadata sstableMetadata,
                                               OpenReason openReason,
                                               SerializationHeader header)
    {
        assert desc != null && dfile != null && bf != null && sstableMetadata != null;

        // Make sure the SSTableReader internalOpen part does the same.
        assert desc.getFormat() == TrieIndexFormat.instance;
        return new TrieIndexSSTableReader(desc, components, metadata, maxDataAge, sstableMetadata, openReason, header, dfile, null, null, bf);
    }

    @Override
    public void setup(boolean trackHotness)
    {
        tidy.setup(this, trackHotness, Arrays.asList(bf, dfile, partitionIndex, rowIndexFile));
        super.setup(trackHotness);
    }

    @Override
    public void addTo(Ref.IdentityCollection identities)
    {
        super.addTo(identities);
        rowIndexFile.addTo(identities);
        partitionIndex.addTo(identities);
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
    protected RowIndexEntry getPosition(PartitionPosition key, Operator op, boolean updateCacheAndStats, boolean permitMatchPastLast, SSTableReadsListener listener)
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
    private RowIndexEntry retrieveEntryIfAcceptable(Operator searchOp, PartitionPosition searchKey, long pos, boolean assumeNoMatch) throws IOException
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
            return new RowIndexEntry(pos);
        }
    }

    @Override
    public DecoratedKey keyAt(RandomAccessReader reader, long dataPosition) throws IOException
    {
        reader.seek(dataPosition);
        if (reader.isEOF()) return null;
        return decorateKey(ByteBufferUtil.readWithShortLength(reader));
    }

    @Override
    public DecoratedKey keyAt(FileDataInput reader) throws IOException
    {
        if (reader.isEOF()) return null;

        return decorateKey(ByteBufferUtil.readWithShortLength(reader));
    }

    public RowIndexEntry getExactPosition(DecoratedKey dk,
                                          SSTableReadsListener listener,
                                          boolean updateStats)
    {
        if (!bf.isPresent(dk))
        {
            listener.onSSTableSkipped(this, SkippingReason.BLOOM_FILTER);
            Tracing.trace("Bloom filter allows skipping sstable {}", descriptor.id);
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

    private RowIndexEntry handleKeyNotFound(boolean updateStats, SSTableReadsListener listener)
    {
        if (updateStats)
            bloomFilterTracker.addFalsePositive();
        listener.onSSTableSkipped(this, SkippingReason.INDEX_ENTRY_NOT_FOUND);
        return null;
    }

    private RowIndexEntry handleKeyFound(boolean updateStats, SSTableReadsListener listener, FileDataInput in, long indexPos) throws IOException
    {
        if (updateStats)
            bloomFilterTracker.addTruePositive();
        RowIndexEntry entry = indexPos >= 0 ? TrieIndexEntry.deserialize(in, in.getFilePointer())
                                            : new RowIndexEntry(~indexPos);

        listener.onSSTableSelected(this, entry, SelectionReason.INDEX_ENTRY_FOUND);
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

    public PartitionIterator allKeysIterator() throws IOException
    {
        return new PartitionIterator(partitionIndex, metadata().partitioner, rowIndexFile, dfile);
    }

    public ScrubPartitionIterator scrubPartitionsIterator() throws IOException
    {
        if (partitionIndex == null)
            return null;
        return new ScrubIterator(partitionIndex, rowIndexFile);
    }

    @Override
    public Iterable<DecoratedKey> getKeySamples(final Range<Token> range)
    {
        Iterator<IndexPosIterator> partitionKeyIterators = TrieIndexScanner.makeBounds(this,
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

            long startPos = left != null ? getPosition(left, GE).position : 0;
            long endPos = right != null ? getPosition(right, GE).position : uncompressedLength();
            selectedDataSize += endPos - startPos;
        }
        return (long) (selectedDataSize / sstableMetadata.estimatedPartitionSize.rawMean());
    }

    @Override
    public UnfilteredRowIterator iterator(DecoratedKey key,
                                          Slices slices,
                                          ColumnFilter selectedColumns,
                                          boolean reversed,
                                          SSTableReadsListener listener)
    {
        return iterator(null, key, getExactPosition(key, listener, true), slices, selectedColumns, reversed);
    }

    public UnfilteredRowIterator iterator(FileDataInput dataFileInput,
                                          DecoratedKey key,
                                          RowIndexEntry indexEntry,
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
    public UnfilteredRowIterator simpleIterator(FileDataInput dfile, DecoratedKey key, boolean tombstoneOnly)
    {
        RowIndexEntry position = getPosition(key, SSTableReader.Operator.EQ, true, false, SSTableReadsListener.NOOP_LISTENER);
        if (position == null)
            return null;
        return SSTableIdentityIterator.create(this, dfile, position, key, tombstoneOnly);
    }

    public UnfilteredRowIterator simpleIterator(FileDataInput dfile, DecoratedKey key, RowIndexEntry indexEntry, boolean tombstoneOnly)
    {
        return SSTableIdentityIterator.create(this, dfile, indexEntry, key, tombstoneOnly);
    }

    @Override
    public ISSTableScanner getScanner()
    {
        return TrieIndexScanner.getScanner(this);
    }

    @Override
    public ISSTableScanner getScanner(Collection<Range<Token>> ranges)
    {
        if (ranges != null)
            return TrieIndexScanner.getScanner(this, ranges);
        else
            return getScanner();
    }

    @Override
    public ISSTableScanner getScanner(Iterator<AbstractBounds<PartitionPosition>> rangeIterator)
    {
        return TrieIndexScanner.getScanner(this, rangeIterator);
    }

    @Override
    public ISSTableScanner getScanner(ColumnFilter columns, DataRange dataRange, SSTableReadsListener listener)
    {
        return TrieIndexScanner.getScanner(this, columns, dataRange, listener);
    }

    @Override
    public boolean hasIndex()
    {
        return descriptor.fileFor(Component.PARTITION_INDEX).exists();
    }

    @Override
    public void setupOnline()
    {
        final ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(metadata().id);
        if (cfs != null)
            setCrcCheckChance(cfs.getCrcCheckChance());
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
    public SSTableReader cloneWithNewStart(DecoratedKey newStart, Runnable runOnClose)
    {
        return runWithLock(d -> {
            assert openReason != OpenReason.EARLY;
            // TODO: merge with caller's firstKeyBeyond() work,to save time
            if (newStart.compareTo(first) > 0)
            {
                final long dataStart = getPosition(newStart, Operator.EQ).position;
                runOnClose(new DropPageCache(dfile, dataStart, null, -1, runOnClose));
            }

            return cloneInternal(newStart, OpenReason.MOVED_START, bf.sharedCopy());
        });
    }

    @Override
    public SSTableReader cloneWithNewSummarySamplingLevel(ColumnFamilyStore parent, int samplingLevel)
    {
        // nothing to do here, the method updates something in index summary which is missing in trie index impl
        return cloneInternal(first, openReason, bf.sharedCopy());
    }

    @Override
    public int getIndexSummarySamplingLevel()
    {
        return 0; // tries do not have index summaries
    }

    @Override
    public long getIndexSummaryOffHeapSize()
    {
        return 0; // tries do not have index summaries
    }

    @Override
    public int getMinIndexInterval()
    {
        return 0; // tries do not have index summaries
    }

    @Override
    public double getEffectiveIndexInterval()
    {
        return 0; // tries do not have index summaries
    }

    @Override
    public void releaseSummary()
    {
        // no-op - tries do not have index summaries
    }

    @Override
    public long getIndexScanPosition(PartitionPosition key)
    {
        // TODO check this
        throw new UnsupportedOperationException();
    }

    @Override
    public int getIndexSummarySize()
    {
        return 0; // tries do not have index summaries
    }

    @Override
    public int getMaxIndexSummarySize()
    {
        return 0; // tries do not have index summaries
    }

    @Override
    public byte[] getIndexSummaryKey(int index)
    {
        // TODO check this
        throw new UnsupportedOperationException();
    }

    @Override
    public void cacheKey(DecoratedKey key, BigTableRowIndexEntry info)
    {
        // no-op - tries do not have index summaries
    }

    @Override
    public BigTableRowIndexEntry getCachedPosition(DecoratedKey key, boolean updateStats)
    {
        return null; // tries do not have index summaries
    }

    @Override
    protected BigTableRowIndexEntry getCachedPosition(KeyCacheKey unifiedKey, boolean updateStats)
    {
        return null; // tries do not have index summaries
    }

    @Override
    public boolean isKeyCacheEnabled()
    {
        return false; // tries do not have index summaries
    }

    @Override
    public DecoratedKey firstKeyBeyond(PartitionPosition token)
    {
        try
        {
            RowIndexEntry pos = getPosition(token, Operator.GT);
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
    public InstrumentingCache<KeyCacheKey, BigTableRowIndexEntry> getKeyCache()
    {
        return null; // tries do not have key cache
    }

    @Override
    public long getKeyCacheHit()
    {
        return 0L; // tries do not have key cache
    }

    @Override
    public long getKeyCacheRequest()
    {
        return 0L;  // tries do not have key cache
    }

    @Override
    public ChannelProxy getIndexChannel()
    {
        throw new UnsupportedOperationException("tries do not have primary index");
    }

    @Override
    public RandomAccessReader openIndexReader()
    {
        throw new UnsupportedOperationException("tries do not have primary index");
    }

    @Override
    public RandomAccessReader openKeyComponentReader()
    {
        return openDataReader();
    }

    @Override
    public FileHandle getIndexFile()
    {
        throw new UnsupportedOperationException("tries do not have primary index");
    }

    private static IFilter deserializeBloomFilter(Descriptor descriptor, boolean oldBfFormat)
    {
        try (DataInputStream stream = new DataInputStream(new BufferedInputStream(Files.newInputStream(Paths.get(descriptor.filenameFor(Component.FILTER))))))
        {
            return BloomFilter.serializer.deserialize(stream, oldBfFormat);
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            logger.error("Failed to deserialize bloom filter: {}", t.getMessage());
            return null;
        }
    }

    private static IFilter recreateBloomFilter(Descriptor descriptor, TableMetadata metadata, long estimatedKeysCount, Map<MetadataType, MetadataComponent> sstableMetadata, double fpChance)
    {
        if (estimatedKeysCount <= 0)
        {
            logger.warn("Cannot recreate bloom filter, cannot estimate number of keys");
            return null;
        }

        IFilter bf = null;
        try
        {
            bf = FilterFactory.getFilter(estimatedKeysCount, fpChance);

            Factory readerFactory = descriptor.getFormat().getReaderFactory();
            try (PartitionIterator iter = (PartitionIterator) readerFactory.indexIterator(descriptor, metadata))
            {
                while (true)
                {
                    DecoratedKey key = iter.decoratedKey();
                    if (key == null)
                        break;
                    bf.add(key);
                    iter.advance();
                }
            }

            File path = descriptor.fileFor(Component.FILTER);
            try (SeekableByteChannel fos = Files.newByteChannel(path.toPath(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
                 DataOutputStreamPlus stream = new BufferedDataOutputStreamPlus(fos))
            {
                BloomFilter.serializer.serialize((BloomFilter) bf, stream);
                stream.flush();
                SyncUtil.sync((FileChannel) fos);
            }

            // Update the sstable metadata to contain the current FP chance
            ValidationMetadata validation = new ValidationMetadata(metadata.partitioner.getClass().getCanonicalName(), fpChance);
            sstableMetadata.put(MetadataType.VALIDATION, validation);
            descriptor.getMetadataSerializer().rewriteSSTableMetadata(descriptor, sstableMetadata);
            return bf;
        }
        catch (Throwable t)
        {
            if (bf != null)
            {
                bf.close();
            }

            JVMStabilityInspector.inspectThrowable(t);
            logger.error("Failed to recreate bloom filter: {}", t.getMessage());

            return null;
        }
    }

    /**
     * Load the bloom filter from Filter.db file, if it exists and if the FP chance has not changed.
     * Otherwise recreate the bloom filter using the current FP chance.
     *
     * @param sstableMetadata the sstable metadata, for extracting and changing the FP chance
     * @param fpChance        the current FP chance taken from the table metadata
     */
    @VisibleForTesting
    static @Nonnull
    IFilter getBloomFilter(Descriptor descriptor, boolean loadIfNeeded, boolean recreateIfNeeded, TableMetadata metadata, long estimatedKeysCount, Map<MetadataType, MetadataComponent> sstableMetadata, double fpChance)
    {
        if (Math.abs(1 - fpChance) <= fpChanceTolerance)
        {
            if (logger.isTraceEnabled())
                logger.trace("Returning pass-through bloom filter, FP chance is equal to 1: {}", fpChance);

            return FilterFactory.AlwaysPresent;
        }

        ValidationMetadata validation = (ValidationMetadata) sstableMetadata.get(MetadataType.VALIDATION);
        boolean fpChanged = Math.abs(fpChance - validation.bloomFilterFPChance) > fpChanceTolerance;

        if (loadIfNeeded && descriptor.fileFor(Component.FILTER).exists())
        {
            if (logger.isTraceEnabled())
                logger.trace("Deserializing bloom filter");

            IFilter bf = deserializeBloomFilter(descriptor, descriptor.version.hasOldBfFormat());
            if (bf != null)
                return bf;
        }

        String reason = fpChanged
                        ? String.format("false positive chance changed from %f to %f", validation.bloomFilterFPChance, fpChance)
                        : (!descriptor.fileFor(Component.FILTER).exists()
                           ? "there is no bloom filter file"
                           : "deserialization failed");

        if (logger.isDebugEnabled())
            logger.debug("Recreating bloom filter because {}", reason);

        IFilter bf = recreateIfNeeded
                     ? recreateBloomFilter(descriptor, metadata, estimatedKeysCount, sstableMetadata, fpChance)
                     : FilterFactory.AlwaysPresent;
        if (bf != null)
            return bf;

        logger.warn("Could not recreate or deserialize existing bloom filter, continuing with a pass-through " +
                    "bloom filter but this will significantly impact reads performance");

        return FilterFactory.AlwaysPresent;
    }

    public static boolean hasBloomFilter(double fpChance)
    {
        Preconditions.checkArgument(fpChance <= 1, "FP chance should be less or equal to 1: " + fpChance);
        return Math.abs(1 - fpChance) > fpChanceTolerance;
    }

    public static TrieIndexSSTableReader open(Descriptor descriptor, Set<Component> components, TableMetadataRef metadata, boolean validate, boolean isOffline)
    {
        checkRequiredComponents(descriptor, components, validate);

        EnumSet<MetadataType> types = EnumSet.of(MetadataType.VALIDATION, MetadataType.STATS, MetadataType.HEADER);
        Map<MetadataType, MetadataComponent> sstableMetadata;
        try
        {
            sstableMetadata = descriptor.getMetadataSerializer().deserialize(descriptor, types);
        }
        catch (IOException e)
        {
            throw new CorruptSSTableException(e, descriptor.filenameFor(Component.STATS));
        }

        ValidationMetadata validationMetadata = (ValidationMetadata) sstableMetadata.get(MetadataType.VALIDATION);
        StatsMetadata statsMetadata = (StatsMetadata) sstableMetadata.get(MetadataType.STATS);
        SerializationHeader.Component header = (SerializationHeader.Component) sstableMetadata.get(MetadataType.HEADER);

        // Check if sstable is created using same partitioner.
        // Partitioner can be null, which indicates older version of sstable or no stats available.
        // In that case, we skip the check.
        String partitionerName = metadata.get().partitioner.getClass().getCanonicalName();
        if (validationMetadata != null && !partitionerName.equals(validationMetadata.partitioner))
        {
            String msg = String.format("Cannot open %s; partitioner %s does not match system partitioner %s. " +
                                       "Note that the default partitioner starting with Cassandra 1.2 is Murmur3Partitioner, " +
                                       "so you will need to edit that to match your old partitioner if upgrading.",
                                       descriptor, validationMetadata.partitioner, partitionerName);
            logger.error("{}", msg);
            System.exit(1);
        }

        long fileLength = descriptor.filenameFor(Component.DATA).length();
        logger.debug("Opening {} ({})", descriptor, FBUtilities.prettyPrintMemory(fileLength));

        double fpChance = metadata.get().params.bloomFilterFpChance;

        FileHandle dataFH = null;
        FileHandle rowIdxFH = null;
        PartitionIndex partitionIndex = null;
        IFilter bloomFilter = null;
        boolean compressedData = descriptor.fileFor(Component.COMPRESSION_INFO).exists();

        boolean loadBFIfNeeded = components.contains(Component.FILTER);
        boolean recreatedBFIfNeeded = !isOffline;

        try (FileHandle.Builder dataFHBuilder = defaultDataHandleBuilder(descriptor).compressed(compressedData);
             @Nonnull IFilter bf = getBloomFilter(descriptor, loadBFIfNeeded, recreatedBFIfNeeded, metadata.get(), statsMetadata.totalRows, sstableMetadata, fpChance))
        {
            TrieIndexSSTableReader sstable;
            dataFH = dataFHBuilder.complete();
            bloomFilter = bf.sharedCopy();

            if (components.contains(Component.PARTITION_INDEX))
            {
                try (FileHandle.Builder partitionIdxFHBuilder = defaultIndexHandleBuilder(descriptor, Component.PARTITION_INDEX);
                     FileHandle.Builder rowIdxFHBuilder = defaultIndexHandleBuilder(descriptor, Component.ROW_INDEX))
                {
                    rowIdxFH = rowIdxFHBuilder.complete();
                    partitionIndex = PartitionIndex.load(partitionIdxFHBuilder, metadata.get().partitioner, loadBFIfNeeded && !hasBloomFilter(fpChance));
                    sstable = TrieIndexSSTableReader.internalOpen(descriptor,
                                                                  components,
                                                                  metadata,
                                                                  rowIdxFH,
                                                                  dataFH,
                                                                  partitionIndex,
                                                                  bloomFilter,
                                                                  System.currentTimeMillis(),
                                                                  statsMetadata,
                                                                  OpenReason.NORMAL,
                                                                  header.toHeader(metadata.get()));
                }
            }
            else
            {
                sstable = TrieIndexSSTableReader.internalOpen(descriptor,
                                                              components,
                                                              metadata,
                                                              dataFH,
                                                              bloomFilter,
                                                              System.currentTimeMillis(),
                                                              statsMetadata,
                                                              OpenReason.NORMAL,
                                                              header.toHeader(metadata.get()));
            }
            if (validate)
                sstable.validate();
            sstable.setup(!isOffline); // this should come last, right before returning sstable
            return sstable;
        }
        catch (RuntimeException e)
        {
            throw Throwables.cleaned(Throwables.close(e, bloomFilter, rowIdxFH, partitionIndex, dataFH));
        }
        catch (IOException e)
        {
            throw new CorruptSSTableException(Throwables.close(e, bloomFilter, rowIdxFH, partitionIndex, dataFH), descriptor.filenameFor(Component.DATA));
        }
    }
}
