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

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.IVerifier;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.io.sstable.SSTableReadsListener.SelectionReason;
import org.apache.cassandra.io.sstable.SSTableReadsListener.SkippingReason;
import org.apache.cassandra.io.sstable.format.SSTableReaderWithFilter;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.IFilter;
import org.apache.cassandra.utils.OutputHandler;

import static org.apache.cassandra.io.sstable.format.SSTableReader.Operator.EQ;
import static org.apache.cassandra.io.sstable.format.SSTableReader.Operator.GE;
import static org.apache.cassandra.io.sstable.format.SSTableReader.Operator.GT;
import static org.apache.cassandra.utils.concurrent.SharedCloseable.sharedCopyOrNull;

/**
 * Reader of SSTable files in BTI format (see {@link BtiFormat}), written by {@link BtiTableWriter}.
 * <p>
 * SSTableReaders are open()ed by Keyspace.onStart; after that they are created by SSTableWriter.renameAndOpen.
 * Do not re-call open() on existing SSTable files; use the references kept by ColumnFamilyStore post-start instead.
 */
public class BtiTableReader extends SSTableReaderWithFilter
{
    private final FileHandle rowIndexFile;
    private final PartitionIndex partitionIndex;

    public BtiTableReader(Builder builder, SSTable.Owner owner)
    {
        super(builder, owner);
        this.rowIndexFile = builder.getRowIndexFile();
        this.partitionIndex = builder.getPartitionIndex();
    }

    protected final Builder unbuildTo(Builder builder, boolean sharedCopy)
    {
        Builder b = super.unbuildTo(builder, sharedCopy);
        if (builder.getPartitionIndex() == null)
            b.setPartitionIndex(sharedCopy ? sharedCopyOrNull(partitionIndex) : partitionIndex);
        if (builder.getRowIndexFile() == null)
            b.setRowIndexFile(sharedCopy ? sharedCopyOrNull(rowIndexFile) : rowIndexFile);

        return b;
    }

    @Override
    protected List<AutoCloseable> setupInstance(boolean trackHotness)
    {
        ArrayList<AutoCloseable> closeables = Lists.newArrayList(rowIndexFile, partitionIndex);
        closeables.addAll(super.setupInstance(trackHotness));
        return closeables;
    }

    /**
     * Whether to filter out data before {@link #first}. Needed for sources of data in a compaction, where the relevant
     * output is opened early -- in this case the sstable's start is changed, but the data can still be found in the
     * file. Range and point queries must filter it out.
     */
    protected boolean filterFirst()
    {
        return openReason == OpenReason.MOVED_START;
    }

    /**
     * Whether to filter out data after {@link #last}. Early-open sstables may contain data beyond the switch point
     * (because an early-opened sstable is not ready until buffers have been flushed), and leaving that data visible
     * will give a redundant copy with all associated overheads.
     */
    protected boolean filterLast()
    {
        return openReason == OpenReason.EARLY && partitionIndex instanceof PartitionIndexEarly;
    }

    public long estimatedKeys()
    {
        return partitionIndex == null ? 0 : partitionIndex.size();
    }

    @Override
    protected TrieIndexEntry getRowIndexEntry(PartitionPosition key,
                                              Operator operator,
                                              boolean updateStats,
                                              SSTableReadsListener listener)
    {
        PartitionPosition searchKey;
        Operator searchOp;

        if (operator == EQ)
            return getExactPosition((DecoratedKey) key, listener, updateStats);

        if (operator == GT || operator == GE)
        {
            if (filterLast() && getLast().compareTo(key) < 0)
            {
                notifySkipped(SkippingReason.MIN_MAX_KEYS, listener, operator, updateStats);
                return null;
            }
            boolean filteredLeft = (filterFirst() && getFirst().compareTo(key) > 0);
            searchKey = filteredLeft ? getFirst() : key;
            searchOp = filteredLeft ? GE : operator;

            try (PartitionIndex.Reader reader = partitionIndex.openReader())
            {
                TrieIndexEntry rie = reader.ceiling(searchKey, (pos, assumeNoMatch, compareKey) -> retrieveEntryIfAcceptable(searchOp, compareKey, pos, assumeNoMatch));
                if (rie != null)
                    notifySelected(SelectionReason.INDEX_ENTRY_FOUND, listener, operator, updateStats, rie);
                else
                    notifySkipped(SkippingReason.INDEX_ENTRY_NOT_FOUND, listener, operator, updateStats);
                return rie;
            }
            catch (IOException e)
            {
                markSuspect();
                throw new CorruptSSTableException(e, rowIndexFile.path());
            }
        }

        throw new IllegalArgumentException("Invalid op: " + operator);
    }

    /**
     * Called by {@link #getRowIndexEntry} above (via Reader.ceiling/floor) to check if the position satisfies the full
     * key constraint. This is called once if there is a prefix match (which can be in any relationship with the sought
     * key, thus assumeNoMatch: false), and if it returns null it is called again for the closest greater position
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
                return TrieIndexEntry.deserialize(in, in.getFilePointer(), descriptor.version);
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

    TrieIndexEntry getExactPosition(DecoratedKey dk,
                                    SSTableReadsListener listener,
                                    boolean updateStats)
    {
        if ((filterFirst() && getFirst().compareTo(dk) > 0) || (filterLast() && getLast().compareTo(dk) < 0))
        {
            notifySkipped(SkippingReason.MIN_MAX_KEYS, listener, EQ, updateStats);
            return null;
        }

        if (!isPresentInFilter(dk))
        {
            notifySkipped(SkippingReason.BLOOM_FILTER, listener, EQ, updateStats);
            return null;
        }

        try (PartitionIndex.Reader reader = partitionIndex.openReader())
        {
            long indexPos = reader.exactCandidate(dk);
            if (indexPos == PartitionIndex.NOT_FOUND)
            {
                notifySkipped(SkippingReason.PARTITION_INDEX_LOOKUP, listener, EQ, updateStats);
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
                if (ByteBufferUtil.equalsWithShortLength(in, dk.getKey()))
                {
                    TrieIndexEntry rie = indexPos >= 0 ? TrieIndexEntry.deserialize(in, in.getFilePointer(), descriptor.version)
                                                       : new TrieIndexEntry(~indexPos);
                    notifySelected(SelectionReason.INDEX_ENTRY_FOUND, listener, EQ, updateStats, rie);
                    return rie;
                }
                else
                {
                    notifySkipped(SkippingReason.INDEX_ENTRY_NOT_FOUND, listener, EQ, updateStats);
                    return null;
                }
            }
        }
        catch (IOException | IllegalArgumentException | ArrayIndexOutOfBoundsException | AssertionError e)
        {
            markSuspect();
            throw new CorruptSSTableException(e, rowIndexFile.path());
        }
    }

    /**
     * Create a PartitionIterator listing all partitions within the given bounds.
     * This method relies on its caller to prepare the bounds correctly.
     *
     * @param bounds A range of keys. Must not be a wraparound range, and will not be checked against
     *               the sstable's bounds (i.e. this will return data before a moved start or after an early-open limit)
     */
    PartitionIterator coveredKeysIterator(AbstractBounds<PartitionPosition> bounds) throws IOException
    {
        return PartitionIterator.create(partitionIndex,
                                        metadata().partitioner,
                                        rowIndexFile,
                                        dfile,
                                        bounds.left, bounds.inclusiveLeft() ? -1 : 0,
                                        bounds.right, bounds.inclusiveRight() ? 0 : -1,
                                        descriptor.version);
    }

    public ScrubPartitionIterator scrubPartitionsIterator() throws IOException
    {
        return new ScrubIterator(partitionIndex, rowIndexFile, descriptor.version);
    }

    @Override
    public PartitionIterator keyReader() throws IOException
    {
        return PartitionIterator.create(partitionIndex, metadata().partitioner, rowIndexFile, dfile, descriptor.version);
    }

    @Override
    public Iterable<DecoratedKey> getKeySamples(final Range<Token> range)
    {
        // BTI does not support key sampling as it would involve walking the index or data file.
        // Validator has an alternate solution for empty key sample lists.
        return Collections.emptyList();
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
            if (left.compareTo(getFirst()) <= 0)
                left = null;
            else if (left.compareTo(getLast()) > 0)
                continue;   // no intersection

            PartitionPosition right = range.right.minKeyBound();
            if (range.right.isMinimum() || right.compareTo(getLast()) >= 0)
                right = null;
            else if (right.compareTo(getFirst()) < 0)
                continue;   // no intersection

            if (left == null && right == null)
                return partitionIndex.size();   // sstable is fully covered, return full partition count to avoid rounding errors

            if (left == null && filterFirst())
                left = getFirst();
            if (right == null && filterLast())
                right = getLast();

            long startPos = left != null ? getPosition(left, GE) : 0;
            long endPos = right != null ? getPosition(right, GE) : uncompressedLength();
            selectedDataSize += endPos - startPos;
        }
        return Math.round(selectedDataSize / sstableMetadata.estimatedPartitionSize.rawMean());
    }


    @Override
    public UnfilteredRowIterator rowIterator(DecoratedKey key,
                                             Slices slices,
                                             ColumnFilter selectedColumns,
                                             boolean reversed,
                                             SSTableReadsListener listener)
    {
        return rowIterator(null, key, getExactPosition(key, listener, true), slices, selectedColumns, reversed);
    }

    public UnfilteredRowIterator rowIterator(FileDataInput dataFileInput,
                                             DecoratedKey key,
                                             TrieIndexEntry indexEntry,
                                             Slices slices,
                                             ColumnFilter selectedColumns,
                                             boolean reversed)
    {
        if (indexEntry == null)
            return UnfilteredRowIterators.noRowsIterator(metadata(), key, Rows.EMPTY_STATIC_ROW, DeletionTime.LIVE, reversed);

        if (reversed)
            return new SSTableReversedIterator(this, dataFileInput, key, indexEntry, slices, selectedColumns, rowIndexFile);
        else
            return new SSTableIterator(this, dataFileInput, key, indexEntry, slices, selectedColumns, rowIndexFile);
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

    @VisibleForTesting
    @Override
    public BtiTableReader cloneAndReplace(IFilter filter)
    {
        return unbuildTo(new Builder(descriptor).setFilter(filter), true).build(owner().orElse(null), true, true);
    }

    @Override
    public BtiTableReader cloneWithRestoredStart(DecoratedKey restoredStart)
    {
        return runWithLock(ignored -> cloneAndReplace(restoredStart, OpenReason.NORMAL));
    }

    @Override
    public BtiTableReader cloneWithNewStart(DecoratedKey newStart)
    {
        return runWithLock(d -> {
            assert openReason != OpenReason.EARLY : "Cannot open early an early-open SSTable";
            if (newStart.compareTo(getFirst()) > 0)
            {
                final long dataStart = getPosition(newStart, Operator.EQ);
                runOnClose(() -> dfile.dropPageCache(dataStart));
            }

            return cloneAndReplace(newStart, OpenReason.MOVED_START);
        });
    }

    /**
     * Clone this reader with the provided start and open reason, and set the clone as replacement.
     *
     * @param newFirst the first key for the replacement (which can be different from the original due to the pre-emptive
     *                 opening of compaction results).
     * @param reason   the {@code OpenReason} for the replacement.
     * @return the cloned reader. That reader is set as a replacement by the method.
     */
    private BtiTableReader cloneAndReplace(DecoratedKey newFirst, OpenReason reason)
    {
        return unbuildTo(new Builder(descriptor), true)
               .setFirst(newFirst)
               .setOpenReason(reason)
               .build(owner().orElse(null), true, true);
    }

    @Override
    public DecoratedKey firstKeyBeyond(PartitionPosition token)
    {
        try
        {
            TrieIndexEntry pos = getRowIndexEntry(token, Operator.GT, true, SSTableReadsListener.NOOP_LISTENER);
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
    public void releaseInMemoryComponents()
    {
        closeInternalComponent(partitionIndex);
    }

    @Override
    public boolean isEstimationInformative()
    {
        return true;
    }

    @Override
    public UnfilteredPartitionIterator partitionIterator(ColumnFilter columnFilter, DataRange dataRange, SSTableReadsListener listener)
    {
        return BtiTableScanner.getScanner(this, columnFilter, dataRange, listener);
    }

    @Override
    public IVerifier getVerifier(ColumnFamilyStore cfs, OutputHandler outputHandler, boolean isOffline, IVerifier.Options options)
    {
        Preconditions.checkArgument(cfs.metadata().equals(metadata()));
        return new BtiTableVerifier(cfs, this, outputHandler, isOffline, options);
    }

    public static class Builder extends SSTableReaderWithFilter.Builder<BtiTableReader, Builder>
    {
        private PartitionIndex partitionIndex;
        private FileHandle rowIndexFile;

        public Builder(Descriptor descriptor)
        {
            super(descriptor);
        }

        public Builder setRowIndexFile(FileHandle rowIndexFile)
        {
            this.rowIndexFile = rowIndexFile;
            return this;
        }

        public Builder setPartitionIndex(PartitionIndex partitionIndex)
        {
            this.partitionIndex = partitionIndex;
            return this;
        }

        public PartitionIndex getPartitionIndex()
        {
            return partitionIndex;
        }

        public FileHandle getRowIndexFile()
        {
            return rowIndexFile;
        }

        @Override
        protected BtiTableReader buildInternal(Owner owner)
        {
            return new BtiTableReader(this, owner);
        }
    }
}
