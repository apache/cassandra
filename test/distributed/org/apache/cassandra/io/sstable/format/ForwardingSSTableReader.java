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

package org.apache.cassandra.io.sstable.format;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.google.common.util.concurrent.RateLimiter;

import org.apache.cassandra.cache.InstrumentingCache;
import org.apache.cassandra.cache.KeyCacheKey;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.metrics.RestorableMeter;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.IFilter;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.Ref;

public abstract class ForwardingSSTableReader extends SSTableReader
{
    // This method is only accessiable via extension and not for calling directly;
    // to work around this, rely on reflection if the method gets called
    private static final Method ESTIMATE_ROWS_FROM_INDEX;

    static
    {
        try
        {
            Method m = SSTable.class.getDeclaredMethod("estimateRowsFromIndex", RandomAccessReader.class);
            m.setAccessible(true);
            ESTIMATE_ROWS_FROM_INDEX = m;
        }
        catch (NoSuchMethodException e)
        {
            throw new AssertionError(e);
        }
    }

    private final SSTableReader delegate;

    public ForwardingSSTableReader(SSTableReader delegate)
    {
        super(delegate.descriptor, SSTable.componentsFor(delegate.descriptor),
              delegate.metadata, delegate.maxDataAge, delegate.getSSTableMetadata(),
              delegate.openReason, delegate.header);
        this.delegate = delegate;
        this.first = delegate.first;
        this.last = delegate.last;
    }

    @Override
    public boolean equals(Object that)
    {
        return delegate.equals(that);
    }

    @Override
    public int hashCode()
    {
        return delegate.hashCode();
    }

    @Override
    public String getFilename()
    {
        return delegate.getFilename();
    }

    @Override
    public void setupOnline()
    {
        delegate.setupOnline();
    }

    @Override
    public boolean isKeyCacheSetup()
    {
        return delegate.isKeyCacheSetup();
    }

    @Override
    public boolean loadSummary()
    {
        return delegate.loadSummary();
    }

    @Override
    public void saveSummary()
    {
        delegate.saveSummary();
    }

    @Override
    public void saveBloomFilter()
    {
        delegate.saveBloomFilter();
    }

    @Override
    public void setReplaced()
    {
        delegate.setReplaced();
    }

    @Override
    public boolean isReplaced()
    {
        return delegate.isReplaced();
    }

    @Override
    public void runOnClose(Runnable runOnClose)
    {
        delegate.runOnClose(runOnClose);
    }

    @Override
    public SSTableReader cloneWithRestoredStart(DecoratedKey restoredStart)
    {
        return delegate.cloneWithRestoredStart(restoredStart);
    }

    @Override
    public SSTableReader cloneWithNewStart(DecoratedKey newStart, Runnable runOnClose)
    {
        return delegate.cloneWithNewStart(newStart, runOnClose);
    }

    @Override
    public SSTableReader cloneWithNewSummarySamplingLevel(ColumnFamilyStore parent, int samplingLevel) throws IOException
    {
        return delegate.cloneWithNewSummarySamplingLevel(parent, samplingLevel);
    }

    @Override
    public RestorableMeter getReadMeter()
    {
        return delegate.getReadMeter();
    }

    @Override
    public int getIndexSummarySamplingLevel()
    {
        return delegate.getIndexSummarySamplingLevel();
    }

    @Override
    public long getIndexSummaryOffHeapSize()
    {
        return delegate.getIndexSummaryOffHeapSize();
    }

    @Override
    public int getMinIndexInterval()
    {
        return delegate.getMinIndexInterval();
    }

    @Override
    public double getEffectiveIndexInterval()
    {
        return delegate.getEffectiveIndexInterval();
    }

    @Override
    public void releaseSummary()
    {
        delegate.releaseSummary();
    }

    @Override
    public long getIndexScanPosition(PartitionPosition key)
    {
        return delegate.getIndexScanPosition(key);
    }

    @Override
    public CompressionMetadata getCompressionMetadata()
    {
        return delegate.getCompressionMetadata();
    }

    @Override
    public long getCompressionMetadataOffHeapSize()
    {
        return delegate.getCompressionMetadataOffHeapSize();
    }

    @Override
    public void forceFilterFailures()
    {
        delegate.forceFilterFailures();
    }

    @Override
    public IFilter getBloomFilter()
    {
        return delegate.getBloomFilter();
    }

    @Override
    public long getBloomFilterSerializedSize()
    {
        return delegate.getBloomFilterSerializedSize();
    }

    @Override
    public long getBloomFilterOffHeapSize()
    {
        return delegate.getBloomFilterOffHeapSize();
    }

    @Override
    public long estimatedKeys()
    {
        return delegate.estimatedKeys();
    }

    @Override
    public long estimatedKeysForRanges(Collection<Range<Token>> ranges)
    {
        return delegate.estimatedKeysForRanges(ranges);
    }

    @Override
    public int getIndexSummarySize()
    {
        return delegate.getIndexSummarySize();
    }

    @Override
    public int getMaxIndexSummarySize()
    {
        return delegate.getMaxIndexSummarySize();
    }

    @Override
    public byte[] getIndexSummaryKey(int index)
    {
        return delegate.getIndexSummaryKey(index);
    }

    @Override
    public Iterable<DecoratedKey> getKeySamples(Range<Token> range)
    {
        return delegate.getKeySamples(range);
    }

    @Override
    public List<Pair<Long, Long>> getPositionsForRanges(Collection<Range<Token>> ranges)
    {
        return delegate.getPositionsForRanges(ranges);
    }

    @Override
    public KeyCacheKey getCacheKey(DecoratedKey key)
    {
        return delegate.getCacheKey(key);
    }

    @Override
    public void cacheKey(DecoratedKey key, RowIndexEntry info)
    {
        delegate.cacheKey(key, info);
    }

    @Override
    public RowIndexEntry getCachedPosition(DecoratedKey key, boolean updateStats)
    {
        return delegate.getCachedPosition(key, updateStats);
    }

    @Override
    protected RowIndexEntry getCachedPosition(KeyCacheKey unifiedKey, boolean updateStats)
    {
        return delegate.getCachedPosition(unifiedKey, updateStats);
    }

    @Override
    protected RowIndexEntry getPosition(PartitionPosition key, Operator op, boolean updateCacheAndStats, boolean permitMatchPastLast, SSTableReadsListener listener)
    {
        return delegate.getPosition(key, op, updateCacheAndStats, permitMatchPastLast, listener);
    }

    @Override
    public UnfilteredRowIterator iterator(DecoratedKey key, Slices slices, ColumnFilter selectedColumns, boolean reversed, boolean isForThrift, SSTableReadsListener listener)
    {
        return delegate.iterator(key, slices, selectedColumns, reversed, isForThrift, listener);
    }

    @Override
    public UnfilteredRowIterator iterator(FileDataInput file, DecoratedKey key, RowIndexEntry indexEntry, Slices slices, ColumnFilter selectedColumns, boolean reversed, boolean isForThrift)
    {
        return delegate.iterator(file, key, indexEntry, slices, selectedColumns, reversed, isForThrift);
    }

    @Override
    public UnfilteredRowIterator simpleIterator(FileDataInput file, DecoratedKey key, RowIndexEntry indexEntry, boolean tombstoneOnly)
    {
        return delegate.simpleIterator(file, key, indexEntry, tombstoneOnly);
    }

    @Override
    public DecoratedKey firstKeyBeyond(PartitionPosition token)
    {
        return delegate.firstKeyBeyond(token);
    }

    @Override
    public long uncompressedLength()
    {
        return delegate.uncompressedLength();
    }

    @Override
    public long onDiskLength()
    {
        return delegate.onDiskLength();
    }

    @Override
    public double getCrcCheckChance()
    {
        return delegate.getCrcCheckChance();
    }

    @Override
    public void setCrcCheckChance(double crcCheckChance)
    {
        delegate.setCrcCheckChance(crcCheckChance);
    }

    @Override
    public void markObsolete(Runnable tidier)
    {
        delegate.markObsolete(tidier);
    }

    @Override
    public boolean isMarkedCompacted()
    {
        return delegate.isMarkedCompacted();
    }

    @Override
    public void markSuspect()
    {
        delegate.markSuspect();
    }

    @Override
    public boolean isMarkedSuspect()
    {
        return delegate.isMarkedSuspect();
    }

    @Override
    public ISSTableScanner getScanner()
    {
        return delegate.getScanner();
    }

    @Override
    public ISSTableScanner getScanner(ColumnFilter columns, DataRange dataRange, boolean isForThrift, SSTableReadsListener listener)
    {
        return delegate.getScanner(columns, dataRange, isForThrift, listener);
    }

    @Override
    public ISSTableScanner getScanner(Range<Token> range, RateLimiter limiter)
    {
        return delegate.getScanner(range, limiter);
    }

    @Override
    public ISSTableScanner getScanner(RateLimiter limiter)
    {
        return delegate.getScanner(limiter);
    }

    @Override
    public ISSTableScanner getScanner(Collection<Range<Token>> ranges, RateLimiter limiter)
    {
        return delegate.getScanner(ranges, limiter);
    }

    @Override
    public ISSTableScanner getScanner(Iterator<AbstractBounds<PartitionPosition>> rangeIterator)
    {
        return delegate.getScanner(rangeIterator);
    }

    @Override
    public ISSTableScanner getScanner(ColumnFilter columns, DataRange dataRange, RateLimiter limiter, boolean isForThrift, SSTableReadsListener listener)
    {
        return delegate.getScanner(columns, dataRange, limiter, isForThrift, listener);
    }

    @Override
    public FileDataInput getFileDataInput(long position)
    {
        return delegate.getFileDataInput(position);
    }

    @Override
    public boolean newSince(long age)
    {
        return delegate.newSince(age);
    }

    @Override
    public void createLinks(String snapshotDirectoryPath)
    {
        delegate.createLinks(snapshotDirectoryPath);
    }

    @Override
    public boolean isRepaired()
    {
        return delegate.isRepaired();
    }

    @Override
    public DecoratedKey keyAt(long indexPosition) throws IOException
    {
        return delegate.keyAt(indexPosition);
    }

    @Override
    public long getBloomFilterFalsePositiveCount()
    {
        return delegate.getBloomFilterFalsePositiveCount();
    }

    @Override
    public long getRecentBloomFilterFalsePositiveCount()
    {
        return delegate.getRecentBloomFilterFalsePositiveCount();
    }

    @Override
    public long getBloomFilterTruePositiveCount()
    {
        return delegate.getBloomFilterTruePositiveCount();
    }

    @Override
    public long getRecentBloomFilterTruePositiveCount()
    {
        return delegate.getRecentBloomFilterTruePositiveCount();
    }

    @Override
    public InstrumentingCache<KeyCacheKey, RowIndexEntry> getKeyCache()
    {
        return delegate.getKeyCache();
    }

    @Override
    public EstimatedHistogram getEstimatedPartitionSize()
    {
        return delegate.getEstimatedPartitionSize();
    }

    @Override
    public EstimatedHistogram getEstimatedColumnCount()
    {
        return delegate.getEstimatedColumnCount();
    }

    @Override
    public double getEstimatedDroppableTombstoneRatio(int gcBefore)
    {
        return delegate.getEstimatedDroppableTombstoneRatio(gcBefore);
    }

    @Override
    public double getDroppableTombstonesBefore(int gcBefore)
    {
        return delegate.getDroppableTombstonesBefore(gcBefore);
    }

    @Override
    public double getCompressionRatio()
    {
        return delegate.getCompressionRatio();
    }

    @Override
    public long getMinTimestamp()
    {
        return delegate.getMinTimestamp();
    }

    @Override
    public long getMaxTimestamp()
    {
        return delegate.getMaxTimestamp();
    }

    @Override
    public int getMinLocalDeletionTime()
    {
        return delegate.getMinLocalDeletionTime();
    }

    @Override
    public int getMaxLocalDeletionTime()
    {
        return delegate.getMaxLocalDeletionTime();
    }

    @Override
    public boolean mayHaveTombstones()
    {
        return delegate.mayHaveTombstones();
    }

    @Override
    public int getMinTTL()
    {
        return delegate.getMinTTL();
    }

    @Override
    public int getMaxTTL()
    {
        return delegate.getMaxTTL();
    }

    @Override
    public long getTotalColumnsSet()
    {
        return delegate.getTotalColumnsSet();
    }

    @Override
    public long getTotalRows()
    {
        return delegate.getTotalRows();
    }

    @Override
    public int getAvgColumnSetPerRow()
    {
        return delegate.getAvgColumnSetPerRow();
    }

    @Override
    public int getSSTableLevel()
    {
        return delegate.getSSTableLevel();
    }

    @Override
    public void reloadSSTableMetadata() throws IOException
    {
        delegate.reloadSSTableMetadata();
    }

    @Override
    public StatsMetadata getSSTableMetadata()
    {
        return delegate.getSSTableMetadata();
    }

    @Override
    public RandomAccessReader openDataReader(RateLimiter limiter)
    {
        return delegate.openDataReader(limiter);
    }

    @Override
    public RandomAccessReader openDataReader()
    {
        return delegate.openDataReader();
    }

    @Override
    public RandomAccessReader openIndexReader()
    {
        return delegate.openIndexReader();
    }

    @Override
    public ChannelProxy getDataChannel()
    {
        return delegate.getDataChannel();
    }

    @Override
    public ChannelProxy getIndexChannel()
    {
        return delegate.getIndexChannel();
    }

    @Override
    public FileHandle getIndexFile()
    {
        return delegate.getIndexFile();
    }

    @Override
    public long getCreationTimeFor(Component component)
    {
        return delegate.getCreationTimeFor(component);
    }

    @Override
    public long getKeyCacheHit()
    {
        return delegate.getKeyCacheHit();
    }

    @Override
    public long getKeyCacheRequest()
    {
        return delegate.getKeyCacheRequest();
    }

    @Override
    public void incrementReadCount()
    {
        delegate.incrementReadCount();
    }

    @Override
    public EncodingStats stats()
    {
        return delegate.stats();
    }

    @Override
    public Ref<SSTableReader> tryRef()
    {
        return delegate.tryRef();
    }

    @Override
    public Ref<SSTableReader> selfRef()
    {
        return delegate.selfRef();
    }

    @Override
    public Ref<SSTableReader> ref()
    {
        return delegate.ref();
    }

    @Override
    void setup(boolean trackHotness)
    {
        delegate.setup(trackHotness);
    }

    @Override
    public void overrideReadMeter(RestorableMeter readMeter)
    {
        delegate.overrideReadMeter(readMeter);
    }

    @Override
    public void addTo(Ref.IdentityCollection identities)
    {
        delegate.addTo(identities);
    }

    @Override
    public IPartitioner getPartitioner()
    {
        return delegate.getPartitioner();
    }

    @Override
    public DecoratedKey decorateKey(ByteBuffer key)
    {
        return delegate.decorateKey(key);
    }

    @Override
    public String getIndexFilename()
    {
        return delegate.getIndexFilename();
    }

    @Override
    public String getColumnFamilyName()
    {
        return delegate.getColumnFamilyName();
    }

    @Override
    public String getKeyspaceName()
    {
        return delegate.getKeyspaceName();
    }

    @Override
    public List<String> getAllFilePaths()
    {
        return delegate.getAllFilePaths();
    }

    @Override
    protected long estimateRowsFromIndex(RandomAccessReader ifile) throws IOException
    {
        try
        {
            return (Long) ESTIMATE_ROWS_FROM_INDEX.invoke(delegate, ifile);
        }
        catch (IllegalAccessException e)
        {
            throw new AssertionError(e);
        }
        catch (InvocationTargetException e)
        {
            Throwable cause = e.getCause();
            if (cause instanceof IOException)
                throw (IOException) cause;
            if (cause instanceof Error)
                throw (Error) cause;
            if (cause instanceof RuntimeException)
                throw (RuntimeException) cause;
            throw new RuntimeException(cause);
        }
    }

    @Override
    public long bytesOnDisk()
    {
        return delegate.bytesOnDisk();
    }

    @Override
    public String toString()
    {
        return delegate.toString();
    }

    @Override
    public void addComponents(Collection<Component> newComponents)
    {
        delegate.addComponents(newComponents);
    }
}