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
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

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
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.IFilter;
import org.apache.cassandra.utils.concurrent.Ref;

public abstract class ForwardingSSTableReader extends SSTableReader
{
    private final SSTableReader delegate;
    private final Ref<SSTableReader> selfRef;

    public ForwardingSSTableReader(SSTableReader delegate)
    {
        super(delegate.descriptor, SSTable.componentsFor(delegate.descriptor),
              TableMetadataRef.forOfflineTools(delegate.metadata()), delegate.maxDataAge, delegate.getSSTableMetadata(),
              delegate.openReason, delegate.header);
        this.delegate = delegate;
        this.first = delegate.first;
        this.last = delegate.last;
        this.selfRef = new Ref<>(this, new Tidy()
        {
            public void tidy() throws Exception
            {
                Ref<SSTableReader> ref = delegate.tryRef();
                if (ref != null)
                    ref.release();
            }

            public String name()
            {
                return descriptor.toString();
            }
        });
    }

    protected RowIndexEntry getPosition(PartitionPosition key, Operator op, boolean updateCacheAndStats, boolean permitMatchPastLast, SSTableReadsListener listener)
    {
        return delegate.getPosition(key, op, updateCacheAndStats, permitMatchPastLast, listener);
    }

    public UnfilteredRowIterator iterator(DecoratedKey key, Slices slices, ColumnFilter selectedColumns, boolean reversed, SSTableReadsListener listener)
    {
        return delegate.iterator(key, slices, selectedColumns, reversed, listener);
    }

    public UnfilteredRowIterator iterator(FileDataInput file, DecoratedKey key, RowIndexEntry indexEntry, Slices slices, ColumnFilter selectedColumns, boolean reversed)
    {
        return delegate.iterator(file, key, indexEntry, slices, selectedColumns, reversed);
    }

    public UnfilteredRowIterator simpleIterator(FileDataInput file, DecoratedKey key, RowIndexEntry indexEntry, boolean tombstoneOnly)
    {
        return delegate.simpleIterator(file, key, indexEntry, tombstoneOnly);
    }

    public ISSTableScanner getScanner()
    {
        return delegate.getScanner();
    }

    public ISSTableScanner getScanner(Collection<Range<Token>> ranges)
    {
        return delegate.getScanner(ranges);
    }

    public ISSTableScanner getScanner(Iterator<AbstractBounds<PartitionPosition>> rangeIterator)
    {
        return delegate.getScanner(rangeIterator);
    }

    public ISSTableScanner getScanner(ColumnFilter columns, DataRange dataRange, SSTableReadsListener listener)
    {
        return delegate.getScanner(columns, dataRange, listener);
    }

    public void setupOnline()
    {
        delegate.setupOnline();
    }

    public String getFilename()
    {
        return delegate.getFilename();
    }

    public boolean equals(Object that)
    {
        return delegate.equals(that);
    }

    public int hashCode()
    {
        return delegate.hashCode();
    }

    public boolean loadSummary()
    {
        return delegate.loadSummary();
    }

    public void saveSummary()
    {
        delegate.saveSummary();
    }

    public void saveBloomFilter()
    {
        delegate.saveBloomFilter();
    }

    public void setReplaced()
    {
        delegate.setReplaced();
    }

    public boolean isReplaced()
    {
        return delegate.isReplaced();
    }

    public void runOnClose(Runnable runOnClose)
    {
        delegate.runOnClose(runOnClose);
    }

    public SSTableReader cloneWithRestoredStart(DecoratedKey restoredStart)
    {
        return delegate.cloneWithRestoredStart(restoredStart);
    }

    public SSTableReader cloneWithNewStart(DecoratedKey newStart, Runnable runOnClose)
    {
        return delegate.cloneWithNewStart(newStart, runOnClose);
    }

    public SSTableReader cloneWithNewSummarySamplingLevel(ColumnFamilyStore parent, int samplingLevel) throws IOException
    {
        return delegate.cloneWithNewSummarySamplingLevel(parent, samplingLevel);
    }

    public RestorableMeter getReadMeter()
    {
        return delegate.getReadMeter();
    }

    public int getIndexSummarySamplingLevel()
    {
        return delegate.getIndexSummarySamplingLevel();
    }

    public long getIndexSummaryOffHeapSize()
    {
        return delegate.getIndexSummaryOffHeapSize();
    }

    public int getMinIndexInterval()
    {
        return delegate.getMinIndexInterval();
    }

    public double getEffectiveIndexInterval()
    {
        return delegate.getEffectiveIndexInterval();
    }

    public void releaseSummary()
    {
        delegate.releaseSummary();
    }

    public long getIndexScanPosition(PartitionPosition key)
    {
        return delegate.getIndexScanPosition(key);
    }

    public CompressionMetadata getCompressionMetadata()
    {
        return delegate.getCompressionMetadata();
    }

    public long getCompressionMetadataOffHeapSize()
    {
        return delegate.getCompressionMetadataOffHeapSize();
    }

    public void forceFilterFailures()
    {
        delegate.forceFilterFailures();
    }

    public IFilter getBloomFilter()
    {
        return delegate.getBloomFilter();
    }

    public long getBloomFilterSerializedSize()
    {
        return delegate.getBloomFilterSerializedSize();
    }

    public long getBloomFilterOffHeapSize()
    {
        return delegate.getBloomFilterOffHeapSize();
    }

    public long estimatedKeys()
    {
        return delegate.estimatedKeys();
    }

    public long estimatedKeysForRanges(Collection<Range<Token>> ranges)
    {
        return delegate.estimatedKeysForRanges(ranges);
    }

    public int getIndexSummarySize()
    {
        return delegate.getIndexSummarySize();
    }

    public int getMaxIndexSummarySize()
    {
        return delegate.getMaxIndexSummarySize();
    }

    public byte[] getIndexSummaryKey(int index)
    {
        return delegate.getIndexSummaryKey(index);
    }

    public Iterable<DecoratedKey> getKeySamples(Range<Token> range)
    {
        return delegate.getKeySamples(range);
    }

    public List<PartitionPositionBounds> getPositionsForRanges(Collection<Range<Token>> ranges)
    {
        return delegate.getPositionsForRanges(ranges);
    }

    public KeyCacheKey getCacheKey(DecoratedKey key)
    {
        return delegate.getCacheKey(key);
    }

    public void cacheKey(DecoratedKey key, RowIndexEntry info)
    {
        delegate.cacheKey(key, info);
    }

    public RowIndexEntry getCachedPosition(DecoratedKey key, boolean updateStats)
    {
        return delegate.getCachedPosition(key, updateStats);
    }

    protected RowIndexEntry getCachedPosition(KeyCacheKey unifiedKey, boolean updateStats)
    {
        return delegate.getCachedPosition(unifiedKey, updateStats);
    }

    public boolean isKeyCacheEnabled()
    {
        return delegate.isKeyCacheEnabled();
    }

    public DecoratedKey firstKeyBeyond(PartitionPosition token)
    {
        return delegate.firstKeyBeyond(token);
    }

    public long uncompressedLength()
    {
        return delegate.uncompressedLength();
    }

    public long onDiskLength()
    {
        return delegate.onDiskLength();
    }

    public double getCrcCheckChance()
    {
        return delegate.getCrcCheckChance();
    }

    public void setCrcCheckChance(double crcCheckChance)
    {
        delegate.setCrcCheckChance(crcCheckChance);
    }

    public void markObsolete(Runnable tidier)
    {
        delegate.markObsolete(tidier);
    }

    public boolean isMarkedCompacted()
    {
        return delegate.isMarkedCompacted();
    }

    public void markSuspect()
    {
        delegate.markSuspect();
    }

    public void unmarkSuspect()
    {
        delegate.unmarkSuspect();
    }

    public boolean isMarkedSuspect()
    {
        return delegate.isMarkedSuspect();
    }

    public ISSTableScanner getScanner(Range<Token> range)
    {
        return delegate.getScanner(range);
    }

    public FileDataInput getFileDataInput(long position)
    {
        return delegate.getFileDataInput(position);
    }

    public boolean newSince(long age)
    {
        return delegate.newSince(age);
    }

    public void createLinks(String snapshotDirectoryPath)
    {
        delegate.createLinks(snapshotDirectoryPath);
    }

    public boolean isRepaired()
    {
        return delegate.isRepaired();
    }

    public DecoratedKey keyAt(long indexPosition) throws IOException
    {
        return delegate.keyAt(indexPosition);
    }

    public boolean isPendingRepair()
    {
        return delegate.isPendingRepair();
    }

    public UUID getPendingRepair()
    {
        return delegate.getPendingRepair();
    }

    public long getRepairedAt()
    {
        return delegate.getRepairedAt();
    }

    public boolean isTransient()
    {
        return delegate.isTransient();
    }

    public boolean intersects(Collection<Range<Token>> ranges)
    {
        return delegate.intersects(ranges);
    }

    public long getBloomFilterFalsePositiveCount()
    {
        return delegate.getBloomFilterFalsePositiveCount();
    }

    public long getRecentBloomFilterFalsePositiveCount()
    {
        return delegate.getRecentBloomFilterFalsePositiveCount();
    }

    public long getBloomFilterTruePositiveCount()
    {
        return delegate.getBloomFilterTruePositiveCount();
    }

    public long getRecentBloomFilterTruePositiveCount()
    {
        return delegate.getRecentBloomFilterTruePositiveCount();
    }

    public InstrumentingCache<KeyCacheKey, RowIndexEntry> getKeyCache()
    {
        return delegate.getKeyCache();
    }

    public EstimatedHistogram getEstimatedPartitionSize()
    {
        return delegate.getEstimatedPartitionSize();
    }

    public EstimatedHistogram getEstimatedCellPerPartitionCount()
    {
        return delegate.getEstimatedCellPerPartitionCount();
    }

    public double getEstimatedDroppableTombstoneRatio(int gcBefore)
    {
        return delegate.getEstimatedDroppableTombstoneRatio(gcBefore);
    }

    public double getDroppableTombstonesBefore(int gcBefore)
    {
        return delegate.getDroppableTombstonesBefore(gcBefore);
    }

    public double getCompressionRatio()
    {
        return delegate.getCompressionRatio();
    }

    public long getMinTimestamp()
    {
        return delegate.getMinTimestamp();
    }

    public long getMaxTimestamp()
    {
        return delegate.getMaxTimestamp();
    }

    public int getMinLocalDeletionTime()
    {
        return delegate.getMinLocalDeletionTime();
    }

    public int getMaxLocalDeletionTime()
    {
        return delegate.getMaxLocalDeletionTime();
    }

    public boolean mayHaveTombstones()
    {
        return delegate.mayHaveTombstones();
    }

    public int getMinTTL()
    {
        return delegate.getMinTTL();
    }

    public int getMaxTTL()
    {
        return delegate.getMaxTTL();
    }

    public long getTotalColumnsSet()
    {
        return delegate.getTotalColumnsSet();
    }

    public long getTotalRows()
    {
        return delegate.getTotalRows();
    }

    public int getAvgColumnSetPerRow()
    {
        return delegate.getAvgColumnSetPerRow();
    }

    public int getSSTableLevel()
    {
        return delegate.getSSTableLevel();
    }

    public void reloadSSTableMetadata() throws IOException
    {
        delegate.reloadSSTableMetadata();
    }

    public StatsMetadata getSSTableMetadata()
    {
        return delegate.getSSTableMetadata();
    }

    public RandomAccessReader openDataReader(RateLimiter limiter)
    {
        return delegate.openDataReader(limiter);
    }

    public RandomAccessReader openDataReader()
    {
        return delegate.openDataReader();
    }

    public RandomAccessReader openIndexReader()
    {
        return delegate.openIndexReader();
    }

    public ChannelProxy getIndexChannel()
    {
        return delegate.getIndexChannel();
    }

    public FileHandle getIndexFile()
    {
        return delegate.getIndexFile();
    }

    public long getCreationTimeFor(Component component)
    {
        return delegate.getCreationTimeFor(component);
    }

    public long getKeyCacheHit()
    {
        return delegate.getKeyCacheHit();
    }

    public long getKeyCacheRequest()
    {
        return delegate.getKeyCacheRequest();
    }

    public void incrementReadCount()
    {
        delegate.incrementReadCount();
    }

    public EncodingStats stats()
    {
        return delegate.stats();
    }

    public Ref<SSTableReader> tryRef()
    {
        return selfRef.tryRef();
    }

    public Ref<SSTableReader> selfRef()
    {
        return selfRef;
    }

    public Ref<SSTableReader> ref()
    {
        return selfRef.ref();
    }

    void setup(boolean trackHotness)
    {
        delegate.setup(trackHotness);
    }

    public void overrideReadMeter(RestorableMeter readMeter)
    {
        delegate.overrideReadMeter(readMeter);
    }

    public void addTo(Ref.IdentityCollection identities)
    {
        delegate.addTo(identities);
    }

    public TableMetadata metadata()
    {
        return delegate.metadata();
    }

    public IPartitioner getPartitioner()
    {
        return delegate.getPartitioner();
    }

    public DecoratedKey decorateKey(ByteBuffer key)
    {
        return delegate.decorateKey(key);
    }

    public String getIndexFilename()
    {
        return delegate.getIndexFilename();
    }

    public String getColumnFamilyName()
    {
        return delegate.getColumnFamilyName();
    }

    public String getKeyspaceName()
    {
        return delegate.getKeyspaceName();
    }

    public List<String> getAllFilePaths()
    {
        return delegate.getAllFilePaths();
    }

//    protected long estimateRowsFromIndex(RandomAccessReader ifile) throws IOException
//    {
//        return delegate.estimateRowsFromIndex(ifile);
//    }

    public long bytesOnDisk()
    {
        return delegate.bytesOnDisk();
    }

    public String toString()
    {
        return delegate.toString();
    }

    public AbstractBounds<Token> getBounds()
    {
        return delegate.getBounds();
    }

    public ChannelProxy getDataChannel()
    {
        return delegate.getDataChannel();
    }
}
