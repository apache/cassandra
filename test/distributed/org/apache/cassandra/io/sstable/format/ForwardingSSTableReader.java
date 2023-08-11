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
import java.util.Set;

import com.google.common.util.concurrent.RateLimiter;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.AbstractRowIndexEntry;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.IVerifier;
import org.apache.cassandra.io.sstable.KeyIterator;
import org.apache.cassandra.io.sstable.KeyReader;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.CheckedFunction;
import org.apache.cassandra.io.util.DataIntegrityMetadata;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.metrics.RestorableMeter;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.Ref;

public abstract class ForwardingSSTableReader extends SSTableReader
{
    private final SSTableReader delegate;

    private static class Builder extends SSTableReader.Builder<ForwardingSSTableReader, Builder>
    {
        public Builder(SSTableReader delegate)
        {
            super(delegate.descriptor);
            delegate.unbuildTo(this, false);
        }

        @Override
        public ForwardingSSTableReader buildInternal(Owner owner)
        {
            throw new UnsupportedOperationException();
        }
    }

    public ForwardingSSTableReader(SSTableReader delegate)
    {
        super(new Builder(delegate), delegate.owner().orElse(null));
        this.delegate = delegate;
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
    public boolean mayContainAssumingKeyIsInRange(DecoratedKey key)
    {
        return delegate.mayContainAssumingKeyIsInRange(key);
    }

    @Override
    public void setupOnline()
    {
        delegate.setupOnline();
    }

    @Override
    public <R, E extends Exception> R runWithLock(CheckedFunction<Descriptor, R, E> task) throws E
    {
        return delegate.runWithLock(task);
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
    public SSTableReader cloneWithNewStart(DecoratedKey newStart)
    {
        return delegate.cloneWithNewStart(newStart);
    }

    @Override
    public RestorableMeter getReadMeter()
    {
        return delegate.getReadMeter();
    }

    @Override
    public void releaseInMemoryComponents()
    {
        delegate.releaseInMemoryComponents();
    }

    @Override
    public void validate()
    {
        delegate.validate();
    }

    @Override
    protected void closeInternalComponent(AutoCloseable closeable)
    {
        delegate.closeInternalComponent(closeable);
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
    public Iterable<DecoratedKey> getKeySamples(Range<Token> range)
    {
        return delegate.getKeySamples(range);
    }

    @Override
    public List<PartitionPositionBounds> getPositionsForRanges(Collection<Range<Token>> ranges)
    {
        return delegate.getPositionsForRanges(ranges);
    }

    @Override
    protected long getPosition(PartitionPosition key, Operator op, boolean updateStats, SSTableReadsListener listener)
    {
        return delegate.getPosition(key, op, updateStats, listener);
    }

    @Override
    protected AbstractRowIndexEntry getRowIndexEntry(PartitionPosition key, Operator op, boolean updateCacheAndStats, SSTableReadsListener listener)
    {
        return delegate.getRowIndexEntry(key, op, updateCacheAndStats, listener);
    }

    @Override
    public UnfilteredRowIterator rowIterator(DecoratedKey key, Slices slices, ColumnFilter selectedColumns, boolean reversed, SSTableReadsListener listener)
    {
        return delegate.rowIterator(key, slices, selectedColumns, reversed, listener);
    }

    @Override
    public UnfilteredRowIterator simpleIterator(FileDataInput file, DecoratedKey key, long dataPosition, boolean tombstoneOnly)
    {
        return delegate.simpleIterator(file, key, dataPosition, tombstoneOnly);
    }

    @Override
    public KeyReader keyReader() throws IOException
    {
        return delegate.keyReader();
    }

    @Override
    public KeyIterator keyIterator() throws IOException
    {
        return delegate.keyIterator();
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
    public void unmarkSuspect()
    {
        delegate.unmarkSuspect();
    }

    @Override
    public boolean isMarkedSuspect()
    {
        return delegate.isMarkedSuspect();
    }

    @Override
    public ISSTableScanner getScanner(Range<Token> range)
    {
        return delegate.getScanner(range);
    }

    @Override
    public ISSTableScanner getScanner()
    {
        return delegate.getScanner();
    }

    @Override
    public ISSTableScanner getScanner(Collection<Range<Token>> ranges)
    {
        return delegate.getScanner(ranges);
    }

    @Override
    public ISSTableScanner getScanner(Iterator<AbstractBounds<PartitionPosition>> rangeIterator)
    {
        return delegate.getScanner(rangeIterator);
    }

    @Override
    public UnfilteredPartitionIterator partitionIterator(ColumnFilter columns, DataRange dataRange, SSTableReadsListener listener)
    {
        return delegate.partitionIterator(columns, dataRange, listener);
    }

    @Override
    public FileDataInput getFileDataInput(long position)
    {
        return delegate.getFileDataInput(position);
    }

    @Override
    public boolean newSince(long timestampMillis)
    {
        return delegate.newSince(timestampMillis);
    }

    @Override
    public void createLinks(String snapshotDirectoryPath)
    {
        delegate.createLinks(snapshotDirectoryPath);
    }

    @Override
    public void createLinks(String snapshotDirectoryPath, RateLimiter rateLimiter)
    {
        delegate.createLinks(snapshotDirectoryPath, rateLimiter);
    }

    @Override
    public boolean isRepaired()
    {
        return delegate.isRepaired();
    }

    @Override
    public DecoratedKey keyAtPositionFromSecondaryIndex(long keyPositionFromSecondaryIndex) throws IOException
    {
        return delegate.keyAtPositionFromSecondaryIndex(keyPositionFromSecondaryIndex);
    }

    @Override
    public boolean isPendingRepair()
    {
        return delegate.isPendingRepair();
    }

    @Override
    public TimeUUID getPendingRepair()
    {
        return delegate.getPendingRepair();
    }

    @Override
    public long getRepairedAt()
    {
        return delegate.getRepairedAt();
    }

    @Override
    public boolean isTransient()
    {
        return delegate.isTransient();
    }

    @Override
    public boolean intersects(Collection<Range<Token>> ranges)
    {
        return delegate.intersects(ranges);
    }

    @Override
    public EstimatedHistogram getEstimatedPartitionSize()
    {
        return delegate.getEstimatedPartitionSize();
    }

    @Override
    public EstimatedHistogram getEstimatedCellPerPartitionCount()
    {
        return delegate.getEstimatedCellPerPartitionCount();
    }

    @Override
    public double getEstimatedDroppableTombstoneRatio(long gcBefore)
    {
        return delegate.getEstimatedDroppableTombstoneRatio(gcBefore);
    }

    @Override
    public double getDroppableTombstonesBefore(long gcBefore)
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
    public long getMinLocalDeletionTime()
    {
        return delegate.getMinLocalDeletionTime();
    }

    @Override
    public long getMaxLocalDeletionTime()
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
    public void mutateLevelAndReload(int newLevel) throws IOException
    {
        delegate.mutateLevelAndReload(newLevel);
    }

    @Override
    public void mutateRepairedAndReload(long newRepairedAt, TimeUUID newPendingRepair, boolean isTransient) throws IOException
    {
        delegate.mutateRepairedAndReload(newRepairedAt, newPendingRepair, isTransient);
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
    public void trySkipFileCacheBefore(DecoratedKey key)
    {
        delegate.trySkipFileCacheBefore(key);
    }

    @Override
    public ChannelProxy getDataChannel()
    {
        return delegate.getDataChannel();
    }

    @Override
    public long getDataCreationTime()
    {
        return delegate.getDataCreationTime();
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
    protected List<AutoCloseable> setupInstance(boolean trackHotness)
    {
        return delegate.setupInstance(trackHotness);
    }

    @Override
    public void setup(boolean trackHotness)
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
    public TableMetadata metadata()
    {
        return delegate.metadata();
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
    public long bytesOnDisk()
    {
        return delegate.bytesOnDisk();
    }

    @Override
    public long logicalBytesOnDisk()
    {
        return delegate.logicalBytesOnDisk();
    }

    @Override
    public Set<Component> getComponents()
    {
        return delegate.getComponents();
    }

    @Override
    public UnfilteredRowIterator rowIterator(DecoratedKey key)
    {
        return delegate.rowIterator(key);
    }

    @Override
    public void maybePersistSSTableReadMeter()
    {
        delegate.maybePersistSSTableReadMeter();
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

    @Override
    public AbstractBounds<Token> getBounds()
    {
        return delegate.getBounds();
    }

    @Override
    public double tokenSpaceCoverage()
    {
        return delegate.tokenSpaceCoverage();
    }

    @Override
    public IVerifier getVerifier(ColumnFamilyStore cfs, OutputHandler outputHandler, boolean isOffline, IVerifier.Options options)
    {
        return delegate.getVerifier(cfs, outputHandler, isOffline, options);
    }

    @Override
    protected void notifySelected(SSTableReadsListener.SelectionReason reason, SSTableReadsListener localListener, Operator op, boolean updateStats, AbstractRowIndexEntry entry)
    {
        delegate.notifySelected(reason, localListener, op, updateStats, entry);
    }

    @Override
    protected void notifySkipped(SSTableReadsListener.SkippingReason reason, SSTableReadsListener localListener, Operator op, boolean updateStats)
    {
        delegate.notifySkipped(reason, localListener, op, updateStats);
    }

    @Override
    public boolean isEstimationInformative()
    {
        return delegate.isEstimationInformative();
    }

    @Override
    public DataIntegrityMetadata.ChecksumValidator maybeGetChecksumValidator() throws IOException
    {
        return delegate.maybeGetChecksumValidator();
    }

    @Override
    public DataIntegrityMetadata.FileDigestValidator maybeGetDigestValidator() throws IOException
    {
        return delegate.maybeGetDigestValidator();
    }
}
