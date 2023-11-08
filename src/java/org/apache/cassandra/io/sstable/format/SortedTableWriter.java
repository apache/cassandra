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
import java.nio.BufferOverflowException;
import java.util.Collection;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionPurger;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.db.guardrails.Threshold;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.PartitionSerializationException;
import org.apache.cassandra.db.rows.RangeTombstoneBoundMarker;
import org.apache.cassandra.db.rows.RangeTombstoneBoundaryMarker;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.compress.CompressedSequentialWriter;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.AbstractRowIndexEntry;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableFlushObserver;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.DataPosition;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.FilterFactory;
import org.apache.cassandra.utils.IFilter;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.Transactional;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A generic implementation of a writer which assumes the existence of some partition index and bloom filter.
 */
public abstract class SortedTableWriter<P extends SortedTablePartitionWriter, I extends SortedTableWriter.AbstractIndexWriter> extends SSTableWriter
{
    private final static Logger logger = LoggerFactory.getLogger(SortedTableWriter.class);

    // TODO dataWriter is not needed to be directly accessible - we can access everything we need for the dataWriter
    //   from a partition writer
    protected final SequentialWriter dataWriter;
    protected final I indexWriter;
    protected final P partitionWriter;
    private final FileHandle.Builder dataFileBuilder = new FileHandle.Builder(descriptor.fileFor(Components.DATA));
    private DecoratedKey lastWrittenKey;
    private DataPosition dataMark;
    private long lastEarlyOpenLength;
    private final Supplier<Double> crcCheckChanceSupplier;

    public SortedTableWriter(Builder<P, I, ?, ?> builder, LifecycleNewTracker lifecycleNewTracker, SSTable.Owner owner)
    {
        super(builder, lifecycleNewTracker, owner);

        TableMetadataRef ref = builder.getTableMetadataRef();
        crcCheckChanceSupplier = () -> ref.getLocal().params.crcCheckChance;
        SequentialWriter dataWriter = null;
        I indexWriter = null;
        P partitionWriter = null;
        try
        {
            dataWriter = builder.openDataWriter();
            checkNotNull(dataWriter);

            indexWriter = builder.openIndexWriter(dataWriter);
            checkNotNull(indexWriter);

            partitionWriter = builder.openPartitionWriter(dataWriter, indexWriter);
            checkNotNull(partitionWriter);

            this.dataWriter = dataWriter;
            this.indexWriter = indexWriter;
            this.partitionWriter = partitionWriter;
        }
        catch (RuntimeException | Error ex)
        {
            Throwables.closeNonNullAndAddSuppressed(ex, partitionWriter, indexWriter, dataWriter);
            handleConstructionFailure(ex);
            throw ex;
        }
    }

    /**
     * Appends partition data to this writer.
     *
     * @param partition the partition to write
     * @return the created index entry if something was written, that is if {@code iterator} wasn't empty,
     * {@code null} otherwise.
     * @throws FSWriteError if write to the dataFile fails
     */
    @Override
    public final AbstractRowIndexEntry append(UnfilteredRowIterator partition)
    {
        if (partition.isEmpty())
            return null;

        try
        {
            if (!verifyPartition(partition.partitionKey()))
                return null;

            startPartition(partition.partitionKey(), partition.partitionLevelDeletion());

            AbstractRowIndexEntry indexEntry;
            if (header.hasStatic())
                addStaticRow(partition.partitionKey(), partition.staticRow());

            while (partition.hasNext())
                addUnfiltered(partition.partitionKey(), partition.next());

            indexEntry = endPartition(partition.partitionKey(), partition.partitionLevelDeletion());

            return indexEntry;
        }
        catch (BufferOverflowException boe)
        {
            throw new PartitionSerializationException(partition, boe);
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, getFilename());
        }
    }

    private boolean verifyPartition(DecoratedKey key)
    {
        assert key != null : "Keys must not be null"; // empty keys ARE allowed b/c of indexed column values

        if (key.getKey().remaining() > FBUtilities.MAX_UNSIGNED_SHORT)
        {
            logger.error("Key size {} exceeds maximum of {}, skipping row", key.getKey().remaining(), FBUtilities.MAX_UNSIGNED_SHORT);
            return false;
        }

        if (lastWrittenKey != null && lastWrittenKey.compareTo(key) >= 0)
            throw new RuntimeException(String.format("Last written key %s >= current key %s, writing into %s", lastWrittenKey, key, getFilename()));

        return true;
    }

    private void startPartition(DecoratedKey key, DeletionTime partitionLevelDeletion) throws IOException
    {
        partitionWriter.start(key, partitionLevelDeletion);
        metadataCollector.updatePartitionDeletion(partitionLevelDeletion);

        onStartPartition(key);
    }

    private void addStaticRow(DecoratedKey key, Row row) throws IOException
    {
        guardCollectionSize(key, row);

        partitionWriter.addStaticRow(row);
        if (!row.isEmpty())
            Rows.collectStats(row, metadataCollector);

        onStaticRow(row);
    }

    private void addUnfiltered(DecoratedKey key, Unfiltered unfiltered) throws IOException
    {
        if (unfiltered.isRow())
            addRow(key, (Row) unfiltered);
        else
            addRangeTomstoneMarker((RangeTombstoneMarker) unfiltered);
    }

    private void addRow(DecoratedKey key, Row row) throws IOException
    {
        guardCollectionSize(key, row);

        partitionWriter.addUnfiltered(row);
        metadataCollector.updateClusteringValues(row.clustering());
        Rows.collectStats(row, metadataCollector);

        onRow(row);
    }

    private void addRangeTomstoneMarker(RangeTombstoneMarker marker) throws IOException
    {
        partitionWriter.addUnfiltered(marker);

        metadataCollector.updateClusteringValuesByBoundOrBoundary(marker.clustering());
        if (marker.isBoundary())
        {
            RangeTombstoneBoundaryMarker bm = (RangeTombstoneBoundaryMarker) marker;
            metadataCollector.update(bm.endDeletionTime());
            metadataCollector.update(bm.startDeletionTime());
        }
        else
        {
            metadataCollector.update(((RangeTombstoneBoundMarker) marker).deletionTime());
        }

        onRangeTombstoneMarker(marker);
    }

    private AbstractRowIndexEntry endPartition(DecoratedKey key, DeletionTime partitionLevelDeletion) throws IOException
    {
        long finishResult = partitionWriter.finish();

        long endPosition = dataWriter.position();
        long rowSize = endPosition - partitionWriter.getInitialPosition();
        guardPartitionThreshold(Guardrails.partitionSize, key, rowSize);
        guardPartitionThreshold(Guardrails.partitionTombstones, key, metadataCollector.totalTombstones);
        metadataCollector.addPartitionSizeInBytes(rowSize);
        metadataCollector.addKey(key.getKey());
        metadataCollector.addCellPerPartitionCount();

        lastWrittenKey = key;
        last = lastWrittenKey;
        if (first == null)
            first = lastWrittenKey;

        if (logger.isTraceEnabled())
            logger.trace("wrote {} at {}", key, endPosition);

        return createRowIndexEntry(key, partitionLevelDeletion, finishResult);
    }

    protected void onStartPartition(DecoratedKey key)
    {
        notifyObservers(o -> o.startPartition(key, partitionWriter.getInitialPosition(), partitionWriter.getInitialPosition()));
    }

    protected void onStaticRow(Row row)
    {
        notifyObservers(o -> o.staticRow(row));
    }

    protected void onRow(Row row)
    {
        notifyObservers(o -> o.nextUnfilteredCluster(row));
    }

    protected void onRangeTombstoneMarker(RangeTombstoneMarker marker)
    {
        notifyObservers(o -> o.nextUnfilteredCluster(marker));
    }

    protected abstract AbstractRowIndexEntry createRowIndexEntry(DecoratedKey key, DeletionTime partitionLevelDeletion, long finishResult) throws IOException;

    protected final void notifyObservers(Consumer<SSTableFlushObserver> action)
    {
        if (observers != null && !observers.isEmpty())
            observers.forEach(action);
    }

    @Override
    public void mark()
    {
        dataMark = dataWriter.mark();
        indexWriter.mark();
    }

    @Override
    public void resetAndTruncate()
    {
        dataWriter.resetAndTruncate(dataMark);
        partitionWriter.reset();
        indexWriter.resetAndTruncate();
    }

    @Override
    protected SSTableWriter.TransactionalProxy txnProxy()
    {
        return new TransactionalProxy(() -> FBUtilities.immutableListWithFilteredNulls(indexWriter, dataWriter));
    }

    protected class TransactionalProxy extends SSTableWriter.TransactionalProxy
    {
        public TransactionalProxy(Supplier<ImmutableList<Transactional>> transactionals)
        {
            super(transactionals);
        }

        @Override
        protected Throwable doPostCleanup(Throwable accumulate)
        {
            accumulate = Throwables.close(accumulate, partitionWriter);
            accumulate = super.doPostCleanup(accumulate);
            return accumulate;
        }
    }

    @Override
    public long getFilePointer()
    {
        return dataWriter.position();
    }

    @Override
    public long getOnDiskFilePointer()
    {
        return dataWriter.getOnDiskFilePointer();
    }

    @Override
    public long getEstimatedOnDiskBytesWritten()
    {
        return dataWriter.getEstimatedOnDiskBytesWritten();
    }

    protected FileHandle openDataFile(long lengthOverride, StatsMetadata statsMetadata)
    {
        int dataBufferSize = ioOptions.diskOptimizationStrategy.bufferSize(statsMetadata.estimatedPartitionSize.percentile(ioOptions.diskOptimizationEstimatePercentile));


        FileHandle dataFile;
        try (CompressionMetadata compressionMetadata = compression ? ((CompressedSequentialWriter) dataWriter).open(lengthOverride) : null)
        {
            dataFile = dataFileBuilder.mmapped(ioOptions.defaultDiskAccessMode)
                                      .withMmappedRegionsCache(mmappedRegionsCache)
                                      .withChunkCache(chunkCache)
                                      .withCompressionMetadata(compressionMetadata)
                                      .bufferSize(dataBufferSize)
                                      .withCrcCheckChance(crcCheckChanceSupplier)
                                      .withLengthOverride(lengthOverride)
                                      .complete();
        }

        try
        {
            if (chunkCache != null)
            {
                if (lastEarlyOpenLength != 0 && dataFile.dataLength() > lastEarlyOpenLength)
                    chunkCache.invalidatePosition(dataFile, lastEarlyOpenLength);
            }
            lastEarlyOpenLength = dataFile.dataLength();
        }
        catch (RuntimeException | Error ex)
        {
            Throwables.closeNonNullAndAddSuppressed(ex, dataFile);
            throw ex;
        }

        return dataFile;
    }

    private void guardPartitionThreshold(Threshold guardrail, DecoratedKey key, long size)
    {
        if (guardrail.triggersOn(size, null))
        {
            String message = String.format("%s.%s:%s on sstable %s",
                                           metadata.keyspace,
                                           metadata.name,
                                           metadata().partitionKeyType.getString(key.getKey()),
                                           getFilename());
            guardrail.guard(size, message, true, null);
        }
    }

    private void guardCollectionSize(DecoratedKey partitionKey, Row row)
    {
        if (!Guardrails.collectionSize.enabled() && !Guardrails.itemsPerCollection.enabled())
            return;

        if (row.isEmpty() || SchemaConstants.isSystemKeyspace(metadata.keyspace))
            return;

        for (ColumnMetadata column : row.columns())
        {
            if (!column.type.isCollection() || !column.type.isMultiCell())
                continue;

            ComplexColumnData cells = row.getComplexColumnData(column);
            if (cells == null)
                continue;

            ComplexColumnData liveCells = cells.purge(DeletionPurger.PURGE_ALL, FBUtilities.nowInSeconds());
            if (liveCells == null)
                continue;

            int cellsSize = liveCells.dataSize();
            int cellsCount = liveCells.cellsCount();

            if (!Guardrails.collectionSize.triggersOn(cellsSize, null) &&
                !Guardrails.itemsPerCollection.triggersOn(cellsCount, null))
                continue;

            String keyString = metadata.getLocal().primaryKeyAsCQLLiteral(partitionKey.getKey(), row.clustering());
            String msg = String.format("%s in row %s in table %s",
                                       column.name.toString(),
                                       keyString,
                                       metadata);
            Guardrails.collectionSize.guard(cellsSize, msg, true, null);
            Guardrails.itemsPerCollection.guard(cellsCount, msg, true, null);
        }
    }

    protected static abstract class AbstractIndexWriter extends AbstractTransactional implements Transactional
    {
        protected final Descriptor descriptor;
        protected final TableMetadataRef metadata;
        protected final Set<Component> components;

        protected final IFilter bf;

        protected AbstractIndexWriter(Builder<?, ?, ?, ?> b)
        {
            this.descriptor = b.descriptor;
            this.metadata = b.getTableMetadataRef();
            this.components = b.getComponents();

            bf = FilterFactory.getFilter(b.getKeyCount(), b.getTableMetadataRef().getLocal().params.bloomFilterFpChance);
        }

        protected void flushBf()
        {
            if (components.contains(Components.FILTER))
            {
                try
                {
                    FilterComponent.save(bf, descriptor, true);
                }
                catch (IOException ex)
                {
                    throw new FSWriteError(ex, descriptor.fileFor(Components.FILTER));
                }
            }
        }

        public abstract void mark();

        public abstract void resetAndTruncate();

        protected void doPrepare()
        {
            flushBf();
        }

        @Override
        protected Throwable doPostCleanup(Throwable accumulate)
        {
            accumulate = bf.close(accumulate);
            return accumulate;
        }

        public IFilter getFilterCopy()
        {
            return bf.sharedCopy();
        }
    }

    public abstract static class Builder<P extends SortedTablePartitionWriter,
                                        I extends AbstractIndexWriter,
                                        W extends SortedTableWriter<P, I>,
                                        B extends Builder<P, I, W, B>> extends SSTableWriter.Builder<W, B>
    {

        public Builder(Descriptor descriptor)
        {
            super(descriptor);
        }

        @Override
        public B addDefaultComponents(Collection<Index.Group> indexGroups)
        {
            super.addDefaultComponents(indexGroups);

            if (FilterComponent.shouldUseBloomFilter(getTableMetadataRef().getLocal().params.bloomFilterFpChance))
            {
                addComponents(ImmutableSet.of(SSTableFormat.Components.FILTER));
            }

            return (B) this;
        }

        protected abstract SequentialWriter openDataWriter();

        protected abstract I openIndexWriter(SequentialWriter dataWriter);

        protected abstract P openPartitionWriter(SequentialWriter dataWriter, I indexWriter);
    }
}
