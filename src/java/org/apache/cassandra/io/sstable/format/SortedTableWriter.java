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
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionPurger;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.RangeTombstoneBoundMarker;
import org.apache.cassandra.db.rows.RangeTombstoneBoundaryMarker;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.guardrails.Guardrails;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.compress.CompressedSequentialWriter;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.StorageHandler;
import org.apache.cassandra.io.sstable.metadata.CompactionMetadata;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.sstable.metadata.ZeroCopyMetadata;
import org.apache.cassandra.io.util.ChecksummedSequentialWriter;
import org.apache.cassandra.io.util.DataPosition;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.IFilter;
import org.apache.cassandra.utils.Throwables;

public abstract class SortedTableWriter extends SSTableWriter
{
    protected static final Logger logger = LoggerFactory.getLogger(SortedTableWriter.class);
    protected final FileHandle.Builder dbuilder;
    protected final SequentialWriter dataFile;
    protected DataPosition dataMark;
    protected DecoratedKey currentKey;
    protected DeletionTime currentPartitionLevelDeletion;
    protected long currentStartPosition;
    private long lastEarlyOpenLength = 0;
    private boolean isInternalKeyspace;


    protected SortedTableWriter(Descriptor descriptor,
                                Set<Component> components,
                                LifecycleNewTracker lifecycleNewTracker,
                                SequentialWriterOption writerOption,
                                long keyCount,
                                long repairedAt,
                                UUID pendingRepair,
                                boolean isTransient,
                                TableMetadataRef metadata,
                                MetadataCollector metadataCollector,
                                SerializationHeader header,
                                Collection<SSTableFlushObserver> observers)
    {
        super(descriptor, components, lifecycleNewTracker, keyCount, repairedAt, pendingRepair, isTransient, metadata, metadataCollector, header, observers);
        lifecycleNewTracker.trackNew(this); // must track before any files are created

        dataFile = constructDataFileWriter(descriptor, metadata, metadataCollector, lifecycleNewTracker, writerOption);
        dbuilder = SSTableReaderBuilder.defaultDataHandleBuilder(descriptor, ZeroCopyMetadata.EMPTY).compressed(compression);
        isInternalKeyspace = SchemaConstants.isInternalKeyspace(metadata.keyspace);
    }

    protected static SequentialWriter constructDataFileWriter(Descriptor descriptor,
                                                              TableMetadataRef metadata,
                                                              MetadataCollector metadataCollector,
                                                              LifecycleNewTracker lifecycleNewTracker,
                                                              SequentialWriterOption writerOption)
    {
        if (metadata.getLocal().params.compression.isEnabled())
        {
            final CompressionParams compressionParams = compressionFor(lifecycleNewTracker.opType(), metadata);

            return new CompressedSequentialWriter(descriptor.fileFor(Component.DATA),
                                                  descriptor.fileFor(Component.COMPRESSION_INFO),
                                                  descriptor.fileFor(Component.DIGEST),
                                                  writerOption,
                                                  compressionParams,
                                                  metadataCollector);
        }
        else
        {
            return new ChecksummedSequentialWriter(descriptor.fileFor(Component.DATA),
                                                   descriptor.fileFor(Component.CRC),
                                                   descriptor.fileFor(Component.DIGEST),
                                                   writerOption);
        }
    }

    /**
     * Given an OpType, determine the correct Compression Parameters
     * @param opType
     * @return {@link CompressionParams}
     */
    public static CompressionParams compressionFor(final OperationType opType, TableMetadataRef metadata)
    {
        CompressionParams compressionParams = metadata.getLocal().params.compression;

        if (compressionParams.getSstableCompressor() != null)
        {
            if (opType == OperationType.FLUSH)
            {
                // When we are flushing out of the memtable throughput of the compressor is critical as flushes,
                // especially of large tables, can queue up and potentially block writes.
                // This optimization allows us to fall back to a faster compressor if a particular
                // compression algorithm indicates we should. See CASSANDRA-15379 for more details.
                compressionParams = compressionParams.forFlush(metadata.getLocal().keyspace);
            }
            else
            {
                // Allows to plugin custom per-keyspace resolution of compressionParams
                compressionParams = compressionParams.forCompaction(metadata.getLocal().keyspace);
            }
        }
        return compressionParams;
    }

    public void mark()
    {
        dataMark = dataFile.mark();
    }

    public void resetAndTruncate()
    {
        dataFile.resetAndTruncate(dataMark);
    }

    protected boolean startPartitionMetadata(DecoratedKey key, DeletionTime partitionLevelDeletion) throws IOException
    {
        if (key.getKeyLength() > FBUtilities.MAX_UNSIGNED_SHORT)
        {
            logger.error("Key size {} exceeds maximum of {}, skipping row", key.getKeyLength(), FBUtilities.MAX_UNSIGNED_SHORT);
            return false;
        }

        checkKeyOrder(key);
        currentKey = key;
        currentPartitionLevelDeletion = partitionLevelDeletion;
        currentStartPosition = dataFile.position();

        metadataCollector.updatePartitionDeletion(partitionLevelDeletion);
        return true;
    }

    private void guardCollectionSize(DecoratedKey partitionKey, Unfiltered unfiltered)
    {
        if (isInternalKeyspace || !unfiltered.isRow())
            return;

        if (!Guardrails.collectionSize.enabled(null) && !Guardrails.itemsPerCollection.enabled(null))
            return;

        Row row = (Row) unfiltered;
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

            String msg = String.format("%s in table %s",
                                       column.name.toString(),
                                       metadata);
            Guardrails.collectionSize.guard(cellsSize, msg, false, null);
            Guardrails.itemsPerCollection.guard(cellsCount, msg, false, null);
        }
    }

    protected void addUnfilteredMetadata(Unfiltered unfiltered)
    {
        guardCollectionSize(currentKey, unfiltered);
        if (unfiltered.isRow())
        {
            Row row = (Row) unfiltered;
            metadataCollector.updateClusteringValues(row.clustering());
            Rows.collectStats(row, metadataCollector);
        }
        else
        {
            RangeTombstoneMarker marker = (RangeTombstoneMarker) unfiltered;
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
        }
    }

    protected void endPartitionMetadata() throws IOException
    {
        metadataCollector.addCellPerPartitionCount();
        long endPosition = dataFile.position();
        long partitionSize = endPosition - currentStartPosition;
        maybeLogLargePartitionWarning(currentKey, partitionSize);
        metadataCollector.addPartitionSizeInBytes(partitionSize);
        metadataCollector.addKey(currentKey.getKey());
        last = currentKey;
        if (first == null)
            first = currentKey;

        if (logger.isTraceEnabled())
            logger.trace("wrote {} at {}", currentKey, currentStartPosition);
    }

    /**
     * Perform sanity checks on @param decoratedKey and @return the position in the data file before any data is written
     */
    protected void checkKeyOrder(DecoratedKey decoratedKey)
    {
        assert decoratedKey != null : "Keys must not be null"; // empty keys ARE allowed b/c of indexed row values
        if (currentKey != null && currentKey.compareTo(decoratedKey) >= 0)
            throw new AssertionError("Last written key " + currentKey + " >= current key " + decoratedKey + " writing into " + getDataFile());
    }

    protected void invalidateCacheAtPreviousBoundary(FileHandle dfile, long newBoundary)
    {
        if (lastEarlyOpenLength != 0 && newBoundary > lastEarlyOpenLength)
            dfile.invalidateIfCached(lastEarlyOpenLength);

        lastEarlyOpenLength = newBoundary;
    }

    public long getFilePointer()
    {
        return dataFile.position();
    }

    public long getOnDiskFilePointer()
    {
        return dataFile.getOnDiskFilePointer();
    }

    public long getEstimatedOnDiskBytesWritten()
    {
        return dataFile.getEstimatedOnDiskBytesWritten();
    }

    protected void writeMetadata(Descriptor desc, Map<MetadataType, MetadataComponent> components, SequentialWriterOption writerOption)
    {
        File file = desc.fileFor(Component.STATS);
        try (SequentialWriter out = new SequentialWriter(file, writerOption))
        {
            desc.getMetadataSerializer().serialize(components, out, desc);
            out.finish();
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, file.path());
        }
    }

    public void openResult(StorageHandler storageHandler)
    {
        txnProxy().openResult(storageHandler);
    }

    public SSTableReader finished()
    {
        txnProxy().finalReaderAccessed = true;
        return txnProxy().finalReader;
    }

    protected SSTableReader openFinal(SSTableReader.OpenReason reason, StorageHandler storageHandler)
    {
        if (maxDataAge < 0)
            maxDataAge = System.currentTimeMillis();

        Map<MetadataType, MetadataComponent> finalMetadata = finalizeMetadata();
        StatsMetadata stats = (StatsMetadata) finalMetadata.get(MetadataType.STATS);
        CompactionMetadata compactionMetadata = (CompactionMetadata) finalMetadata.get(MetadataType.COMPACTION);

        int dataBufferSize = optimizationStrategy.bufferSize(stats.estimatedPartitionSize.percentile(DatabaseDescriptor.getDiskOptimizationEstimatePercentile()));
        // Note that creating the `CompressionMetadata` below does not read from disk: the compression metadata is
        // kept in memory by the writer and used to build the final instance. If we were reading from disk, we would
        // need to move this inside the `try` so that the `storageHandler` callback can intercept reading issues.
        // Which would imply being able to get at the compressed/uncompressed sizes upfront (directly from the
        // writer, without reading the compression metadata written file) in some other way.
        dataFile.updateFileHandle(dbuilder);

        FileHandle dfile = dbuilder.bufferSize(dataBufferSize).complete();
        invalidateCacheAtPreviousBoundary(dfile, Long.MAX_VALUE);

        DecoratedKey firstMinimized = getMinimalKey(first);
        DecoratedKey lastMinimized = getMinimalKey(last);
        try
        {
            SSTableReader reader = openReader(reason, dfile, stats, Optional.of(compactionMetadata));
            reader.first = firstMinimized;
            reader.last = lastMinimized;
            return reader;
        }
        catch (Throwable t)
        {
            Throwable err = Throwables.close(t, dfile);

            if (storageHandler != null)
                return storageHandler.onOpeningWrittenSSTableFailure(reason, descriptor, components(), dfile.onDiskLength, dfile.dataLength(), stats, firstMinimized, lastMinimized, keyCount, err);

            throw Throwables.unchecked(err);
        }
    }

    abstract protected SSTableReader openReader(SSTableReader.OpenReason reason, FileHandle dataFileHandle, StatsMetadata stats, Optional<CompactionMetadata> compactionMetadata);

    abstract protected SequentialWriterOption writerOption();

    abstract protected TransactionalProxy txnProxy();

    protected class TransactionalProxy extends AbstractTransactional
    {
        // should be set during doPrepare()
        private SSTableReader finalReader;
        protected boolean finalReaderAccessed;

        // finalise our state on disk, including renaming
        protected void doPrepare()
        {
            // write sstable statistics
            dataFile.prepareToCommit();
            writeMetadata(descriptor, finalizeMetadata(), writerOption());

            // save the table of components
            SSTable.appendTOC(descriptor, components());
        }

        private void openResult(StorageHandler storageHandler)
        {
            finalReader = openFinal(SSTableReader.OpenReason.NORMAL, storageHandler);
            finalReaderAccessed = false;
        }

        protected Throwable doCommit(Throwable accumulate)
        {
            accumulate = dataFile.commit(accumulate);
            return accumulate;
        }

        @Override
        protected Throwable doPostCleanup(Throwable accumulate)
        {
            accumulate = dbuilder.close(accumulate);
            return accumulate;
        }

        protected Throwable doAbort(Throwable accumulate)
        {
            accumulate = dataFile.abort(accumulate);

            if (!finalReaderAccessed && finalReader != null)
            {
                accumulate = Throwables.perform(accumulate, () -> finalReader.selfRef().release());
                finalReader = null;
                finalReaderAccessed = false;
            }

            return accumulate;
        }
    }

}
