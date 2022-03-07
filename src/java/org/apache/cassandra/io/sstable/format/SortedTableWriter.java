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
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.rows.RangeTombstoneBoundMarker;
import org.apache.cassandra.db.rows.RangeTombstoneBoundaryMarker;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.compress.CompressedSequentialWriter;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.big.BigTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.util.ChecksummedSequentialWriter;
import org.apache.cassandra.io.util.DataPosition;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.FBUtilities;

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
        super(descriptor, components, keyCount, repairedAt, pendingRepair, isTransient, metadata, metadataCollector, header, observers);
        lifecycleNewTracker.trackNew(this); // must track before any files are created

        dataFile = constructDataFileWriter(descriptor, metadata, metadataCollector, lifecycleNewTracker, writerOption);
        dbuilder = SSTableReaderBuilder.defaultDataHandleBuilder(descriptor).compressed(compression);
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
        final ICompressor compressor = compressionParams.getSstableCompressor();

        if (null != compressor && opType == OperationType.FLUSH)
        {
            // When we are flushing out of the memtable throughput of the compressor is critical as flushes,
            // especially of large tables, can queue up and potentially block writes.
            // This optimization allows us to fall back to a faster compressor if a particular
            // compression algorithm indicates we should. See CASSANDRA-15379 for more details.
            switch (DatabaseDescriptor.getFlushCompression())
            {
                // It is relatively easier to insert a Noop compressor than to disable compressed writing
                // entirely as the "compression" member field is provided outside the scope of this class.
                // It may make sense in the future to refactor the ownership of the compression flag so that
                // We can bypass the CompressedSequentialWriter in this case entirely.
                case none:
                    compressionParams = CompressionParams.NOOP;
                    break;
                case fast:
                    if (!compressor.recommendedUses().contains(ICompressor.Uses.FAST_COMPRESSION))
                    {
                        // The default compressor is generally fast (LZ4 with 16KiB block size)
                        compressionParams = CompressionParams.DEFAULT;
                        break;
                    }
                    // else fall through
                case table:
                default:
                    break;
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

    protected void addUnfilteredMetadata(Unfiltered unfiltered)
    {
        SSTableWriter.guardCollectionSize(metadata(), currentKey, unfiltered);

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
            throw new RuntimeException("Last written key " + currentKey + " >= current key " + decoratedKey + " writing into " + getDataFile());
    }

    protected void invalidateCacheAtBoundary(FileHandle dfile)
    {
        if (lastEarlyOpenLength != 0 && dfile.dataLength() > lastEarlyOpenLength)
            dfile.invalidateIfCached(lastEarlyOpenLength);

        lastEarlyOpenLength = dfile.dataLength();
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
            desc.getMetadataSerializer().serialize(components, out, desc.version);
            out.finish();
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, file.path());
        }
    }

    public SSTableWriter setOpenResult(boolean openResult)
    {
        txnProxy().openResult = openResult;
        return this;
    }

    public SSTableReader finished()
    {
        return txnProxy().finalReader;
    }

    abstract protected SSTableReader openFinal(SSTableReader.OpenReason reason);
    abstract protected SequentialWriterOption writerOption();

    abstract protected TransactionalProxy txnProxy();

    protected class TransactionalProxy extends AbstractTransactional
    {
        // should be set during doPrepare()
        private SSTableReader finalReader;
        private boolean openResult;

        // finalise our state on disk, including renaming
        protected void doPrepare()
        {
            // write sstable statistics
            dataFile.prepareToCommit();
            writeMetadata(descriptor, finalizeMetadata(), writerOption());

            // save the table of components
            SSTable.appendTOC(descriptor, components);

            if (openResult)
                finalReader = openFinal(SSTableReader.OpenReason.NORMAL);
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
            return accumulate;
        }
    }

}
