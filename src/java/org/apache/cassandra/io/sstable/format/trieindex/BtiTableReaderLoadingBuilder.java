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

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.KeyReader;
import org.apache.cassandra.io.sstable.SSTableBuilder;
import org.apache.cassandra.io.sstable.format.CompressionInfoComponent;
import org.apache.cassandra.io.sstable.format.FilterComponent;
import org.apache.cassandra.io.sstable.format.SSTableReaderLoadingBuilder;
import org.apache.cassandra.io.sstable.format.StatsComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.sstable.metadata.ValidationMetadata;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.utils.AlwaysPresentFilter;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.FilterFactory;
import org.apache.cassandra.utils.IFilter;
import org.apache.cassandra.utils.Throwables;

import static com.google.common.base.Preconditions.checkNotNull;

public class BtiTableReaderLoadingBuilder extends SSTableReaderLoadingBuilder<BtiTableReader, BtiTableReaderBuilder>
{
    private final static Logger logger = LoggerFactory.getLogger(BtiTableReaderLoadingBuilder.class);

    private FileHandle.Builder dataFileBuilder;
    private FileHandle.Builder partitionIndexFileBuilder;
    private FileHandle.Builder rowIndexFileBuilder;


    public BtiTableReaderLoadingBuilder(SSTableBuilder<?, ?> builder)
    {
        super(builder);
    }

    @Override
    public KeyReader buildKeyReader() throws IOException
    {
        StatsComponent statsComponent = StatsComponent.load(descriptor, MetadataType.STATS, MetadataType.HEADER, MetadataType.VALIDATION);
        return createKeyReader(statsComponent.statsMetadata());
    }

    private KeyReader createKeyReader(StatsMetadata statsMetadata) throws IOException
    {
        checkNotNull(statsMetadata);

        try (PartitionIndex index = PartitionIndex.load(partitionIndexFileBuilder(), tableMetadataRef.getLocal().partitioner, false);
             FileHandle dFile = dataFileBuilder(statsMetadata).complete();
             FileHandle riFile = rowIndexFileBuilder().complete())
        {
            return new PartitionIterator(index.sharedCopy(),
                                         tableMetadataRef.getLocal().partitioner,
                                         riFile.sharedCopy(),
                                         dFile.sharedCopy()).closeHandles();
        }
    }

    @Override
    protected void openComponents(BtiTableReaderBuilder builder, boolean validate, boolean online) throws IOException
    {
        try
        {
            StatsComponent statsComponent = StatsComponent.load(descriptor, MetadataType.STATS, MetadataType.VALIDATION, MetadataType.HEADER);
            builder.setSerializationHeader(statsComponent.serializationHeader(builder.getTableMetadataRef().getLocal()));
            assert !online || builder.getSerializationHeader() != null;

            builder.setStatsMetadata(statsComponent.statsMetadata());
            ValidationMetadata validationMetadata = statsComponent.validationMetadata();
            validatePartitioner(builder.getTableMetadataRef().getLocal(), validationMetadata);

            boolean filterNeeded = online;
            if (filterNeeded)
                builder.setFilter(loadFilter(validationMetadata));
            boolean rebuildFilter = filterNeeded && builder.getFilter() == null;

            if (rebuildFilter)
            {
                IFilter filter = buildBloomFilter(statsComponent.statsMetadata());

                if (filter != null)
                {
                    builder.setFilter(filter);

                    if (online)
                        FilterComponent.save(filter, descriptor);
                }
            }

            assert !filterNeeded || builder.getFilter() != null;

            if (builder.getFilter() == null)
                builder.setFilter(FilterFactory.AlwaysPresent);

            builder.setDataFile(dataFileBuilder(builder.getStatsMetadata()).complete());
            builder.setRowIndexFile(rowIndexFileBuilder().complete());
            builder.setPartitionIndex(openPartitionIndex(builder.getFilter() instanceof AlwaysPresentFilter));
            builder.setFirst(builder.getPartitionIndex().firstKey());
            builder.setLast(builder.getPartitionIndex().lastKey());
        }
        catch (IOException | RuntimeException | Error ex)
        {
            // in case of failure, close only those components which have been opened in this try-catch block
            Throwables.closeAndAddSuppressed(ex, builder.getPartitionIndex(), builder.getRowIndexFile(), builder.getDataFile(), builder.getFilter());
            throw ex;
        }
    }

    private IFilter buildBloomFilter(StatsMetadata statsMetadata) throws IOException
    {
        IFilter bf = null;

        try (KeyReader keyReader = createKeyReader(statsMetadata))
        {
            if (keyReader == null)
                return null;

            bf = FilterFactory.getFilter(statsMetadata.totalRows, tableMetadataRef.getLocal().params.bloomFilterFpChance);

            while (!keyReader.isExhausted())
            {
                DecoratedKey key = tableMetadataRef.getLocal().partitioner.decorateKey(keyReader.key());
                bf.add(key);

                keyReader.advance();
            }
        }
        catch (IOException | RuntimeException | Error ex)
        {
            Throwables.closeAndAddSuppressed(ex, bf);
            throw ex;
        }

        return bf;
    }

    private IFilter loadFilter(ValidationMetadata validationMetadata)
    {
        return FilterComponent.maybeLoadBloomFilter(descriptor,
                                                    components,
                                                    tableMetadataRef.get(),
                                                    validationMetadata);
    }

    private PartitionIndex openPartitionIndex(boolean preload) throws IOException
    {
        try (FileHandle indexFile = partitionIndexFileBuilder().complete())
        {
            return PartitionIndex.load(indexFile, tableMetadataRef.getLocal().partitioner, preload);
        }
        catch (IOException ex)
        {
            logger.debug("Partition index file is corrupted: " + descriptor.filenameFor(Component.PARTITION_INDEX), ex);
            throw ex;
        }
    }

    private FileHandle.Builder dataFileBuilder(StatsMetadata statsMetadata)
    {
        assert this.dataFileBuilder == null || this.dataFileBuilder.file.equals(descriptor.fileFor(Component.DATA));

        logger.info("Opening {} ({})", descriptor, FBUtilities.prettyPrintMemory(descriptor.fileFor(Component.DATA).length()));

        long recordSize = statsMetadata.estimatedPartitionSize.percentile(ioOptions.diskOptimizationEstimatePercentile);
        int bufferSize = ioOptions.diskOptimizationStrategy.bufferSize(recordSize);

        if (dataFileBuilder == null)
            dataFileBuilder = new FileHandle.Builder(descriptor.fileFor(Component.DATA));

        dataFileBuilder.bufferSize(bufferSize);
        dataFileBuilder.withChunkCache(chunkCache);
        dataFileBuilder.mmapped(ioOptions.defaultDiskAccessMode);
        if (components.contains(Component.COMPRESSION_INFO))
            dataFileBuilder.withCompressionMetadata(CompressionInfoComponent.load(descriptor));

        return dataFileBuilder;
    }

    private FileHandle.Builder rowIndexFileBuilder()
    {
        assert rowIndexFileBuilder == null || rowIndexFileBuilder.file.equals(descriptor.fileFor(Component.ROW_INDEX));

        if (rowIndexFileBuilder == null)
            rowIndexFileBuilder = new FileHandle.Builder(descriptor.fileFor(Component.ROW_INDEX));

        rowIndexFileBuilder.withChunkCache(chunkCache);
        rowIndexFileBuilder.mmapped(ioOptions.indexDiskAccessMode);

        return rowIndexFileBuilder;
    }

    private FileHandle.Builder partitionIndexFileBuilder()
    {
        assert partitionIndexFileBuilder == null || partitionIndexFileBuilder.file.equals(descriptor.fileFor(Component.PARTITION_INDEX));

        if (partitionIndexFileBuilder == null)
            partitionIndexFileBuilder = new FileHandle.Builder(descriptor.fileFor(Component.PARTITION_INDEX));

        partitionIndexFileBuilder.withChunkCache(chunkCache);
        partitionIndexFileBuilder.mmapped(ioOptions.indexDiskAccessMode);

        return partitionIndexFileBuilder;
    }
}
