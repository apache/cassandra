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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.KeyReader;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.CompressionInfoComponent;
import org.apache.cassandra.io.sstable.format.FilterComponent;
import org.apache.cassandra.io.sstable.format.SortedTableReaderLoadingBuilder;
import org.apache.cassandra.io.sstable.format.StatsComponent;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat.Components;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.sstable.metadata.ValidationMetadata;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.utils.FilterFactory;
import org.apache.cassandra.utils.IFilter;
import org.apache.cassandra.utils.Throwables;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class BtiTableReaderLoadingBuilder extends SortedTableReaderLoadingBuilder<BtiTableReader, BtiTableReader.Builder>
{
    private final static Logger logger = LoggerFactory.getLogger(BtiTableReaderLoadingBuilder.class);

    private FileHandle.Builder partitionIndexFileBuilder;
    private FileHandle.Builder rowIndexFileBuilder;

    public BtiTableReaderLoadingBuilder(SSTable.Builder<?, ?> builder)
    {
        super(builder);
    }

    @Override
    public KeyReader buildKeyReader(TableMetrics tableMetrics) throws IOException
    {
        StatsComponent statsComponent = StatsComponent.load(descriptor, MetadataType.STATS, MetadataType.HEADER, MetadataType.VALIDATION);
        return createKeyReader(statsComponent.statsMetadata());
    }

    private KeyReader createKeyReader(StatsMetadata statsMetadata) throws IOException
    {
        checkNotNull(statsMetadata);

        try (PartitionIndex index = PartitionIndex.load(partitionIndexFileBuilder(), tableMetadataRef.getLocal().partitioner, false);
             CompressionMetadata compressionMetadata = CompressionInfoComponent.maybeLoad(descriptor, components);
             FileHandle dFile = dataFileBuilder(statsMetadata).withCompressionMetadata(compressionMetadata)
                                                              .withCrcCheckChance(() -> tableMetadataRef.getLocal().params.crcCheckChance)
                                                              .complete();
             FileHandle riFile = rowIndexFileBuilder().complete())
        {
            return PartitionIterator.create(index,
                                            tableMetadataRef.getLocal().partitioner,
                                            riFile,
                                            dFile,
                                            descriptor.version);
        }
    }

    @Override
    protected void openComponents(BtiTableReader.Builder builder, SSTable.Owner owner, boolean validate, boolean online) throws IOException
    {
        try
        {
            StatsComponent statsComponent = StatsComponent.load(descriptor, MetadataType.STATS, MetadataType.VALIDATION, MetadataType.HEADER);
            builder.setSerializationHeader(statsComponent.serializationHeader(builder.getTableMetadataRef().getLocal()));
            checkArgument(!online || builder.getSerializationHeader() != null);

            builder.setStatsMetadata(statsComponent.statsMetadata());
            ValidationMetadata validationMetadata = statsComponent.validationMetadata();
            validatePartitioner(builder.getTableMetadataRef().getLocal(), validationMetadata);

            boolean filterNeeded = online;
            if (filterNeeded)
                builder.setFilter(loadFilter(validationMetadata));
            boolean rebuildFilter = filterNeeded && builder.getFilter() == null;

            if (builder.getComponents().contains(Components.PARTITION_INDEX) && builder.getComponents().contains(Components.ROW_INDEX) && rebuildFilter)
            {
                IFilter filter = buildBloomFilter(statsComponent.statsMetadata());
                builder.setFilter(filter);
                FilterComponent.save(filter, descriptor, false);
            }

            if (builder.getFilter() == null)
                builder.setFilter(FilterFactory.AlwaysPresent);

            if (builder.getComponents().contains(Components.ROW_INDEX))
                builder.setRowIndexFile(rowIndexFileBuilder().complete());

            if (descriptor.version.hasKeyRange() && builder.getStatsMetadata() != null)
            {
                IPartitioner partitioner = tableMetadataRef.getLocal().partitioner;
                builder.setFirst(partitioner.decorateKey(builder.getStatsMetadata().firstKey));
                builder.setLast(partitioner.decorateKey(builder.getStatsMetadata().lastKey));
            }

            if (builder.getComponents().contains(Components.PARTITION_INDEX))
            {
                builder.setPartitionIndex(openPartitionIndex(!builder.getFilter().isInformative()));
                if (builder.getFirst() == null || builder.getLast() == null)
                {
                    builder.setFirst(builder.getPartitionIndex().firstKey());
                    builder.setLast(builder.getPartitionIndex().lastKey());
                }
            }

            try (CompressionMetadata compressionMetadata = CompressionInfoComponent.maybeLoad(descriptor, components))
            {
                builder.setDataFile(dataFileBuilder(builder.getStatsMetadata())
                                    .withCompressionMetadata(compressionMetadata)
                                    .withCrcCheckChance(() -> tableMetadataRef.getLocal().params.crcCheckChance)
                                    .complete());
            }
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

    private PartitionIndex openPartitionIndex(boolean preload) throws IOException
    {
        try (FileHandle indexFile = partitionIndexFileBuilder().complete())
        {
            return PartitionIndex.load(indexFile, tableMetadataRef.getLocal().partitioner, preload);
        }
        catch (IOException ex)
        {
            logger.debug("Partition index file is corrupted: " + descriptor.fileFor(Components.PARTITION_INDEX), ex);
            throw ex;
        }
    }

    private FileHandle.Builder rowIndexFileBuilder()
    {
        assert rowIndexFileBuilder == null || rowIndexFileBuilder.file.equals(descriptor.fileFor(Components.ROW_INDEX));

        if (rowIndexFileBuilder == null)
            rowIndexFileBuilder = new FileHandle.Builder(descriptor.fileFor(Components.ROW_INDEX));

        rowIndexFileBuilder.withChunkCache(chunkCache);
        rowIndexFileBuilder.mmapped(ioOptions.indexDiskAccessMode);

        return rowIndexFileBuilder;
    }

    private FileHandle.Builder partitionIndexFileBuilder()
    {
        assert partitionIndexFileBuilder == null || partitionIndexFileBuilder.file.equals(descriptor.fileFor(Components.PARTITION_INDEX));

        if (partitionIndexFileBuilder == null)
            partitionIndexFileBuilder = new FileHandle.Builder(descriptor.fileFor(Components.PARTITION_INDEX));

        partitionIndexFileBuilder.withChunkCache(chunkCache);
        partitionIndexFileBuilder.mmapped(ioOptions.indexDiskAccessMode);

        return partitionIndexFileBuilder;
    }
}
