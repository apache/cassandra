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
package org.apache.cassandra.io.sstable;


import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.compression.CompressionDictionary;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.sstable.format.FilterComponent;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReaderLoadingBuilder;
import org.apache.cassandra.io.sstable.format.SSTableReaderWithFilter;
import org.apache.cassandra.io.sstable.format.StatsComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.FilterFactory;
import org.apache.cassandra.utils.IFilter;

/**
 * Base class for the sstable writers used by CQLSSTableWriter.
 */
public abstract class AbstractSSTableSimpleWriter implements Closeable
{
    protected final File directory;
    protected final TableMetadataRef metadata;
    protected final RegularAndStaticColumns columns;
    protected SSTableFormat<?, ?> format = DatabaseDescriptor.getSelectedSSTableFormat();
    protected static final AtomicReference<SSTableId> id = new AtomicReference<>(SSTableIdFactory.instance.defaultBuilder().generator(Stream.empty()).get());
    protected boolean makeRangeAware = false;
    protected final Collection<Index.Group> indexGroups;
    protected Consumer<Collection<SSTableReader>> sstableProducedListener;
    protected boolean openSSTableOnProduced = false;
    protected CompressionDictionary compressionDictionary;
    protected SSTable.Owner owner;

    protected AbstractSSTableSimpleWriter(File directory, TableMetadataRef metadata, RegularAndStaticColumns columns)
    {
        this.metadata = metadata;
        this.directory = directory;
        this.columns = columns;
        indexGroups = new ArrayList<>();
    }

    protected void setSSTableFormatType(SSTableFormat<?, ?> type)
    {
        this.format = type;
    }

    protected void setRangeAwareWriting(boolean makeRangeAware)
    {
        this.makeRangeAware = makeRangeAware;
    }

    protected void addIndexGroup(Index.Group indexGroup)
    {
        this.indexGroups.add(indexGroup);
    }

    public void setCompressionDictionary(CompressionDictionary compressionDictionary)
    {
        this.compressionDictionary = compressionDictionary;
    }

    protected void setSSTableProducedListener(Consumer<Collection<SSTableReader>> listener)
    {
        this.sstableProducedListener = Objects.requireNonNull(listener, "sstableProducedListener cannot be null");
    }

    protected void setShouldOpenProducedSSTable(boolean openSSTableOnProduced)
    {
        this.openSSTableOnProduced = openSSTableOnProduced;
    }

    /**
     * Indicate whether the produced sstable should be opened or not.
     */
    protected boolean shouldOpenSSTables()
    {
        return openSSTableOnProduced;
    }

    protected void notifySSTableProduced(Collection<SSTableReader> sstables)
    {
        if (sstableProducedListener == null)
            return;

        sstableProducedListener.accept(sstables);
    }

    /**
     * Rebuilds the Bloom filter of a finished SSTable at the correct size and swaps it into any opened readers.
     */
    protected Collection<SSTableReader> rebuildBloomFilter(SSTableTxnWriter writer, Collection<SSTableReader> produced) throws IOException
    {
        TableMetadata tableMetadata = metadata.getLocal();
        double fpChance = tableMetadata.params.bloomFilterFpChance;
        if (!FilterComponent.shouldUseBloomFilter(fpChance))
            return produced;

        // output directory does not have to follow the keyspace/table layout, so do not validate it
        Descriptor descriptor = Descriptor.fromFileWithComponent(new File(writer.getFilename()), false).left;
        long partitionCount = StatsComponent.load(descriptor, MetadataType.STATS).statsMetadata().estimatedPartitionSize.count();
        SSTableReaderLoadingBuilder<?, ?> loader = descriptor.getFormat().getReaderFactory().loadingBuilder(descriptor, metadata, null);

        try (IFilter filter = FilterFactory.getFilter(partitionCount, fpChance);
             KeyReader keys = loader.buildKeyReader(null))
        {
            while (!keys.isExhausted())
            {
                filter.add(tableMetadata.partitioner.decorateKey(keys.key())); // token is unused
                keys.advance();
            }
            FilterComponent.save(filter, descriptor, true);

            List<SSTableReader> result = new ArrayList<>(produced.size());
            for (SSTableReader reader : produced)
            {
                if (reader instanceof SSTableReaderWithFilter)
                {
                    result.add(((SSTableReaderWithFilter) reader).cloneAndReplace(filter.sharedCopy()));
                    reader.selfRef().release();
                }
                else
                {
                    result.add(reader);
                }
            }
            return result;
        }
        catch (IOException | RuntimeException | Error e)
        {
            descriptor.fileFor(SSTableFormat.Components.FILTER).deleteIfExists();
            throw e;
        }
    }

    protected SSTableTxnWriter createWriter(SSTable.Owner owner, long keyCount) throws IOException
    {
        SerializationHeader header = new SerializationHeader(true, metadata.get(), columns, EncodingStats.NO_STATS);

        if (makeRangeAware)
            return SSTableTxnWriter.createRangeAware(metadata, keyCount, ActiveRepairService.UNREPAIRED_SSTABLE, ActiveRepairService.NO_PENDING_REPAIR, false, format, header);


        SSTable.Owner effectiveOwner;

        if (this.owner != null && this.owner.compressionDictionaryManager() != null && compressionDictionary != null)
        {
            // already checks if it is cached or not
            this.owner.compressionDictionaryManager().add(compressionDictionary);
            effectiveOwner = this.owner;
        }
        else
        {
            effectiveOwner = owner;
        }

        return SSTableTxnWriter.create(metadata,
                                       createDescriptor(directory, metadata.keyspace, metadata.name, format),
                                       keyCount,
                                       ActiveRepairService.UNREPAIRED_SSTABLE,
                                       ActiveRepairService.NO_PENDING_REPAIR,
                                       false,
                                       header,
                                       indexGroups,
                                       effectiveOwner);
    }

    private static Descriptor createDescriptor(File directory, final String keyspace, final String columnFamily, final SSTableFormat<?, ?> fmt) throws IOException
    {
        SSTableId nextGen = getNextId(directory, columnFamily);
        return new Descriptor(directory, keyspace, columnFamily, nextGen, fmt);
    }

    private static SSTableId getNextId(File directory, final String columnFamily) throws IOException
    {
        while (true)
        {
            try (Stream<Path> existingPaths = Files.list(directory.toPath()))
            {
                Stream<SSTableId> existingIds = existingPaths.map(File::new)
                                                             .map(SSTable::tryDescriptorFromFile)
                                                             .filter(d -> d != null && d.cfname.equals(columnFamily))
                                                             .map(d -> d.id);

                SSTableId lastId = id.get();
                SSTableId newId = SSTableIdFactory.instance.defaultBuilder().generator(Stream.concat(existingIds, Stream.of(lastId))).get();
                if (id.compareAndSet(lastId, newId))
                    return newId;
            }
        }
    }

    public PartitionUpdate.Builder getUpdateFor(ByteBuffer key) throws IOException
    {
        return getUpdateFor(metadata.get().partitioner.decorateKey(key));
    }

    /**
     * Returns a PartitionUpdate suitable to write on this writer for the provided key.
     *
     * @param key they partition key for which the returned update will be.
     * @return an update on partition {@code key} that is tied to this writer.
     */
    abstract PartitionUpdate.Builder getUpdateFor(DecoratedKey key) throws IOException;
}

