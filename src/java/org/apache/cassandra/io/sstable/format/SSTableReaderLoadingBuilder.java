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
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.IOOptions;
import org.apache.cassandra.io.sstable.KeyReader;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.sstable.metadata.ValidationMetadata;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.JVMStabilityInspector;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.cassandra.db.Directories.SECONDARY_INDEX_NAME_SEPARATOR;
import static org.apache.cassandra.io.sstable.format.SSTableReader.OpenReason.NORMAL;

public abstract class SSTableReaderLoadingBuilder<R extends SSTableReader, B extends SSTableReader.Builder<R, B>>
{
    private final static Logger logger = LoggerFactory.getLogger(SSTableReaderLoadingBuilder.class);

    protected final Descriptor descriptor;
    protected final Set<Component> components;
    protected final TableMetadataRef tableMetadataRef;
    protected final IOOptions ioOptions;
    protected final ChunkCache chunkCache;

    public SSTableReaderLoadingBuilder(SSTable.Builder<?, ?> builder)
    {
        this.descriptor = builder.descriptor;
        this.components = builder.getComponents() != null ? ImmutableSet.copyOf(builder.getComponents()) : TOCComponent.loadOrCreate(this.descriptor);
        this.tableMetadataRef = builder.getTableMetadataRef() != null ? builder.getTableMetadataRef() : resolveTableMetadataRef();
        this.ioOptions = builder.getIOOptions() != null ? builder.getIOOptions() : IOOptions.fromDatabaseDescriptor();
        this.chunkCache = builder.getChunkCache() != null ? builder.getChunkCache() : ChunkCache.instance;

        checkNotNull(this.components);
        checkNotNull(this.tableMetadataRef);
    }

    public R build(SSTable.Owner owner, boolean validate, boolean online)
    {
        checkArgument(components.contains(Components.DATA), "Data component is missing for sstable %s", descriptor);
        if (validate)
            checkArgument(this.components.containsAll(descriptor.getFormat().primaryComponents()), "Some required components (%s) are missing for sstable %s", Sets.difference(descriptor.getFormat().primaryComponents(), this.components), descriptor);

        B builder = (B) descriptor.getFormat().getReaderFactory().builder(descriptor);
        builder.setOpenReason(NORMAL);
        builder.setMaxDataAge(Clock.Global.currentTimeMillis());
        builder.setTableMetadataRef(tableMetadataRef);
        builder.setComponents(components);

        R reader = null;

        try
        {
            CompressionInfoComponent.verifyCompressionInfoExistenceIfApplicable(descriptor, builder.getComponents());

            long t0 = Clock.Global.currentTimeMillis();

            openComponents(builder, owner, validate, online);

            if (logger.isTraceEnabled())
                logger.trace("SSTable {} loaded in {}ms", descriptor, Clock.Global.currentTimeMillis() - t0);

            reader = builder.build(owner, validate, online);

            return reader;
        }
        catch (RuntimeException | IOException | Error ex)
        {
            if (reader != null)
                reader.selfRef().release();

            JVMStabilityInspector.inspectThrowable(ex);

            if (ex instanceof CorruptSSTableException)
                throw (CorruptSSTableException) ex;

            throw new CorruptSSTableException(ex, descriptor.baseFile());
        }
    }

    public abstract KeyReader buildKeyReader(TableMetrics tableMetrics) throws IOException;

    protected abstract void openComponents(B builder, SSTable.Owner owner, boolean validate, boolean online) throws IOException;

    /**
     * Check if sstable is created using same partitioner.
     * Partitioner can be null, which indicates older version of sstable or no stats available.
     * In that case, we skip the check.
     */
    protected void validatePartitioner(TableMetadata metadata, ValidationMetadata validationMetadata)
    {
        String partitionerName = metadata.partitioner.getClass().getCanonicalName();
        if (validationMetadata != null && !partitionerName.equals(validationMetadata.partitioner))
        {
            throw new CorruptSSTableException(new IOException(String.format("Cannot open %s; partitioner %s does not match system partitioner %s. " +
                                                                            "Note that the default partitioner starting with Cassandra 1.2 is Murmur3Partitioner, " +
                                                                            "so you will need to edit that to match your old partitioner if upgrading.",
                                                                            descriptor, validationMetadata.partitioner, partitionerName)),
                                              descriptor.fileFor(Components.STATS));
        }
    }

    private TableMetadataRef resolveTableMetadataRef()
    {
        TableMetadataRef metadata;
        if (descriptor.cfname.contains(SECONDARY_INDEX_NAME_SEPARATOR))
        {
            int i = descriptor.cfname.indexOf(SECONDARY_INDEX_NAME_SEPARATOR);
            String indexName = descriptor.cfname.substring(i + 1);
            metadata = Schema.instance.getIndexTableMetadataRef(descriptor.ksname, indexName);
            if (metadata == null)
                throw new AssertionError("Could not find index metadata for index cf " + i);
        }
        else
        {
            metadata = Schema.instance.getTableMetadataRef(descriptor.ksname, descriptor.cfname);
        }

        return metadata;
    }
}
