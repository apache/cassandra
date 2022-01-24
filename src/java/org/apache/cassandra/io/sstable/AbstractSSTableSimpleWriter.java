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


import java.io.IOException;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.service.ActiveRepairService;

/**
 * Base class for the sstable writers used by CQLSSTableWriter.
 */
abstract class AbstractSSTableSimpleWriter implements Closeable
{
    protected final File directory;
    protected final TableMetadataRef metadata;
    protected final RegularAndStaticColumns columns;
    protected SSTableFormat.Type formatType = SSTableFormat.Type.current();
    protected static final AtomicReference<SSTableId> id = new AtomicReference<>(SSTableIdFactory.instance.defaultBuilder().generator(Stream.empty()).get());
    protected boolean makeRangeAware = false;

    protected AbstractSSTableSimpleWriter(File directory, TableMetadataRef metadata, RegularAndStaticColumns columns)
    {
        this.metadata = metadata;
        this.directory = directory;
        this.columns = columns;
    }

    protected void setSSTableFormatType(SSTableFormat.Type type)
    {
        this.formatType = type;
    }

    protected void setRangeAwareWriting(boolean makeRangeAware)
    {
        this.makeRangeAware = makeRangeAware;
    }

    protected SSTableTxnWriter createWriter() throws IOException
    {
        SerializationHeader header = new SerializationHeader(true, metadata.get(), columns, EncodingStats.NO_STATS);

        if (makeRangeAware)
            return SSTableTxnWriter.createRangeAware(metadata, 0,  ActiveRepairService.UNREPAIRED_SSTABLE, ActiveRepairService.NO_PENDING_REPAIR, false, formatType, 0, header);

        return SSTableTxnWriter.create(metadata,
                                       createDescriptor(directory, metadata.keyspace, metadata.name, formatType),
                                       0,
                                       ActiveRepairService.UNREPAIRED_SSTABLE,
                                       ActiveRepairService.NO_PENDING_REPAIR,
                                       false,
                                       0,
                                       header,
                                       Collections.emptySet());
    }

    private static Descriptor createDescriptor(File directory, final String keyspace, final String columnFamily, final SSTableFormat.Type fmt) throws IOException
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
                                                             .map(SSTable::tryDescriptorFromFilename)
                                                             .filter(d -> d != null && d.cfname.equals(columnFamily))
                                                             .map(d -> d.id);

                SSTableId lastId = id.get();
                SSTableId newId = SSTableIdFactory.instance.defaultBuilder().generator(Stream.concat(existingIds, Stream.of(lastId))).get();
                if (id.compareAndSet(lastId, newId))
                    return newId;
            }
        }
    }

    PartitionUpdate.Builder getUpdateFor(ByteBuffer key) throws IOException
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

