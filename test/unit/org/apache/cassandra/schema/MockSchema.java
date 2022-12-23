/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.schema;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.memtable.SkipListMemtable;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.IndexSummary;
import org.apache.cassandra.io.sstable.SSTableId;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.Memory;
import org.apache.cassandra.utils.AlwaysPresentFilter;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.service.ActiveRepairService.UNREPAIRED_SSTABLE;

public class MockSchema
{
    public static Supplier<? extends SSTableId> sstableIdGenerator = Util.newSeqGen();

    public static final ConcurrentMap<Integer, SSTableId> sstableIds = new ConcurrentHashMap<>();

    public static SSTableId sstableId(int idx)
    {
        return sstableIds.computeIfAbsent(idx, ignored -> sstableIdGenerator.get());
    }

    public static Collection<Object[]> sstableIdGenerators()
    {
        return Arrays.asList(new Object[]{ Util.newSeqGen() },
                             new Object[]{ Util.newUUIDGen() });
    }

    static
    {
        Memory offsets = Memory.allocate(4);
        offsets.setInt(0, 0);
        indexSummary = new IndexSummary(Murmur3Partitioner.instance, offsets, 0, Memory.allocate(4), 0, 0, 0, 1);
    }
    private static final AtomicInteger id = new AtomicInteger();
    public static final Keyspace ks = Keyspace.mockKS(KeyspaceMetadata.create("mockks", KeyspaceParams.simpleTransient(1)));

    public static final IndexSummary indexSummary;

    private static final File tempFile = temp("mocksegmentedfile");

    public static Memtable memtable(ColumnFamilyStore cfs)
    {
        return SkipListMemtable.FACTORY.create(null, cfs.metadata, cfs);
    }

    public static SSTableReader sstable(int generation, ColumnFamilyStore cfs)
    {
        return sstable(generation, false, cfs);
    }

    public static SSTableReader sstable(int generation, long first, long last, ColumnFamilyStore cfs)
    {
        return sstable(generation, 0, false, first, last, cfs);
    }

    public static SSTableReader sstable(int generation, boolean keepRef, ColumnFamilyStore cfs)
    {
        return sstable(generation, 0, keepRef, cfs);
    }

    public static SSTableReader sstable(int generation, int size, ColumnFamilyStore cfs)
    {
        return sstable(generation, size, false, cfs);
    }
    public static SSTableReader sstable(int generation, int size, boolean keepRef, ColumnFamilyStore cfs)
    {
        return sstable(generation, size, keepRef, generation, generation, cfs);
    }

    public static SSTableReader sstableWithLevel(int generation, long firstToken, long lastToken, int level, ColumnFamilyStore cfs)
    {
        return sstable(generation, 0, false, firstToken, lastToken, level, cfs);
    }

    public static SSTableReader sstableWithLevel(int generation, int size, int level, ColumnFamilyStore cfs)
    {
        return sstable(generation, size, false, generation, generation, level, cfs);
    }

    public static SSTableReader sstableWithTimestamp(int generation, long timestamp, ColumnFamilyStore cfs)
    {
        return sstable(generation, 0, false, 0, 1000, 0, Integer.MAX_VALUE, timestamp, cfs);
    }

    public static SSTableReader sstable(int generation, int size, boolean keepRef, long firstToken, long lastToken, ColumnFamilyStore cfs)
    {
        return sstable(generation, size, keepRef, firstToken, lastToken, 0, cfs);
    }

    public static SSTableReader sstable(int generation, int size, boolean keepRef, long firstToken, long lastToken, int level, ColumnFamilyStore cfs)
    {
        return sstable(generation, size, keepRef, firstToken, lastToken, level, Integer.MAX_VALUE, System.currentTimeMillis() * 1000, cfs);
    }

    public static SSTableReader sstable(int generation, int size, boolean keepRef, long firstToken, long lastToken, int level, int minLocalDeletionTime, long timestamp, ColumnFamilyStore cfs)
    {
        Descriptor descriptor = new Descriptor(cfs.getDirectories().getDirectoryForNewSSTables(),
                                               cfs.keyspace.getName(),
                                               cfs.getTableName(),
                                               sstableId(generation), SSTableFormat.Type.BIG);
        Set<Component> components = ImmutableSet.of(Component.DATA, Component.PRIMARY_INDEX, Component.FILTER, Component.TOC);
        for (Component component : components)
        {
            File file = new File(descriptor.filenameFor(component));
            file.createFileIfNotExists();
        }
        // .complete() with size to make sstable.onDiskLength work
        try (FileHandle.Builder builder = new FileHandle.Builder(new ChannelProxy(tempFile)).bufferSize(size);
             FileHandle fileHandle = builder.complete(size))
        {
            if (size > 0)
            {
                try
                {
                    File file = new File(descriptor.filenameFor(Component.DATA));
                    Util.setFileLength(file, size);
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
            }
            SerializationHeader header = SerializationHeader.make(cfs.metadata(), Collections.emptyList());
            MetadataCollector collector = new MetadataCollector(cfs.metadata().comparator);
            collector.update(new DeletionTime(timestamp, minLocalDeletionTime));
            StatsMetadata metadata = (StatsMetadata) collector.sstableLevel(level)
                                                              .finalizeMetadata(cfs.metadata().partitioner.getClass().getCanonicalName(), 0.01f, UNREPAIRED_SSTABLE, null, false, header)
                                                              .get(MetadataType.STATS);
            SSTableReader reader = SSTableReader.internalOpen(descriptor, components, cfs.metadata,
                                                              fileHandle.sharedCopy(), fileHandle.sharedCopy(), indexSummary.sharedCopy(),
                                                              new AlwaysPresentFilter(), 1L, metadata, SSTableReader.OpenReason.NORMAL, header);
            reader.first = readerBounds(firstToken);
            reader.last = readerBounds(lastToken);
            if (!keepRef)
                reader.selfRef().release();
            return reader;
        }

    }

    public static ColumnFamilyStore newCFS()
    {
        return newCFS(ks.getName());
    }

    public static ColumnFamilyStore newCFS(String ksname)
    {
        return newCFS(newTableMetadata(ksname));
    }

    public static ColumnFamilyStore newCFS(Function<TableMetadata.Builder, TableMetadata.Builder> options)
    {
        return newCFS(ks.getName(), options);
    }

    public static ColumnFamilyStore newCFS(String ksname, Function<TableMetadata.Builder, TableMetadata.Builder> options)
    {
        return newCFS(options.apply(newTableMetadataBuilder(ksname)).build());
    }

    public static ColumnFamilyStore newCFS(TableMetadata metadata)
    {
        return new ColumnFamilyStore(ks, metadata.name, Util.newSeqGen(), new TableMetadataRef(metadata), new Directories(metadata), false, false, false);
    }

    public static TableMetadata newTableMetadata(String ksname)
    {
        return newTableMetadata(ksname, "mockcf" + (id.incrementAndGet()));
    }

    public static TableMetadata newTableMetadata(String ksname, String cfname)
    {
        return newTableMetadataBuilder(ksname, cfname).build();
    }

    public static TableMetadata.Builder newTableMetadataBuilder(String ksname)
    {
        return newTableMetadataBuilder(ksname, "mockcf" + (id.incrementAndGet()));
    }

    public static TableMetadata.Builder newTableMetadataBuilder(String ksname, String cfname)
    {
        return TableMetadata.builder(ksname, cfname)
                            .partitioner(Murmur3Partitioner.instance)
                            .addPartitionKeyColumn("key", UTF8Type.instance)
                            .addClusteringColumn("col", UTF8Type.instance)
                            .addRegularColumn("value", UTF8Type.instance)
                            .caching(CachingParams.CACHE_NOTHING);
    }

    public static BufferDecoratedKey readerBounds(long generation)
    {
        return new BufferDecoratedKey(new Murmur3Partitioner.LongToken(generation), ByteBufferUtil.EMPTY_BYTE_BUFFER);
    }

    private static File temp(String id)
    {
        File file = FileUtils.createTempFile(id, "tmp");
        file.deleteOnExit();
        return file;
    }

    public static void cleanup()
    {
        // clean up data directory which are stored as data directory/keyspace/data files
        for (String dirName : DatabaseDescriptor.getAllDataFileLocations())
        {
            File dir = new File(dirName);
            if (!dir.exists())
                continue;
            String[] children = dir.tryListNames();
            for (String child : children)
                FileUtils.deleteRecursive(new File(dir, child));
        }
    }
}
