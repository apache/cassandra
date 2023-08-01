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
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.memtable.SkipListMemtable;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableId;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.format.big.BigFormat.Components;
import org.apache.cassandra.io.sstable.format.big.BigTableReader;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;
import org.apache.cassandra.io.sstable.format.bti.BtiTableReader;
import org.apache.cassandra.io.sstable.format.bti.PartitionIndex;
import org.apache.cassandra.io.sstable.indexsummary.IndexSummary;
import org.apache.cassandra.io.sstable.keycache.KeyCache;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.Memory;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FilterFactory;
import org.apache.cassandra.utils.Throwables;

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

    private static final File tempFile = temp("mocksegmentedfile");

    static
    {
        Memory offsets = Memory.allocate(4);
        offsets.setInt(0, 0);
        indexSummary = new IndexSummary(Murmur3Partitioner.instance, offsets, 0, Memory.allocate(4), 0, 0, 0, 1);

        try (DataOutputStreamPlus out = tempFile.newOutputStream(File.WriteMode.OVERWRITE))
        {
            out.write(new byte[10]);
        }
        catch (IOException ex)
        {
            throw Throwables.throwAsUncheckedException(ex);
        }
    }
    private static final AtomicInteger id = new AtomicInteger();
    public static final Keyspace ks = Keyspace.mockKS(KeyspaceMetadata.create("mockks", KeyspaceParams.simpleTransient(1)));

    public static final IndexSummary indexSummary;

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

    public static SSTableReader sstable(int generation, long first, long last, int minLocalDeletionTime, ColumnFamilyStore cfs)
    {
        return sstable(generation, 0, false, first, last, 0, cfs, minLocalDeletionTime);
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
        return sstable(generation, 0, false, 0, 1000, 0, cfs, Integer.MAX_VALUE, timestamp);
    }

    public static SSTableReader sstable(int generation, int size, boolean keepRef, long firstToken, long lastToken, int level, ColumnFamilyStore cfs)
    {
        return sstable(generation, size, keepRef, firstToken, lastToken, level, cfs, Integer.MAX_VALUE);
    }

    public static SSTableReader sstable(int generation, int size, boolean keepRef, long firstToken, long lastToken, ColumnFamilyStore cfs)
    {
        return sstable(generation, size, keepRef, firstToken, lastToken, 0, cfs);
    }

    public static SSTableReader sstable(int generation, int size, boolean keepRef, long firstToken, long lastToken, int level, ColumnFamilyStore cfs, int minLocalDeletionTime)
    {
        return sstable(generation, size, keepRef, firstToken, lastToken, level, cfs, minLocalDeletionTime, System.currentTimeMillis() * 1000);
    }

    public static SSTableReader sstable(int generation, int size, boolean keepRef, long firstToken, long lastToken, int level, ColumnFamilyStore cfs, int minLocalDeletionTime, long timestamp)
    {
        SSTableFormat<?, ?> format = DatabaseDescriptor.getSelectedSSTableFormat();
        Descriptor descriptor = new Descriptor(cfs.getDirectories().getDirectoryForNewSSTables(),
                                               cfs.getKeyspaceName(),
                                               cfs.getTableName(),
                                               sstableId(generation),
                                               format);

        if (BigFormat.is(format))
        {
            Set<Component> components = ImmutableSet.of(Components.DATA, Components.PRIMARY_INDEX, Components.FILTER, Components.TOC);
            for (Component component : components)
            {
                File file = descriptor.fileFor(component);
                file.createFileIfNotExists();
            }
            // .complete() with size to make sstable.onDiskLength work
            try (FileHandle fileHandle = new FileHandle.Builder(tempFile).bufferSize(size).withLengthOverride(size).complete())
            {
                maybeSetDataLength(descriptor, size);
                SerializationHeader header = SerializationHeader.make(cfs.metadata(), Collections.emptyList());
                MetadataCollector collector = new MetadataCollector(cfs.metadata().comparator);
                collector.update(DeletionTime.build(timestamp, minLocalDeletionTime));
                BufferDecoratedKey first = readerBounds(firstToken);
                BufferDecoratedKey last = readerBounds(lastToken);
                StatsMetadata metadata =
                                       (StatsMetadata) collector.sstableLevel(level)
                                                                .finalizeMetadata(cfs.metadata().partitioner.getClass().getCanonicalName(),
                                                                                  0.01f,
                                                                                  UNREPAIRED_SSTABLE,
                                                                                  null,
                                                                                  false,
                                                                                  header,
                                                                                  first.retainable().getKey().slice(),
                                                                                  last.retainable().getKey().slice())
                                                                .get(MetadataType.STATS);
                BigTableReader reader = new BigTableReader.Builder(descriptor).setComponents(components)
                                                                              .setTableMetadataRef(cfs.metadata)
                                                                              .setDataFile(fileHandle.sharedCopy())
                                                                              .setIndexFile(fileHandle.sharedCopy())
                                                                              .setIndexSummary(indexSummary.sharedCopy())
                                                                              .setFilter(FilterFactory.AlwaysPresent)
                                                                              .setMaxDataAge(1L)
                                                                              .setStatsMetadata(metadata)
                                                                              .setOpenReason(SSTableReader.OpenReason.NORMAL)
                                                                              .setSerializationHeader(header)
                                                                              .setFirst(first)
                                                                              .setLast(last)
                                                                              .setKeyCache(cfs.metadata().params.caching.cacheKeys ? new KeyCache(CacheService.instance.keyCache)
                                                                                                                                   : KeyCache.NO_CACHE)
                                                                              .build(cfs, false, false);
                if (!keepRef)
                    reader.selfRef().release();
                return reader;
            }
        }
        else if (BtiFormat.is(format))
        {
            Set<Component> components = ImmutableSet.of(Components.DATA, BtiFormat.Components.PARTITION_INDEX, BtiFormat.Components.ROW_INDEX, Components.FILTER, Components.TOC);
            for (Component component : components)
            {
                File file = descriptor.fileFor(component);
                file.createFileIfNotExists();
            }
            // .complete() with size to make sstable.onDiskLength work
            try (FileHandle fileHandle = new FileHandle.Builder(tempFile).bufferSize(size).withLengthOverride(size).complete())
            {
                maybeSetDataLength(descriptor, size);
                SerializationHeader header = SerializationHeader.make(cfs.metadata(), Collections.emptyList());
                MetadataCollector collector = new MetadataCollector(cfs.metadata().comparator);
                collector.update(DeletionTime.build(timestamp, minLocalDeletionTime));
                BufferDecoratedKey first = readerBounds(firstToken);
                BufferDecoratedKey last = readerBounds(lastToken);
                StatsMetadata metadata = (StatsMetadata) collector.sstableLevel(level)
                                                                  .finalizeMetadata(cfs.metadata().partitioner.getClass().getCanonicalName(), 0.01f, UNREPAIRED_SSTABLE, null, false, header, first.retainable().getKey(), last.retainable().getKey())
                                                                  .get(MetadataType.STATS);
                BtiTableReader reader = new BtiTableReader.Builder(descriptor).setComponents(components)
                                                                              .setTableMetadataRef(cfs.metadata)
                                                                              .setDataFile(fileHandle.sharedCopy())
                                                                              .setPartitionIndex(new PartitionIndex(fileHandle.sharedCopy(), 0, 0, readerBounds(firstToken), readerBounds(lastToken)))
                                                                              .setRowIndexFile(fileHandle.sharedCopy())
                                                                              .setFilter(FilterFactory.AlwaysPresent)
                                                                              .setMaxDataAge(1L)
                                                                              .setStatsMetadata(metadata)
                                                                              .setOpenReason(SSTableReader.OpenReason.NORMAL)
                                                                              .setSerializationHeader(header)
                                                                              .setFirst(readerBounds(firstToken))
                                                                              .setLast(readerBounds(lastToken))
                                                                              .build(cfs, false, false);
                if (!keepRef)
                    reader.selfRef().release();
                return reader;
            }
        }
        else
        {
            throw Util.testMustBeImplementedForSSTableFormat();
        }
    }

    private static void maybeSetDataLength(Descriptor descriptor, long size)
    {
        if (size > 0)
        {
            try
            {
                File file = descriptor.fileFor(Components.DATA);
                Util.setFileLength(file, size);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
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
