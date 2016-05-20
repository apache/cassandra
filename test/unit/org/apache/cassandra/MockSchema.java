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
package org.apache.cassandra;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.IndexSummary;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.Memory;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.schema.CachingParams;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.AlwaysPresentFilter;
import org.apache.cassandra.utils.ByteBufferUtil;

public class MockSchema
{
    static
    {
        Memory offsets = Memory.allocate(4);
        offsets.setInt(0, 0);
        indexSummary = new IndexSummary(Murmur3Partitioner.instance, offsets, 0, Memory.allocate(4), 0, 0, 0, 1);
    }
    private static final AtomicInteger id = new AtomicInteger();
    public static final Keyspace ks = Keyspace.mockKS(KeyspaceMetadata.create("mockks", KeyspaceParams.simpleTransient(1)));

    public static final IndexSummary indexSummary;
    private static final FileHandle RANDOM_ACCESS_READER_FACTORY = new FileHandle.Builder(temp("mocksegmentedfile").getAbsolutePath()).complete();

    public static Memtable memtable(ColumnFamilyStore cfs)
    {
        return new Memtable(cfs.metadata);
    }

    public static SSTableReader sstable(int generation, ColumnFamilyStore cfs)
    {
        return sstable(generation, false, cfs);
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
        Descriptor descriptor = new Descriptor(cfs.getDirectories().getDirectoryForNewSSTables(),
                                               cfs.keyspace.getName(),
                                               cfs.getColumnFamilyName(),
                                               generation, SSTableFormat.Type.BIG);
        Set<Component> components = ImmutableSet.of(Component.DATA, Component.PRIMARY_INDEX, Component.FILTER, Component.TOC);
        for (Component component : components)
        {
            File file = new File(descriptor.filenameFor(component));
            try
            {
                file.createNewFile();
            }
            catch (IOException e)
            {
            }
        }
        if (size > 0)
        {
            try
            {
                File file = new File(descriptor.filenameFor(Component.DATA));
                try (RandomAccessFile raf = new RandomAccessFile(file, "rw"))
                {
                    raf.setLength(size);
                }
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }
        SerializationHeader header = SerializationHeader.make(cfs.metadata, Collections.emptyList());
        StatsMetadata metadata = (StatsMetadata) new MetadataCollector(cfs.metadata.comparator)
                                                 .finalizeMetadata(cfs.metadata.partitioner.getClass().getCanonicalName(), 0.01f, -1, header)
                                                 .get(MetadataType.STATS);
        SSTableReader reader = SSTableReader.internalOpen(descriptor, components, cfs.metadata,
                                                          RANDOM_ACCESS_READER_FACTORY.sharedCopy(), RANDOM_ACCESS_READER_FACTORY.sharedCopy(), indexSummary.sharedCopy(),
                                                          new AlwaysPresentFilter(), 1L, metadata, SSTableReader.OpenReason.NORMAL, header);
        reader.first = reader.last = readerBounds(generation);
        if (!keepRef)
            reader.selfRef().release();
        return reader;
    }

    public static ColumnFamilyStore newCFS()
    {
        return newCFS(ks.getName());
    }

    public static ColumnFamilyStore newCFS(String ksname)
    {
        String cfname = "mockcf" + (id.incrementAndGet());
        CFMetaData metadata = newCFMetaData(ksname, cfname);
        return new ColumnFamilyStore(ks, cfname, 0, metadata, new Directories(metadata), false, false, false);
    }

    public static CFMetaData newCFMetaData(String ksname, String cfname)
    {
        CFMetaData metadata = CFMetaData.Builder.create(ksname, cfname)
                                                .addPartitionKey("key", UTF8Type.instance)
                                                .addClusteringColumn("col", UTF8Type.instance)
                                                .addRegularColumn("value", UTF8Type.instance)
                                                .withPartitioner(Murmur3Partitioner.instance)
                                                .build();
        metadata.caching(CachingParams.CACHE_NOTHING);
        return metadata;
    }

    public static BufferDecoratedKey readerBounds(int generation)
    {
        return new BufferDecoratedKey(new Murmur3Partitioner.LongToken(generation), ByteBufferUtil.EMPTY_BYTE_BUFFER);
    }

    private static File temp(String id)
    {
        try
        {
            File file = File.createTempFile(id, "tmp");
            file.deleteOnExit();
            return file;
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static void cleanup()
    {
        // clean up data directory which are stored as data directory/keyspace/data files
        for (String dirName : DatabaseDescriptor.getAllDataFileLocations())
        {
            File dir = new File(dirName);
            if (!dir.exists())
                continue;
            String[] children = dir.list();
            for (String child : children)
                FileUtils.deleteRecursive(new File(dir, child));
        }
    }
}
