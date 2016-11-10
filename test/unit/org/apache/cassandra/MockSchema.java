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
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.cache.CachingOptions;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.composites.SimpleSparseCellNameType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.IndexSummary;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.BufferedSegmentedFile;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.Memory;
import org.apache.cassandra.io.util.SegmentedFile;
import org.apache.cassandra.locator.SimpleStrategy;
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
    public static final Keyspace ks = Keyspace.mockKS(new KSMetaData("mockks", SimpleStrategy.class, ImmutableMap.of("replication_factor", "1"), false));

    private static final IndexSummary indexSummary;
    private static final SegmentedFile segmentedFile = new BufferedSegmentedFile(new ChannelProxy(temp("mocksegmentedfile")), 0);

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
        Descriptor descriptor = new Descriptor(cfs.directories.getDirectoryForNewSSTables(),
                                               cfs.keyspace.getName(),
                                               cfs.getColumnFamilyName(),
                                               generation,
                                               Descriptor.Type.FINAL);
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
        StatsMetadata metadata = (StatsMetadata) new MetadataCollector(cfs.metadata.comparator)
                                                 .finalizeMetadata(Murmur3Partitioner.instance.getClass().getCanonicalName(), 0.01f, -1)
                                                 .get(MetadataType.STATS);
        SSTableReader reader = SSTableReader.internalOpen(descriptor, components, cfs.metadata, Murmur3Partitioner.instance,
                                                          segmentedFile.sharedCopy(), segmentedFile.sharedCopy(), indexSummary.sharedCopy(),
                                                          new AlwaysPresentFilter(), 1L, metadata, SSTableReader.OpenReason.NORMAL);
        reader.first = reader.last = readerBounds(generation);
        if (!keepRef)
            reader.selfRef().release();
        return reader;
    }

    public static ColumnFamilyStore newCFS()
    {
        String cfname = "mockcf" + (id.incrementAndGet());
        CFMetaData metadata = newCFMetaData(ks.getName(), cfname);
        return new ColumnFamilyStore(ks, cfname, Murmur3Partitioner.instance, 0, metadata, new Directories(metadata), false, false);
    }

    private static CFMetaData newCFMetaData(String ksname, String cfname)
    {
        CFMetaData metadata = new CFMetaData(ksname,
                                             cfname,
                                             ColumnFamilyType.Standard,
                                             new SimpleSparseCellNameType(UTF8Type.instance));
        metadata.caching(CachingOptions.NONE);
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
