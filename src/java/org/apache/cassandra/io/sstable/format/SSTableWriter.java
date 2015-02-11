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

import com.google.common.collect.Sets;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.compaction.AbstractCompactedRow;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * This is the API all table writers must implement.
 *
 * TableWriter.create() is the primary way to create a writer for a particular format.
 * The format information is part of the Descriptor.
 */
public abstract class SSTableWriter extends SSTable
{
    private static final Logger logger = LoggerFactory.getLogger(SSTableWriter.class);

    public static enum FinishType
    {
        CLOSE(null, true),
        NORMAL(SSTableReader.OpenReason.NORMAL, true),
        EARLY(SSTableReader.OpenReason.EARLY, false), // no renaming
        FINISH_EARLY(SSTableReader.OpenReason.NORMAL, true); // tidy up an EARLY finish
        public final SSTableReader.OpenReason openReason;

        public final boolean isFinal;
        FinishType(SSTableReader.OpenReason openReason, boolean isFinal)
        {
            this.openReason = openReason;
            this.isFinal = isFinal;
        }
    }

    protected final long repairedAt;
    protected final long keyCount;
    protected final MetadataCollector metadataCollector;
    protected final RowIndexEntry.IndexSerializer rowIndexEntrySerializer;

    protected SSTableWriter(Descriptor descriptor, long keyCount, long repairedAt, CFMetaData metadata, IPartitioner partitioner, MetadataCollector metadataCollector)
    {
        super(descriptor, components(metadata), metadata, partitioner);
        this.keyCount = keyCount;
        this.repairedAt = repairedAt;
        this.metadataCollector = metadataCollector;
        this.rowIndexEntrySerializer = descriptor.version.getSSTableFormat().getIndexSerializer(metadata);
    }

    public static SSTableWriter create(Descriptor descriptor, Long keyCount, Long repairedAt, CFMetaData metadata,  IPartitioner partitioner, MetadataCollector metadataCollector)
    {
        Factory writerFactory = descriptor.getFormat().getWriterFactory();
        return writerFactory.open(descriptor, keyCount, repairedAt, metadata, partitioner, metadataCollector);
    }

    public static SSTableWriter create(Descriptor descriptor, long keyCount, long repairedAt)
    {
        return create(descriptor, keyCount, repairedAt, 0);
    }

    public static SSTableWriter create(Descriptor descriptor, long keyCount, long repairedAt, int sstableLevel)
    {
        CFMetaData metadata = Schema.instance.getCFMetaData(descriptor);
        MetadataCollector collector = new MetadataCollector(metadata.comparator).sstableLevel(sstableLevel);

        return create(descriptor, keyCount, repairedAt, metadata, DatabaseDescriptor.getPartitioner(), collector);
    }

    public static SSTableWriter create(String filename, long keyCount, long repairedAt, int sstableLevel)
    {
        return create(Descriptor.fromFilename(filename), keyCount, repairedAt, sstableLevel);
    }

    public static SSTableWriter create(String filename, long keyCount, long repairedAt)
    {
        return create(Descriptor.fromFilename(filename), keyCount, repairedAt, 0);
    }

    private static Set<Component> components(CFMetaData metadata)
    {
        Set<Component> components = new HashSet<Component>(Arrays.asList(Component.DATA,
                Component.PRIMARY_INDEX,
                Component.STATS,
                Component.SUMMARY,
                Component.TOC,
                Component.DIGEST));

        if (metadata.getBloomFilterFpChance() < 1.0)
            components.add(Component.FILTER);

        if (metadata.compressionParameters().sstableCompressor != null)
        {
            components.add(Component.COMPRESSION_INFO);
        }
        else
        {
            // it would feel safer to actually add this component later in maybeWriteDigest(),
            // but the components are unmodifiable after construction
            components.add(Component.CRC);
        }
        return components;
    }


    public abstract void mark();


    /**
     * @param row
     * @return null if the row was compacted away entirely; otherwise, the PK index entry for this row
     */
    public abstract RowIndexEntry append(AbstractCompactedRow row);

    public abstract void append(DecoratedKey decoratedKey, ColumnFamily cf);

    public abstract long appendFromStream(DecoratedKey key, CFMetaData metadata, DataInput in, Version version) throws IOException;

    public abstract long getFilePointer();

    public abstract long getOnDiskFilePointer();

    public abstract void isolateReferences();

    public abstract void resetAndTruncate();

    public SSTableReader closeAndOpenReader()
    {
        return closeAndOpenReader(System.currentTimeMillis());
    }

    public SSTableReader closeAndOpenReader(long maxDataAge)
    {
        return finish(FinishType.NORMAL, maxDataAge, repairedAt);
    }

    public abstract SSTableReader finish(FinishType finishType, long maxDataAge, long repairedAt);

    public abstract SSTableReader openEarly(long maxDataAge);

    // Close the writer and return the descriptor to the new sstable and it's metadata
    public abstract Pair<Descriptor, StatsMetadata> close();



    public static Descriptor rename(Descriptor tmpdesc, Set<Component> components)
    {
        Descriptor newdesc = tmpdesc.asType(Descriptor.Type.FINAL);
        rename(tmpdesc, newdesc, components);
        return newdesc;
    }

    public static void rename(Descriptor tmpdesc, Descriptor newdesc, Set<Component> components)
    {
        for (Component component : Sets.difference(components, Sets.newHashSet(Component.DATA, Component.SUMMARY)))
        {
            FileUtils.renameWithConfirm(tmpdesc.filenameFor(component), newdesc.filenameFor(component));
        }

        // do -Data last because -Data present should mean the sstable was completely renamed before crash
        FileUtils.renameWithConfirm(tmpdesc.filenameFor(Component.DATA), newdesc.filenameFor(Component.DATA));

        // rename it without confirmation because summary can be available for loadNewSSTables but not for closeAndOpenReader
        FileUtils.renameWithOutConfirm(tmpdesc.filenameFor(Component.SUMMARY), newdesc.filenameFor(Component.SUMMARY));
    }


    /**
     * After failure, attempt to close the index writer and data file before deleting all temp components for the sstable
     */
    public abstract void abort();


    public static abstract class Factory
    {
        public abstract SSTableWriter open(Descriptor descriptor, long keyCount, long repairedAt, CFMetaData metadata, IPartitioner partitioner, MetadataCollector metadataCollector);
    }
}
