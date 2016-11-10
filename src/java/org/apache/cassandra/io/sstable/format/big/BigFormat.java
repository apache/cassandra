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
package org.apache.cassandra.io.sstable.format.big;

import com.google.common.collect.ImmutableList;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.AbstractCell;
import org.apache.cassandra.db.ColumnSerializer;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.db.compaction.AbstractCompactedRow;
import org.apache.cassandra.db.compaction.CompactionController;
import org.apache.cassandra.db.compaction.LazilyCompactedRow;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.IndexHelper;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.FileDataInput;

import java.util.Iterator;
import java.util.Set;

/**
 * Legacy bigtable format
 */
public class BigFormat implements SSTableFormat
{
    public static final BigFormat instance = new BigFormat();
    public static final BigVersion latestVersion = new BigVersion(BigVersion.current_version);
    private static final SSTableReader.Factory readerFactory = new ReaderFactory();
    private static final SSTableWriter.Factory writerFactory = new WriterFactory();

    private BigFormat()
    {

    }

    @Override
    public Version getLatestVersion()
    {
        return latestVersion;
    }

    @Override
    public Version getVersion(String version)
    {
        return new BigVersion(version);
    }

    @Override
    public SSTableWriter.Factory getWriterFactory()
    {
        return writerFactory;
    }

    @Override
    public SSTableReader.Factory getReaderFactory()
    {
        return readerFactory;
    }

    @Override
    public Iterator<OnDiskAtom> getOnDiskIterator(FileDataInput in, ColumnSerializer.Flag flag, int expireBefore, CFMetaData cfm, Version version)
    {
        return AbstractCell.onDiskIterator(in, flag, expireBefore, version, cfm.comparator);
    }

    @Override
    public AbstractCompactedRow getCompactedRowWriter(CompactionController controller, ImmutableList<OnDiskAtomIterator> onDiskAtomIterators)
    {
        return new LazilyCompactedRow(controller, onDiskAtomIterators);
    }

    @Override
    public RowIndexEntry.IndexSerializer getIndexSerializer(CFMetaData cfMetaData)
    {
        return new RowIndexEntry.Serializer(new IndexHelper.IndexInfo.Serializer(cfMetaData.comparator));
    }

    static class WriterFactory extends SSTableWriter.Factory
    {
        @Override
        public SSTableWriter open(Descriptor descriptor, long keyCount, long repairedAt, CFMetaData metadata, IPartitioner partitioner, MetadataCollector metadataCollector)
        {
            return new BigTableWriter(descriptor, keyCount, repairedAt, metadata, partitioner, metadataCollector);
        }
    }

    static class ReaderFactory extends SSTableReader.Factory
    {
        @Override
        public SSTableReader open(Descriptor descriptor, Set<Component> components, CFMetaData metadata, IPartitioner partitioner, Long maxDataAge, StatsMetadata sstableMetadata, SSTableReader.OpenReason openReason)
        {
            return new BigTableReader(descriptor, components, metadata, partitioner, maxDataAge, sstableMetadata, openReason);
        }
    }

    // versions are denoted as [major][minor].  Minor versions must be forward-compatible:
    // new fields are allowed in e.g. the metadata component, but fields can't be removed
    // or have their size changed.
    //
    // Minor versions were introduced with version "hb" for Cassandra 1.0.3; prior to that,
    // we always incremented the major version.
    static class BigVersion extends Version
    {
        public static final String current_version = "lb";
        public static final String earliest_supported_version = "jb";

        // jb (2.0.1): switch from crc32 to adler32 for compression checksums
        //             checksum the compressed data
        // ka (2.1.0): new Statistics.db file format
        //             index summaries can be downsampled and the sampling level is persisted
        //             switch uncompressed checksums to adler32
        //             tracks presense of legacy (local and remote) counter shards
        // la (2.2.0): new file name format
        // lb (2.2.7): commit log lower bound included

        private final boolean isLatestVersion;
        private final boolean hasSamplingLevel;
        private final boolean newStatsFile;
        private final boolean hasAllAdlerChecksums;
        private final boolean hasRepairedAt;
        private final boolean tracksLegacyCounterShards;
        private final boolean newFileName;
        private final boolean hasCommitLogLowerBound;

        public BigVersion(String version)
        {
            super(instance,version);

            isLatestVersion = version.compareTo(current_version) == 0;
            hasSamplingLevel = version.compareTo("ka") >= 0;
            newStatsFile = version.compareTo("ka") >= 0;
            hasAllAdlerChecksums = version.compareTo("ka") >= 0;
            hasRepairedAt = version.compareTo("ka") >= 0;
            tracksLegacyCounterShards = version.compareTo("ka") >= 0;
            newFileName = version.compareTo("la") >= 0;
            hasCommitLogLowerBound = version.compareTo("lb") >= 0;
        }

        @Override
        public boolean isLatestVersion()
        {
            return isLatestVersion;
        }

        @Override
        public boolean hasSamplingLevel()
        {
            return hasSamplingLevel;
        }

        @Override
        public boolean hasNewStatsFile()
        {
            return newStatsFile;
        }

        @Override
        public boolean hasAllAdlerChecksums()
        {
            return hasAllAdlerChecksums;
        }

        @Override
        public boolean hasRepairedAt()
        {
            return hasRepairedAt;
        }

        @Override
        public boolean tracksLegacyCounterShards()
        {
            return tracksLegacyCounterShards;
        }

        @Override
        public boolean hasNewFileName()
        {
            return newFileName;
        }

        public boolean hasCommitLogLowerBound()
        {
            return hasCommitLogLowerBound;
        }

        @Override
        public boolean isCompatible()
        {
            return version.compareTo(earliest_supported_version) >= 0 && version.charAt(0) <= current_version.charAt(0);
        }
    }
}
