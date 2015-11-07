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

import java.util.Collection;
import java.util.Set;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.*;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ChecksumType;

/**
 * Legacy bigtable format
 */
public class BigFormat implements SSTableFormat
{
    public static final BigFormat instance = new BigFormat();
    public static final Version latestVersion = new BigVersion(BigVersion.current_version);
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
    public RowIndexEntry.IndexSerializer getIndexSerializer(CFMetaData metadata, Version version, SerializationHeader header)
    {
        return new RowIndexEntry.Serializer(metadata, version, header);
    }

    static class WriterFactory extends SSTableWriter.Factory
    {
        @Override
        public SSTableWriter open(Descriptor descriptor,
                                  long keyCount,
                                  long repairedAt,
                                  CFMetaData metadata,
                                  MetadataCollector metadataCollector,
                                  SerializationHeader header,
                                  Collection<SSTableFlushObserver> observers,
                                  LifecycleTransaction txn)
        {
            return new BigTableWriter(descriptor, keyCount, repairedAt, metadata, metadataCollector, header, observers, txn);
        }
    }

    static class ReaderFactory extends SSTableReader.Factory
    {
        @Override
        public SSTableReader open(Descriptor descriptor, Set<Component> components, CFMetaData metadata, Long maxDataAge, StatsMetadata sstableMetadata, SSTableReader.OpenReason openReason, SerializationHeader header)
        {
            return new BigTableReader(descriptor, components, metadata, maxDataAge, sstableMetadata, openReason, header);
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
        public static final String current_version = "ma";
        public static final String earliest_supported_version = "jb";

        // jb (2.0.1): switch from crc32 to adler32 for compression checksums
        //             checksum the compressed data
        // ka (2.1.0): new Statistics.db file format
        //             index summaries can be downsampled and the sampling level is persisted
        //             switch uncompressed checksums to adler32
        //             tracks presense of legacy (local and remote) counter shards
        // la (2.2.0): new file name format
        // ma (3.0.0): swap bf hash order
        //             store rows natively
        //
        // NOTE: when adding a new version, please add that to LegacySSTableTest, too.

        private final boolean isLatestVersion;
        private final boolean hasSamplingLevel;
        private final boolean newStatsFile;
        private final ChecksumType compressedChecksumType;
        private final ChecksumType uncompressedChecksumType;
        private final boolean hasRepairedAt;
        private final boolean tracksLegacyCounterShards;
        private final boolean newFileName;
        public final boolean storeRows;
        public final int correspondingMessagingVersion; // Only use by storage that 'storeRows' so far
        public final boolean hasBoundaries;
        /**
         * CASSANDRA-8413: 3.0 bloom filter representation changed (two longs just swapped)
         * have no 'static' bits caused by using the same upper bits for both bloom filter and token distribution.
         */
        private final boolean hasOldBfHashOrder;

        /**
         * CASSANDRA-7066: compaction ancerstors are no longer used and have been removed.
         */
        private final boolean hasCompactionAncestors;

        BigVersion(String version)
        {
            super(instance, version);

            isLatestVersion = version.compareTo(current_version) == 0;
            hasSamplingLevel = version.compareTo("ka") >= 0;
            newStatsFile = version.compareTo("ka") >= 0;

            //For a while Adler32 was in use, now the CRC32 instrinsic is very good especially after Haswell
            //PureJavaCRC32 was always faster than Adler32. See CASSANDRA-8684
            ChecksumType checksumType = ChecksumType.CRC32;
            if (version.compareTo("ka") >= 0 && version.compareTo("ma") < 0)
                checksumType = ChecksumType.Adler32;
            this.uncompressedChecksumType = checksumType;

            checksumType = ChecksumType.CRC32;
            if (version.compareTo("jb") >= 0 && version.compareTo("ma") < 0)
                checksumType = ChecksumType.Adler32;
            this.compressedChecksumType = checksumType;

            hasRepairedAt = version.compareTo("ka") >= 0;
            tracksLegacyCounterShards = version.compareTo("ka") >= 0;

            newFileName = version.compareTo("la") >= 0;

            hasOldBfHashOrder = version.compareTo("ma") < 0;
            hasCompactionAncestors = version.compareTo("ma") < 0;
            storeRows = version.compareTo("ma") >= 0;
            correspondingMessagingVersion = storeRows
                                          ? MessagingService.VERSION_30
                                          : MessagingService.VERSION_21;

            hasBoundaries = version.compareTo("ma") < 0;
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
        public ChecksumType compressedChecksumType()
        {
            return compressedChecksumType;
        }

        @Override
        public ChecksumType uncompressedChecksumType()
        {
            return uncompressedChecksumType;
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
        public boolean hasOldBfHashOrder()
        {
            return hasOldBfHashOrder;
        }

        @Override
        public boolean hasCompactionAncestors()
        {
            return hasCompactionAncestors;
        }

        @Override
        public boolean hasNewFileName()
        {
            return newFileName;
        }

        @Override
        public boolean storeRows()
        {
            return storeRows;
        }

        @Override
        public int correspondingMessagingVersion()
        {
            return correspondingMessagingVersion;
        }

        @Override
        public boolean hasBoundaries()
        {
            return hasBoundaries;
        }

        @Override
        public boolean isCompatible()
        {
            return version.compareTo(earliest_supported_version) >= 0 && version.charAt(0) <= current_version.charAt(0);
        }

        @Override
        public boolean isCompatibleForStreaming()
        {
            return isCompatible() && version.charAt(0) == current_version.charAt(0);
        }
    }
}
