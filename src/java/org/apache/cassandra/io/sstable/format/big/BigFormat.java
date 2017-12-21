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
import java.util.UUID;

import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.*;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.net.MessagingService;

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
    public RowIndexEntry.IndexSerializer getIndexSerializer(TableMetadata metadata, Version version, SerializationHeader header)
    {
        return new RowIndexEntry.Serializer(version, header);
    }

    static class WriterFactory extends SSTableWriter.Factory
    {
        @Override
        public SSTableWriter open(Descriptor descriptor,
                                  long keyCount,
                                  long repairedAt,
                                  UUID pendingRepair,
                                  TableMetadataRef metadata,
                                  MetadataCollector metadataCollector,
                                  SerializationHeader header,
                                  Collection<SSTableFlushObserver> observers,
                                  LifecycleTransaction txn)
        {
            return new BigTableWriter(descriptor, keyCount, repairedAt, pendingRepair, metadata, metadataCollector, header, observers, txn);
        }
    }

    static class ReaderFactory extends SSTableReader.Factory
    {
        @Override
        public SSTableReader open(Descriptor descriptor, Set<Component> components, TableMetadataRef metadata, Long maxDataAge, StatsMetadata sstableMetadata, SSTableReader.OpenReason openReason, SerializationHeader header)
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
        public static final String current_version = "na";
        public static final String earliest_supported_version = "ma";

        // ma (3.0.0): swap bf hash order
        //             store rows natively
        // mb (3.0.7, 3.7): commit log lower bound included
        // mc (3.0.8, 3.9): commit log intervals included

        // na (4.0.0): uncompressed chunks, pending repair session, checksummed sstable metadata file
        //
        // NOTE: when adding a new version, please add that to LegacySSTableTest, too.

        private final boolean isLatestVersion;
        public final int correspondingMessagingVersion;
        private final boolean hasCommitLogLowerBound;
        private final boolean hasCommitLogIntervals;
        public final boolean hasMaxCompressedLength;
        private final boolean hasPendingRepair;
        private final boolean hasMetadataChecksum;

        BigVersion(String version)
        {
            super(instance, version);

            isLatestVersion = version.compareTo(current_version) == 0;
            correspondingMessagingVersion = MessagingService.VERSION_30;

            hasCommitLogLowerBound = version.compareTo("mb") >= 0;
            hasCommitLogIntervals = version.compareTo("mc") >= 0;
            hasMaxCompressedLength = version.compareTo("na") >= 0;
            hasPendingRepair = version.compareTo("na") >= 0;
            hasMetadataChecksum = version.compareTo("na") >= 0;
        }

        @Override
        public boolean isLatestVersion()
        {
            return isLatestVersion;
        }

        @Override
        public boolean hasCommitLogLowerBound()
        {
            return hasCommitLogLowerBound;
        }

        @Override
        public boolean hasCommitLogIntervals()
        {
            return hasCommitLogIntervals;
        }

        public boolean hasPendingRepair()
        {
            return hasPendingRepair;
        }

        @Override
        public int correspondingMessagingVersion()
        {
            return correspondingMessagingVersion;
        }

        @Override
        public boolean hasMetadataChecksum()
        {
            return hasMetadataChecksum;
        }

        public boolean isCompatible()
        {
            return version.compareTo(earliest_supported_version) >= 0 && version.charAt(0) <= current_version.charAt(0);
        }

        @Override
        public boolean isCompatibleForStreaming()
        {
            return isCompatible() && version.charAt(0) == current_version.charAt(0);
        }

        @Override
        public boolean hasMaxCompressedLength()
        {
            return hasMaxCompressedLength;
        }
    }
}
