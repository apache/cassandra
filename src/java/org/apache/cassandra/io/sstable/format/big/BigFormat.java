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

import java.io.IOException;
import java.util.Collection;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.*;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.net.MessagingService;

import static org.apache.cassandra.io.sstable.format.SSTableReaderBuilder.defaultIndexHandleBuilder;

/**
 * Legacy bigtable format
 */
public class BigFormat implements SSTableFormat
{
    public static final BigFormat instance = new BigFormat();
    public static final Version latestVersion = new BigVersion(BigVersion.current_version);
    private static final SSTableReader.Factory readerFactory = new ReaderFactory();
    private static final SSTableWriter.Factory writerFactory = new WriterFactory();

    private final static Set<Component> REQUIRED_COMPONENTS = ImmutableSet.of(Component.DATA,
                                                                              Component.PRIMARY_INDEX,
                                                                              Component.STATS);

    private final static Set<Component> SUPPORTED_COMPONENTS = ImmutableSet.of(Component.DATA,
                                                                               Component.PRIMARY_INDEX,
                                                                               Component.FILTER,
                                                                               Component.COMPRESSION_INFO,
                                                                               Component.STATS,
                                                                               Component.DIGEST,
                                                                               Component.CRC,
                                                                               Component.SUMMARY,
                                                                               Component.TOC);

    private final static Set<Component> STREAMING_COMPONENTS = ImmutableSet.of(Component.DATA,
                                                                               Component.PRIMARY_INDEX,
                                                                               Component.SUMMARY,
                                                                               Component.STATS,
                                                                               Component.COMPRESSION_INFO,
                                                                               Component.FILTER,
                                                                               Component.DIGEST,
                                                                               Component.CRC);

    private BigFormat()
    {

    }

    @Override
    public Type getType()
    {
        return Type.BIG;
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
    public Set<Component> requiredComponents()
    {
        return REQUIRED_COMPONENTS;
    }

    @Override
    public Set<Component> supportedComponents()
    {
        return SUPPORTED_COMPONENTS;
    }

    @Override
    public Set<Component> streamingComponents()
    {
        return STREAMING_COMPONENTS;
    }

    static class WriterFactory extends SSTableWriter.Factory
    {
        @Override
        public long estimateSize(SSTableWriter.SSTableSizeParameters parameters)
        {
            return (long) ((parameters.partitionCount() // index entries
                            + parameters.partitionCount() // keys in data file
                            + parameters.dataSize()) // data
                           * 1.2); // bloom filter and row index overhead
        }

        @Override
        public SSTableWriter open(Descriptor descriptor,
                                  long keyCount,
                                  long repairedAt,
                                  UUID pendingRepair,
                                  boolean isTransient,
                                  TableMetadataRef metadata,
                                  MetadataCollector metadataCollector,
                                  SerializationHeader header,
                                  Collection<SSTableFlushObserver> observers,
                                  LifecycleNewTracker lifecycleNewTracker,
                                  Set<Component> indexComponents)
        {
            SSTable.validateRepairedMetadata(repairedAt, pendingRepair, isTransient);
            return new BigTableWriter(descriptor, keyCount, repairedAt, pendingRepair, isTransient, metadata, metadataCollector, header, observers, lifecycleNewTracker, indexComponents);
        }
    }

    static class ReaderFactory extends SSTableReader.AbstractBigTableReaderFactory
    {
        @Override
        public PartitionIndexIterator indexIterator(Descriptor descriptor, TableMetadata metadata)
        {
            try (FileHandle iFile = defaultIndexHandleBuilder(descriptor, Component.PRIMARY_INDEX).complete()) {
                SerializationHeader.Component headerComponent = (SerializationHeader.Component)
                                                                descriptor.getMetadataSerializer()
                                                                          .deserialize(descriptor, MetadataType.HEADER);
                SerializationHeader header = headerComponent.toHeader(metadata);
                BigTableRowIndexEntry.Serializer serializer = new BigTableRowIndexEntry.Serializer(descriptor.version, header);
                return BigTablePartitionIndexIterator.create(iFile, serializer);
            }
            catch (IOException ex)
            {
                throw new RuntimeException(ex);
            }
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
        public static final String current_version = "nb";
        public static final String earliest_supported_version = "ma";

        // ma (3.0.0): swap bf hash order
        //             store rows natively
        // mb (3.0.7, 3.7): commit log lower bound included
        // mc (3.0.8, 3.9): commit log intervals included
        // md (3.0.18, 3.11.4): corrected sstable min/max clustering
        // me (3.0.25, 3.11.11): added hostId of the node from which the sstable originated

        // na (4.0-rc1): uncompressed chunks, pending repair session, isTransient, checksummed sstable metadata file, new Bloomfilter format
        // nb (4.0.0): originating host id
        //
        // NOTE: when adding a new version, please add that to LegacySSTableTest, too.

        private final boolean isLatestVersion;
        public final int correspondingMessagingVersion;
        private final boolean hasCommitLogLowerBound;
        private final boolean hasCommitLogIntervals;
        private final boolean hasAccurateMinMax;
        private final boolean hasOriginatingHostId;
        private final boolean hasImprovedMinMax;
        private final boolean hasPartitionLevelDeletionPresenceMarker;
        public final boolean hasMaxCompressedLength;
        private final boolean hasPendingRepair;
        private final boolean hasMetadataChecksum;
        private final boolean hasIsTransient;

        /**
         * CASSANDRA-9067: 4.0 bloom filter representation changed (two longs just swapped)
         * have no 'static' bits caused by using the same upper bits for both bloom filter and token distribution.
         */
        private final boolean hasOldBfFormat;

        BigVersion(String version)
        {
            super(instance, version);

            isLatestVersion = version.compareTo(current_version) == 0;
            correspondingMessagingVersion = MessagingService.VERSION_30;

            hasCommitLogLowerBound = version.compareTo("mb") >= 0;
            hasCommitLogIntervals = version.compareTo("mc") >= 0;
            hasAccurateMinMax = version.compareTo("md") >= 0;
            hasOriginatingHostId = version.matches("(m[e-z])|(n[b-z])");
            hasImprovedMinMax = false;
            hasPartitionLevelDeletionPresenceMarker = false;
            hasMaxCompressedLength = version.compareTo("na") >= 0;
            hasPendingRepair = version.compareTo("na") >= 0;
            hasIsTransient = version.compareTo("na") >= 0;
            hasMetadataChecksum = version.compareTo("na") >= 0;
            hasOldBfFormat = version.compareTo("na") < 0;
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

        @Override
        public boolean hasPendingRepair()
        {
            return hasPendingRepair;
        }

        @Override
        public boolean hasIsTransient()
        {
            return hasIsTransient;
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

        @Override
        public boolean hasAccurateMinMax()
        {
            return hasAccurateMinMax;
        }

        @Override
        public boolean hasImprovedMinMax()
        {
            return hasImprovedMinMax;
        }

        @Override
        public boolean hasPartitionLevelDeletionsPresenceMarker()
        {
            return hasPartitionLevelDeletionPresenceMarker;
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

        public boolean hasOriginatingHostId()
        {
            return hasOriginatingHostId;
        }

        @Override
        public boolean hasMaxCompressedLength()
        {
            return hasMaxCompressedLength;
        }

        @Override
        public boolean hasOldBfFormat()
        {
            return hasOldBfFormat;
        }

        @Override
        public boolean hasZeroCopyMetadata()
        {
            return false;
        }

        @Override
        public boolean hasIncrementalNodeSyncMetadata()
        {
            return false;
        }

        @Override
        public boolean hasMaxColumnValueLengths()
        {
            return false;
        }
    }
}
