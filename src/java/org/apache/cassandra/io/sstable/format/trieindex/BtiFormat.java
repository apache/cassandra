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
package org.apache.cassandra.io.sstable.format.trieindex;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.GaugeProvider;
import org.apache.cassandra.io.sstable.SSTableBuilder;
import org.apache.cassandra.io.sstable.format.AbstractRowIndexEntry;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReaderBuilder;
import org.apache.cassandra.io.sstable.format.SSTableReaderLoadingBuilder;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.TableMetadataRef;

import static org.apache.cassandra.io.sstable.Component.COMPRESSION_INFO;
import static org.apache.cassandra.io.sstable.Component.CRC;
import static org.apache.cassandra.io.sstable.Component.DATA;
import static org.apache.cassandra.io.sstable.Component.DIGEST;
import static org.apache.cassandra.io.sstable.Component.FILTER;
import static org.apache.cassandra.io.sstable.Component.PARTITION_INDEX;
import static org.apache.cassandra.io.sstable.Component.ROW_INDEX;
import static org.apache.cassandra.io.sstable.Component.STATS;
import static org.apache.cassandra.io.sstable.Component.TOC;

/**
 * Bigtable format with trie indices
 */
public class BtiFormat implements SSTableFormat<BtiTableReader, BtiTableWriter>
{
    private final static Set<Component> STREAMING_COMPONENTS = ImmutableSet.of(DATA,
                                                                               PARTITION_INDEX,
                                                                               ROW_INDEX,
                                                                               STATS,
                                                                               COMPRESSION_INFO,
                                                                               FILTER,
                                                                               DIGEST,
                                                                               CRC);

    private final static Set<Component> PRIMARY_COMPONENTS = ImmutableSet.of(DATA,
                                                                             PARTITION_INDEX);

    private final static Set<Component> MUTABLE_COMPONENTS = ImmutableSet.of(STATS);

    private final static Set<Component> WRITE_COMPONENTS = ImmutableSet.of(DATA,
                                                                           PARTITION_INDEX,
                                                                           ROW_INDEX,
                                                                           STATS,
                                                                           TOC,
                                                                           DIGEST);

    private static final Set<Component> UPLOAD_COMPONENTS = ImmutableSet.of(DATA,
                                                                            PARTITION_INDEX,
                                                                            ROW_INDEX,
                                                                            COMPRESSION_INFO,
                                                                            STATS);

    private static final Set<Component> BATCH_COMPONENTS = ImmutableSet.of(DATA,
                                                                           PARTITION_INDEX,
                                                                           ROW_INDEX,
                                                                           COMPRESSION_INFO,
                                                                           FILTER,
                                                                           STATS);

    private final static Set<Component> SUPPORTED_COMPONENTS = ImmutableSet.of(DATA,
                                                                               PARTITION_INDEX,
                                                                               ROW_INDEX,
                                                                               STATS,
                                                                               COMPRESSION_INFO,
                                                                               FILTER,
                                                                               DIGEST,
                                                                               CRC,
                                                                               TOC);

    private final static Set<Component> GENERATED_ON_LOAD_COMPONENTS = ImmutableSet.of(FILTER);

    public static final BtiFormat instance = new BtiFormat();
    public static final Version latestVersion = new TrieIndexVersion(TrieIndexVersion.current_version);
    static final BtiTableReaderFactory readerFactory = new BtiTableReaderFactory();
    static final BtiTableWriterFactory writerFactory = new BtiTableWriterFactory();


    private BtiFormat()
    {

    }

    @Override
    public Type getType()
    {
        return Type.BTI;
    }

    @Override
    public Version getLatestVersion()
    {
        return latestVersion;
    }

    @Override
    public Version getVersion(String version)
    {
        return new TrieIndexVersion(version);
    }

    @Override
    public BtiTableWriterFactory getWriterFactory()
    {
        return writerFactory;
    }

    @Override
    public BtiTableReaderFactory getReaderFactory()
    {
        return readerFactory;
    }

    @Override
    public Set<Component> streamingComponents()
    {
        return STREAMING_COMPONENTS;
    }

    @Override
    public Set<Component> primaryComponents()
    {
        return PRIMARY_COMPONENTS;
    }

    @Override
    public Set<Component> batchComponents()
    {
        return BATCH_COMPONENTS;
    }

    @Override
    public Set<Component> uploadComponents()
    {
        return UPLOAD_COMPONENTS;
    }

    @Override
    public Set<Component> mutableComponents()
    {
        return MUTABLE_COMPONENTS;
    }

    @Override
    public Set<Component> writeComponents()
    {
        return WRITE_COMPONENTS;
    }

    @Override
    public Set<Component> supportedComponents()
    {
        return SUPPORTED_COMPONENTS;
    }

    @Override
    public Set<Component> generatedOnLoadComponents()
    {
        return GENERATED_ON_LOAD_COMPONENTS;
    }

    @Override
    public boolean isKeyCacheSupported()
    {
        return false;
    }

    @Override
    public AbstractRowIndexEntry.KeyCacheValueSerializer<?, ?> getKeyCacheValueSerializer()
    {
        return TrieIndexEntry.KeyCacheValueSerializer.instance;
    }

    @Override
    public BtiTableReader cast(SSTableReader sstr)
    {
        return (BtiTableReader) sstr;
    }

    @Override
    public BtiTableWriter cast(SSTableWriter sstw)
    {
        return (BtiTableWriter) sstw;
    }

    @Override
    public FormatSpecificMetricsProviders getFormatSpecificMetricsProviders()
    {
        return BtiTableSpecificMetricsProviders.instance;
    }

    static class BtiTableReaderFactory implements SSTableReaderFactory<BtiTableReader, BtiTableReaderBuilder>
    {
        @Override
        public SSTableReaderBuilder<BtiTableReader, BtiTableReaderBuilder> builder(Descriptor descriptor)
        {
            return new BtiTableReaderBuilder(descriptor);
        }

        @Override
        public SSTableReaderLoadingBuilder<BtiTableReader, BtiTableReaderBuilder> builder(Descriptor descriptor, TableMetadataRef tableMetadataRef, Set<Component> components)
        {
            return new BtiTableReaderLoadingBuilder(new SSTableBuilder<>(descriptor).setTableMetadataRef(tableMetadataRef)
                                                                                    .setComponents(components));
        }
    }

    static class BtiTableWriterFactory implements SSTableWriter.Factory<BtiTableWriter, BtiTableWriterBuilder>
    {
        @Override
        public BtiTableWriterBuilder builder(Descriptor descriptor)
        {
            return new BtiTableWriterBuilder(descriptor);
        }

        @Override
        public long estimateSize(SSTableWriter.SSTableSizeParameters parameters)
        {
            return (long) ((parameters.partitionCount() // index entries
                            + parameters.partitionCount() // keys in data file
                            + parameters.dataSize()) // data
                           * 1.2); // bloom filter and row index overhead
        }
    }

    // versions are denoted as [major][minor].  Minor versions must be forward-compatible:
    // new fields are allowed in e.g. the metadata component, but fields can't be removed
    // or have their size changed.
    //
    static class TrieIndexVersion extends Version
    {
        public static final String current_version = "ca";
        public static final String earliest_supported_version = "aa";

        // aa (DSE 6.0): trie index format
        // ab (DSE pre-6.8): ILLEGAL - handled as 'b' (predates 'ba'). Pre-GA "LABS" releases of DSE 6.8 used this
        //                   sstable version.
        // ac (DSE 6.0.11, 6.7.6): corrected sstable min/max clustering (DB-3691/CASSANDRA-14861)
        // ad (DSE 6.0.14, 6.7.11): added hostId of the node from which the sstable originated (DB-4629)
        // b  (DSE early 6.8 "LABS") has some of 6.8 features but not all
        // ba (DSE 6.8): encrypted indices and metadata
        //               new BloomFilter serialization format
        //               add incremental NodeSync information to metadata
        //               improved min/max clustering representation
        //               presence marker for partition level deletions
        // bb (DSE 6.8.5): added hostId of the node from which the sstable originated (DB-4629)
        // ca (DSE-DB aka Stargazer based on OSS 4.0): bb fields without maxColumnValueLengths + all OSS fields
        // NOTE: when adding a new version, please add that to LegacySSTableTest, too.

        private final boolean isLatestVersion;

        /**
         * DB-2648/CASSANDRA-9067: DSE 6.8/OSS 4.0 bloom filter representation changed (bitset data is no longer stored
         * as BIG_ENDIAN longs, which avoids some redundant bit twiddling).
         */
        private final boolean hasOldBfFormat;
        private final boolean hasAccurateLegacyMinMax;
        private final boolean hasOriginatingHostId;
        private final boolean hasMaxColumnValueLengths;

        private final int correspondingMessagingVersion;

        TrieIndexVersion(String version)
        {
            super(instance, version = mapAb(version));

            isLatestVersion = version.compareTo(current_version) == 0;
            hasOldBfFormat = version.compareTo("b") < 0;
            hasAccurateLegacyMinMax = version.compareTo("ac") >= 0;
            hasOriginatingHostId = version.matches("(a[d-z])|(b[b-z])") || version.compareTo("ca") >= 0;
            hasMaxColumnValueLengths = version.matches("b[a-z]"); // DSE only field
            correspondingMessagingVersion = version.compareTo("ca") >= 0 ? MessagingService.VERSION_40 : MessagingService.VERSION_3014;
        }

        // this is for the ab version which was used in the LABS, and then has been renamed to ba
        private static String mapAb(String version)
        {
            return "ab".equals(version) ? "ba" : version;
        }

        @Override
        public boolean isLatestVersion()
        {
            return isLatestVersion;
        }

        @Override
        public boolean hasCommitLogLowerBound()
        {
            return true;
        }

        @Override
        public boolean hasCommitLogIntervals()
        {
            return true;
        }

        @Override
        public boolean hasMaxCompressedLength()
        {
            return true;
        }

        @Override
        public boolean hasPendingRepair()
        {
            return true;
        }

        @Override
        public boolean hasMetadataChecksum()
        {
            return true;
        }

        @Override
        public boolean hasZeroCopyMetadata()
        {
            return version.compareTo("b") >= 0 && version.compareTo("c") < 0;
        }

        @Override
        public boolean hasIncrementalNodeSyncMetadata()
        {
            return version.compareTo("b") >= 0 && version.compareTo("c") < 0;
        }

        @Override
        public boolean hasAccurateMinMax()
        {
            return hasAccurateLegacyMinMax;
        }

        @Override
        public boolean hasPartitionLevelDeletionsPresenceMarker()
        {
            return version.compareTo("ba") >= 0;
        }

        @Override
        public boolean hasImprovedMinMax()
        {
            return version.compareTo("ba") >= 0;
        }

        @Override
        public boolean hasMaxColumnValueLengths()
        {
            return hasMaxColumnValueLengths;
        }

        @Override
        public boolean hasOriginatingHostId()
        {
            return hasOriginatingHostId;
        }

        @Override
        public boolean isCompatible()
        {
            return version.compareTo(earliest_supported_version) >= 0 && version.charAt(0) <= current_version.charAt(0);
        }

        @Override
        public boolean hasOldBfFormat()
        {
            return hasOldBfFormat;
        }

        // this field is not present in DSE
        @Override
        public int correspondingMessagingVersion()
        {
            return correspondingMessagingVersion;
        }

        // this field is not present in DSE
        @Override
        public boolean isCompatibleForStreaming()
        {
            return isCompatible() && version.charAt(0) == current_version.charAt(0);
        }

        // this field is not present in DSE
        @Override
        public boolean hasIsTransient()
        {
            return version.compareTo("ca") >= 0;
        }
    }

    private static class BtiTableSpecificMetricsProviders implements FormatSpecificMetricsProviders
    {
        private final static BtiTableSpecificMetricsProviders instance = new BtiTableSpecificMetricsProviders();

        private final List<GaugeProvider<?, ?>> gaugeProviders = Arrays.asList();

        @Override
        public List<GaugeProvider<?, ?>> getGaugeProviders()
        {
            return gaugeProviders;
        }
    }
}