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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.KeyCacheKey;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.memtable.Flushing;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.GaugeProvider;
import org.apache.cassandra.io.sstable.IScrubber;
import org.apache.cassandra.io.sstable.MetricsProviders;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.filter.BloomFilterMetrics;
import org.apache.cassandra.io.sstable.format.AbstractSSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReaderLoadingBuilder;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.format.SortedTableScrubber;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.indexsummary.IndexSummaryMetrics;
import org.apache.cassandra.io.sstable.keycache.KeyCacheMetrics;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.io.sstable.format.SSTableFormat.Components.DATA;

/**
 * Legacy bigtable format. Components and approximate lifecycle:
 * <br>
 * {@link SSTableFormat.Components}
 * <br>
 * {@link Components#ALL_COMPONENTS}
 *  <ul>
 *     <li>
 *       {@link Components#SUMMARY}: When searching for a PK we go here for a first approximation on where to look in the index file. It is
 *       a small sampling of the Index entries intended for a first fast search in-memory.
 *       <p></p>
 *       {@link org.apache.cassandra.io.sstable.indexsummary.IndexSummary}
 *       <br>
 *       {@link IndexSummaryComponent}
 *       <p></p>
 *     </li>
 *     <li>
 *       {@link Components#PRIMARY_INDEX}: We'll land here in the approximate area where to look for the PK thanks to the Summary. Now we'll search for
 *       the exact PK to get it's exact position in the data file.
 *       <p></p>
 *       {@link BigTableWriter#indexWriter}
 *       <br>
 *       {@link RowIndexEntry}
 *       <br>
 *       {@link org.apache.cassandra.io.sstable.IndexInfo}
 *       <br>
 *       {@link org.apache.cassandra.io.sstable.format.IndexComponent}
 *       <p></p>
 *     </li>
 *     <li>
 *       {@link Components#DATA}: The actual data/partitions file as an array or partitions. Each partition has the form:
 *       <ul>
 *       <li>A partition header</li>
 *       <li>Maybe a static row</li>
 *       <li>Rows or range tombstone</li>
 *       </ul>
 *       I.e. upon flush {@link Flushing.FlushRunnable#writeSortedContents}
 *       <br>
 *       Down to {@link org.apache.cassandra.io.sstable.format.SortedTableWriter#startPartition}
 *       <br>
 *       Down to {@link org.apache.cassandra.io.sstable.format.SortedTablePartitionWriter#start}
 *       <br>
 *       {@link org.apache.cassandra.io.sstable.format.DataComponent}
 *       <p></p>
 *     </li>
 *     <li>
 *       {@link Components#STATS}: Stats on the data such as min timestamps to later vint encode TTL, markForDeleteAt, etc
 *       <p></p>
 *       {@link org.apache.cassandra.db.rows.EncodingStats}
 *       <br>
 *       {@link org.apache.cassandra.io.sstable.format.StatsComponent}
 *       <p></p>
 *     </li>
 *     <li>
 *       {@link Components#COMPRESSION_INFO}: Contains compresion metadata
 *       <p></p>
 *       {@link org.apache.cassandra.io.compress.CompressedSequentialWriter}
 *       <br>
 *       {@link org.apache.cassandra.io.compress.CompressionMetadata}
 *       <br>
 *       {@link org.apache.cassandra.io.sstable.format.CompressionInfoComponent}
 *       <p></p>
 *     </li>
 *     <li>
 *       {@link Components#DIGEST}: The digest supporting the compression
 *       <p></p>
 *       {@link org.apache.cassandra.io.compress.CompressedSequentialWriter}
 *       <br>
 *       {@link org.apache.cassandra.io.util.ChecksumWriter}
 *       <p></p>
 *     </li>
 *     <li>
 *       {@link Components#FILTER}: Bloom filter for data files
 *       <p></p>
 *       {@link org.apache.cassandra.io.sstable.format.FilterComponent}
 *       <br>
 *       {@link org.apache.cassandra.utils.BloomFilterSerializer}
 *       <p></p>
 *     </li>
 *     <li>
 *       {@link Components#CRC}: CRC for the data
 *       <p></p>
 *       {@link org.apache.cassandra.io.util.ChecksummedSequentialWriter}
 *       <br>
 *       {@link org.apache.cassandra.io.util.ChecksumWriter}
 *       <p></p>
 *     </li>
 *     <li>
 *       {@link Components#TOC}: List of all the components for the SSTable
 *       <p></p>
 *       {@link org.apache.cassandra.io.sstable.format.TOCComponent}
 *     </li>
 *   </ul>
 *
 */
public class BigFormat extends AbstractSSTableFormat<BigTableReader, BigTableWriter>
{
    private final static Logger logger = LoggerFactory.getLogger(BigFormat.class);

    public static final String NAME = "big";

    private final Version latestVersion = new BigVersion(this, BigVersion.current_version);
    private final BigTableReaderFactory readerFactory = new BigTableReaderFactory();
    private final BigTableWriterFactory writerFactory = new BigTableWriterFactory();

    public static class Components extends SSTableFormat.Components
    {
        public static class Types extends SSTableFormat.Components.Types
        {
            // index of the row keys with pointers to their positions in the data file
            public static final Component.Type PRIMARY_INDEX = Component.Type.createSingleton("PRIMARY_INDEX", "Index.db", true, BigFormat.class);
            // holds SSTable Index Summary (sampling of Index component)
            public static final Component.Type SUMMARY = Component.Type.createSingleton("SUMMARY", "Summary.db", true, BigFormat.class);
        }

        public final static Component PRIMARY_INDEX = Types.PRIMARY_INDEX.getSingleton();
        public final static Component SUMMARY = Types.SUMMARY.getSingleton();

        private static final Set<Component> BATCH_COMPONENTS = ImmutableSet.of(DATA,
                                                                               PRIMARY_INDEX,
                                                                               COMPRESSION_INFO,
                                                                               FILTER,
                                                                               STATS);

        private static final Set<Component> PRIMARY_COMPONENTS = ImmutableSet.of(DATA,
                                                                                 PRIMARY_INDEX);

        private static final Set<Component> GENERATED_ON_LOAD_COMPONENTS = ImmutableSet.of(FILTER, SUMMARY);

        private static final Set<Component> MUTABLE_COMPONENTS = ImmutableSet.of(STATS,
                                                                                 SUMMARY);

        private static final Set<Component> UPLOAD_COMPONENTS = ImmutableSet.of(DATA,
                                                                                PRIMARY_INDEX,
                                                                                SUMMARY,
                                                                                COMPRESSION_INFO,
                                                                                STATS);
        private static final Set<Component> ALL_COMPONENTS = ImmutableSet.of(DATA,
                                                                             PRIMARY_INDEX,
                                                                             STATS,
                                                                             COMPRESSION_INFO,
                                                                             FILTER,
                                                                             SUMMARY,
                                                                             DIGEST,
                                                                             CRC,
                                                                             TOC);
    }

    public BigFormat(Map<String, String> options)
    {
        super(NAME, options);
    }

    public static boolean is(SSTableFormat<?, ?> format)
    {
        return format.name().equals(NAME);
    }

    public static BigFormat getInstance()
    {
        return (BigFormat) Objects.requireNonNull(DatabaseDescriptor.getSSTableFormats().get(NAME), "Unknown SSTable format: " + NAME);
    }

    public static boolean isSelected()
    {
        return is(DatabaseDescriptor.getSelectedSSTableFormat());
    }

    @Override
    public Version getLatestVersion()
    {
        return latestVersion;
    }

    @Override
    public Version getVersion(String version)
    {
        return new BigVersion(this, version);
    }

    @Override
    public BigTableWriterFactory getWriterFactory()
    {
        return writerFactory;
    }

    @Override
    public BigTableReaderFactory getReaderFactory()
    {
        return readerFactory;
    }

    @Override
    public Set<Component> allComponents()
    {
        return Components.ALL_COMPONENTS;
    }

    @Override
    public Set<Component> primaryComponents()
    {
        return Components.PRIMARY_COMPONENTS;
    }

    @Override
    public Set<Component> batchComponents()
    {
        return Components.BATCH_COMPONENTS;
    }

    @Override
    public Set<Component> uploadComponents()
    {
        return Components.UPLOAD_COMPONENTS;
    }

    @Override
    public Set<Component> mutableComponents()
    {
        return Components.MUTABLE_COMPONENTS;
    }

    @Override
    public Set<Component> generatedOnLoadComponents()
    {
        return Components.GENERATED_ON_LOAD_COMPONENTS;
    }

    @Override
    public SSTableFormat.KeyCacheValueSerializer<BigTableReader, RowIndexEntry> getKeyCacheValueSerializer()
    {
        return KeyCacheValueSerializer.instance;
    }

    @Override
    public IScrubber getScrubber(ColumnFamilyStore cfs, LifecycleTransaction transaction, OutputHandler outputHandler, IScrubber.Options options)
    {
        Preconditions.checkArgument(cfs.metadata().equals(transaction.onlyOne().metadata()), "SSTable metadata does not match current definition");
        return new BigTableScrubber(cfs, transaction, outputHandler, options);
    }

    @Override
    public MetricsProviders getFormatSpecificMetricsProviders()
    {
        return BigTableSpecificMetricsProviders.instance;
    }

    @Override
    public void deleteOrphanedComponents(Descriptor descriptor, Set<Component> components)
    {
        SortedTableScrubber.deleteOrphanedComponents(descriptor, components);
    }

    private void delete(Descriptor desc, List<Component> components)
    {
        logger.info("Deleting sstable: {}", desc);

        if (components.remove(DATA))
            components.add(0, DATA); // DATA component should be first
        if (components.remove(Components.SUMMARY))
            components.add(Components.SUMMARY); // SUMMARY component should be last (IDK why)

        for (Component component : components)
        {
            logger.trace("Deleting component {} of {}", component, desc);
            desc.fileFor(component).deleteIfExists();
        }
    }

    @Override
    public void delete(Descriptor desc)
    {
        try
        {
            // remove key cache entries for the sstable being deleted
            Iterator<KeyCacheKey> it = CacheService.instance.keyCache.keyIterator();
            while (it.hasNext())
            {
                KeyCacheKey key = it.next();
                if (key.desc.equals(desc))
                    it.remove();
            }

            delete(desc, Lists.newArrayList(Sets.intersection(allComponents(), desc.discoverComponents())));
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
        }
    }

    static class KeyCacheValueSerializer implements SSTableFormat.KeyCacheValueSerializer<BigTableReader, RowIndexEntry>
    {
        private final static KeyCacheValueSerializer instance = new KeyCacheValueSerializer();

        @Override
        public void skip(DataInputPlus input) throws IOException
        {
            RowIndexEntry.Serializer.skipForCache(input, getInstance().latestVersion);
        }

        @Override
        public RowIndexEntry deserialize(BigTableReader reader, DataInputPlus input) throws IOException
        {
            return reader.deserializeKeyCacheValue(input);
        }

        @Override
        public void serialize(RowIndexEntry entry, DataOutputPlus output) throws IOException
        {
            entry.serializeForCache(output);
        }
    }

    static class BigTableReaderFactory implements SSTableReaderFactory<BigTableReader, BigTableReader.Builder>
    {
        @Override
        public BigTableReader.Builder builder(Descriptor descriptor)
        {
            return new BigTableReader.Builder(descriptor);
        }

        @Override
        public SSTableReaderLoadingBuilder<BigTableReader, BigTableReader.Builder> loadingBuilder(Descriptor descriptor,
                                                                                                  TableMetadataRef tableMetadataRef,
                                                                                                  Set<Component> components)
        {
            return new BigSSTableReaderLoadingBuilder(new SSTable.Builder<>(descriptor).setTableMetadataRef(tableMetadataRef)
                                                                                       .setComponents(components));
        }

        @Override
        public Pair<DecoratedKey, DecoratedKey> readKeyRange(Descriptor descriptor, IPartitioner partitioner) throws IOException
        {
            return IndexSummaryComponent.loadFirstAndLastKey(descriptor.fileFor(Components.SUMMARY), partitioner);
        }

        @Override
        public Class<BigTableReader> getReaderClass()
        {
            return BigTableReader.class;
        }
    }

    static class BigTableWriterFactory implements SSTableWriterFactory<BigTableWriter, BigTableWriter.Builder>
    {
        @Override
        public long estimateSize(SSTableWriter.SSTableSizeParameters parameters)
        {
            return (long) ((parameters.partitionKeysSize() // index entries
                            + parameters.partitionKeysSize() // keys in data file
                            + parameters.dataSize()) // data
                           * 1.2); // bloom filter and row index overhead
        }

        @Override
        public BigTableWriter.Builder builder(Descriptor descriptor)
        {
            return new BigTableWriter.Builder(descriptor);
        }
    }

    static class BigVersion extends Version
    {
        public static final String current_version = DatabaseDescriptor.getStorageCompatibilityMode().isBefore(5) ? "nc" : "oa";
        public static final String earliest_supported_version = "ma";

        // ma (3.0.0): swap bf hash order
        //             store rows natively
        // mb (3.0.7, 3.7): commit log lower bound included
        // mc (3.0.8, 3.9): commit log intervals included
        // md (3.0.18, 3.11.4): corrected sstable min/max clustering
        // me (3.0.25, 3.11.11): added hostId of the node from which the sstable originated

        // na (4.0-rc1): uncompressed chunks, pending repair session, isTransient, checksummed sstable metadata file, new Bloomfilter format
        // nb (4.0.0): originating host id
        // nc (5.0): improved min/max, partition level deletion presence marker, key range (CASSANDRA-18134)
        // oa (5.0): Long deletionTime to prevent TTL overflow
        //           token space coverage
        //
        // NOTE: When adding a new version:
        //  - Please add it to LegacySSTableTest
        //  - Please maybe add it to hasOriginatingHostId's regexp

        private final boolean isLatestVersion;
        private final int correspondingMessagingVersion;
        private final boolean hasCommitLogLowerBound;
        private final boolean hasCommitLogIntervals;
        private final boolean hasAccurateMinMax;
        private final boolean hasLegacyMinMax;
        private final boolean hasOriginatingHostId;
        private final boolean hasMaxCompressedLength;
        private final boolean hasPendingRepair;
        private final boolean hasMetadataChecksum;
        private final boolean hasIsTransient;
        private final boolean hasImprovedMinMax;
        private final boolean hasPartitionLevelDeletionPresenceMarker;
        private final boolean hasKeyRange;
        private final boolean hasUintDeletionTime;
        private final boolean hasTokenSpaceCoverage;

        /**
         * CASSANDRA-9067: 4.0 bloom filter representation changed (two longs just swapped)
         * have no 'static' bits caused by using the same upper bits for both bloom filter and token distribution.
         */
        private final boolean hasOldBfFormat;

        BigVersion(BigFormat format, String version)
        {
            super(format, version);

            isLatestVersion = version.compareTo(current_version) == 0;
            correspondingMessagingVersion = MessagingService.VERSION_30;

            hasCommitLogLowerBound = version.compareTo("mb") >= 0;
            hasCommitLogIntervals = version.compareTo("mc") >= 0;
            hasAccurateMinMax = version.matches("(m[d-z])|(n[a-z])"); // deprecated in 'nc' and to be removed in 'oa'
            hasLegacyMinMax = version.matches("(m[a-z])|(n[a-z])"); // deprecated in 'nc' and to be removed in 'oa'
            // When adding a new version you might need to add it here
            hasOriginatingHostId = version.compareTo("nb") >= 0 || version.matches("(m[e-z])");
            hasMaxCompressedLength = version.compareTo("na") >= 0;
            hasPendingRepair = version.compareTo("na") >= 0;
            hasIsTransient = version.compareTo("na") >= 0;
            hasMetadataChecksum = version.compareTo("na") >= 0;
            hasOldBfFormat = version.compareTo("na") < 0;
            hasImprovedMinMax = version.compareTo("nc") >= 0;
            hasPartitionLevelDeletionPresenceMarker = version.compareTo("nc") >= 0;
            hasKeyRange = version.compareTo("nc") >= 0;
            hasUintDeletionTime = version.compareTo("oa") >= 0;
            hasTokenSpaceCoverage = version.compareTo("oa") >= 0;
        }

        @Override
        public boolean isLatestVersion()
        {
            return isLatestVersion;
        }

        @Override
        public int correspondingMessagingVersion()
        {
            return correspondingMessagingVersion;
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
        public boolean hasMaxCompressedLength()
        {
            return hasMaxCompressedLength;
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
        public boolean hasMetadataChecksum()
        {
            return hasMetadataChecksum;
        }

        @Override
        public boolean hasOldBfFormat()
        {
            return hasOldBfFormat;
        }

        @Override
        public boolean hasAccurateMinMax()
        {
            return hasAccurateMinMax;
        }

        @Override
        public boolean hasLegacyMinMax()
        {
            return hasLegacyMinMax;
        }

        @Override
        public boolean hasOriginatingHostId()
        {
            return hasOriginatingHostId;
        }

        @Override
        public boolean hasImprovedMinMax()
        {
            return hasImprovedMinMax;
        }

        @Override
        public boolean hasTokenSpaceCoverage()
        {
            return hasTokenSpaceCoverage;
        }

        @Override
        public boolean hasPartitionLevelDeletionsPresenceMarker()
        {
            return hasPartitionLevelDeletionPresenceMarker;
        }

        @Override
        public boolean hasUIntDeletionTime()
        {
            return hasUintDeletionTime;
        }

        @Override
        public boolean hasKeyRange()
        {
            return hasKeyRange;
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

    private static class BigTableSpecificMetricsProviders implements MetricsProviders
    {
        private final static BigTableSpecificMetricsProviders instance = new BigTableSpecificMetricsProviders();

        private final Iterable<GaugeProvider<?>> gaugeProviders = Iterables.concat(BloomFilterMetrics.instance.getGaugeProviders(),
                                                                                   IndexSummaryMetrics.instance.getGaugeProviders(),
                                                                                   KeyCacheMetrics.instance.getGaugeProviders());

        @Override
        public Iterable<GaugeProvider<?>> getGaugeProviders()
        {
            return gaugeProviders;
        }
    }

    @SuppressWarnings("unused")
    public static class BigFormatFactory implements Factory
    {
        @Override
        public String name()
        {
            return NAME;
        }

        @Override
        public SSTableFormat<?, ?> getInstance(Map<String, String> options)
        {
            return new BigFormat(options);
        }
    }
}
