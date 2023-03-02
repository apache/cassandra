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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CharMatcher;
import com.google.common.collect.ImmutableList;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.AbstractRowIndexEntry;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.IScrubber;
import org.apache.cassandra.io.sstable.MetricsProviders;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.cassandra.utils.Pair;

/**
 * Provides the accessors to data on disk.
 */
public interface SSTableFormat<R extends SSTableReader, W extends SSTableWriter>
{
    int ordinal();
    String name();

    Version getLatestVersion();
    Version getVersion(String version);

    SSTableWriterFactory<W, ?> getWriterFactory();

    SSTableReaderFactory<R, ?> getReaderFactory();

    /**
     * All the components that the writter can produce when saving an sstable, as well as all the components
     * that the reader can read.
     */
    Set<Component> allComponents();

    Set<Component> streamingComponents();

    Set<Component> primaryComponents();

    /**
     * Returns components required by offline compaction tasks - like splitting sstables.
     */
    Set<Component> batchComponents();

    /**
     * Returns the components which should be selected for upload by the sstable loader.
     */
    Set<Component> uploadComponents();

    /**
     * Returns a set of the components that can be changed after an sstable was written.
     */
    Set<Component> mutableComponents();

    /**
     * Returns a set of components that can be automatically generated when loading sstable and thus are not mandatory.
     */
    Set<Component> generatedOnLoadComponents();

    KeyCacheValueSerializer<R, ?> getKeyCacheValueSerializer();

    /**
     * Returns a new scrubber for an sstable. Note that the transaction must contain only one reader
     * and the reader must match the provided cfs.
     */
    IScrubber getScrubber(ColumnFamilyStore cfs,
                          LifecycleTransaction transaction,
                          OutputHandler outputHandler,
                          IScrubber.Options options);

    R cast(SSTableReader sstr);

    W cast(SSTableWriter sstw);

    MetricsProviders getFormatSpecificMetricsProviders();

    void deleteOrphanedComponents(Descriptor descriptor, Set<Component> components);

    void setup(int id, String name, Map<String, String> options);

    Type getType();

    /**
     * Deletes the existing components of the sstables represented by the provided descriptor.
     * The method is also responsible for cleaning up the in-memory resources occupied by the stuff related to that
     * sstables, such as row key cache entries.
     */
    void delete(Descriptor descriptor);

    class Type
    {
        private final static ImmutableList<Type> types;
        private final static Type[] typesById;

        static
        {
            Pair<List<Type>, Type[]> factories = readFactories(DatabaseDescriptor.getSSTableFormatFactories());
            types = ImmutableList.copyOf(factories.left);
            typesById = factories.right;
        }

        @VisibleForTesting
        public static Pair<List<Type>, Type[]> readFactories(Map<String, Supplier<SSTableFormat<?, ?>>> factories)
        {
            List<Type> typesList = new ArrayList<>(factories.size());
            factories.forEach((key, factory) -> {
                SSTableFormat<?, ?> format = factory.get();
                typesList.add(new Type(format.ordinal(), format.name(), format));
            });
            List<Type> types = ImmutableList.copyOf(typesList);
            int maxId = typesList.stream().mapToInt(t -> t.ordinal).max().getAsInt();
            Type[] typesById = new Type[maxId + 1];
            typesList.forEach(t -> typesById[t.ordinal] = t);
            return Pair.create(types, typesById);
        }

        public final int ordinal;
        public final SSTableFormat<?, ?> info;
        public final String name;

        private static Type currentType;

        public static Type current()
        {
            if (currentType != null)
                return currentType;

            String name = CassandraRelevantProperties.SSTABLE_FORMAT_DEFAULT.getString();
            if (name == null)
                return types.get(0);

            try
            {
                Type type = getByName(name);
                currentType = type;
                return type;
            }
            catch (RuntimeException ex)
            {
                throw new ConfigurationException("SSTable format " + name + " is not registered. Registered formats are: " + types);
            }
        }

        private Type(int ordinal, String name, SSTableFormat<?, ?> info)
        {
            //Since format comes right after generation
            //we disallow formats with numeric names
            assert !CharMatcher.digit().matchesAllOf(name);
            this.ordinal = ordinal;
            this.name = name;
            this.info = info;
        }

        public static Type getByName(String name)
        {
            for (int i = 0; i < types.size(); i++)
                if (types.get(i).name.equals(name))
                    return types.get(i);
            throw new NoSuchElementException(name);
        }

        public static Type getByOrdinal(int ordinal)
        {
            if (ordinal < 0 || ordinal >= typesById.length || typesById[ordinal] == null)
                throw new NoSuchElementException(String.valueOf(ordinal));
            return typesById[ordinal];
        }

        public static Iterable<Type> values()
        {
            return types;
        }
    }

    interface SSTableReaderFactory<R extends SSTableReader, B extends SSTableReader.Builder<R, B>>
    {
        /**
         * A simple builder which creates an instnace of {@link SSTableReader} with the provided parameters.
         * It expects that all the required resources to be opened/loaded externally by the caller.
         * <p>
         * The builder is expected to perform basic validation of the provided parameters.
         */
        SSTableReader.Builder<R, B> builder(Descriptor descriptor);

        /**
         * A builder which opens/loads all the required resources upon execution of
         * {@link SSTableReaderLoadingBuilder#build(SSTable.Owner, boolean, boolean)} and passed them to the created
         * reader instance. If the creation of {@link SSTableReader} fails, no resources should be left opened.
         * <p>
         * The builder is expected to perform basic validation of the provided parameters.
         */
        SSTableReaderLoadingBuilder<R, B> loadingBuilder(Descriptor descriptor, TableMetadataRef tableMetadataRef, Set<Component> components);

        /**
         * Retrieves a key range for the given sstable at the lowest cost - that is, without opening all sstables files
         * if possible.
         */
        Pair<DecoratedKey, DecoratedKey> readKeyRange(Descriptor descriptor, IPartitioner partitioner) throws IOException;

        Class<R> getReaderClass();
    }

    interface SSTableWriterFactory<W extends SSTableWriter, B extends SSTableWriter.Builder<W, B>>
    {
        /**
         * Returns a new builder which can create instance of {@link SSTableWriter} with the provided parameters.
         * Similarly to the loading builder, it should open the required resources when
         * the {@link SSTableWriter.Builder#build(LifecycleNewTracker, SSTable.Owner)} method is called.
         * It should not let the caller passing any closeable resources directly, that is, via setters.
         * If building fails, all the opened resources should be released.
         */
        B builder(Descriptor descriptor);

        /**
         * Tries to estimate the size of all the sstable files from the provided parameters.
         */
        long estimateSize(SSTableWriter.SSTableSizeParameters parameters);
    }

    class Components
    {
        public static class Types
        {
            // the base data for an sstable: the remaining components can be regenerated
            // based on the data component
            public static final Component.Type DATA = Component.Type.createSingleton("DATA", "Data.db", null);
            // file to hold information about uncompressed data length, chunk offsets etc.
            public static final Component.Type COMPRESSION_INFO = Component.Type.createSingleton("COMPRESSION_INFO", "CompressionInfo.db", null);
            // statistical metadata about the content of the sstable
            public static final Component.Type STATS = Component.Type.createSingleton("STATS", "Statistics.db", null);
            // serialized bloom filter for the row keys in the sstable
            public static final Component.Type FILTER = Component.Type.createSingleton("FILTER", "Filter.db", null);
            // holds CRC32 checksum of the data file
            public static final Component.Type DIGEST = Component.Type.createSingleton("DIGEST", "Digest.crc32", null);
            // holds the CRC32 for chunks in an uncompressed file.
            public static final Component.Type CRC = Component.Type.createSingleton("CRC", "CRC.db", null);
            // table of contents, stores the list of all components for the sstable
            public static final Component.Type TOC = Component.Type.createSingleton("TOC", "TOC.txt", null);
            // built-in secondary index (may exist multiple per sstable)
            public static final Component.Type SECONDARY_INDEX = Component.Type.create("SECONDARY_INDEX", "SI_.*.db", null);
            // custom component, used by e.g. custom compaction strategy
            public static final Component.Type CUSTOM = Component.Type.create("CUSTOM", null, null);
        }

        // singleton components for types that don't need ids
        public final static Component DATA = Types.DATA.getSingleton();
        public final static Component COMPRESSION_INFO = Types.COMPRESSION_INFO.getSingleton();
        public final static Component STATS = Types.STATS.getSingleton();
        public final static Component FILTER = Types.FILTER.getSingleton();
        public final static Component DIGEST = Types.DIGEST.getSingleton();
        public final static Component CRC = Types.CRC.getSingleton();
        public final static Component TOC = Types.TOC.getSingleton();
    }

    interface KeyCacheValueSerializer<R extends SSTableReader, T extends AbstractRowIndexEntry>
    {
        void skip(DataInputPlus input) throws IOException;

        T deserialize(R reader, DataInputPlus input) throws IOException;

        void serialize(T entry, DataOutputPlus output) throws IOException;
    }

}
