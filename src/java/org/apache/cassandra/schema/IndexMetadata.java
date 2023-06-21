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

package org.apache.cassandra.schema;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.CqlBuilder;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.UnknownIndexException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.internal.CassandraIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sasi.SASIIndex;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDSerializer;

/**
 * An immutable representation of secondary index metadata.
 */
public final class IndexMetadata
{
    private static final Logger logger = LoggerFactory.getLogger(IndexMetadata.class);

    private static final Pattern PATTERN_NON_WORD_CHAR = Pattern.compile("\\W");
    private static final Pattern PATTERN_WORD_CHARS = Pattern.compile("\\w+");


    public static final Serializer serializer = new Serializer();

    /**
     * A mapping of user-friendly index names to their fully qualified index class names.
     */
    private static final Map<String, String> indexNameAliases = new ConcurrentHashMap<>();

    static
    {
        indexNameAliases.put(StorageAttachedIndex.NAME, StorageAttachedIndex.class.getCanonicalName());
        indexNameAliases.put(StorageAttachedIndex.class.getSimpleName().toLowerCase(), StorageAttachedIndex.class.getCanonicalName());
        indexNameAliases.put(SASIIndex.class.getSimpleName(), SASIIndex.class.getCanonicalName());
    }

    public enum Kind
    {
        KEYS, CUSTOM, COMPOSITES
    }

    // UUID for serialization. This is a deterministic UUID generated from the index name
    // Both the id and name are guaranteed unique per keyspace.
    public final UUID id;
    public final String name;
    public final Kind kind;
    public final Map<String, String> options;

    private IndexMetadata(String name,
                          Map<String, String> options,
                          Kind kind)
    {
        this.id = UUID.nameUUIDFromBytes(name.getBytes());
        this.name = name;
        this.options = options == null ? ImmutableMap.of() : ImmutableMap.copyOf(options);
        this.kind = kind;
    }

    public static IndexMetadata fromSchemaMetadata(String name, Kind kind, Map<String, String> options)
    {
        return new IndexMetadata(name, options, kind);
    }

    public static IndexMetadata fromIndexTargets(List<IndexTarget> targets,
                                                 String name,
                                                 Kind kind,
                                                 Map<String, String> options)
    {
        Map<String, String> newOptions = new HashMap<>(options);
        newOptions.put(IndexTarget.TARGET_OPTION_NAME, targets.stream()
                                                              .map(target -> target.asCqlString())
                                                              .collect(Collectors.joining(", ")));
        return new IndexMetadata(name, newOptions, kind);
    }

    public static boolean isNameValid(String name)
    {
        return name != null && !name.isEmpty() && PATTERN_WORD_CHARS.matcher(name).matches();
    }

    public static String generateDefaultIndexName(String table, ColumnIdentifier column)
    {
        return PATTERN_NON_WORD_CHAR.matcher(table + "_" + column.toString() + "_idx").replaceAll("");
    }

    public static String generateDefaultIndexName(String table)
    {
        return PATTERN_NON_WORD_CHAR.matcher(table + "_" + "idx").replaceAll("");
    }

    public void validate(TableMetadata table)
    {
        if (!isNameValid(name))
            throw new ConfigurationException("Illegal index name " + name);

        if (kind == null)
            throw new ConfigurationException("Index kind is null for index " + name);

        if (kind == Kind.CUSTOM)
        {
            if (options == null || !options.containsKey(IndexTarget.CUSTOM_INDEX_OPTION_NAME))
                throw new ConfigurationException(String.format("Required option missing for index %s : %s",
                                                               name, IndexTarget.CUSTOM_INDEX_OPTION_NAME));

            // Get the fully qualified class name:
            String className = getIndexClassName();

            Class<Index> indexerClass = FBUtilities.classForName(className, "custom indexer");
            if (!Index.class.isAssignableFrom(indexerClass))
                throw new ConfigurationException(String.format("Specified Indexer class (%s) does not implement the Indexer interface", className));
            validateCustomIndexOptions(table, indexerClass, options);
        }
    }

    public String getIndexClassName()
    {
        if (isCustom())
        {
            String className = options.get(IndexTarget.CUSTOM_INDEX_OPTION_NAME);
            return indexNameAliases.getOrDefault(className.toLowerCase(), className);
        }
        return CassandraIndex.class.getName();
    }

    private void validateCustomIndexOptions(TableMetadata table, Class<? extends Index> indexerClass, Map<String, String> options)
    {
        try
        {
            Map<String, String> filteredOptions = Maps.filterKeys(options, key -> !key.equals(IndexTarget.CUSTOM_INDEX_OPTION_NAME));

            if (filteredOptions.isEmpty())
                return;

            Map<?, ?> unknownOptions;
            try
            {
                unknownOptions = (Map) indexerClass.getMethod("validateOptions", Map.class, TableMetadata.class).invoke(null, filteredOptions, table);
            }
            catch (NoSuchMethodException e)
            {
                unknownOptions = (Map) indexerClass.getMethod("validateOptions", Map.class).invoke(null, filteredOptions);
            }

            if (!unknownOptions.isEmpty())
                throw new ConfigurationException(String.format("Properties specified %s are not understood by %s", unknownOptions.keySet(), indexerClass.getSimpleName()));
        }
        catch (NoSuchMethodException e)
        {
            logger.info("Indexer {} does not have a static validateOptions method. Validation ignored",
                        indexerClass.getName());
        }
        catch (InvocationTargetException e)
        {
            if (e.getTargetException() instanceof InvalidRequestException)
                throw (InvalidRequestException) e.getTargetException();
            if (e.getTargetException() instanceof ConfigurationException)
                throw (ConfigurationException) e.getTargetException();
            throw new ConfigurationException("Failed to validate custom indexer options: " + options);
        }
        catch (ConfigurationException e)
        {
            throw e;
        }
        catch (Exception e)
        {
            throw new ConfigurationException("Failed to validate custom indexer options: " + options);
        }
    }

    public boolean isCustom()
    {
        return kind == Kind.CUSTOM;
    }

    public boolean isKeys()
    {
        return kind == Kind.KEYS;
    }

    public boolean isComposites()
    {
        return kind == Kind.COMPOSITES;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(id, name, kind, options);
    }

    public boolean equalsWithoutName(IndexMetadata other)
    {
        return Objects.equal(kind, other.kind)
               && Objects.equal(options, other.options);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this)
            return true;

        if (!(obj instanceof IndexMetadata))
            return false;

        IndexMetadata other = (IndexMetadata) obj;

        return Objects.equal(id, other.id) && Objects.equal(name, other.name) && equalsWithoutName(other);
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder(this)
               .append("id", id.toString())
               .append("name", name)
               .append("kind", kind)
               .append("options", options)
               .build();
    }

    public String toCqlString(TableMetadata table, boolean ifNotExists)
    {
        CqlBuilder builder = new CqlBuilder();
        appendCqlTo(builder, table, ifNotExists);
        return builder.toString();
    }

    /**
     * Appends to the specified builder the CQL used to create this index.
     * @param builder the builder to which the CQL myst be appended
     * @param table the parent table
     * @param ifNotExists includes "IF NOT EXISTS" into statement
     */
    public void appendCqlTo(CqlBuilder builder, TableMetadata table, boolean ifNotExists)
    {
        if (isCustom())
        {
            Map<String, String> copyOptions = new HashMap<>(options);

            builder.append("CREATE CUSTOM INDEX ");

            if (ifNotExists)
            {
                builder.append("IF NOT EXISTS ");
            }

            builder.appendQuotingIfNeeded(name)
                   .append(" ON ")
                   .append(table.toString())
                   .append(" (")
                   .append(copyOptions.remove(IndexTarget.TARGET_OPTION_NAME))
                   .append(") USING ")
                   .appendWithSingleQuotes(copyOptions.remove(IndexTarget.CUSTOM_INDEX_OPTION_NAME));

            if (!copyOptions.isEmpty())
                builder.append(" WITH OPTIONS = ")
                       .append(copyOptions);
        }
        else
        {
            builder.append("CREATE INDEX ");

            if (ifNotExists)
            {
                builder.append("IF NOT EXISTS ");
            }

            builder.appendQuotingIfNeeded(name)
                   .append(" ON ")
                   .append(table.toString())
                   .append(" (")
                   .append(options.get(IndexTarget.TARGET_OPTION_NAME))
                   .append(')');
        }
        builder.append(';');
    }

    public static class Serializer
    {
        public void serialize(IndexMetadata metadata, DataOutputPlus out, int version) throws IOException
        {
            UUIDSerializer.serializer.serialize(metadata.id, out, version);
        }

        public IndexMetadata deserialize(DataInputPlus in, int version, TableMetadata table) throws IOException
        {
            UUID id = UUIDSerializer.serializer.deserialize(in, version);
            return table.indexes.get(id).orElseThrow(() -> new UnknownIndexException(table, id));
        }

        public long serializedSize(IndexMetadata metadata, int version)
        {
            return UUIDSerializer.serializer.serializedSize(metadata.id, version);
        }
    }
}
