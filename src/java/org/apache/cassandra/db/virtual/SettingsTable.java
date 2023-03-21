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
package org.apache.cassandra.db.virtual;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.Converters;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Replacement;
import org.apache.cassandra.config.Replacements;
import org.apache.cassandra.config.registry.Registry;
import org.apache.cassandra.config.registry.TypeConverter;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientWarn;

import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;
import static org.apache.cassandra.utils.FBUtilities.runExceptionally;

final class SettingsTable extends AbstractMutableVirtualTable
{
    private static final String NAME = "name";
    private static final String VALUE = "value";
    private static final Map<String, String> BACKWARDS_COMPATABLE_NAMES = ImmutableMap.copyOf(getBackwardsCompatableNames());
    private final BackwardsCompatableRegistry registry;

    SettingsTable(String keyspace)
    {
        this(keyspace, DatabaseDescriptor.getConfigRegistry());
    }

    SettingsTable(String keyspace, Registry registry)
    {
        super(TableMetadata.builder(keyspace, "settings")
                           .comment("current settings")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(UTF8Type.instance))
                           .addPartitionKeyColumn(NAME, UTF8Type.instance)
                           .addRegularColumn(VALUE, UTF8Type.instance)
                           .build());
        this.registry = new BackwardsCompatableRegistry(registry);
    }

    @Override
    protected void applyColumnDeletion(ColumnValues partitionKey, ColumnValues clusteringColumns, String columnName)
    {
        String key = partitionKey.value(0);
        runExceptionally(() -> registry.set(key, null), e -> invalidRequest("Invalid deletion request; cause: '%s'", e.getMessage()));
    }

    @Override
    protected void applyColumnUpdate(ColumnValues partitionKey,
                                     ColumnValues clusteringColumns,
                                     Optional<ColumnValue> columnValue)
    {
        String key = partitionKey.value(0);
        String value = columnValue.map(v -> v.value().toString()).orElse(null);
        runExceptionally(() -> registry.set(key, value), e -> invalidRequest("Invalid update request; cause: '%s'", e.getMessage()));
    }

    @Override
    public DataSet data(DecoratedKey partitionKey)
    {
        SimpleDataSet result = new SimpleDataSet(metadata());
        String name = UTF8Type.instance.compose(partitionKey.getKey());
        if (BACKWARDS_COMPATABLE_NAMES.containsKey(name))
            ClientWarn.instance.warn("key '" + name + "' is deprecated; should switch to '" + BACKWARDS_COMPATABLE_NAMES.get(name) + '\'');
        if (registry.contains(name))
            runExceptionally(() -> result.row(name).column(VALUE, registry.getString(name)),
                                         e -> invalidRequest("Invalid configuration request during searching by key; cause: '%s'", e.getMessage()));
        return result;
    }

    @Override
    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());
        for (String name : registry.keys())
            runExceptionally(() -> result.row(name).column(VALUE, registry.getString(name)),
                                         e -> invalidRequest("Invalid configuration request; cause: '%s'", e.getMessage()));
        return result;
    }

    @VisibleForTesting
    Registry registry()
    {
        return registry;
    }

    private static Map<String, Replacement> replacements(Registry registry)
    {
        // only handling top-level replacements for now, previous logic was only top level so not a regression
        Map<String, Replacement> replacements = Replacements.getNameReplacements(Config.class).get(Config.class);
        assert replacements != null;
        for (Replacement r : replacements.values())
        {
            if (!registry.contains(r.newName))
                throw new AssertionError("Unable to find replacement new name: " + r.newName);
        }
        for (Map.Entry<String, String> e : BACKWARDS_COMPATABLE_NAMES.entrySet())
        {
            String oldName = e.getKey();
            if (registry.contains(oldName))
                throw new AssertionError("Name " + oldName + " is present in Config, this adds a conflict as this name had a different meaning in " + SettingsTable.class.getSimpleName());
            String newName = e.getValue();
            replacements.put(oldName, new Replacement(Config.class, oldName, registry.type(newName), newName, Converters.IDENTITY, true));
        }
        return replacements;
    }

    /**
     * settings table was released in 4.0 and attempted to support nested properties for a few hand selected properties.
     * The issue is that 4.0 used '_' to seperate the names, which makes it hard to map back to the yaml names; to solve
     * this 4.1+ uses '.' to avoid possible conflicts, this class provides mappings from old names to the '.' names.
     *
     * There were a handle full of properties which had custom names, names not present in the yaml, this map also
     * fixes this and returns the proper (what is accessable via yaml) names.
     */
    private static Map<String, String> getBackwardsCompatableNames()
    {
        Map<String, String> names = new HashMap<>();
        // Names that dont match yaml
        names.put("audit_logging_options_logger", "audit_logging_options.logger.class_name");
        names.put("server_encryption_options_client_auth", "server_encryption_options.require_client_auth");
        names.put("server_encryption_options_endpoint_verification", "server_encryption_options.require_endpoint_verification");
        names.put("server_encryption_options_legacy_ssl_storage_port", "server_encryption_options.legacy_ssl_storage_port_enabled");
        names.put("server_encryption_options_protocol", "server_encryption_options.accepted_protocols");

        // matching names
        names.put("audit_logging_options_audit_logs_dir", "audit_logging_options.audit_logs_dir");
        names.put("audit_logging_options_enabled", "audit_logging_options.enabled");
        names.put("audit_logging_options_excluded_categories", "audit_logging_options.excluded_categories");
        names.put("audit_logging_options_excluded_keyspaces", "audit_logging_options.excluded_keyspaces");
        names.put("audit_logging_options_excluded_users", "audit_logging_options.excluded_users");
        names.put("audit_logging_options_included_categories", "audit_logging_options.included_categories");
        names.put("audit_logging_options_included_keyspaces", "audit_logging_options.included_keyspaces");
        names.put("audit_logging_options_included_users", "audit_logging_options.included_users");
        names.put("server_encryption_options_algorithm", "server_encryption_options.algorithm");
        names.put("server_encryption_options_cipher_suites", "server_encryption_options.cipher_suites");
        names.put("server_encryption_options_enabled", "server_encryption_options.enabled");
        names.put("server_encryption_options_internode_encryption", "server_encryption_options.internode_encryption");
        names.put("server_encryption_options_optional", "server_encryption_options.optional");
        names.put("transparent_data_encryption_options_chunk_length_kb", "transparent_data_encryption_options.chunk_length_kb");
        names.put("transparent_data_encryption_options_cipher", "transparent_data_encryption_options.cipher");
        names.put("transparent_data_encryption_options_enabled", "transparent_data_encryption_options.enabled");
        names.put("transparent_data_encryption_options_iv_length", "transparent_data_encryption_options.iv_length");

        return names;
    }

    /**
     * This class is used to provide backwards compatable support for the settings table in case the {@link Config}
     * metadata changes. This class will provide the old names for the properties, but will use the new name to
     * get the value from the {@link Config} object.
     * <p>
     * Updating a configuration property object will throw an exception if you will try to update a deprecated property.
     */
    private static class BackwardsCompatableRegistry implements Registry
    {
        private final Registry registry;
        private final Map<String, Replacement> replacements;
        private final Set<String> uniquePropertyKeys;
        public BackwardsCompatableRegistry(Registry registry)
        {
            this.registry = registry;
            this.replacements = replacements(registry);
            // Some configs kept the same name, but changed the type, so we need to make sure we don't return the same name twice.
            this.uniquePropertyKeys = ImmutableSet.<String>builder().addAll(registry.keys()).addAll(replacements.keySet()).build();
        }

        @Override
        public void set(String name, @Nullable Object value)
        {
            Replacement replacement = replacements.get(name);
            if (replacement == null)
                registry.set(name, value);
            else
                throw new ConfigurationException(String.format("Unable to set '%s' as it is deprecated and is read only; use '%s' instead", name, replacement.newName));
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> T get(Class<T> cls, String name)
        {
            Replacement replacement = replacements.get(name);
            return replacement == null ?
                   registry.get(cls, name) :
                   (T) replacement.converter.unconvert(registry.get(newReplacementType(replacement), replacement.newName));
        }

        @Override
        public String getString(String name)
        {
            Replacement replacement = replacements.get(name);

            return replacement == null ? registry.getString(name) : TypeConverter.DEFAULT.convertNullable(get(newReplacementType(replacement), name));
        }

        @Override
        public boolean contains(String name)
        {
            return replacements.containsKey(name) || registry.contains(name);
        }

        @Override
        public Iterable<String> keys()
        {
            return uniquePropertyKeys;
        }

        @Override
        public Class<?> type(String name)
        {
            if (replacements.containsKey(name))
                return replacements.get(name).oldType;
            return registry.type(name);
        }

        @Override
        public int size()
        {
            return uniquePropertyKeys.size();
        }

        private static Class<?> newReplacementType(Replacement replacement)
        {
            // If the new type is null, then the old type is the same as the new type (e.g. the property's name changed, but not its type).
            return replacement.converter.getNewType() == null ? replacement.oldType : replacement.converter.getNewType();
        }
    }
}
