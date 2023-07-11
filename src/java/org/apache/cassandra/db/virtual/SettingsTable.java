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
import java.util.function.Function;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.YamlConfigurationLoader;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.PropertyNotFoundException;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientWarn;

import static org.apache.cassandra.config.DatabaseDescriptor.getPropertyType;
import static org.apache.cassandra.config.DatabaseDescriptor.getPropertyValue;
import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;
import static org.apache.cassandra.utils.FBUtilities.runExceptionally;
import static org.apache.commons.lang3.ClassUtils.primitiveToWrapper;

final class SettingsTable extends AbstractMutableVirtualTable
{
    private static final String NAME = "name";
    private static final String VALUE = "value";
    private static final String MUTABLE = "is_mutable";
    static final Map<String, String> BACKWARDS_COMPATABLE_NAMES = ImmutableMap.copyOf(getBackwardsCompatableNames());

    SettingsTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, "settings")
                           .comment("current settings")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(UTF8Type.instance))
                           .addPartitionKeyColumn(NAME, UTF8Type.instance)
                           .addRegularColumn(VALUE, UTF8Type.instance)
                           .addRegularColumn(MUTABLE, BooleanType.instance)
                           .build());
        for (String name : BACKWARDS_COMPATABLE_NAMES.keySet())
        {
            if (DatabaseDescriptor.hasProperty(name))
            {
                throw new AssertionError(String.format("Name '%s' is present in Config, this adds a conflict as this " +
                                                       "name had a different meaning in the '%s",
                                                       name, SettingsTable.class.getSimpleName()));
            }
        }
    }

    @Override
    protected void applyColumnUpdate(AbstractMutableVirtualTable.ColumnValues partitionKey,
                                     AbstractMutableVirtualTable.ColumnValues clusteringColumns,
                                     Optional<AbstractMutableVirtualTable.ColumnValue> columnValue)
    {
        String name = partitionKey.value(0);
        String value = columnValue.map(v -> v.value().toString()).orElse(null);
        if (value == null)
            throw new InvalidRequestException(String.format("Setting the value to null is equivalent to the deletion operation. " +
                                              "Column deletion is not supported by the '%s.%s'", metadata().keyspace, metadata().name));
        else
            runExceptionally(() -> setPropertyFromString(getKeyAndWarnIfObsolete(name), value),
                             t -> invalidRequest("Invalid update request '%s'. Cause: %s", name, t.getMessage()));
    }

    @Override
    public DataSet data(DecoratedKey partitionKey)
    {
        SimpleDataSet result = new SimpleDataSet(metadata());
        String name = UTF8Type.instance.compose(partitionKey.getKey());
        try
        {
            Object value = getPropertyAsString(getKeyAndWarnIfObsolete(name));
            result.row(name).column(VALUE, value).column(MUTABLE, DatabaseDescriptor.isMutableProperty(name));
        }
        catch (PropertyNotFoundException e)
        {
            // ignore
        }
        return result;
    }

    @Override
    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());
        for (String key : DatabaseDescriptor.getAllProperties())
        {
            if (BACKWARDS_COMPATABLE_NAMES.containsKey(key))
                continue;
            runExceptionally(() -> result.row(key)
                                         .column(VALUE, getPropertyAsString(key))
                                         .column(MUTABLE, DatabaseDescriptor.isMutableProperty(key)),
                             t -> new ConfigurationException(t.getMessage(), false));
        }

        runExceptionally(() -> BACKWARDS_COMPATABLE_NAMES.forEach((oldName, newName) ->
                                           result.row(oldName)
                                                 .column(VALUE, getPropertyAsString(newName))
                                                 .column(MUTABLE, DatabaseDescriptor.isMutableProperty(newName))),
                         t -> new ConfigurationException(t.getMessage(), false));
        return result;
    }

    static String getKeyAndWarnIfObsolete(String name)
    {
        String key = BACKWARDS_COMPATABLE_NAMES.getOrDefault(name, name);
        if (BACKWARDS_COMPATABLE_NAMES.containsKey(name))
            ClientWarn.instance.warn("key '" + name + "' is deprecated; should switch to '" + BACKWARDS_COMPATABLE_NAMES.get(name) + '\'');
        return key;
    }

    public static String getPropertyAsString(String name)
    {
        return getPropertyType(name).equals(String.class) ?
               (String) getPropertyValue(name) :
               propertyToStringConverter().apply(getPropertyValue(name));
    }

    public static void setPropertyFromString(String name, String value)
    {
        if (value == null)
            DatabaseDescriptor.setProperty(name, null);
        else
        {
            Class<?> fromType = primitiveToWrapper(getPropertyType(name));
            Object obj = YamlConfigurationLoader.YamlFactory.instance.newYamlInstance(fromType)
                                                                     .loadAs(value, fromType);
            DatabaseDescriptor.setProperty(name, obj);
        }
    }

    /**
     * @return A converter that converts given object to a string.
     * @param <T> Type to convert from.
     */
    public static <T> Function<T, String> propertyToStringConverter()
    {
        return obj -> obj == null ? null : YamlConfigurationLoader.YamlFactory.instance.newYamlInstance(Config.class)
                                                                                       .dump(obj)
                                                                                       .trim();
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
}
