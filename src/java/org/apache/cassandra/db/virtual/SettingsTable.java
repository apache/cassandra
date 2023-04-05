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
import java.util.Objects;
import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Loader;
import org.apache.cassandra.config.Properties;
import org.apache.cassandra.config.Replacement;
import org.apache.cassandra.config.Replacements;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientWarn;
import org.yaml.snakeyaml.introspector.Property;

final class SettingsTable extends AbstractVirtualTable
{
    private static final String NAME = "name";
    private static final String VALUE = "value";

    private static final Map<String, String> BACKWARDS_COMPATABLE_NAMES = ImmutableMap.copyOf(getBackwardsCompatableNames());
    protected static final Map<String, Property> PROPERTIES = ImmutableMap.copyOf(getProperties());

    private final Config config;

    SettingsTable(String keyspace)
    {
        this(keyspace, DatabaseDescriptor.getRawConfig());
    }

    SettingsTable(String keyspace, Config config)
    {
        super(TableMetadata.builder(keyspace, "settings")
                           .comment("current settings")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(UTF8Type.instance))
                           .addPartitionKeyColumn(NAME, UTF8Type.instance)
                           .addRegularColumn(VALUE, UTF8Type.instance)
                           .build());
        this.config = config;
    }

    @Override
    public DataSet data(DecoratedKey partitionKey)
    {
        SimpleDataSet result = new SimpleDataSet(metadata());
        String name = UTF8Type.instance.compose(partitionKey.getKey());
        if (BACKWARDS_COMPATABLE_NAMES.containsKey(name))
            ClientWarn.instance.warn("key '" + name + "' is deprecated; should switch to '" + BACKWARDS_COMPATABLE_NAMES.get(name) + "'");
        if (PROPERTIES.containsKey(name))
            result.row(name).column(VALUE, getValue(PROPERTIES.get(name)));
        return result;
    }

    @Override
    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());
        for (Map.Entry<String, Property> e : PROPERTIES.entrySet())
            result.row(e.getKey()).column(VALUE, getValue(e.getValue()));
        return result;
    }

    private String getValue(Property prop)
    {
        Object value = prop.get(config);
        return value == null ? null : value.toString();
    }

    private static Map<String, Property> getProperties()
    {
        Loader loader = Properties.defaultLoader();
        Map<String, Property> properties = loader.flatten(Config.class);
        // only handling top-level replacements for now, previous logic was only top level so not a regression
        Map<String, Replacement> replacements = Replacements.getNameReplacements(Config.class).get(Config.class);
        if (replacements != null)
        {
            for (Replacement r : replacements.values())
            {
                Property latest = properties.get(r.newName);
                assert latest != null : "Unable to find replacement new name: " + r.newName;
                Property conflict = properties.put(r.oldName, r.toProperty(latest));
                // some configs kept the same name, but changed the type, if this is detected then rely on the replaced property
                assert conflict == null || r.oldName.equals(r.newName) : String.format("New property %s attempted to replace %s, but this property already exists", latest.getName(), conflict.getName());
            }
        }
        for (Map.Entry<String, String> e : BACKWARDS_COMPATABLE_NAMES.entrySet())
        {
            String oldName = e.getKey();
            if (properties.containsKey(oldName))
                throw new AssertionError("Name " + oldName + " is present in Config, this adds a conflict as this name had a different meaning in " + SettingsTable.class.getSimpleName());
            String newName = e.getValue();
            Property prop = Objects.requireNonNull(properties.get(newName), newName + " cant be found for " + oldName);
            properties.put(oldName, Properties.rename(oldName, prop));
        }
        return properties;
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
