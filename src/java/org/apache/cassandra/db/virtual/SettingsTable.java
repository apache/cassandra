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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Map;
import java.util.SortedMap;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.apache.cassandra.audit.AuditLogOptions;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.config.TransparentDataEncryptionOptions;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.transport.ServerError;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Ordering;

final class SettingsTable extends AbstractVirtualTable
{
    private static final String VALUE = "value";
    private static final String SETTING = "setting";

    @VisibleForTesting
    static final Map<String, Field> FIELDS = Arrays.stream(Config.class.getFields())
            .filter(f -> !Modifier.isStatic(f.getModifiers()))
            .collect(Collectors.toMap(Field::getName, Functions.identity()));

    @VisibleForTesting
    final SortedMap<String, BiConsumer<SimpleDataSet, Field>> overrides =
        new ImmutableSortedMap.Builder<String, BiConsumer<SimpleDataSet, Field>>(Ordering.natural())
            .put("server_encryption_options", this::addEncryptionOptions)
            .put("client_encryption_options", this::addEncryptionOptions)
            .put("transparent_data_encryption_options", this::addTransparentEncryptionOptions)
            .put("audit_logging_options", this::addAuditLoggingOptions)
            .build();

    @VisibleForTesting
    Config config;

    SettingsTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, "settings")
                           .comment("current settings")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(UTF8Type.instance))
                           .addPartitionKeyColumn(SETTING, UTF8Type.instance)
                           .addRegularColumn(VALUE, UTF8Type.instance)
                           .build());
        config = DatabaseDescriptor.getRawConfig();
    }

    @VisibleForTesting
    Object getValue(Field f)
    {
        Object value;
        try
        {
            value = f.get(config);
        }
        catch (IllegalAccessException | IllegalArgumentException e)
        {
            throw new ServerError(e);
        }
        return value;
    }

    private void addValue(SimpleDataSet result, Field f)
    {
        Object value = getValue(f);
        if (value == null)
        {
            result.row(f.getName());
        }
        else if (overrides.containsKey(f.getName()))
        {
            overrides.get(f.getName()).accept(result, f);
        }
        else
        {
            if (value.getClass().isArray())
            {
                value = Arrays.toString((Object[]) value);
            }

            result.row(f.getName()).column(VALUE, value.toString());
        }
    }

    @Override
    public DataSet data(DecoratedKey partitionKey)
    {
        SimpleDataSet result = new SimpleDataSet(metadata());
        String setting = UTF8Type.instance.compose(partitionKey.getKey());
        Field field = FIELDS.get(setting);
        if (field != null)
            addValue(result, field);
        else
        {
            // rows created by overrides might be directly queried so include them in result to be possibly filtered
            for (Map.Entry<String, Field> f : FIELDS.entrySet())
            {
                if (setting.startsWith(f.getKey()))
                {
                    addValue(result, f.getValue());
                }
            }
        }
        return result;
    }

    @Override
    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());
        for (Field setting : FIELDS.values())
        {
            addValue(result, setting);
        }
        return result;
    }

    private void addAuditLoggingOptions(SimpleDataSet result, Field f)
    {
        Preconditions.checkArgument(AuditLogOptions.class.isAssignableFrom(f.getType()));

        AuditLogOptions value = (AuditLogOptions) getValue(f);
        result.row(f.getName() + "_enabled").column(VALUE, Boolean.toString(value.enabled));
        if (value.enabled)
        {
            result.row(f.getName() + "_logger").column(VALUE, value.logger);
            result.row(f.getName() + "_audit_logs_dir").column(VALUE, value.audit_logs_dir);
            result.row(f.getName() + "_included_keyspaces").column(VALUE, value.included_keyspaces);
            result.row(f.getName() + "_excluded_keyspaces").column(VALUE, value.excluded_keyspaces);
            result.row(f.getName() + "_included_categories").column(VALUE, value.included_categories);
            result.row(f.getName() + "_excluded_categories").column(VALUE, value.excluded_categories);
            result.row(f.getName() + "_included_users").column(VALUE, value.included_users);
            result.row(f.getName() + "_excluded_users").column(VALUE, value.excluded_users);
        }
    }

    private void addTransparentEncryptionOptions(SimpleDataSet result, Field f)
    {
        Preconditions.checkArgument(TransparentDataEncryptionOptions.class.isAssignableFrom(f.getType()));

        TransparentDataEncryptionOptions value = (TransparentDataEncryptionOptions) getValue(f);
        result.row(f.getName() + "_enabled").column(VALUE, Boolean.toString(value.enabled));
        if (value.enabled)
        {
            result.row(f.getName() + "_cipher").column(VALUE, value.cipher);
            result.row(f.getName() + "_chunk_length_kb").column(VALUE, Integer.toString(value.chunk_length_kb));
            result.row(f.getName() + "_iv_length").column(VALUE, Integer.toString(value.iv_length));
        }
    }

    private void addEncryptionOptions(SimpleDataSet result, Field f)
    {
        Preconditions.checkArgument(EncryptionOptions.class.isAssignableFrom(f.getType()));

        EncryptionOptions value = (EncryptionOptions) getValue(f);
        result.row(f.getName() + "_enabled").column(VALUE, Boolean.toString(value.enabled));
        if (value.enabled)
        {
            result.row(f.getName() + "_algorithm").column(VALUE, value.algorithm);
            result.row(f.getName() + "_protocol").column(VALUE, value.protocol);
            result.row(f.getName() + "_cipher_suites").column(VALUE, Arrays.toString(value.cipher_suites));
            result.row(f.getName() + "_client_auth").column(VALUE, Boolean.toString(value.require_client_auth));
            result.row(f.getName() + "_endpoint_verification")
                  .column(VALUE, Boolean.toString(value.require_endpoint_verification));
            result.row(f.getName() + "_optional").column(VALUE, Boolean.toString(value.optional));

            if (value instanceof EncryptionOptions.ServerEncryptionOptions)
            {
                EncryptionOptions.ServerEncryptionOptions server = (EncryptionOptions.ServerEncryptionOptions) value;
                result.row(f.getName() + "_internode_encryption")
                      .column(VALUE, server.internode_encryption.toString());
                result.row(f.getName() + "_legacy_ssl_storage_port")
                      .column(VALUE, Boolean.toString(server.enable_legacy_ssl_storage_port));
            }
        }
    }
}
