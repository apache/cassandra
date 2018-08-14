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
import java.util.stream.Collectors;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.transport.ServerError;

import com.google.common.base.Functions;

final class SettingsTable extends AbstractVirtualTable
{
    private static final String VALUE = "value";
    private static final String SETTING = "setting";

    private static final Map<String, Field> FIELDS = Arrays.stream(Config.class.getFields())
            .filter(f -> !Modifier.isStatic(f.getModifiers()))
            .collect(Collectors.toMap(Field::getName, Functions.identity()));

    SettingsTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, "settings")
                .comment("current settings")
                .kind(TableMetadata.Kind.VIRTUAL)
                .partitioner(new LocalPartitioner(UTF8Type.instance))
                .addPartitionKeyColumn(SETTING, UTF8Type.instance)
                .addRegularColumn(VALUE, UTF8Type.instance)
                .build());
    }

    private void addValue(SimpleDataSet result, Field f)
    {
        Config config = DatabaseDescriptor.getRawConfig();

        Object value;
        try
        {
            value = f.get(config);
        }
        catch (IllegalAccessException | IllegalArgumentException e)
        {
            throw new ServerError(e);
        }

        if (value != null && value.getClass().isArray())
        {
            value = Arrays.toString((Object[]) value);
        }
        result.row(f.getName());
        if (value != null)
            result.column(VALUE, value.toString());
    }

    public DataSet data(DecoratedKey partitionKey)
    {
        SimpleDataSet result = new SimpleDataSet(metadata());
        String setting = UTF8Type.instance.compose(partitionKey.getKey());
        if (FIELDS.containsKey(setting))
            addValue(result, FIELDS.get(setting));
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
}
