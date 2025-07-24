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

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.guardrails.GuardrailsProxy;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.TableMetadata;

import static java.lang.String.format;
import static org.apache.cassandra.schema.SchemaConstants.VIRTUAL_VIEWS;

public class GuardrailEnableFlagsTable extends AbstractMutableVirtualTable
{
    public static final String TABLE_NAME = "guardrails_flags";

    public static final String NAME_COLUMN = "name";
    public static final String VALUE_COLUMN = "value";

    private final GuardrailsProxy cache = GuardrailsProxy.instance;

    public GuardrailEnableFlagsTable()
    {
        this(VIRTUAL_VIEWS);
    }

    public GuardrailEnableFlagsTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, TABLE_NAME)
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .comment("Guardrails configuration table for enablement flags")
                           .partitioner(new LocalPartitioner(UTF8Type.instance))
                           .addPartitionKeyColumn(NAME_COLUMN, UTF8Type.instance)
                           .addRegularColumn(VALUE_COLUMN, BooleanType.instance)
                           .build());
    }

    @Override
    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());

        for (Map.Entry<String, List<Method>> entry : cache.getFlagsGetters().entrySet())
        {
            Method getter = entry.getValue().get(0);
            Object enabled = cache.invoke(getter);
            result.row(entry.getKey()).column(VALUE_COLUMN, enabled);
        }

        return result;
    }

    @Override
    public DataSet data(DecoratedKey partitionKey)
    {
        SimpleDataSet result = new SimpleDataSet(metadata());
        String guardrailname = UTF8Type.instance.getString(partitionKey.getKey());
        List<Method> methods = cache.getFlagsGetters().get(guardrailname);

        if (methods == null)
            throw new InvalidRequestException(format("there is no associated getter for guardrail with name %s", guardrailname));

        result.row(guardrailname).column(VALUE_COLUMN, cache.invoke(methods.get(0)));

        return result;
    }

    @Override
    protected void applyColumnUpdate(ColumnValues partitionKey, ColumnValues clusteringColumns, Optional<ColumnValue> columnValue)
    {
        if (columnValue.isEmpty())
            return;

        String key = partitionKey.value(0);
        Method setter = cache.getSetter(key);

        if (setter == null)
            throw new InvalidRequestException(format("there is no associated setter for guardrail with name %s", key));

        Object value = columnValue.get().value();

        cache.invoke(setter, value);
    }
}
