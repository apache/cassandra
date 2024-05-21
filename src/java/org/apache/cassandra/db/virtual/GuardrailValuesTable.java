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

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.db.guardrails.Guardrails.ValuesGuardrailsMapper;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.TableMetadata;

import static java.lang.String.format;
import static org.apache.cassandra.schema.SchemaConstants.VIRTUAL_GUARDRAILS;

public class GuardrailValuesTable extends AbstractMutableVirtualTable
{
    public static final String TABLE_NAME = "values";

    public static final String NAME_COLUMN = "name";
    public static final String WARNED_COLUMN = "warned";
    public static final String IGNORED_COLUMN = "ignored";
    public static final String DISALLOWED_COLUMN = "disallowed";

    public GuardrailValuesTable()
    {
        this(VIRTUAL_GUARDRAILS);
    }

    public GuardrailValuesTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, TABLE_NAME)
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .comment("Guardrails configuration table for values")
                           .partitioner(new LocalPartitioner(UTF8Type.instance))
                           .addPartitionKeyColumn(NAME_COLUMN, UTF8Type.instance)
                           .addRegularColumn(WARNED_COLUMN, SetType.getInstance(UTF8Type.instance, false))
                           .addRegularColumn(IGNORED_COLUMN, SetType.getInstance(UTF8Type.instance, false))
                           .addRegularColumn(DISALLOWED_COLUMN, SetType.getInstance(UTF8Type.instance, false))
                           .build());
    }

    @Override
    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());

        for (Map.Entry<String, ValuesGuardrailsMapper> entry : Guardrails.getValueGuardrails().entrySet())
        {
            String name = entry.getKey();
            ValuesGuardrailsMapper mapper = entry.getValue();

            result.row(name)
                  .column(WARNED_COLUMN, mapper.warnedValuesSupplier.get())
                  .column(DISALLOWED_COLUMN, mapper.disallowedValuesSupplier.get())
                  .column(IGNORED_COLUMN, mapper.ignoredValuesSupplier.get());
        }

        return result;
    }

    @Override
    protected void applyColumnUpdate(ColumnValues partitionKey, ColumnValues clusteringColumns, Optional<ColumnValue> columnValue)
    {
        if (columnValue.isEmpty())
            return;

        String key = partitionKey.value(0);
        ValuesGuardrailsMapper mapper = Guardrails.getValueGuardrails().get(key);
        if (mapper == null)
            throw new InvalidRequestException(format("there is no such guardrail with name '%s'", key));

        String name = columnValue.get().name();
        Set<String> value = columnValue.get().value();
        if (value == null)
            value = Collections.emptySet();

        try
        {
            if (WARNED_COLUMN.equals(name))
                mapper.warnedValuesConsumer.accept(value);
            else if (DISALLOWED_COLUMN.equals(name))
                mapper.disallowedValuesConsumer.accept(value);
            else if (IGNORED_COLUMN.equals(name))
                mapper.ignoredValuesConsumer.accept(value);
        }
        catch (Exception ex)
        {
            throw new InvalidRequestException(ex.getMessage());
        }
    }
}
