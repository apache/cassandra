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

import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.cassandra.db.guardrails.CustomGuardrailConfig;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.db.guardrails.Guardrails.CustomGuardrailsMapper;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.transformations.GuardrailTransformations.Custom;
import org.apache.cassandra.utils.JsonUtils;

import static java.lang.String.format;
import static org.apache.cassandra.schema.SchemaConstants.VIRTUAL_GUARDRAILS;

public class GuardrailCustomsTable extends AbstractMutableVirtualTable
{
    public static final String TABLE_NAME = "custom";

    public static final String NAME_COLUMN = "name";
    public static final String VALUE_COLUMN = "value";

    public GuardrailCustomsTable()
    {
        this(VIRTUAL_GUARDRAILS);
    }

    public GuardrailCustomsTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, TABLE_NAME)
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .comment("Guardrails configuration table for custom guardrails")
                           .partitioner(new LocalPartitioner(UTF8Type.instance))
                           .addPartitionKeyColumn(NAME_COLUMN, UTF8Type.instance)
                           .addRegularColumn(VALUE_COLUMN, UTF8Type.instance)
                           .build());
    }

    @Override
    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());

        for (Map.Entry<String, CustomGuardrailsMapper> entry : Guardrails.getCustomGuardrails().entrySet())
        {
            String guardrailName = entry.getKey();
            CustomGuardrailsMapper setterAndGetter = entry.getValue();
            if (setterAndGetter == null)
                continue;

            Supplier<CustomGuardrailConfig> getter = setterAndGetter.getter;
            if (getter == null)
                continue;

            CustomGuardrailConfig config = getter.get();
            if (config == null)
                continue;

            result.row(guardrailName).column(VALUE_COLUMN, JsonUtils.writeAsJsonString(config));
        }

        return result;
    }

    @Override
    protected void applyColumnUpdate(ColumnValues partitionKey, ColumnValues clusteringColumns, Optional<ColumnValue> columnValue)
    {
        if (columnValue.isEmpty())
            return;

        String guardrailName = partitionKey.value(0);
        CustomGuardrailsMapper mapper = Guardrails.getCustomGuardrails().get(guardrailName);
        if (mapper == null)
            throw new InvalidRequestException(format("there is no such guardrail with name '%s'", guardrailName));

        Consumer<CustomGuardrailConfig> setter = mapper.setter;
        if (setter == null)
            throw new InvalidRequestException(format("there is no associated setter for guardrail with name %s", guardrailName));

        Supplier<CustomGuardrailConfig> getter = mapper.getter;
        if (getter == null)
            throw new InvalidRequestException(format("there is no associated getter for guardrail with name %s", guardrailName));

        if (!mapper.getter.get().isReconfigurable())
            throw new InvalidRequestException("Guardrail '" + guardrailName + "' is not configurable. You need to set " +
                                              "'reconfigurable' in cassandra.yaml to true and restart the node.");

        String value = columnValue.get().value();

        try
        {
            CustomGuardrailConfig config = new CustomGuardrailConfig(JsonUtils.fromJsonMap(value));
            config.makeReconfigurable();

            mapper.validator.accept(config);

            ClusterMetadataService.instance().commit(new Custom(guardrailName, config),
                                                     clusterMetadata -> clusterMetadata,
                                                     (code, message) -> ClusterMetadata.current());
        }
        catch (Exception ex)
        {
            throw new InvalidRequestException(ex.getMessage());
        }
    }
}
