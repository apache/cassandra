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
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.transformations.GuardrailTransformations.Flag;
import org.apache.cassandra.utils.Pair;

import static java.lang.String.format;
import static org.apache.cassandra.schema.SchemaConstants.VIRTUAL_GUARDRAILS;

public class GuardrailEnableFlagsTable extends AbstractMutableVirtualTable
{
    public static final String TABLE_NAME = "flags";

    public static final String NAME_COLUMN = "name";
    public static final String VALUE_COLUMN = "value";

    public GuardrailEnableFlagsTable()
    {
        this(VIRTUAL_GUARDRAILS);
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

        for (Map.Entry<String, Pair<Consumer<Boolean>, BooleanSupplier>> entry : Guardrails.getEnableFlagGuardrails().entrySet())
            result.row(entry.getKey()).column(VALUE_COLUMN, entry.getValue().right.getAsBoolean());

        return result;
    }

    @Override
    protected void applyColumnUpdate(ColumnValues partitionKey, ColumnValues clusteringColumns, Optional<ColumnValue> columnValue)
    {
        if (columnValue.isEmpty())
            return;

        try
        {
            String guardrailName = partitionKey.value(0);

            Pair<Consumer<Boolean>, BooleanSupplier> setterAndGetter = Guardrails.getEnableFlagGuardrails().get(guardrailName);
            if (setterAndGetter == null)
                throw new InvalidRequestException(format("there is no such guardrail with name '%s'", guardrailName));

            ClusterMetadataService.instance().commit(new Flag(guardrailName, columnValue.get().value()),
                                                     clusterMetadata -> clusterMetadata,
                                                     (code, message) -> ClusterMetadata.current());
        }
        catch (Exception ex)
        {
            throw new InvalidRequestException(ex.getMessage());
        }
    }
}
