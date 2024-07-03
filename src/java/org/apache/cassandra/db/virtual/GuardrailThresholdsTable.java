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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;

import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.db.guardrails.Guardrails.ThresholdsGuardrailsMapper;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.transformations.GuardrailTransformations.Thresholds;

import static java.lang.String.format;
import static org.apache.cassandra.schema.SchemaConstants.VIRTUAL_GUARDRAILS;

public class GuardrailThresholdsTable extends AbstractMutableVirtualTable
{
    public static final String TABLE_NAME = "thresholds";

    public static final String NAME_COLUMN = "name";
    public static final String VALUE_COLUMN = "value";

    public GuardrailThresholdsTable()
    {
        this(VIRTUAL_GUARDRAILS);
    }

    private static final TupleType tupleType = TupleType.getInstance(new TypeParser(format("(%s, %s)", LongType.instance, LongType.instance)));

    public GuardrailThresholdsTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, TABLE_NAME)
                           .comment("Guardrails configuration table for thresholds")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(UTF8Type.instance))
                           .addPartitionKeyColumn(NAME_COLUMN, UTF8Type.instance)
                           .addRegularColumn(VALUE_COLUMN, tupleType)
                           .build());
    }

    @Override
    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());

        for (ThresholdsGuardrailsMapper mapper : Guardrails.getThresholdGuardails().values())
        {
            result.row(mapper.name).column(VALUE_COLUMN, tupleType.pack(LongType.instance.decompose(mapper.warnGetter.get().longValue()),
                                                                        LongType.instance.decompose(mapper.failGetter.get().longValue())));
        }

        return result;
    }

    @Override
    protected void applyColumnUpdate(ColumnValues partitionKey, ColumnValues clusteringColumns, Optional<ColumnValue> columnValue)
    {
        if (columnValue.isEmpty())
            return;

        String guardrailName = partitionKey.value(0);
        ThresholdsGuardrailsMapper mapper = Guardrails.getThresholdGuardails().get(guardrailName);

        if (mapper == null)
            throw new InvalidRequestException(format("there is no such guardrail with name '%s'", guardrailName));

        Object value = columnValue.get().value();

        List<ByteBuffer> unpack = tupleType.unpack((ByteBuffer) value);

        ByteBuffer warnBuffer = unpack.get(0);
        ByteBuffer failBuffer = unpack.get(1);

        if (warnBuffer == null || failBuffer == null)
            throw new InvalidRequestException("Both elements of a tuple must not be null.");

        try
        {
            long warnThreshold = LongType.instance.compose(warnBuffer);
            long failThreshold = LongType.instance.compose(failBuffer);

            mapper.validator.accept(warnThreshold, failThreshold);

            ClusterMetadataService.instance().commit(new Thresholds(guardrailName, warnThreshold, failThreshold),
                                                     clusterMetadata -> clusterMetadata,
                                                     (code, message) -> ClusterMetadata.current());
        }
        catch (Exception ex)
        {
            throw new InvalidRequestException(ex.getMessage());
        }
    }
}
