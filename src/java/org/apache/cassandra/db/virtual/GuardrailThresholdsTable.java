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
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Pair;

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

        for (Map.Entry<String, Pair<BiConsumer<Number, Number>, Pair<Supplier<Number>, Supplier<Number>>>> entry : Guardrails.getThresholdGuardails().entrySet())
        {
            String guardrailName = entry.getKey();
            Pair<Supplier<Number>, Supplier<Number>> getter = entry.getValue().right;

            if (getter == null)
                continue;

            result.row(guardrailName).column(VALUE_COLUMN, tupleType.pack(LongType.instance.decompose(getter.left.get().longValue()),
                                                                          LongType.instance.decompose(getter.right.get().longValue())));
        }

        return result;
    }

    @Override
    protected void applyColumnUpdate(ColumnValues partitionKey, ColumnValues clusteringColumns, Optional<ColumnValue> columnValue)
    {
        if (columnValue.isEmpty())
            return;

        String key = partitionKey.value(0);
        Pair<BiConsumer<Number, Number>, Pair<Supplier<Number>, Supplier<Number>>> setterAndGetter = Guardrails.getThresholdGuardails().get(key);

        if (setterAndGetter == null)
            throw new InvalidRequestException(format("there is no such guardrail with name '%s'", key));

        ColumnValue value = columnValue.get();
        Object val = value.value();

        List<ByteBuffer> unpack = tupleType.unpack((ByteBuffer) val);

        ByteBuffer warnBuffer = unpack.get(0);
        ByteBuffer failBuffer = unpack.get(1);

        if (warnBuffer == null || failBuffer == null)
            throw new InvalidRequestException("Both elements of a tuple must not be null.");

        try
        {
            setterAndGetter.left.accept(LongType.instance.compose(warnBuffer),
                                        LongType.instance.compose(failBuffer));
        }
        catch (Exception ex)
        {
            throw new InvalidRequestException(ex.getMessage());
        }
    }
}
