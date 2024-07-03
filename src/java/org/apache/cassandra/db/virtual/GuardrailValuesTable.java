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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.db.guardrails.Guardrails.ValuesGuardrailsMapper;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.transformations.GuardrailTransformations.Values;

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
    public void apply(PartitionUpdate update)
    {
        ColumnValues partitionKey = ColumnValues.from(metadata(), update.partitionKey());

        if (update.deletionInfo().isLive())
            update.forEach(row ->
                           {
                               ColumnValues clusteringColumns = ColumnValues.from(metadata(), row.clustering());

                               if (row.deletion().isLive())
                               {
                                   if (row.columnCount() == 0)
                                   {
                                       applyColumnUpdate(partitionKey, clusteringColumns, Optional.empty());
                                   }
                                   else
                                   {
                                       List<String> toDelete = new ArrayList<>();
                                       List<Optional<ColumnValue>> toUpdate = new ArrayList<>();

                                       for (ColumnData cd : row.columnData())
                                       {
                                           Cell<?> cell = (Cell<?>) cd;
                                           if (cell.isTombstone())
                                               toDelete.add(columnName(cell));
                                           else
                                               toUpdate.add(Optional.of(ColumnValue.from(cell)));
                                       }

                                       applyColumnUpdates(partitionKey, toUpdate, toDelete);
                                   }
                               }
                               else
                                   applyRowDeletion(partitionKey, clusteringColumns);
                           });
        else
        {
            // MutableDeletionInfo may have partition delete or range tombstone list or both
            if (update.deletionInfo().hasRanges())
                update.deletionInfo()
                      .rangeIterator(false)
                      .forEachRemaining(rt -> applyRangeTombstone(partitionKey, toRange(rt.deletedSlice())));

            if (!update.deletionInfo().getPartitionDeletion().isLive())
                applyPartitionDeletion(partitionKey);
        }
    }

    private void applyColumnUpdates(ColumnValues partitionKey, List<Optional<ColumnValue>> toUpdate, List<String> toDelete)
    {
        try
        {
            String guardrailName = partitionKey.value(0);
            ValuesGuardrailsMapper mapper = Guardrails.getValueGuardrails().get(guardrailName);
            if (mapper == null)
                throw new InvalidRequestException(format("there is no such guardrail with name '%s'", guardrailName));

            Set<String> warned = null;
            Set<String> ignored = null;
            Set<String> disallowed = null;

            for (Optional<ColumnValue> columnValueOptional : toUpdate)
            {
                if (columnValueOptional.isEmpty())
                    continue;

                ColumnValue columnValue = columnValueOptional.get();

                String name = columnValue.name();

                if (WARNED_COLUMN.equals(name))
                {
                    warned = columnValue.value();
                    if (warned != null)
                        mapper.warnedValuesValidator.accept(warned);
                }
                else if (DISALLOWED_COLUMN.equals(name))
                {
                    disallowed = columnValue.value();
                    if (disallowed != null)
                        mapper.disallowedValuesValidator.accept(disallowed);
                }
                else if (IGNORED_COLUMN.equals(name))
                {
                    ignored = columnValue.value();
                    if (ignored != null)
                        mapper.ignoredValuesValidator.accept(ignored);
                }
            }

            for (String columnName : toDelete)
            {
                if (WARNED_COLUMN.equals(columnName))
                    warned = Set.of();
                else if (DISALLOWED_COLUMN.equals(columnName))
                    disallowed = Set.of();
                else if (IGNORED_COLUMN.equals(columnName))
                    ignored = Set.of();
            }

            ClusterMetadataService.instance().commit(new Values(guardrailName, warned, disallowed, ignored),
                                                     clusterMetadata -> clusterMetadata,
                                                     (code, message) -> ClusterMetadata.current());
        }
        catch (Exception ex)
        {
            throw new InvalidRequestException(ex.getMessage());
        }
    }
}
