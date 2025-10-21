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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.topology.TopologyManager.EpochsSnapshot;
import accord.topology.TopologyManager.EpochsSnapshot.Epoch;
import accord.topology.TopologyManager.EpochsSnapshot.EpochReady;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.TokenRange;

import static accord.topology.TopologyManager.EpochsSnapshot.ResultStatus.SUCCESS;

public class AccordVirtualTables
{
    public static final String EPOCHS = "accord_epochs";
    public static final String TABLE_EPOCHS = "accord_table_epochs";

    private AccordVirtualTables()
    {
    }

    public static Collection<VirtualTable> getAll(String keyspace)
    {
        if (!DatabaseDescriptor.getAccordTransactionsEnabled())
            return Collections.emptyList();

        return List.of(new EpochReadyTable(keyspace),
                       new EpochSyncRanges(keyspace)
        );
    }

    private static TableMetadata.Builder parse(String keyspace, String query)
    {
        return CreateTableStatement.parse(query, keyspace)
                                   .kind(TableMetadata.Kind.VIRTUAL);
    }

    public static class EpochReadyTable extends AbstractVirtualTable
    {
        public EpochReadyTable(String keyspace)
        {
            super(parse(keyspace, "CREATE TABLE " + EPOCHS + " (\n" +
                                  "  epoch bigint PRIMARY KEY,\n" +
                                  "  ready_metadata text,\n" +
                                  "  ready_coordinate text,\n" +
                                  "  ready_data text,\n" +
                                  "  ready_reads text,\n" +
                                  "  ready boolean,\n" +
                                  ")")
                  .partitioner(new LocalPartitioner(ReversedType.getInstance(LongType.instance)))
                  .comment("Exposes the epoch ready state for recieved epochs in Accord")
                  .build());
        }

        @Override
        public DataSet data()
        {
            SimpleDataSet ds = new SimpleDataSet(metadata());
            EpochsSnapshot snapshot = epochsSnapshot();
            for (Epoch epoch : snapshot)
            {
                ds.row(epoch.epoch);
                EpochReady ready = epoch.ready;
                ds.column("ready_metadata", ready.metadata.value);
                ds.column("ready_coordinate", ready.coordinate.value);
                ds.column("ready_data", ready.data.value);
                ds.column("ready_reads", ready.reads.value);
                ds.column("ready", ready.reads == SUCCESS);
            }
            return ds;
        }
    }

    public static class EpochSyncRanges extends AbstractVirtualTable
    {
        protected EpochSyncRanges(String keyspace)
        {
            super(parse(keyspace, "CREATE TABLE " + TABLE_EPOCHS + " (\n" +
                                  "  epoch bigint,\n" +
                                  "  keyspace_name text,\n" +
                                  "  table_name text,\n" +
                                  "  added frozen<list<text>>,\n" +
                                  "  removed frozen<list<text>>,\n" +
                                  "  synced frozen<list<text>>,\n" +
                                  "  closed frozen<list<text>>,\n" +
                                  "  retired frozen<list<text>>,\n" +
                                  "  PRIMARY KEY (epoch, keyspace_name, table_name)\n" +
                                  ")")
                  .partitioner(new LocalPartitioner(ReversedType.getInstance(LongType.instance)))
                  .comment("Shows details on a per-table basis about what ranges are synced per epoch")
                  .build());
        }

        @Override
        public DataSet data()
        {
            SimpleDataSet ds = new SimpleDataSet(metadata());
            EpochsSnapshot snapshot = epochsSnapshot();
            for (Epoch state : snapshot)
            {
                Map<TableId, List<TokenRange>> addedRanges = groupByTable(state.addedRanges);
                Map<TableId, List<TokenRange>> removedRanges = groupByTable(state.removedRanges);
                Map<TableId, List<TokenRange>> synced = groupByTable(state.synced);
                Map<TableId, List<TokenRange>> closed = groupByTable(state.closed);
                Map<TableId, List<TokenRange>> retired = groupByTable(state.retired);

                Set<TableId> allTables = union(addedRanges.keySet(), removedRanges.keySet(), synced.keySet(), closed.keySet(), retired.keySet());
                for (TableId table : allTables)
                {
                    TableMetadata metadata = Schema.instance.getTableMetadata(table);
                    if (metadata == null) continue; // table dropped, ignore
                    ds.row(state.epoch, metadata.keyspace, metadata.name);

                    ds.column("added", format(addedRanges.get(table)));
                    ds.column("removed", format(removedRanges.get(table)));
                    ds.column("synced", format(synced.get(table)));
                    ds.column("closed", format(closed.get(table)));
                    ds.column("retired", format(retired.get(table)));
                }
            }
            return ds;
        }

        private static <T> Set<T> union(Set<T>... sets)
        {
            Preconditions.checkArgument(sets.length > 0);
            if (sets.length == 1) return sets[0];
            Sets.SetView accum = Sets.union(sets[0], sets[1]);
            for (int i = 2; i < sets.length; i++)
                accum = Sets.union(accum, sets[i]);
            return accum;
        }

        private static List<String> format(@Nullable List<TokenRange> list)
        {
            if (list == null || list.isEmpty()) return Collections.emptyList();
            List<String> result = new ArrayList<>(list.size());
            for (TokenRange tr : list)
                result.add(toStringNoTable(tr));
            return result;
        }
    }

    private static EpochsSnapshot epochsSnapshot()
    {
        return AccordService.instance().topology().epochsSnapshot();
    }

    private static String toStringNoTable(TokenRange tr)
    {
        // TokenRange extends Range.EndInclusive
        return "(" + tr.start().printableSuffix() + ", " + tr.end().printableSuffix() + "]";
    }

    private static Map<TableId, List<TokenRange>> groupByTable(Ranges ranges)
    {
        Map<TableId, List<TokenRange>> map = new HashMap<>();
        for (Range range : ranges)
        {
            TokenRange tr = (TokenRange) range;
            map.computeIfAbsent(tr.table(), i -> new ArrayList<>()).add(tr);
        }
        return map;
    }
}
