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
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.Utils;
import accord.api.MessageSink;
import accord.api.TopologySorter;
import accord.impl.mock.MockCluster;
import accord.local.Node;
import accord.primitives.Ranges;
import accord.topology.ActiveEpoch;
import accord.topology.ActiveEpochs;
import accord.topology.EpochReady;
import accord.topology.Shard;
import accord.topology.Topologies;
import accord.topology.Topology;
import accord.topology.TopologyManager;
import accord.utils.SortedArrays;
import accord.utils.async.AsyncResults;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.IAccordService;
import org.apache.cassandra.service.accord.TokenRange;
import org.mockito.Mockito;

import static org.apache.cassandra.config.DatabaseDescriptor.getPartitioner;
import static org.apache.cassandra.schema.SchemaConstants.VIRTUAL_VIEWS;

public class AccordVirtualTablesTest extends CQLTester
{
    public static final Node.Id N1 = new Node.Id(1);
    public static final SortedArrays.SortedArrayList<Node.Id> ALL = SortedArrays.SortedArrayList.ofSorted(N1);
    public static final Set<Node.Id> FP = Collections.singleton(N1);
    public static final String SUCCESS = "Success";
    public static final String PENDING = "SettableResult{status=pending}";
    public static final List<String> FULL_RANGE = List.of("(-Inf, +Inf]");

    public static TableId T1;
    public static TableMetadata T1_META;

    @BeforeClass
    public static void setup()
    {
        addVirtualKeyspace();
    }

    @Before
    public void setupTables()
    {
        if (T1_META != null) return;

        String tbl1 = createTable("CREATE TABLE %s(pk int primary key)");
        T1_META = Schema.instance.getTableMetadata(keyspace(), tbl1);
        T1 = T1_META.id;

        ServerTestUtils.markCMS();
    }

    @Test
    public void emptyEpochs()
    {
        TopologyManager tm = empty();
        assertRows(execute("SELECT * FROM " + VIRTUAL_VIEWS + "." + AccordVirtualTables.EPOCHS));
        assertRows(execute("SELECT * FROM " + VIRTUAL_VIEWS + "." + AccordVirtualTables.TABLE_EPOCHS));
    }

    @Test
    public void epochUpdates()
    {
        TopologyManager tm = empty();
        long e1 = 1;
        ActiveEpoch a1 = active(topology(e1, T1), EpochReady.done(e1), Topology.EMPTY);
        tm.unsafeSetActive(ActiveEpochs.unsafeNew(tm, new ActiveEpoch[] { a1 }, -1));
        assertRows(execute("SELECT * FROM " + VIRTUAL_VIEWS + "." + AccordVirtualTables.EPOCHS),
                   row(e1, true, SUCCESS, SUCCESS, SUCCESS, SUCCESS));

        long e2 = 2;
        ActiveEpoch a2 = active(topology(e2, T1), pendingReady(e2), Topology.EMPTY);
        tm.unsafeSetActive(ActiveEpochs.unsafeNew(tm, new ActiveEpoch[] { a2, a1 }, -1));
        assertRows(execute("SELECT * FROM " + VIRTUAL_VIEWS + "." + AccordVirtualTables.EPOCHS),
                   row(e2, false, PENDING, PENDING, PENDING, PENDING),
                   row(e1, true, SUCCESS, SUCCESS, SUCCESS, SUCCESS));
    }

    @Test
    public void tableUpdates()
    {
        TopologyManager tm = empty();
        long e1 = 1;
        ActiveEpoch a1 = active(topology(e1, T1), EpochReady.done(e1), Topology.EMPTY);
        tm.unsafeSetActive(ActiveEpochs.unsafeNew(tm, new ActiveEpoch[] { a1 }, -1));

        // the range was added in the first epoch, so its fully synced
        assertRows(execute("SELECT * FROM " + VIRTUAL_VIEWS + "." + AccordVirtualTables.TABLE_EPOCHS),
                   row(e1, T1_META.keyspace, T1_META.name, FULL_RANGE, List.of(), List.of(), List.of(), FULL_RANGE));

        // range is no longer "added" so doesn't show up as synced!
        long e2 = 2;
        ActiveEpoch a2 = active(topology(e2, T1), EpochReady.done(e2), a1.global());
        tm.unsafeSetActive(ActiveEpochs.unsafeNew(tm, new ActiveEpoch[] { a2, a1 }, -1));
        assertRows(execute("SELECT * FROM " + VIRTUAL_VIEWS + "." + AccordVirtualTables.TABLE_EPOCHS),
                   row(e1, T1_META.keyspace, T1_META.name, FULL_RANGE, List.of(), List.of(), List.of(), FULL_RANGE));

        // sync the range
        tm.onReadyToCoordinate(N1, e2);
        assertRows(execute("SELECT * FROM " + VIRTUAL_VIEWS + "." + AccordVirtualTables.TABLE_EPOCHS),
                   row(e2, T1_META.keyspace, T1_META.name, List.of(), List.of(), List.of(), List.of(), FULL_RANGE),
                   row(e1, T1_META.keyspace, T1_META.name, FULL_RANGE, List.of(), List.of(), List.of(), FULL_RANGE));

        // lets close e2
        tm.onEpochClosed(Ranges.single(TokenRange.fullRange(T1, getPartitioner())), e2);
        assertRows(execute("SELECT * FROM " + VIRTUAL_VIEWS + "." + AccordVirtualTables.TABLE_EPOCHS),
                   row(e2, T1_META.keyspace, T1_META.name, List.of(), FULL_RANGE, List.of(), List.of(), FULL_RANGE),
                   row(e1, T1_META.keyspace, T1_META.name, FULL_RANGE, FULL_RANGE, List.of(), List.of(), FULL_RANGE));

        // enjoy retirement!
        tm.onEpochRetired(Ranges.single(TokenRange.fullRange(T1, getPartitioner())), e2);
        assertRows(execute("SELECT * FROM " + VIRTUAL_VIEWS + "." + AccordVirtualTables.TABLE_EPOCHS),
                   row(e2, T1_META.keyspace, T1_META.name, List.of(), FULL_RANGE, List.of(), FULL_RANGE, FULL_RANGE),
                   row(e1, T1_META.keyspace, T1_META.name, FULL_RANGE, FULL_RANGE, List.of(), FULL_RANGE, FULL_RANGE));
    }

    private static EpochReady pendingReady(long epoch)
    {
        return new EpochReady(epoch, AsyncResults.settable(), AsyncResults.settable(), AsyncResults.settable(), AsyncResults.settable());
    }

    private static Topology topology(long epoch, TableId tableId)
    {
        TokenRange all = TokenRange.fullRange(tableId, getPartitioner());
        return new Topology(epoch, Shard.create(all, ALL, FP));
    }

    private static TopologyManager empty()
    {
        TopologySorter.Supplier supplier = new TopologySorter.Supplier()
        {
            @Override
            public TopologySorter get(Topology topologies)
            {
                return SORTER;
            }

            @Override
            public TopologySorter get(Topologies topologies)
            {
                return SORTER;
            }
        };

        Node node = Utils.createNode(N1, Topology.EMPTY, new MessageSink.NoOpSink(), new MockCluster.Clock(0));
        TopologyManager tm = new TopologyManager(supplier, node, ignore -> { throw new UnsupportedOperationException(); }, null, null);

        var mock = Mockito.mock(IAccordService.class);
        Mockito.when(mock.topology()).thenReturn(tm);
        AccordService.unsafeSetNewAccordService(mock);
        return tm;
    }

    private static final TopologySorter SORTER = (TopologySorter.StaticSorter) (node1, node2, shards) -> 0;

    private static ActiveEpoch active(Topology topology, EpochReady ready, Topology prev)
    {
        return ActiveEpoch.unsafeNew(N1, topology, ready, SORTER, prev.ranges());
    }
}