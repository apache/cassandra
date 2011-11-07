/**
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
package org.apache.cassandra.db;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import org.junit.Test;
import static org.junit.Assert.fail;

import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.*;
import org.apache.cassandra.Util;
import static org.apache.cassandra.db.context.CounterContext.ContextState;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

public class CounterMutationTest extends CleanupHelper
{
    @Test
    public void testMergeOldShards() throws IOException
    {
        RowMutation rm;
        CounterMutation cm;

        NodeId id1 = NodeId.getLocalId();

        rm = new RowMutation("Keyspace1", ByteBufferUtil.bytes("key1"));
        rm.addCounter(new QueryPath("Counter1", null, ByteBufferUtil.bytes("Column1")), 3);
        cm = new CounterMutation(rm, ConsistencyLevel.ONE);
        cm.apply();

        NodeId.renewLocalId(2L); // faking time of renewal for test
        NodeId id2 = NodeId.getLocalId();

        rm = new RowMutation("Keyspace1", ByteBufferUtil.bytes("key1"));
        rm.addCounter(new QueryPath("Counter1", null, ByteBufferUtil.bytes("Column1")), 4);
        cm = new CounterMutation(rm, ConsistencyLevel.ONE);
        cm.apply();

        NodeId.renewLocalId(4L); // faking time of renewal for test
        NodeId id3 = NodeId.getLocalId();

        rm = new RowMutation("Keyspace1", ByteBufferUtil.bytes("key1"));
        rm.addCounter(new QueryPath("Counter1", null, ByteBufferUtil.bytes("Column1")), 5);
        rm.addCounter(new QueryPath("Counter1", null, ByteBufferUtil.bytes("Column2")), 1);
        cm = new CounterMutation(rm, ConsistencyLevel.ONE);
        cm.apply();

        DecoratedKey dk = Util.dk("key1");
        ColumnFamily cf = Util.getColumnFamily(Table.open("Keyspace1"), dk, "Counter1");

        // First merges old shards
        CounterColumn.mergeAndRemoveOldShards(dk, cf, Integer.MIN_VALUE, Integer.MAX_VALUE, false);
        long now = System.currentTimeMillis();
        IColumn c = cf.getColumn(ByteBufferUtil.bytes("Column1"));
        assert c != null;
        assert c instanceof CounterColumn;
        assert ((CounterColumn)c).total() == 12L;
        ContextState s = new ContextState(c.value());
        assert s.getNodeId().equals(id1);
        assert s.getCount() == 0L;
        assert -s.getClock() > now - 1000 : " >";
        assert -s.getClock() <= now;
        s.moveToNext();
        assert s.getNodeId().equals(id2);
        assert s.getCount() == 0L;
        assert -s.getClock() > now - 1000;
        assert -s.getClock() <= now;
        s.moveToNext();
        assert s.getNodeId().equals(id3);
        assert s.getCount() == 12L;

        // Then collect old shards
        CounterColumn.mergeAndRemoveOldShards(dk, cf, Integer.MAX_VALUE, Integer.MIN_VALUE, false);
        c = cf.getColumn(ByteBufferUtil.bytes("Column1"));
        assert c != null;
        assert c instanceof CounterColumn;
        assert ((CounterColumn)c).total() == 12L;
        s = new ContextState(c.value());
        assert s.getNodeId().equals(id3);
        assert s.getCount() == 12L;
    }

    @Test
    public void testMutateSuperColumns() throws IOException
    {
        RowMutation rm;
        CounterMutation cm;

        rm = new RowMutation("Keyspace1", bytes("key1"));
        rm.addCounter(new QueryPath("SuperCounter1", bytes("sc1"), bytes("Column1")), 1);
        rm.addCounter(new QueryPath("SuperCounter1", bytes("sc2"), bytes("Column1")), 1);
        cm = new CounterMutation(rm, ConsistencyLevel.ONE);
        cm.apply();

        rm = new RowMutation("Keyspace1", bytes("key1"));
        rm.addCounter(new QueryPath("SuperCounter1", bytes("sc1"), bytes("Column2")), 1);
        rm.addCounter(new QueryPath("SuperCounter1", bytes("sc2"), bytes("Column2")), 1);
        cm = new CounterMutation(rm, ConsistencyLevel.ONE);
        cm.apply();

        RowMutation reprm = cm.makeReplicationMutation();
        ColumnFamily cf = reprm.getColumnFamilies().iterator().next();

        assert cf.getColumnCount() == 2;

        IColumn sc1 = cf.getColumn(bytes("sc1"));
        assert sc1 != null && sc1 instanceof SuperColumn;
        assert sc1.getSubColumns().size() == 1;
        assert sc1.getSubColumn(bytes("Column2")) != null;

        IColumn sc2 = cf.getColumn(bytes("sc2"));
        assert sc2 != null && sc2 instanceof SuperColumn;
        assert sc2.getSubColumns().size() == 1;
        assert sc2.getSubColumn(bytes("Column2")) != null;
    }

    @Test
    public void testGetOldShardFromSystemTable() throws IOException
    {
        // Renewing a bunch of times and checking we get the same thing from
        // the system table that what is in memory
        NodeId.renewLocalId();
        NodeId.renewLocalId();
        NodeId.renewLocalId();

        List<NodeId.NodeIdRecord> inMem = NodeId.getOldLocalNodeIds();
        List<NodeId.NodeIdRecord> onDisk = SystemTable.getOldLocalNodeIds();

        assert inMem.equals(onDisk);
    }

    @Test
    public void testRemoveOldShardFixCorrupted() throws IOException
    {
        CounterContext ctx = CounterContext.instance();
        int now = (int) (System.currentTimeMillis() / 1000);

        // Check that corrupted context created prior to #2968 are fixed by removeOldShards
        NodeId id1 = NodeId.getLocalId();
        NodeId.renewLocalId();
        NodeId id2 = NodeId.getLocalId();

        ContextState state = ContextState.allocate(3, 2);
        state.writeElement(NodeId.fromInt(1), 1, 4, false);
        state.writeElement(id1, 3, 2, true);
        state.writeElement(id2, -100, 5, true); // corrupted!

        assert ctx.total(state.context) == 11;

        try
        {
            ByteBuffer merger = ctx.computeOldShardMerger(state.context, Collections.<NodeId.NodeIdRecord>emptyList(), 0);
            ctx.removeOldShards(ctx.merge(state.context, merger, HeapAllocator.instance), now);
            fail("RemoveOldShards should throw an exception if the current id is non-sensical");
        }
        catch (RuntimeException e) {}

        NodeId.renewLocalId();
        ByteBuffer merger = ctx.computeOldShardMerger(state.context, Collections.<NodeId.NodeIdRecord>emptyList(), 0);
        ByteBuffer cleaned = ctx.removeOldShards(ctx.merge(state.context, merger, HeapAllocator.instance), now);
        assert ctx.total(cleaned) == 11;

        // Check it is not corrupted anymore
        ContextState state2 = new ContextState(cleaned);
        while (state2.hasRemaining())
        {
            assert state2.getClock() >= 0 || state2.getCount() == 0;
            state2.moveToNext();
        }

        // Check that if we merge old and clean on another node, we keep the right count
        ByteBuffer onRemote = ctx.merge(ctx.clearAllDelta(state.context), ctx.clearAllDelta(cleaned), HeapAllocator.instance);
        assert ctx.total(onRemote) == 11;
    }
}

