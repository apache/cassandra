/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */
package org.apache.cassandra.db.context;

import static org.junit.Assert.*;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.apache.cassandra.Util;

import org.apache.cassandra.db.context.IContext.ContextRelationship;
import static org.apache.cassandra.db.context.CounterContext.ContextState;
import org.apache.cassandra.utils.*;

public class CounterContextTest
{
    private static final CounterContext cc = new CounterContext();

    private static final int idLength;
    private static final int clockLength;
    private static final int countLength;

    private static final int stepLength;

    static
    {
        idLength       = NodeId.LENGTH; // size of int
        clockLength    = 8; // size of long
        countLength    = 8; // size of long
        stepLength     = idLength + clockLength + countLength;
    }

    /** Allocates 1 byte from a new SlabAllocator and returns it. */
    private Allocator bumpedSlab()
    {
        SlabAllocator allocator = new SlabAllocator();
        allocator.allocate(1);
        return allocator;
    }

    @Test
    public void testCreate()
    {
        runCreate(HeapAllocator.instance);
        runCreate(bumpedSlab());
    }

    private void runCreate(Allocator allocator)
    {
        ByteBuffer bytes = cc.create(4, allocator);
        assertEquals(stepLength + 4, bytes.remaining());
    }

    @Test
    public void testDiff()
    {
        runDiff(HeapAllocator.instance);
        runDiff(bumpedSlab());
    }

    private void runDiff(Allocator allocator)
    {
        ContextState left = ContextState.allocate(3, 0, allocator);
        ContextState right;

        // equality: equal nodes, all counts same
        left.writeElement(NodeId.fromInt(3), 3L, 0L);
        left.writeElement(NodeId.fromInt(6), 2L, 0L);
        left.writeElement(NodeId.fromInt(9), 1L, 0L);
        right = new ContextState(ByteBufferUtil.clone(left.context), left.headerLength);

        assert ContextRelationship.EQUAL ==
            cc.diff(left.context, right.context);

        // greater than: left has superset of nodes (counts equal)
        left = ContextState.allocate(4, 0, allocator);
        left.writeElement(NodeId.fromInt(3),  3L, 0L);
        left.writeElement(NodeId.fromInt(6),  2L, 0L);
        left.writeElement(NodeId.fromInt(9),  1L, 0L);
        left.writeElement(NodeId.fromInt(12), 0L, 0L);

        right = ContextState.allocate(3, 0, allocator);
        right.writeElement(NodeId.fromInt(3), 3L, 0L);
        right.writeElement(NodeId.fromInt(6), 2L, 0L);
        right.writeElement(NodeId.fromInt(9), 1L, 0L);

        assert ContextRelationship.GREATER_THAN ==
            cc.diff(left.context, right.context);
        
        // less than: left has subset of nodes (counts equal)
        left = ContextState.allocate(3, 0, allocator);
        left.writeElement(NodeId.fromInt(3), 3L, 0L);
        left.writeElement(NodeId.fromInt(6), 2L, 0L);
        left.writeElement(NodeId.fromInt(9), 1L, 0L);

        right = ContextState.allocate(4, 0, allocator);
        right.writeElement(NodeId.fromInt(3),  3L, 0L);
        right.writeElement(NodeId.fromInt(6),  2L, 0L);
        right.writeElement(NodeId.fromInt(9),  1L, 0L);
        right.writeElement(NodeId.fromInt(12), 0L, 0L);

        assert ContextRelationship.LESS_THAN ==
            cc.diff(left.context, right.context);

        // greater than: equal nodes, but left has higher counts
        left = ContextState.allocate(3, 0, allocator);
        left.writeElement(NodeId.fromInt(3), 3L, 0L);
        left.writeElement(NodeId.fromInt(6), 2L, 0L);
        left.writeElement(NodeId.fromInt(9), 3L, 0L);

        right = ContextState.allocate(3, 0, allocator);
        right.writeElement(NodeId.fromInt(3), 3L, 0L);
        right.writeElement(NodeId.fromInt(6), 2L, 0L);
        right.writeElement(NodeId.fromInt(9), 1L, 0L);

        assert ContextRelationship.GREATER_THAN ==
            cc.diff(left.context, right.context);

        // less than: equal nodes, but right has higher counts
        left = ContextState.allocate(3, 0, allocator);
        left.writeElement(NodeId.fromInt(3), 3L, 0L);
        left.writeElement(NodeId.fromInt(6), 2L, 0L);
        left.writeElement(NodeId.fromInt(9), 3L, 0L);

        right = ContextState.allocate(3, 0, allocator);
        right.writeElement(NodeId.fromInt(3), 3L, 0L);
        right.writeElement(NodeId.fromInt(6), 9L, 0L);
        right.writeElement(NodeId.fromInt(9), 3L, 0L);

        assert ContextRelationship.LESS_THAN ==
            cc.diff(left.context, right.context);

        // disjoint: right and left have disjoint node sets
        left = ContextState.allocate(3, 0, allocator);
        left.writeElement(NodeId.fromInt(3), 1L, 0L);
        left.writeElement(NodeId.fromInt(4), 1L, 0L);
        left.writeElement(NodeId.fromInt(9), 1L, 0L);

        right = ContextState.allocate(3, 0, allocator);
        right.writeElement(NodeId.fromInt(3), 1L, 0L);
        right.writeElement(NodeId.fromInt(6), 1L, 0L);
        right.writeElement(NodeId.fromInt(9), 1L, 0L);

        assert ContextRelationship.DISJOINT ==
            cc.diff(left.context, right.context);

        left = ContextState.allocate(3, 0, allocator);
        left.writeElement(NodeId.fromInt(3), 1L, 0L);
        left.writeElement(NodeId.fromInt(4), 1L, 0L);
        left.writeElement(NodeId.fromInt(9), 1L, 0L);

        right = ContextState.allocate(3, 0, allocator);
        right.writeElement(NodeId.fromInt(2),  1L, 0L);
        right.writeElement(NodeId.fromInt(6),  1L, 0L);
        right.writeElement(NodeId.fromInt(12), 1L, 0L);

        assert ContextRelationship.DISJOINT ==
            cc.diff(left.context, right.context);

        // disjoint: equal nodes, but right and left have higher counts in differing nodes
        left = ContextState.allocate(3, 0, allocator);
        left.writeElement(NodeId.fromInt(3), 1L, 0L);
        left.writeElement(NodeId.fromInt(6), 3L, 0L);
        left.writeElement(NodeId.fromInt(9), 1L, 0L);

        right = ContextState.allocate(3, 0, allocator);
        right.writeElement(NodeId.fromInt(3), 1L, 0L);
        right.writeElement(NodeId.fromInt(6), 1L, 0L);
        right.writeElement(NodeId.fromInt(9), 5L, 0L);

        assert ContextRelationship.DISJOINT ==
            cc.diff(left.context, right.context);

        left = ContextState.allocate(3, 0, allocator);
        left.writeElement(NodeId.fromInt(3), 2L, 0L);
        left.writeElement(NodeId.fromInt(6), 3L, 0L);
        left.writeElement(NodeId.fromInt(9), 1L, 0L);

        right = ContextState.allocate(3, 0, allocator);
        right.writeElement(NodeId.fromInt(3), 1L, 0L);
        right.writeElement(NodeId.fromInt(6), 9L, 0L);
        right.writeElement(NodeId.fromInt(9), 5L, 0L);

        assert ContextRelationship.DISJOINT ==
            cc.diff(left.context, right.context);

        // disjoint: left has more nodes, but lower counts
        left = ContextState.allocate(4, 0, allocator);
        left.writeElement(NodeId.fromInt(3),  2L, 0L);
        left.writeElement(NodeId.fromInt(6),  3L, 0L);
        left.writeElement(NodeId.fromInt(9),  1L, 0L);
        left.writeElement(NodeId.fromInt(12), 1L, 0L);

        right = ContextState.allocate(3, 0, allocator);
        right.writeElement(NodeId.fromInt(3), 4L, 0L);
        right.writeElement(NodeId.fromInt(6), 9L, 0L);
        right.writeElement(NodeId.fromInt(9), 5L, 0L);

        assert ContextRelationship.DISJOINT ==
            cc.diff(left.context, right.context);
        
        // disjoint: left has less nodes, but higher counts
        left = ContextState.allocate(3, 0, allocator);
        left.writeElement(NodeId.fromInt(3), 5L, 0L);
        left.writeElement(NodeId.fromInt(6), 3L, 0L);
        left.writeElement(NodeId.fromInt(9), 2L, 0L);

        right = ContextState.allocate(4, 0, allocator);
        right.writeElement(NodeId.fromInt(3),  4L, 0L);
        right.writeElement(NodeId.fromInt(6),  3L, 0L);
        right.writeElement(NodeId.fromInt(9),  2L, 0L);
        right.writeElement(NodeId.fromInt(12), 1L, 0L);

        assert ContextRelationship.DISJOINT ==
            cc.diff(left.context, right.context);

        // disjoint: mixed nodes and counts
        left = ContextState.allocate(3, 0, allocator);
        left.writeElement(NodeId.fromInt(3), 5L, 0L);
        left.writeElement(NodeId.fromInt(6), 2L, 0L);
        left.writeElement(NodeId.fromInt(9), 2L, 0L);

        right = ContextState.allocate(4, 0, allocator);
        right.writeElement(NodeId.fromInt(3),  4L, 0L);
        right.writeElement(NodeId.fromInt(6),  3L, 0L);
        right.writeElement(NodeId.fromInt(9),  2L, 0L);
        right.writeElement(NodeId.fromInt(12), 1L, 0L);

        assert ContextRelationship.DISJOINT ==
            cc.diff(left.context, right.context);

        left = ContextState.allocate(4, 0, allocator);
        left.writeElement(NodeId.fromInt(3), 5L, 0L);
        left.writeElement(NodeId.fromInt(6), 2L, 0L);
        left.writeElement(NodeId.fromInt(7), 2L, 0L);
        left.writeElement(NodeId.fromInt(9), 2L, 0L);

        right = ContextState.allocate(3, 0, allocator);
        right.writeElement(NodeId.fromInt(3), 4L, 0L);
        right.writeElement(NodeId.fromInt(6), 3L, 0L);
        right.writeElement(NodeId.fromInt(9), 2L, 0L);

        assert ContextRelationship.DISJOINT ==
            cc.diff(left.context, right.context);
    }

    @Test
    public void testMerge()
    {
        runMerge(HeapAllocator.instance);
        runMerge(bumpedSlab());
    }

    private void runMerge(Allocator allocator)
    {
        // note: local counts aggregated; remote counts are reconciled (i.e. take max)
        ContextState left = ContextState.allocate(4, 1, allocator);
        left.writeElement(NodeId.fromInt(1), 1L, 1L);
        left.writeElement(NodeId.fromInt(2), 2L, 2L);
        left.writeElement(NodeId.fromInt(4), 6L, 3L);
        left.writeElement(NodeId.getLocalId(), 7L, 3L, true);

        ContextState right = ContextState.allocate(3, 1, allocator);
        right.writeElement(NodeId.fromInt(4), 4L, 4L);
        right.writeElement(NodeId.fromInt(5), 5L, 5L);
        right.writeElement(NodeId.getLocalId(), 2L, 9L, true);

        ByteBuffer merged = cc.merge(left.context, right.context, allocator);
        int hd = 4;

        assertEquals(hd + 5 * stepLength, merged.remaining());
        // local node id's counts are aggregated
        assert Util.equalsNodeId(NodeId.getLocalId(), merged, hd + 4*stepLength);
        assertEquals(  9L, merged.getLong(merged.position() + hd + 4*stepLength + idLength));
        assertEquals(12L,  merged.getLong(merged.position() + hd + 4*stepLength + idLength + clockLength));

        // remote node id counts are reconciled (i.e. take max)
        assert Util.equalsNodeId(NodeId.fromInt(4), merged, hd + 2*stepLength);
        assertEquals( 6L,  merged.getLong(merged.position() + hd + 2*stepLength + idLength));
        assertEquals( 3L,  merged.getLong(merged.position() + hd + 2*stepLength + idLength + clockLength));

        assert Util.equalsNodeId(NodeId.fromInt(5), merged, hd + 3*stepLength);
        assertEquals( 5L,  merged.getLong(merged.position() + hd + 3*stepLength + idLength));
        assertEquals( 5L,  merged.getLong(merged.position() + hd + 3*stepLength + idLength + clockLength));

        assert Util.equalsNodeId(NodeId.fromInt(2), merged, hd + 1*stepLength);
        assertEquals( 2L,  merged.getLong(merged.position() + hd + 1*stepLength + idLength));
        assertEquals( 2L,  merged.getLong(merged.position() + hd + 1*stepLength + idLength + clockLength));

        assert Util.equalsNodeId(NodeId.fromInt(1), merged, hd + 0*stepLength);
        assertEquals( 1L,  merged.getLong(merged.position() + hd + 0*stepLength + idLength));
        assertEquals( 1L,  merged.getLong(merged.position() + hd + 0*stepLength + idLength + clockLength));
    }

    @Test
    public void testTotal()
    {
        runTotal(HeapAllocator.instance);
        runTotal(bumpedSlab());
    }

    private void runTotal(Allocator allocator)
    {
        ContextState left = ContextState.allocate(4, 1, allocator);
        left.writeElement(NodeId.fromInt(1), 1L, 1L);
        left.writeElement(NodeId.fromInt(2), 2L, 2L);
        left.writeElement(NodeId.fromInt(4), 3L, 3L);
        left.writeElement(NodeId.getLocalId(), 3L, 3L, true);

        ContextState right = ContextState.allocate(3, 1, allocator);
        right.writeElement(NodeId.fromInt(4), 4L, 4L);
        right.writeElement(NodeId.fromInt(5), 5L, 5L);
        right.writeElement(NodeId.getLocalId(), 9L, 9L, true);

        ByteBuffer merged = cc.merge(left.context, right.context, allocator);

        // 127.0.0.1: 12 (3+9)
        // 0.0.0.1:    1
        // 0.0.0.2:    2
        // 0.0.0.4:    4
        // 0.0.0.5:    5

        assertEquals(24L, cc.total(merged));
    }

    @Test
    public void testMergeOldShards()
    {
        runMergeOldShards(HeapAllocator.instance);
        runMergeOldShards(bumpedSlab());
    }

    private void runMergeOldShards(Allocator allocator)
    {
        long now = System.currentTimeMillis();
        NodeId id1 = NodeId.fromInt(1);
        NodeId id3 = NodeId.fromInt(3);
        List<NodeId.NodeIdRecord> records = new ArrayList<NodeId.NodeIdRecord>();
        records.add(new NodeId.NodeIdRecord(id1, 2L));
        records.add(new NodeId.NodeIdRecord(id3, 4L));

        ContextState ctx = ContextState.allocate(5, 3, allocator);
        ctx.writeElement(id1, 1L, 1L, true);
        ctx.writeElement(NodeId.fromInt(2), 2L, 2L);
        ctx.writeElement(id3, 3L, 3L, true);
        ctx.writeElement(NodeId.fromInt(4), 6L, 3L);
        ctx.writeElement(NodeId.fromInt(5), 7L, 3L, true);

        ByteBuffer merger = cc.computeOldShardMerger(ctx.context, records, Integer.MAX_VALUE);

        ContextState m = new ContextState(merger);

        assert m.getNodeId().equals(id1);
        assert m.getClock() <= -now;
        assert m.getCount() == -1L;
        assert m.isDelta();
        m.moveToNext();
        assert m.getNodeId().equals(id3);
        assert m.getClock() <= -now;
        assert m.getCount() == -3L;
        assert m.isDelta();
        m.moveToNext();
        assert m.getNodeId().equals(NodeId.getLocalId());
        assert m.getClock() == 1L;
        assert m.getCount() == 4L;
        assert m.isDelta();
        assert cc.total(ctx.context) == cc.total(cc.merge(ctx.context, merger, allocator));
    }

    @Test
    public void testRemoveOldShards()
    {
        runRemoveOldShards(HeapAllocator.instance);
        runRemoveOldShards(bumpedSlab());
    }

    private void runRemoveOldShards(Allocator allocator)
    {
        NodeId id1 = NodeId.fromInt(1);
        NodeId id3 = NodeId.fromInt(3);
        NodeId id6 = NodeId.fromInt(6);
        List<NodeId.NodeIdRecord> records = new ArrayList<NodeId.NodeIdRecord>();
        records.add(new NodeId.NodeIdRecord(id1, 2L));
        records.add(new NodeId.NodeIdRecord(id3, 4L));
        records.add(new NodeId.NodeIdRecord(id6, 10L));

        ContextState ctx = ContextState.allocate(6, 3, allocator);
        ctx.writeElement(id1, 1L, 1L, true);
        ctx.writeElement(NodeId.fromInt(2), 2L, 2L);
        ctx.writeElement(id3, 3L, 3L, true);
        ctx.writeElement(NodeId.fromInt(4), 6L, 3L);
        ctx.writeElement(NodeId.fromInt(5), 7L, 3L, true);
        ctx.writeElement(id6, 5L, 6L);

        ByteBuffer merger = cc.computeOldShardMerger(ctx.context, records, Integer.MAX_VALUE);
        ByteBuffer merged = cc.merge(ctx.context, merger, allocator);
        assert cc.total(ctx.context) == cc.total(merged);

        ByteBuffer cleaned = cc.removeOldShards(merged, (int)(System.currentTimeMillis() / 1000) + 1);
        assert cc.total(ctx.context) == cc.total(cleaned);
        assert cleaned.remaining() == ctx.context.remaining() - stepLength - 2;
    }

    @Test
    public void testRemoveOldShardsNotAllExpiring()
    {
        runRemoveOldShardsNotAllExpiring(HeapAllocator.instance);
        runRemoveOldShardsNotAllExpiring(bumpedSlab());
    }

    private void runRemoveOldShardsNotAllExpiring(Allocator allocator)
    {
        NodeId id1 = NodeId.fromInt(1);
        NodeId id3 = NodeId.fromInt(3);
        NodeId id6 = NodeId.fromInt(6);
        List<NodeId.NodeIdRecord> records = new ArrayList<NodeId.NodeIdRecord>();
        records.add(new NodeId.NodeIdRecord(id1, 2L));
        records.add(new NodeId.NodeIdRecord(id3, 4L));
        records.add(new NodeId.NodeIdRecord(id6, 10L));

        ContextState ctx = ContextState.allocate(6, 3, allocator);
        ctx.writeElement(id1, 0L, 1L, true);
        ctx.writeElement(NodeId.fromInt(2), 0L, 2L);
        ctx.writeElement(id3, 0L, 3L, true);
        ctx.writeElement(NodeId.fromInt(4), 0L, 3L);
        ctx.writeElement(NodeId.fromInt(5), 0L, 3L, true);
        ctx.writeElement(id6, 0L, 6L);

        int timeFirstMerge = (int)(System.currentTimeMillis() / 1000);

        // First, only merge the first id
        ByteBuffer merger = cc.computeOldShardMerger(ctx.context, records, 3L);
        ByteBuffer merged = cc.merge(ctx.context, merger, allocator);
        assert cc.total(ctx.context) == cc.total(merged);

        try
        {
            Thread.sleep(2000);
        }
        catch (InterruptedException e)
        {
            throw new AssertionError();
        }

        // merge the second one
        ByteBuffer merger2 = cc.computeOldShardMerger(merged, records, 7L);
        ByteBuffer merged2 = cc.merge(merged, merger2, allocator);
        assert cc.total(ctx.context) == cc.total(merged2);

        ByteBuffer cleaned = cc.removeOldShards(merged2, timeFirstMerge + 1);
        assert cc.total(ctx.context) == cc.total(cleaned);
        assert cleaned.remaining() == ctx.context.remaining();

        // We should have cleaned id1 but not id3
        ContextState m = new ContextState(cleaned);
        m.moveToNext();
        assert m.getNodeId().equals(id3);

    }

    @Test
    public void testRemoveNotDeltaOldShards()
    {
        runRemoveNotDeltaOldShards(HeapAllocator.instance);
        runRemoveNotDeltaOldShards(bumpedSlab());
    }

    private void runRemoveNotDeltaOldShards(Allocator allocator)
    {
        ContextState ctx = ContextState.allocate(4, 1, allocator);
        ctx.writeElement(NodeId.fromInt(1), 1L, 1L, true);
        ctx.writeElement(NodeId.fromInt(2), -System.currentTimeMillis(), 0L);
        ctx.writeElement(NodeId.fromInt(3), -System.currentTimeMillis(), 0L);
        ctx.writeElement(NodeId.fromInt(4), -System.currentTimeMillis(), 0L);

        ByteBuffer cleaned = cc.removeOldShards(ctx.context, (int)(System.currentTimeMillis() / 1000) + 1);
        assert cc.total(ctx.context) == cc.total(cleaned);
        assert cleaned.remaining() == ctx.context.remaining() - 3 * stepLength;
    }
}
