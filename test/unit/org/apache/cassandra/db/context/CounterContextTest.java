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

import java.nio.ByteBuffer;

import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.db.ClockAndCount;
import org.apache.cassandra.db.context.CounterContext.Relationship;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CounterId;

import static org.apache.cassandra.db.context.CounterContext.ContextState;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class CounterContextTest
{
    private static final CounterContext cc = new CounterContext();

    private static final int headerSizeLength = 2;
    private static final int headerEltLength = 2;
    private static final int idLength = 16;
    private static final int clockLength = 8;
    private static final int countLength = 8;
    private static final int stepLength = idLength + clockLength + countLength;

    @Test
    public void testAllocate()
    {
        ContextState allGlobal = ContextState.allocate(3, 0, 0);
        assertEquals(headerSizeLength + 3 * headerEltLength + 3 * stepLength, allGlobal.context.remaining());

        ContextState allLocal = ContextState.allocate(0, 3, 0);
        assertEquals(headerSizeLength + 3 * headerEltLength + 3 * stepLength, allLocal.context.remaining());

        ContextState allRemote = ContextState.allocate(0, 0, 3);
        assertEquals(headerSizeLength + 3 * stepLength, allRemote.context.remaining());

        ContextState mixed = ContextState.allocate(1, 1, 1);
        assertEquals(headerSizeLength + 2 * headerEltLength + 3 * stepLength, mixed.context.remaining());
    }

    @Test
    public void testDiff()
    {
        ContextState left;
        ContextState right;

        // equality: equal nodes, all counts same
        left = ContextState.allocate(0, 0, 3);
        left.writeRemote(CounterId.fromInt(3), 3L, 0L);
        left.writeRemote(CounterId.fromInt(6), 2L, 0L);
        left.writeRemote(CounterId.fromInt(9), 1L, 0L);
        right = ContextState.wrap(ByteBufferUtil.clone(left.context));

        assertEquals(Relationship.EQUAL, cc.diff(left.context, right.context));

        // greater than: left has superset of nodes (counts equal)
        left = ContextState.allocate(0, 0, 4);
        left.writeRemote(CounterId.fromInt(3),  3L, 0L);
        left.writeRemote(CounterId.fromInt(6),  2L, 0L);
        left.writeRemote(CounterId.fromInt(9),  1L, 0L);
        left.writeRemote(CounterId.fromInt(12), 0L, 0L);

        right = ContextState.allocate(0, 0, 3);
        right.writeRemote(CounterId.fromInt(3), 3L, 0L);
        right.writeRemote(CounterId.fromInt(6), 2L, 0L);
        right.writeRemote(CounterId.fromInt(9), 1L, 0L);

        assertEquals(Relationship.GREATER_THAN, cc.diff(left.context, right.context));

        // less than: left has subset of nodes (counts equal)
        left = ContextState.allocate(0, 0, 3);
        left.writeRemote(CounterId.fromInt(3), 3L, 0L);
        left.writeRemote(CounterId.fromInt(6), 2L, 0L);
        left.writeRemote(CounterId.fromInt(9), 1L, 0L);

        right = ContextState.allocate(0, 0, 4);
        right.writeRemote(CounterId.fromInt(3),  3L, 0L);
        right.writeRemote(CounterId.fromInt(6),  2L, 0L);
        right.writeRemote(CounterId.fromInt(9),  1L, 0L);
        right.writeRemote(CounterId.fromInt(12), 0L, 0L);

        assertEquals(Relationship.LESS_THAN, cc.diff(left.context, right.context));

        // greater than: equal nodes, but left has higher counts
        left = ContextState.allocate(0, 0, 3);
        left.writeRemote(CounterId.fromInt(3), 3L, 0L);
        left.writeRemote(CounterId.fromInt(6), 2L, 0L);
        left.writeRemote(CounterId.fromInt(9), 3L, 0L);

        right = ContextState.allocate(0, 0, 3);
        right.writeRemote(CounterId.fromInt(3), 3L, 0L);
        right.writeRemote(CounterId.fromInt(6), 2L, 0L);
        right.writeRemote(CounterId.fromInt(9), 1L, 0L);

        assertEquals(Relationship.GREATER_THAN, cc.diff(left.context, right.context));

        // less than: equal nodes, but right has higher counts
        left = ContextState.allocate(0, 0, 3);
        left.writeRemote(CounterId.fromInt(3), 3L, 0L);
        left.writeRemote(CounterId.fromInt(6), 2L, 0L);
        left.writeRemote(CounterId.fromInt(9), 3L, 0L);

        right = ContextState.allocate(0, 0, 3);
        right.writeRemote(CounterId.fromInt(3), 3L, 0L);
        right.writeRemote(CounterId.fromInt(6), 9L, 0L);
        right.writeRemote(CounterId.fromInt(9), 3L, 0L);

        assertEquals(Relationship.LESS_THAN, cc.diff(left.context, right.context));

        // disjoint: right and left have disjoint node sets
        left = ContextState.allocate(0, 0, 3);
        left.writeRemote(CounterId.fromInt(3), 1L, 0L);
        left.writeRemote(CounterId.fromInt(4), 1L, 0L);
        left.writeRemote(CounterId.fromInt(9), 1L, 0L);

        right = ContextState.allocate(0, 0, 3);
        right.writeRemote(CounterId.fromInt(3), 1L, 0L);
        right.writeRemote(CounterId.fromInt(6), 1L, 0L);
        right.writeRemote(CounterId.fromInt(9), 1L, 0L);

        assertEquals(Relationship.DISJOINT, cc.diff(left.context, right.context));

        left = ContextState.allocate(0, 0, 3);
        left.writeRemote(CounterId.fromInt(3), 1L, 0L);
        left.writeRemote(CounterId.fromInt(4), 1L, 0L);
        left.writeRemote(CounterId.fromInt(9), 1L, 0L);

        right = ContextState.allocate(0, 0, 3);
        right.writeRemote(CounterId.fromInt(2),  1L, 0L);
        right.writeRemote(CounterId.fromInt(6),  1L, 0L);
        right.writeRemote(CounterId.fromInt(12), 1L, 0L);

        assertEquals(Relationship.DISJOINT, cc.diff(left.context, right.context));

        // disjoint: equal nodes, but right and left have higher counts in differing nodes
        left = ContextState.allocate(0, 0, 3);
        left.writeRemote(CounterId.fromInt(3), 1L, 0L);
        left.writeRemote(CounterId.fromInt(6), 3L, 0L);
        left.writeRemote(CounterId.fromInt(9), 1L, 0L);

        right = ContextState.allocate(0, 0, 3);
        right.writeRemote(CounterId.fromInt(3), 1L, 0L);
        right.writeRemote(CounterId.fromInt(6), 1L, 0L);
        right.writeRemote(CounterId.fromInt(9), 5L, 0L);

        assertEquals(Relationship.DISJOINT, cc.diff(left.context, right.context));

        left = ContextState.allocate(0, 0, 3);
        left.writeRemote(CounterId.fromInt(3), 2L, 0L);
        left.writeRemote(CounterId.fromInt(6), 3L, 0L);
        left.writeRemote(CounterId.fromInt(9), 1L, 0L);

        right = ContextState.allocate(0, 0, 3);
        right.writeRemote(CounterId.fromInt(3), 1L, 0L);
        right.writeRemote(CounterId.fromInt(6), 9L, 0L);
        right.writeRemote(CounterId.fromInt(9), 5L, 0L);

        assertEquals(Relationship.DISJOINT, cc.diff(left.context, right.context));

        // disjoint: left has more nodes, but lower counts
        left = ContextState.allocate(0, 0, 4);
        left.writeRemote(CounterId.fromInt(3),  2L, 0L);
        left.writeRemote(CounterId.fromInt(6),  3L, 0L);
        left.writeRemote(CounterId.fromInt(9),  1L, 0L);
        left.writeRemote(CounterId.fromInt(12), 1L, 0L);

        right = ContextState.allocate(0, 0, 3);
        right.writeRemote(CounterId.fromInt(3), 4L, 0L);
        right.writeRemote(CounterId.fromInt(6), 9L, 0L);
        right.writeRemote(CounterId.fromInt(9), 5L, 0L);

        assertEquals(Relationship.DISJOINT, cc.diff(left.context, right.context));

        // disjoint: left has less nodes, but higher counts
        left = ContextState.allocate(0, 0, 3);
        left.writeRemote(CounterId.fromInt(3), 5L, 0L);
        left.writeRemote(CounterId.fromInt(6), 3L, 0L);
        left.writeRemote(CounterId.fromInt(9), 2L, 0L);

        right = ContextState.allocate(0, 0, 4);
        right.writeRemote(CounterId.fromInt(3),  4L, 0L);
        right.writeRemote(CounterId.fromInt(6),  3L, 0L);
        right.writeRemote(CounterId.fromInt(9),  2L, 0L);
        right.writeRemote(CounterId.fromInt(12), 1L, 0L);

        assertEquals(Relationship.DISJOINT, cc.diff(left.context, right.context));

        // disjoint: mixed nodes and counts
        left = ContextState.allocate(0, 0, 3);
        left.writeRemote(CounterId.fromInt(3), 5L, 0L);
        left.writeRemote(CounterId.fromInt(6), 2L, 0L);
        left.writeRemote(CounterId.fromInt(9), 2L, 0L);

        right = ContextState.allocate(0, 0, 4);
        right.writeRemote(CounterId.fromInt(3),  4L, 0L);
        right.writeRemote(CounterId.fromInt(6),  3L, 0L);
        right.writeRemote(CounterId.fromInt(9),  2L, 0L);
        right.writeRemote(CounterId.fromInt(12), 1L, 0L);

        assertEquals(Relationship.DISJOINT, cc.diff(left.context, right.context));

        left = ContextState.allocate(0, 0, 4);
        left.writeRemote(CounterId.fromInt(3), 5L, 0L);
        left.writeRemote(CounterId.fromInt(6), 2L, 0L);
        left.writeRemote(CounterId.fromInt(7), 2L, 0L);
        left.writeRemote(CounterId.fromInt(9), 2L, 0L);

        right = ContextState.allocate(0, 0, 3);
        right.writeRemote(CounterId.fromInt(3), 4L, 0L);
        right.writeRemote(CounterId.fromInt(6), 3L, 0L);
        right.writeRemote(CounterId.fromInt(9), 2L, 0L);

        assertEquals(Relationship.DISJOINT, cc.diff(left.context, right.context));
    }

    @Test
    public void testMerge()
    {
        // note: local counts aggregated; remote counts are reconciled (i.e. take max)
        ContextState left = ContextState.allocate(0, 1, 3);
        left.writeRemote(CounterId.fromInt(1), 1L, 1L);
        left.writeRemote(CounterId.fromInt(2), 2L, 2L);
        left.writeRemote(CounterId.fromInt(4), 6L, 3L);
        left.writeLocal(CounterId.getLocalId(), 7L, 3L);

        ContextState right = ContextState.allocate(0, 1, 2);
        right.writeRemote(CounterId.fromInt(4), 4L, 4L);
        right.writeRemote(CounterId.fromInt(5), 5L, 5L);
        right.writeLocal(CounterId.getLocalId(), 2L, 9L);

        ByteBuffer merged = cc.merge(left.context, right.context);
        int hd = 4;

        assertEquals(hd + 5 * stepLength, merged.remaining());
        // local node id's counts are aggregated
        assertTrue(Util.equalsCounterId(CounterId.getLocalId(), merged, hd + 4 * stepLength));
        assertEquals(9L, merged.getLong(merged.position() + hd + 4 * stepLength + idLength));
        assertEquals(12L,  merged.getLong(merged.position() + hd + 4*stepLength + idLength + clockLength));

        // remote node id counts are reconciled (i.e. take max)
        assertTrue(Util.equalsCounterId(CounterId.fromInt(4), merged, hd + 2 * stepLength));
        assertEquals(6L, merged.getLong(merged.position() + hd + 2 * stepLength + idLength));
        assertEquals( 3L,  merged.getLong(merged.position() + hd + 2*stepLength + idLength + clockLength));

        assertTrue(Util.equalsCounterId(CounterId.fromInt(5), merged, hd + 3 * stepLength));
        assertEquals(5L, merged.getLong(merged.position() + hd + 3 * stepLength + idLength));
        assertEquals( 5L,  merged.getLong(merged.position() + hd + 3*stepLength + idLength + clockLength));

        assertTrue(Util.equalsCounterId(CounterId.fromInt(2), merged, hd + stepLength));
        assertEquals(2L, merged.getLong(merged.position() + hd + stepLength + idLength));
        assertEquals( 2L,  merged.getLong(merged.position() + hd + stepLength + idLength + clockLength));

        assertTrue(Util.equalsCounterId(CounterId.fromInt(1), merged, hd));
        assertEquals( 1L,  merged.getLong(merged.position() + hd + idLength));
        assertEquals( 1L,  merged.getLong(merged.position() + hd + idLength + clockLength));

        //
        // Test merging two exclusively global contexts
        //
        left = ContextState.allocate(3, 0, 0);
        left.writeGlobal(CounterId.fromInt(1), 1L, 1L);
        left.writeGlobal(CounterId.fromInt(2), 2L, 2L);
        left.writeGlobal(CounterId.fromInt(3), 3L, 3L);

        right = ContextState.allocate(3, 0, 0);
        right.writeGlobal(CounterId.fromInt(3), 6L, 6L);
        right.writeGlobal(CounterId.fromInt(4), 4L, 4L);
        right.writeGlobal(CounterId.fromInt(5), 5L, 5L);

        merged = cc.merge(left.context, right.context);
        assertEquals(headerSizeLength + 5 * headerEltLength + 5 * stepLength, merged.remaining());
        assertEquals(18L, cc.total(merged));
        assertEquals(5, merged.getShort(merged.position()));

        int headerLength = headerSizeLength + 5 * headerEltLength;
        assertTrue(Util.equalsCounterId(CounterId.fromInt(1), merged, headerLength));
        assertEquals(1L, merged.getLong(merged.position() + headerLength + idLength));
        assertEquals(1L, merged.getLong(merged.position() + headerLength + idLength + clockLength));
        assertTrue(Util.equalsCounterId(CounterId.fromInt(2), merged, headerLength + stepLength));
        assertEquals(2L, merged.getLong(merged.position() + headerLength + stepLength + idLength));
        assertEquals(2L, merged.getLong(merged.position() + headerLength + stepLength + idLength + clockLength));
        // pick the global shard with the largest clock
        assertTrue(Util.equalsCounterId(CounterId.fromInt(3), merged, headerLength + 2 * stepLength));
        assertEquals(6L, merged.getLong(merged.position() + headerLength + 2 * stepLength + idLength));
        assertEquals(6L, merged.getLong(merged.position() + headerLength + 2 * stepLength + idLength + clockLength));
        assertTrue(Util.equalsCounterId(CounterId.fromInt(4), merged, headerLength + 3 * stepLength));
        assertEquals(4L, merged.getLong(merged.position() + headerLength + 3 * stepLength + idLength));
        assertEquals(4L, merged.getLong(merged.position() + headerLength + 3 * stepLength + idLength + clockLength));
        assertTrue(Util.equalsCounterId(CounterId.fromInt(5), merged, headerLength + 4 * stepLength));
        assertEquals(5L, merged.getLong(merged.position() + headerLength + 4 * stepLength + idLength));
        assertEquals(5L, merged.getLong(merged.position() + headerLength + 4 * stepLength + idLength + clockLength));

        //
        // Test merging two global contexts w/ 'invalid shards'
        //
        left = ContextState.allocate(1, 0, 0);
        left.writeGlobal(CounterId.fromInt(1), 10L, 20L);

        right = ContextState.allocate(1, 0, 0);
        right.writeGlobal(CounterId.fromInt(1), 10L, 30L);

        merged = cc.merge(left.context, right.context);
        headerLength = headerSizeLength + headerEltLength;
        assertEquals(headerLength + stepLength, merged.remaining());
        assertEquals(30L, cc.total(merged));
        assertEquals(1, merged.getShort(merged.position()));
        assertTrue(Util.equalsCounterId(CounterId.fromInt(1), merged, headerLength));
        assertEquals(10L, merged.getLong(merged.position() + headerLength + idLength));
        // with equal clock, we should pick the largest value
        assertEquals(30L, merged.getLong(merged.position() + headerLength + idLength + clockLength));

        //
        // Test merging global w/ mixed contexts
        //
        left = ContextState.allocate(2, 0, 0);
        left.writeGlobal(CounterId.fromInt(1), 1L, 1L);
        left.writeGlobal(CounterId.fromInt(2), 1L, 1L);

        right = ContextState.allocate(0, 1, 1);
        right.writeLocal(CounterId.fromInt(1), 100L, 100L);
        right.writeRemote(CounterId.fromInt(2), 100L, 100L);

        // global shards should dominate local/remote, even with lower clock and value
        merged = cc.merge(left.context, right.context);
        headerLength = headerSizeLength + 2 * headerEltLength;
        assertEquals(headerLength + 2 * stepLength, merged.remaining());
        assertEquals(2L, cc.total(merged));
        assertEquals(2, merged.getShort(merged.position()));
        assertTrue(Util.equalsCounterId(CounterId.fromInt(1), merged, headerLength));
        assertEquals(1L, merged.getLong(merged.position() + headerLength + idLength));
        assertEquals(1L, merged.getLong(merged.position() + headerLength + idLength + clockLength));
        assertTrue(Util.equalsCounterId(CounterId.fromInt(2), merged, headerLength + stepLength));
        assertEquals(1L, merged.getLong(merged.position() + headerLength + stepLength + idLength));
        assertEquals(1L, merged.getLong(merged.position() + headerLength + stepLength + idLength + clockLength));
    }

    @Test
    public void testTotal()
    {
        ContextState mixed = ContextState.allocate(0, 1, 4);
        mixed.writeRemote(CounterId.fromInt(1), 1L, 1L);
        mixed.writeRemote(CounterId.fromInt(2), 2L, 2L);
        mixed.writeRemote(CounterId.fromInt(4), 4L, 4L);
        mixed.writeRemote(CounterId.fromInt(5), 5L, 5L);
        mixed.writeLocal(CounterId.getLocalId(), 12L, 12L);
        assertEquals(24L, cc.total(mixed.context));

        ContextState global = ContextState.allocate(3, 0, 0);
        global.writeGlobal(CounterId.fromInt(1), 1L, 1L);
        global.writeGlobal(CounterId.fromInt(2), 2L, 2L);
        global.writeGlobal(CounterId.fromInt(3), 3L, 3L);
        assertEquals(6L, cc.total(global.context));
    }

    @Test
    public void testClearLocal()
    {
        ContextState state;
        ByteBuffer marked;
        ByteBuffer cleared;

        // mark/clear for remote-only contexts is a no-op
        state = ContextState.allocate(0, 0, 1);
        state.writeRemote(CounterId.fromInt(1), 1L, 1L);

        assertFalse(cc.shouldClearLocal(state.context));
        marked = cc.markLocalToBeCleared(state.context);
        assertEquals(0, marked.getShort(marked.position()));
        assertSame(state.context, marked); // should return the original context

        cleared = cc.clearAllLocal(marked);
        assertSame(cleared, marked); // shouldn't alter anything either

        // a single local shard
        state = ContextState.allocate(0, 1, 0);
        state.writeLocal(CounterId.fromInt(1), 1L, 1L);

        assertFalse(cc.shouldClearLocal(state.context));
        marked = cc.markLocalToBeCleared(state.context);
        assertTrue(cc.shouldClearLocal(marked));
        assertEquals(-1, marked.getShort(marked.position()));
        assertNotSame(state.context, marked); // shouldn't alter in place, as it used to do

        cleared = cc.clearAllLocal(marked);
        assertFalse(cc.shouldClearLocal(cleared));
        assertEquals(0, cleared.getShort(cleared.position()));

        // 2 global + 1 local shard
        state = ContextState.allocate(2, 1, 0);
        state.writeLocal(CounterId.fromInt(1), 1L, 1L);
        state.writeGlobal(CounterId.fromInt(2), 2L, 2L);
        state.writeGlobal(CounterId.fromInt(3), 3L, 3L);

        assertFalse(cc.shouldClearLocal(state.context));
        marked = cc.markLocalToBeCleared(state.context);
        assertTrue(cc.shouldClearLocal(marked));

        assertEquals(-3, marked.getShort(marked.position()));
        assertEquals(0, marked.getShort(marked.position() + headerSizeLength));
        assertEquals(Short.MIN_VALUE + 1, marked.getShort(marked.position() + headerSizeLength + headerEltLength));
        assertEquals(Short.MIN_VALUE + 2, marked.getShort(marked.position() + headerSizeLength + 2 * headerEltLength));

        int headerLength = headerSizeLength + 3 * headerEltLength;
        assertTrue(Util.equalsCounterId(CounterId.fromInt(1), marked, headerLength));
        assertEquals(1L, marked.getLong(marked.position() + headerLength + idLength));
        assertEquals(1L, marked.getLong(marked.position() + headerLength + idLength + clockLength));

        assertTrue(Util.equalsCounterId(CounterId.fromInt(2), marked, headerLength + stepLength));
        assertEquals(2L, marked.getLong(marked.position() + headerLength + stepLength + idLength));
        assertEquals(2L, marked.getLong(marked.position() + headerLength + stepLength + idLength + clockLength));

        assertTrue(Util.equalsCounterId(CounterId.fromInt(3), marked, headerLength + 2 * stepLength));
        assertEquals(3L, marked.getLong(marked.position() + headerLength + 2 * stepLength + idLength));
        assertEquals(3L, marked.getLong(marked.position() + headerLength + 2 * stepLength + idLength + clockLength));

        cleared = cc.clearAllLocal(marked);
        assertFalse(cc.shouldClearLocal(cleared));

        assertEquals(2, cleared.getShort(cleared.position())); // 2 global shards
        assertEquals(Short.MIN_VALUE + 1, cleared.getShort(marked.position() + headerEltLength));
        assertEquals(Short.MIN_VALUE + 2, cleared.getShort(marked.position() + headerSizeLength + headerEltLength));

        headerLength = headerSizeLength + 2 * headerEltLength;
        assertTrue(Util.equalsCounterId(CounterId.fromInt(1), cleared, headerLength));
        assertEquals(1L, cleared.getLong(cleared.position() + headerLength + idLength));
        assertEquals(1L, cleared.getLong(cleared.position() + headerLength + idLength + clockLength));

        assertTrue(Util.equalsCounterId(CounterId.fromInt(2), cleared, headerLength + stepLength));
        assertEquals(2L, cleared.getLong(cleared.position() + headerLength + stepLength + idLength));
        assertEquals(2L, cleared.getLong(cleared.position() + headerLength + stepLength + idLength + clockLength));

        assertTrue(Util.equalsCounterId(CounterId.fromInt(3), cleared, headerLength + 2 * stepLength));
        assertEquals(3L, cleared.getLong(cleared.position() + headerLength + 2 * stepLength + idLength));
        assertEquals(3L, cleared.getLong(cleared.position() + headerLength + 2 * stepLength + idLength + clockLength));

        // a single global shard - no-op
        state = ContextState.allocate(1, 0, 0);
        state.writeGlobal(CounterId.fromInt(1), 1L, 1L);

        assertFalse(cc.shouldClearLocal(state.context));
        marked = cc.markLocalToBeCleared(state.context);
        assertEquals(1, marked.getShort(marked.position()));
        assertSame(state.context, marked);

        cleared = cc.clearAllLocal(marked);
        assertSame(cleared, marked);
    }

    @Test
    public void testFindPositionOf()
    {
        ContextState state = ContextState.allocate(3, 3, 3);

        state.writeGlobal(CounterId.fromInt(1), 1L, 1L);
        state.writeRemote(CounterId.fromInt(2), 2L, 2L);
        state.writeLocal( CounterId.fromInt(3), 3L, 3L);
        state.writeGlobal(CounterId.fromInt(4), 4L, 4L);
        state.writeRemote(CounterId.fromInt(5), 5L, 5L);
        state.writeLocal( CounterId.fromInt(6), 6L, 6L);
        state.writeGlobal(CounterId.fromInt(7), 7L, 7L);
        state.writeRemote(CounterId.fromInt(8), 8L, 8L);
        state.writeLocal(CounterId.fromInt(9), 9L, 9L);

        int headerLength = headerSizeLength + 6 * headerEltLength;
        assertEquals(headerLength, cc.findPositionOf(state.context, CounterId.fromInt(1)));
        assertEquals(headerLength + stepLength, cc.findPositionOf(state.context, CounterId.fromInt(2)));
        assertEquals(headerLength + 2 * stepLength, cc.findPositionOf(state.context, CounterId.fromInt(3)));
        assertEquals(headerLength + 3 * stepLength, cc.findPositionOf(state.context, CounterId.fromInt(4)));
        assertEquals(headerLength + 4 * stepLength, cc.findPositionOf(state.context, CounterId.fromInt(5)));
        assertEquals(headerLength + 5 * stepLength, cc.findPositionOf(state.context, CounterId.fromInt(6)));
        assertEquals(headerLength + 6 * stepLength, cc.findPositionOf(state.context, CounterId.fromInt(7)));
        assertEquals(headerLength + 7 * stepLength, cc.findPositionOf(state.context, CounterId.fromInt(8)));
        assertEquals(headerLength + 8 * stepLength, cc.findPositionOf(state.context, CounterId.fromInt(9)));

        assertEquals(-1, cc.findPositionOf(state.context, CounterId.fromInt(0)));
        assertEquals(-1, cc.findPositionOf(state.context, CounterId.fromInt(10)));
        assertEquals(-1, cc.findPositionOf(state.context, CounterId.fromInt(15)));
        assertEquals(-1, cc.findPositionOf(state.context, CounterId.fromInt(20)));
    }

    @Test
    public void testGetGlockAndCountOf()
    {
        ContextState state = ContextState.allocate(3, 3, 3);

        state.writeGlobal(CounterId.fromInt(1), 1L, 1L);
        state.writeRemote(CounterId.fromInt(2), 2L, 2L);
        state.writeLocal( CounterId.fromInt(3), 3L, 3L);
        state.writeGlobal(CounterId.fromInt(4), 4L, 4L);
        state.writeRemote(CounterId.fromInt(5), 5L, 5L);
        state.writeLocal( CounterId.fromInt(6), 6L, 6L);
        state.writeGlobal(CounterId.fromInt(7), 7L, 7L);
        state.writeRemote(CounterId.fromInt(8), 8L, 8L);
        state.writeLocal(CounterId.fromInt(9), 9L, 9L);

        assertEquals(ClockAndCount.create(1L, 1L), cc.getClockAndCountOf(state.context, CounterId.fromInt(1)));
        assertEquals(ClockAndCount.create(2L, 2L), cc.getClockAndCountOf(state.context, CounterId.fromInt(2)));
        assertEquals(ClockAndCount.create(3L, 3L), cc.getClockAndCountOf(state.context, CounterId.fromInt(3)));
        assertEquals(ClockAndCount.create(4L, 4L), cc.getClockAndCountOf(state.context, CounterId.fromInt(4)));
        assertEquals(ClockAndCount.create(5L, 5L), cc.getClockAndCountOf(state.context, CounterId.fromInt(5)));
        assertEquals(ClockAndCount.create(6L, 6L), cc.getClockAndCountOf(state.context, CounterId.fromInt(6)));
        assertEquals(ClockAndCount.create(7L, 7L), cc.getClockAndCountOf(state.context, CounterId.fromInt(7)));
        assertEquals(ClockAndCount.create(8L, 8L), cc.getClockAndCountOf(state.context, CounterId.fromInt(8)));
        assertEquals(ClockAndCount.create(9L, 9L), cc.getClockAndCountOf(state.context, CounterId.fromInt(9)));

        assertEquals(ClockAndCount.create(0L, 0L), cc.getClockAndCountOf(state.context, CounterId.fromInt(0)));
        assertEquals(ClockAndCount.create(0L, 0L), cc.getClockAndCountOf(state.context, CounterId.fromInt(10)));
        assertEquals(ClockAndCount.create(0L, 0L), cc.getClockAndCountOf(state.context, CounterId.fromInt(15)));
        assertEquals(ClockAndCount.create(0L, 0L), cc.getClockAndCountOf(state.context, CounterId.fromInt(20)));
    }
}
