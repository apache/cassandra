/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.db;

import java.security.MessageDigest;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.SimpleDenseCellNameType;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.*;

import static org.apache.cassandra.Util.cellname;
import static org.apache.cassandra.db.context.CounterContext.ContextState;

public class CounterCellTest
{
    private static final CounterContext cc = new CounterContext();

    private static final int idLength;
    private static final int clockLength;
    private static final int countLength;

    private static final int stepLength;

    static
    {
        idLength      = CounterId.LENGTH;
        clockLength   = 8; // size of long
        countLength   = 8; // size of long

        stepLength    = idLength + clockLength + countLength;
    }

    @Test
    public void testCreate()
    {
        long delta = 3L;
        CounterCell cell = new BufferCounterCell(Util.cellname("x"),
                                           CounterContext.instance().createLocal(delta),
                                           1L,
                                           Long.MIN_VALUE);

        Assert.assertEquals(delta, cell.total());
        Assert.assertEquals(1, cell.value().getShort(0));
        Assert.assertEquals(0, cell.value().getShort(2));
        Assert.assertTrue(CounterId.wrap(cell.value(), 4).isLocalId());
        Assert.assertEquals(1L, cell.value().getLong(4 + idLength));
        Assert.assertEquals(delta, cell.value().getLong(4 + idLength + clockLength));
    }

    @Test
    public void testReconcile()
    {
        Cell left;
        Cell right;
        Cell reconciled;

        ByteBuffer context;

        // tombstone + tombstone
        left  = new BufferDeletedCell(cellname("x"), 1, 1L);
        right = new BufferDeletedCell(cellname("x"), 2, 2L);

        assert left.reconcile(right).timestamp() == right.timestamp();
        assert right.reconcile(left).timestamp() == right.timestamp();

        // tombstone > live
        left  = new BufferDeletedCell(cellname("x"), 1, 2L);
        right = BufferCounterCell.createLocal(cellname("x"), 0L, 1L, Long.MIN_VALUE);

        assert left.reconcile(right) == left;

        // tombstone < live last delete
        left  = new BufferDeletedCell(cellname("x"), 1, 1L);
        right = BufferCounterCell.createLocal(cellname("x"), 0L, 4L, 2L);

        assert left.reconcile(right) == left;

        // tombstone == live last delete
        left  = new BufferDeletedCell(cellname("x"), 1, 2L);
        right = BufferCounterCell.createLocal(cellname("x"), 0L, 4L, 2L);

        assert left.reconcile(right) == left;

        // tombstone > live last delete
        left  = new BufferDeletedCell(cellname("x"), 1, 4L);
        right = BufferCounterCell.createLocal(cellname("x"), 0L, 9L, 1L);

        assert left.reconcile(right) == left;

        // live < tombstone
        left  = BufferCounterCell.createLocal(cellname("x"), 0L, 1L, Long.MIN_VALUE);
        right = new BufferDeletedCell(cellname("x"), 1, 2L);

        assert left.reconcile(right) == right;

        // live last delete > tombstone
        left  = BufferCounterCell.createLocal(cellname("x"), 0L, 4L, 2L);
        right = new BufferDeletedCell(cellname("x"), 1, 1L);

        assert left.reconcile(right) == right;

        // live last delete == tombstone
        left  = BufferCounterCell.createLocal(cellname("x"), 0L, 4L, 2L);
        right = new BufferDeletedCell(cellname("x"), 1, 2L);

        assert left.reconcile(right) == right;

        // live last delete < tombstone
        left  = BufferCounterCell.createLocal(cellname("x"), 0L, 9L, 1L);
        right = new BufferDeletedCell(cellname("x"), 1, 4L);

        assert left.reconcile(right) == right;

        // live < live last delete
        left  = new BufferCounterCell(cellname("x"), cc.createRemote(CounterId.fromInt(1), 2L, 3L), 1L, Long.MIN_VALUE);
        right = new BufferCounterCell(cellname("x"), cc.createRemote(CounterId.fromInt(1), 1L, 1L), 4L, 3L);

        assert left.reconcile(right) == right;

        // live last delete > live
        left  = new BufferCounterCell(cellname("x"), cc.createRemote(CounterId.fromInt(1), 2L, 3L), 6L, 5L);
        right = new BufferCounterCell(cellname("x"), cc.createRemote(CounterId.fromInt(1), 1L, 1L), 4L, 3L);

        assert left.reconcile(right) == left;

        // live + live
        left = new BufferCounterCell(cellname("x"), cc.createRemote(CounterId.fromInt(1), 1L, 1L), 4L, Long.MIN_VALUE);
        right = new BufferCounterCell(cellname("x"), cc.createRemote(CounterId.fromInt(1), 2L, 3L), 1L, Long.MIN_VALUE);

        reconciled = left.reconcile(right);
        assert reconciled.name().equals(left.name());
        assert ((CounterCell)reconciled).total() == 3L;
        assert reconciled.timestamp() == 4L;

        left = reconciled;
        right = new BufferCounterCell(cellname("x"), cc.createRemote(CounterId.fromInt(2), 1L, 5L), 2L, Long.MIN_VALUE);

        reconciled = left.reconcile(right);
        assert reconciled.name().equals(left.name());
        assert ((CounterCell)reconciled).total() == 8L;
        assert reconciled.timestamp() == 4L;

        left = reconciled;
        right = new BufferCounterCell(cellname("x"), cc.createRemote(CounterId.fromInt(2), 2L, 2L), 6L, Long.MIN_VALUE);

        reconciled = left.reconcile(right);
        assert reconciled.name().equals(left.name());
        assert ((CounterCell)reconciled).total() == 5L;
        assert reconciled.timestamp() == 6L;

        context = reconciled.value();
        int hd = 2; // header
        assert hd + 2 * stepLength == context.remaining();

        assert Util.equalsCounterId(CounterId.fromInt(1), context, hd);
        assert 2L == context.getLong(hd + idLength);
        assert 3L == context.getLong(hd + idLength + clockLength);

        assert Util.equalsCounterId(CounterId.fromInt(2), context, hd + stepLength);
        assert 2L == context.getLong(hd + stepLength + idLength);
        assert 2L == context.getLong(hd + stepLength + idLength + clockLength);

        assert ((CounterCell)reconciled).timestampOfLastDelete() == Long.MIN_VALUE;
    }

    @Test
    public void testDiff()
    {
        ContextState left;
        ContextState right;

        CounterCell leftCell;
        CounterCell rightCell;

        // timestamp
        leftCell = BufferCounterCell.createLocal(cellname("x"), 0, 1L, Long.MIN_VALUE);
        rightCell = BufferCounterCell.createLocal(cellname("x"), 0, 2L, Long.MIN_VALUE);

        assert rightCell == leftCell.diff(rightCell);
        assert null      == rightCell.diff(leftCell);

        // timestampOfLastDelete
        leftCell = BufferCounterCell.createLocal(cellname("x"), 0, 1L, 1L);
        rightCell = BufferCounterCell.createLocal(cellname("x"), 0, 1L, 2L);

        assert rightCell == leftCell.diff(rightCell);
        assert null      == rightCell.diff(leftCell);

        // equality: equal nodes, all counts same
        left = ContextState.allocate(0, 0, 3);
        left.writeRemote(CounterId.fromInt(3), 3L, 0L);
        left.writeRemote(CounterId.fromInt(6), 2L, 0L);
        left.writeRemote(CounterId.fromInt(9), 1L, 0L);
        right = ContextState.wrap(ByteBufferUtil.clone(left.context));

        leftCell  = new BufferCounterCell(cellname("x"), left.context,  1L);
        rightCell = new BufferCounterCell(cellname("x"), right.context, 1L);
        assert leftCell.diff(rightCell) == null;

        // greater than: left has superset of nodes (counts equal)
        left = ContextState.allocate(0, 0, 4);
        left.writeRemote(CounterId.fromInt(3), 3L, 0L);
        left.writeRemote(CounterId.fromInt(6), 2L, 0L);
        left.writeRemote(CounterId.fromInt(9), 1L, 0L);
        left.writeRemote(CounterId.fromInt(12), 0L, 0L);

        right = ContextState.allocate(0, 0, 3);
        right.writeRemote(CounterId.fromInt(3), 3L, 0L);
        right.writeRemote(CounterId.fromInt(6), 2L, 0L);
        right.writeRemote(CounterId.fromInt(9), 1L, 0L);

        leftCell  = new BufferCounterCell(cellname("x"), left.context,  1L);
        rightCell = new BufferCounterCell(cellname("x"), right.context, 1L);
        assert leftCell.diff(rightCell) == null;

        // less than: right has subset of nodes (counts equal)
        assert leftCell == rightCell.diff(leftCell);

        // disjoint: right and left have disjoint node sets
        left = ContextState.allocate(0, 0, 3);
        left.writeRemote(CounterId.fromInt(3), 1L, 0L);
        left.writeRemote(CounterId.fromInt(4), 1L, 0L);
        left.writeRemote(CounterId.fromInt(9), 1L, 0L);

        right = ContextState.allocate(0, 0, 3);
        right.writeRemote(CounterId.fromInt(3), 1L, 0L);
        right.writeRemote(CounterId.fromInt(6), 1L, 0L);
        right.writeRemote(CounterId.fromInt(9), 1L, 0L);

        leftCell  = new BufferCounterCell(cellname("x"), left.context,  1L);
        rightCell = new BufferCounterCell(cellname("x"), right.context, 1L);
        assert rightCell == leftCell.diff(rightCell);
        assert leftCell  == rightCell.diff(leftCell);
    }

    @Test
    public void testSerializeDeserialize() throws IOException
    {
        CounterContext.ContextState state = CounterContext.ContextState.allocate(0, 2, 2);
        state.writeRemote(CounterId.fromInt(1), 4L, 4L);
        state.writeLocal(CounterId.fromInt(2), 4L, 4L);
        state.writeRemote(CounterId.fromInt(3), 4L, 4L);
        state.writeLocal(CounterId.fromInt(4), 4L, 4L);

        CellNameType type = new SimpleDenseCellNameType(UTF8Type.instance);
        CounterCell original = new BufferCounterCell(cellname("x"), state.context, 1L);
        byte[] serialized;
        try (DataOutputBuffer bufOut = new DataOutputBuffer())
        {
            type.columnSerializer().serialize(original, bufOut);
            serialized = bufOut.getData();
        }


        ByteArrayInputStream bufIn = new ByteArrayInputStream(serialized, 0, serialized.length);
        CounterCell deserialized = (CounterCell) type.columnSerializer().deserialize(new DataInputStream(bufIn));
        Assert.assertEquals(original, deserialized);

        bufIn = new ByteArrayInputStream(serialized, 0, serialized.length);
        CounterCell deserializedOnRemote = (CounterCell) type.columnSerializer().deserialize(new DataInputStream(bufIn), ColumnSerializer.Flag.FROM_REMOTE);
        Assert.assertEquals(deserializedOnRemote.name(), original.name());
        Assert.assertEquals(deserializedOnRemote.total(), original.total());
        Assert.assertEquals(deserializedOnRemote.value(), cc.clearAllLocal(original.value()));
        Assert.assertEquals(deserializedOnRemote.timestamp(), deserialized.timestamp());
        Assert.assertEquals(deserializedOnRemote.timestampOfLastDelete(), deserialized.timestampOfLastDelete());
    }

    @Test
    public void testUpdateDigest() throws Exception
    {
        MessageDigest digest1 = MessageDigest.getInstance("md5");
        MessageDigest digest2 = MessageDigest.getInstance("md5");

        CounterContext.ContextState state = CounterContext.ContextState.allocate(0, 2, 2);
        state.writeRemote(CounterId.fromInt(1), 4L, 4L);
        state.writeLocal(CounterId.fromInt(2), 4L, 4L);
        state.writeRemote(CounterId.fromInt(3), 4L, 4L);
        state.writeLocal(CounterId.fromInt(4), 4L, 4L);

        CounterCell original = new BufferCounterCell(cellname("x"), state.context, 1L);
        CounterCell cleared = new BufferCounterCell(cellname("x"), cc.clearAllLocal(state.context), 1L);

        original.updateDigest(digest1);
        cleared.updateDigest(digest2);

        assert Arrays.equals(digest1.digest(), digest2.digest());
    }
}
