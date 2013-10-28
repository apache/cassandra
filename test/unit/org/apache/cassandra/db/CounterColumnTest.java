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

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.context.CounterContext;
import static org.apache.cassandra.db.context.CounterContext.ContextState;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.*;

public class CounterColumnTest extends SchemaLoader
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
        CounterUpdateColumn cuc = new CounterUpdateColumn(ByteBufferUtil.bytes("x"), delta, 1L);
        CounterColumn column = cuc.localCopy(Keyspace.open("Keyspace5").getColumnFamilyStore("Counter1"));

        Assert.assertEquals(delta, column.total());
        Assert.assertEquals(1, column.value().getShort(0));
        Assert.assertEquals(0, column.value().getShort(2));
        Assert.assertTrue(CounterId.wrap(column.value(), 4).isLocalId());
        Assert.assertEquals(1L, column.value().getLong(4 + 0*stepLength + idLength));
        Assert.assertEquals(delta, column.value().getLong(4 + 0*stepLength + idLength + clockLength));
    }

    @Test
    public void testReconcile()
    {
        Column left;
        Column right;
        Column reconciled;

        ByteBuffer context;

        // tombstone + tombstone
        left  = new DeletedColumn(ByteBufferUtil.bytes("x"), 1, 1L);
        right = new DeletedColumn(ByteBufferUtil.bytes("x"), 2, 2L);

        Assert.assertEquals(left.reconcile(right).getMarkedForDeleteAt(), right.getMarkedForDeleteAt());
        Assert.assertEquals(right.reconcile(left).getMarkedForDeleteAt(), right.getMarkedForDeleteAt());

        // tombstone > live
        left  = new DeletedColumn(ByteBufferUtil.bytes("x"), 1, 2L);
        right = new CounterColumn(ByteBufferUtil.bytes("x"), 0L, 1L);

        Assert.assertEquals(left.reconcile(right), left);

        // tombstone < live last delete
        left  = new DeletedColumn(ByteBufferUtil.bytes("x"), 1, 1L);
        right = new CounterColumn(ByteBufferUtil.bytes("x"), 0L, 4L, 2L);

        Assert.assertEquals(left.reconcile(right), right);

        // tombstone == live last delete
        left  = new DeletedColumn(ByteBufferUtil.bytes("x"), 1, 2L);
        right = new CounterColumn(ByteBufferUtil.bytes("x"), 0L, 4L, 2L);

        Assert.assertEquals(left.reconcile(right), right);

        // tombstone > live last delete
        left  = new DeletedColumn(ByteBufferUtil.bytes("x"), 1, 4L);
        right = new CounterColumn(ByteBufferUtil.bytes("x"), 0L, 9L, 1L);

        reconciled = left.reconcile(right);
        Assert.assertEquals(reconciled.name(), right.name());
        Assert.assertEquals(reconciled.value(), right.value());
        Assert.assertEquals(reconciled.timestamp(), right.timestamp());
        Assert.assertEquals(((CounterColumn)reconciled).timestampOfLastDelete(), left.getMarkedForDeleteAt());

        // live < tombstone
        left  = new CounterColumn(ByteBufferUtil.bytes("x"), 0L, 1L);
        right = new DeletedColumn(ByteBufferUtil.bytes("x"), 1, 2L);

        Assert.assertEquals(left.reconcile(right), right);

        // live last delete > tombstone
        left  = new CounterColumn(ByteBufferUtil.bytes("x"), 0L, 4L, 2L);
        right = new DeletedColumn(ByteBufferUtil.bytes("x"), 1, 1L);

        Assert.assertEquals(left.reconcile(right), left);

        // live last delete == tombstone
        left  = new CounterColumn(ByteBufferUtil.bytes("x"), 0L, 4L, 2L);
        right = new DeletedColumn(ByteBufferUtil.bytes("x"), 1, 2L);

        Assert.assertEquals(left.reconcile(right), left);

        // live last delete < tombstone
        left  = new CounterColumn(ByteBufferUtil.bytes("x"), 0L, 9L, 1L);
        right = new DeletedColumn(ByteBufferUtil.bytes("x"), 1, 4L);

        reconciled = left.reconcile(right);
        Assert.assertEquals(reconciled.name(), left.name());
        Assert.assertEquals(reconciled.value(), left.value());
        Assert.assertEquals(reconciled.timestamp(), left.timestamp());
        Assert.assertEquals(((CounterColumn)reconciled).timestampOfLastDelete(), right.getMarkedForDeleteAt());

        // live < live last delete
        left  = new CounterColumn(ByteBufferUtil.bytes("x"), cc.create(CounterId.fromInt(1), 2L, 3L, false), 1L, Long.MIN_VALUE);
        right = new CounterColumn(ByteBufferUtil.bytes("x"), cc.create(CounterId.fromInt(1), 1L, 1L, false), 4L, 3L);

        Assert.assertEquals(left.reconcile(right), right);

        // live last delete > live
        left  = new CounterColumn(ByteBufferUtil.bytes("x"), cc.create(CounterId.fromInt(1), 2L, 3L, false), 6L, 5L);
        right = new CounterColumn(ByteBufferUtil.bytes("x"), cc.create(CounterId.fromInt(1), 1L, 1L, false), 4L, 3L);

        Assert.assertEquals(left.reconcile(right), left);

        // live + live
        left = new CounterColumn(ByteBufferUtil.bytes("x"), cc.create(CounterId.fromInt(1), 1L, 1L, false), 4L, Long.MIN_VALUE);
        right = new CounterColumn(ByteBufferUtil.bytes("x"), cc.create(CounterId.fromInt(1), 2L, 3L, false), 1L, Long.MIN_VALUE);

        reconciled = left.reconcile(right);
        Assert.assertEquals(reconciled.name(), left.name());
        Assert.assertEquals(3L, ((CounterColumn)reconciled).total());
        Assert.assertEquals(4L, reconciled.timestamp());

        left = reconciled;
        right = new CounterColumn(ByteBufferUtil.bytes("x"), cc.create(CounterId.fromInt(2), 1L, 5L, false), 2L, Long.MIN_VALUE);

        reconciled = left.reconcile(right);
        Assert.assertEquals(reconciled.name(), left.name());
        Assert.assertEquals(8L, ((CounterColumn)reconciled).total());
        Assert.assertEquals(4L, reconciled.timestamp());

        left = reconciled;
        right = new CounterColumn(ByteBufferUtil.bytes("x"), cc.create(CounterId.fromInt(2), 2L, 2L, false), 6L, Long.MIN_VALUE);

        reconciled = left.reconcile(right);
        Assert.assertEquals(reconciled.name(), left.name());
        Assert.assertEquals(5L, ((CounterColumn)reconciled).total());
        Assert.assertEquals(6L, reconciled.timestamp());

        context = reconciled.value();
        int hd = 2; // header
        Assert.assertEquals(hd + 2 * stepLength, context.remaining());

        Assert.assertTrue(Util.equalsCounterId(CounterId.fromInt(1), context, hd + 0 * stepLength));
        Assert.assertEquals(2L, context.getLong(hd + 0*stepLength + idLength));
        Assert.assertEquals(3L, context.getLong(hd + 0*stepLength + idLength + clockLength));

        Assert.assertTrue(Util.equalsCounterId(CounterId.fromInt(2), context, hd + 1 * stepLength));
        Assert.assertEquals(2L, context.getLong(hd + 1*stepLength + idLength));
        Assert.assertEquals(2L, context.getLong(hd + 1*stepLength + idLength + clockLength));

        Assert.assertEquals(Long.MIN_VALUE, ((CounterColumn)reconciled).timestampOfLastDelete());
    }

    @Test
    public void testDiff()
    {
        Allocator allocator = HeapAllocator.instance;
        ContextState left;
        ContextState right;

        CounterColumn leftCol;
        CounterColumn rightCol;

        // timestamp
        leftCol = new CounterColumn(ByteBufferUtil.bytes("x"), 0, 1L);
        rightCol = new CounterColumn(ByteBufferUtil.bytes("x"), 0, 2L);

        Assert.assertEquals(rightCol, leftCol.diff(rightCol));
        Assert.assertNull(rightCol.diff(leftCol));

        // timestampOfLastDelete
        leftCol = new CounterColumn(ByteBufferUtil.bytes("x"), 0, 1L, 1L);
        rightCol = new CounterColumn(ByteBufferUtil.bytes("x"), 0, 1L, 2L);

        Assert.assertEquals(rightCol, leftCol.diff(rightCol));
        Assert.assertNull(rightCol.diff(leftCol));

        // equality: equal nodes, all counts same
        left = ContextState.allocate(3, 0, allocator);
        left.writeElement(CounterId.fromInt(3), 3L, 0L);
        left.writeElement(CounterId.fromInt(6), 2L, 0L);
        left.writeElement(CounterId.fromInt(9), 1L, 0L);
        right = new ContextState(ByteBufferUtil.clone(left.context), 2);

        leftCol  = new CounterColumn(ByteBufferUtil.bytes("x"), left.context,  1L);
        rightCol = new CounterColumn(ByteBufferUtil.bytes("x"), right.context, 1L);
        Assert.assertNull(leftCol.diff(rightCol));

        // greater than: left has superset of nodes (counts equal)
        left = ContextState.allocate(4, 0, allocator);
        left.writeElement(CounterId.fromInt(3), 3L, 0L);
        left.writeElement(CounterId.fromInt(6), 2L, 0L);
        left.writeElement(CounterId.fromInt(9), 1L, 0L);
        left.writeElement(CounterId.fromInt(12), 0L, 0L);

        right = ContextState.allocate(3, 0, allocator);
        right.writeElement(CounterId.fromInt(3), 3L, 0L);
        right.writeElement(CounterId.fromInt(6), 2L, 0L);
        right.writeElement(CounterId.fromInt(9), 1L, 0L);

        leftCol  = new CounterColumn(ByteBufferUtil.bytes("x"), left.context,  1L);
        rightCol = new CounterColumn(ByteBufferUtil.bytes("x"), right.context, 1L);
        Assert.assertNull(leftCol.diff(rightCol));

        // less than: right has subset of nodes (counts equal)
        Assert.assertEquals(leftCol, rightCol.diff(leftCol));

        // disjoint: right and left have disjoint node sets
        left = ContextState.allocate(3, 0, allocator);
        left.writeElement(CounterId.fromInt(3), 1L, 0L);
        left.writeElement(CounterId.fromInt(4), 1L, 0L);
        left.writeElement(CounterId.fromInt(9), 1L, 0L);

        right = ContextState.allocate(3, 0, allocator);
        right.writeElement(CounterId.fromInt(3), 1L, 0L);
        right.writeElement(CounterId.fromInt(6), 1L, 0L);
        right.writeElement(CounterId.fromInt(9), 1L, 0L);

        leftCol  = new CounterColumn(ByteBufferUtil.bytes("x"), left.context,  1L);
        rightCol = new CounterColumn(ByteBufferUtil.bytes("x"), right.context, 1L);
        Assert.assertEquals(rightCol, leftCol.diff(rightCol));
        Assert.assertEquals(leftCol, rightCol.diff(leftCol));
    }

    @Test
    public void testSerializeDeserialize() throws IOException
    {
        Allocator allocator = HeapAllocator.instance;
        CounterContext.ContextState state = CounterContext.ContextState.allocate(4, 2, allocator);
        state.writeElement(CounterId.fromInt(1), 4L, 4L);
        state.writeElement(CounterId.fromInt(2), 4L, 4L, true);
        state.writeElement(CounterId.fromInt(3), 4L, 4L);
        state.writeElement(CounterId.fromInt(4), 4L, 4L, true);

        CounterColumn original = new CounterColumn(ByteBufferUtil.bytes("x"), state.context, 1L);
        byte[] serialized;
        try (DataOutputBuffer bufOut = new DataOutputBuffer())
        {
            Column.serializer.serialize(original, bufOut);
            serialized = bufOut.getData();
        }

        ByteArrayInputStream bufIn = new ByteArrayInputStream(serialized, 0, serialized.length);
        CounterColumn deserialized = (CounterColumn) Column.serializer.deserialize(new DataInputStream(bufIn));
        Assert.assertEquals(original, deserialized);

        bufIn = new ByteArrayInputStream(serialized, 0, serialized.length);
        CounterColumn deserializedOnRemote = (CounterColumn) Column.serializer.deserialize(new DataInputStream(bufIn), ColumnSerializer.Flag.FROM_REMOTE);
        Assert.assertEquals(deserializedOnRemote.name(), original.name());
        Assert.assertEquals(deserializedOnRemote.total(), original.total());
        Assert.assertEquals(deserializedOnRemote.value(), cc.clearAllDelta(original.value()));
        Assert.assertEquals(deserializedOnRemote.timestamp(), deserialized.timestamp());
        Assert.assertEquals(deserializedOnRemote.timestampOfLastDelete(), deserialized.timestampOfLastDelete());
    }

    @Test
    public void testUpdateDigest() throws Exception
    {
        Allocator allocator = HeapAllocator.instance;
        MessageDigest digest1 = MessageDigest.getInstance("md5");
        MessageDigest digest2 = MessageDigest.getInstance("md5");

        CounterContext.ContextState state = CounterContext.ContextState.allocate(4, 2, allocator);
        state.writeElement(CounterId.fromInt(1), 4L, 4L);
        state.writeElement(CounterId.fromInt(2), 4L, 4L, true);
        state.writeElement(CounterId.fromInt(3), 4L, 4L);
        state.writeElement(CounterId.fromInt(4), 4L, 4L, true);

        CounterColumn original = new CounterColumn(ByteBufferUtil.bytes("x"), state.context, 1L);
        CounterColumn cleared = new CounterColumn(ByteBufferUtil.bytes("x"), cc.clearAllDelta(state.context), 1L);

        original.updateDigest(digest1);
        cleared.updateDigest(digest2);

        Assert.assertTrue(Arrays.equals(digest1.digest(), digest2.digest()));
    }
}
