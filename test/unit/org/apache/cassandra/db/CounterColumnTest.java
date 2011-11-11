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

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.commons.lang.ArrayUtils;

import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.context.CounterContext;
import static org.apache.cassandra.db.context.CounterContext.ContextState;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.io.IColumnSerializer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.Allocator;
import org.apache.cassandra.utils.HeapAllocator;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NodeId;

public class CounterColumnTest extends SchemaLoader
{
    private static final CounterContext cc = new CounterContext();

    private static final int idLength;
    private static final int clockLength;
    private static final int countLength;

    private static final int stepLength;

    static
    {
        idLength      = NodeId.LENGTH;
        clockLength   = 8; // size of long
        countLength   = 8; // size of long

        stepLength    = idLength + clockLength + countLength;
    }

    @Test
    public void testCreate() throws UnknownHostException
    {
        long delta = 3L;
        CounterUpdateColumn cuc = new CounterUpdateColumn(ByteBufferUtil.bytes("x"), delta, 1L);
        CounterColumn column = cuc.localCopy(Table.open("Keyspace5").getColumnFamilyStore("Counter1"));

        assert delta == column.total();
        assert 1 == column.value().getShort(0);
        assert 0 == column.value().getShort(2);
        assert NodeId.wrap(column.value(), 4).isLocalId();
        assert 1L == column.value().getLong(4 + 0*stepLength + idLength);
        assert delta == column.value().getLong(4 + 0*stepLength + idLength + clockLength);
    }

    @Test
    public void testReconcile() throws UnknownHostException
    {
        IColumn left;
        IColumn right;
        IColumn reconciled;

        ByteBuffer context;

        // tombstone + tombstone
        left  = new DeletedColumn(ByteBufferUtil.bytes("x"), 1, 1L);
        right = new DeletedColumn(ByteBufferUtil.bytes("x"), 2, 2L);

        assert left.reconcile(right).getMarkedForDeleteAt() == right.getMarkedForDeleteAt();
        assert right.reconcile(left).getMarkedForDeleteAt() == right.getMarkedForDeleteAt();

        // tombstone > live
        left  = new DeletedColumn(ByteBufferUtil.bytes("x"), 1, 2L);
        right = new CounterColumn(ByteBufferUtil.bytes("x"), 0L, 1L);

        assert left.reconcile(right) == left;

        // tombstone < live last delete
        left  = new DeletedColumn(ByteBufferUtil.bytes("x"), 1, 1L);
        right = new CounterColumn(ByteBufferUtil.bytes("x"), 0L, 4L, 2L);

        assert left.reconcile(right) == right;

        // tombstone == live last delete
        left  = new DeletedColumn(ByteBufferUtil.bytes("x"), 1, 2L);
        right = new CounterColumn(ByteBufferUtil.bytes("x"), 0L, 4L, 2L);

        assert left.reconcile(right) == right;

        // tombstone > live last delete
        left  = new DeletedColumn(ByteBufferUtil.bytes("x"), 1, 4L);
        right = new CounterColumn(ByteBufferUtil.bytes("x"), 0L, 9L, 1L);

        reconciled = left.reconcile(right);
        assert reconciled.name() == right.name();
        assert reconciled.value() == right.value();
        assert reconciled.timestamp() == right.timestamp();
        assert ((CounterColumn)reconciled).timestampOfLastDelete() == left.getMarkedForDeleteAt();

        // live < tombstone
        left  = new CounterColumn(ByteBufferUtil.bytes("x"), 0L, 1L);
        right = new DeletedColumn(ByteBufferUtil.bytes("x"), 1, 2L);

        assert left.reconcile(right) == right;

        // live last delete > tombstone
        left  = new CounterColumn(ByteBufferUtil.bytes("x"), 0L, 4L, 2L);
        right = new DeletedColumn(ByteBufferUtil.bytes("x"), 1, 1L);

        assert left.reconcile(right) == left;

        // live last delete == tombstone
        left  = new CounterColumn(ByteBufferUtil.bytes("x"), 0L, 4L, 2L);
        right = new DeletedColumn(ByteBufferUtil.bytes("x"), 1, 2L);

        assert left.reconcile(right) == left;

        // live last delete < tombstone
        left  = new CounterColumn(ByteBufferUtil.bytes("x"), 0L, 9L, 1L);
        right = new DeletedColumn(ByteBufferUtil.bytes("x"), 1, 4L);

        reconciled = left.reconcile(right);
        assert reconciled.name() == left.name();
        assert reconciled.value() == left.value();
        assert reconciled.timestamp() == left.timestamp();
        assert ((CounterColumn)reconciled).timestampOfLastDelete() == right.getMarkedForDeleteAt();

        // live < live last delete
        left  = new CounterColumn(ByteBufferUtil.bytes("x"), cc.create(NodeId.fromInt(1), 2L, 3L, false), 1L, Long.MIN_VALUE);
        right = new CounterColumn(ByteBufferUtil.bytes("x"), cc.create(NodeId.fromInt(1), 1L, 1L, false), 4L, 3L);

        assert left.reconcile(right) == right;

        // live last delete > live
        left  = new CounterColumn(ByteBufferUtil.bytes("x"), cc.create(NodeId.fromInt(1), 2L, 3L, false), 6L, 5L);
        right = new CounterColumn(ByteBufferUtil.bytes("x"), cc.create(NodeId.fromInt(1), 1L, 1L, false), 4L, 3L);

        assert left.reconcile(right) == left;

        // live + live
        left = new CounterColumn(ByteBufferUtil.bytes("x"), cc.create(NodeId.fromInt(1), 1L, 1L, false), 4L, Long.MIN_VALUE);
        right = new CounterColumn(ByteBufferUtil.bytes("x"), cc.create(NodeId.fromInt(1), 2L, 3L, false), 1L, Long.MIN_VALUE);

        reconciled = left.reconcile(right);
        assert reconciled.name().equals(left.name());
        assert ((CounterColumn)reconciled).total() == 3L;
        assert reconciled.timestamp() == 4L;

        left = reconciled;
        right = new CounterColumn(ByteBufferUtil.bytes("x"), cc.create(NodeId.fromInt(2), 1L, 5L, false), 2L, Long.MIN_VALUE);

        reconciled = left.reconcile(right);
        assert reconciled.name().equals(left.name());
        assert ((CounterColumn)reconciled).total() == 8L;
        assert reconciled.timestamp() == 4L;

        left = reconciled;
        right = new CounterColumn(ByteBufferUtil.bytes("x"), cc.create(NodeId.fromInt(2), 2L, 2L, false), 6L, Long.MIN_VALUE);

        reconciled = left.reconcile(right);
        assert reconciled.name().equals(left.name());
        assert ((CounterColumn)reconciled).total() == 5L;
        assert reconciled.timestamp() == 6L;

        context = reconciled.value();
        int hd = 2; // header
        assert hd + 2 * stepLength == context.remaining();

        assert Util.equalsNodeId(NodeId.fromInt(1), context, hd + 0*stepLength);
        assert 2L == context.getLong(hd + 0*stepLength + idLength);
        assert 3L == context.getLong(hd + 0*stepLength + idLength + clockLength);

        assert Util.equalsNodeId(NodeId.fromInt(2), context, hd + 1*stepLength);
        assert 2L == context.getLong(hd + 1*stepLength + idLength);
        assert 2L == context.getLong(hd + 1*stepLength + idLength + clockLength);

        assert ((CounterColumn)reconciled).timestampOfLastDelete() == Long.MIN_VALUE;
    }

    @Test
    public void testDiff() throws UnknownHostException
    {
        Allocator allocator = HeapAllocator.instance;
        ContextState left;
        ContextState right;

        CounterColumn leftCol;
        CounterColumn rightCol;

        // timestamp
        leftCol = new CounterColumn(ByteBufferUtil.bytes("x"), 0, 1L);
        rightCol = new CounterColumn(ByteBufferUtil.bytes("x"), 0, 2L);

        assert rightCol == leftCol.diff(rightCol);
        assert null     == rightCol.diff(leftCol);

        // timestampOfLastDelete
        leftCol = new CounterColumn(ByteBufferUtil.bytes("x"), 0, 1L, 1L);
        rightCol = new CounterColumn(ByteBufferUtil.bytes("x"), 0, 1L, 2L);

        assert rightCol == leftCol.diff(rightCol);
        assert null     == rightCol.diff(leftCol);

        // equality: equal nodes, all counts same
        left = ContextState.allocate(3, 0, allocator);
        left.writeElement(NodeId.fromInt(3), 3L, 0L);
        left.writeElement(NodeId.fromInt(6), 2L, 0L);
        left.writeElement(NodeId.fromInt(9), 1L, 0L);
        right = new ContextState(ByteBufferUtil.clone(left.context), 2);

        leftCol  = new CounterColumn(ByteBufferUtil.bytes("x"), left.context,  1L);
        rightCol = new CounterColumn(ByteBufferUtil.bytes("x"), right.context, 1L);
        assert null == leftCol.diff(rightCol);

        // greater than: left has superset of nodes (counts equal)
        left = ContextState.allocate(4, 0, allocator);
        left.writeElement(NodeId.fromInt(3), 3L, 0L);
        left.writeElement(NodeId.fromInt(6), 2L, 0L);
        left.writeElement(NodeId.fromInt(9), 1L, 0L);
        left.writeElement(NodeId.fromInt(12), 0L, 0L);

        right = ContextState.allocate(3, 0, allocator);
        right.writeElement(NodeId.fromInt(3), 3L, 0L);
        right.writeElement(NodeId.fromInt(6), 2L, 0L);
        right.writeElement(NodeId.fromInt(9), 1L, 0L);

        leftCol  = new CounterColumn(ByteBufferUtil.bytes("x"), left.context,  1L);
        rightCol = new CounterColumn(ByteBufferUtil.bytes("x"), right.context, 1L);
        assert null == leftCol.diff(rightCol);

        // less than: right has subset of nodes (counts equal)
        assert leftCol == rightCol.diff(leftCol);

        // disjoint: right and left have disjoint node sets
        left = ContextState.allocate(3, 0, allocator);
        left.writeElement(NodeId.fromInt(3), 1L, 0L);
        left.writeElement(NodeId.fromInt(4), 1L, 0L);
        left.writeElement(NodeId.fromInt(9), 1L, 0L);

        right = ContextState.allocate(3, 0, allocator);
        right.writeElement(NodeId.fromInt(3), 1L, 0L);
        right.writeElement(NodeId.fromInt(6), 1L, 0L);
        right.writeElement(NodeId.fromInt(9), 1L, 0L);

        leftCol  = new CounterColumn(ByteBufferUtil.bytes("x"), left.context,  1L);
        rightCol = new CounterColumn(ByteBufferUtil.bytes("x"), right.context, 1L);
        assert rightCol == leftCol.diff(rightCol);
        assert leftCol  == rightCol.diff(leftCol);
    }

    @Test
    public void testSerializeDeserialize() throws IOException
    {
        Allocator allocator = HeapAllocator.instance;
        CounterContext.ContextState state = CounterContext.ContextState.allocate(4, 2, allocator);
        state.writeElement(NodeId.fromInt(1), 4L, 4L);
        state.writeElement(NodeId.fromInt(2), 4L, 4L, true);
        state.writeElement(NodeId.fromInt(3), 4L, 4L);
        state.writeElement(NodeId.fromInt(4), 4L, 4L, true);

        CounterColumn original = new CounterColumn(ByteBufferUtil.bytes("x"), state.context, 1L);
        DataOutputBuffer bufOut = new DataOutputBuffer();
        Column.serializer().serialize(original, bufOut);
        byte[] serialized = bufOut.getData();

        ByteArrayInputStream bufIn = new ByteArrayInputStream(serialized, 0, serialized.length);
        CounterColumn deserialized = (CounterColumn)Column.serializer().deserialize(new DataInputStream(bufIn));
        assert original.equals(deserialized);

        bufIn = new ByteArrayInputStream(serialized, 0, serialized.length);
        CounterColumn deserializedOnRemote = (CounterColumn)Column.serializer().deserialize(new DataInputStream(bufIn), IColumnSerializer.Flag.FROM_REMOTE);
        assert deserializedOnRemote.name().equals(original.name());
        assert deserializedOnRemote.total() == original.total();
        assert deserializedOnRemote.value().equals(cc.clearAllDelta(original.value()));
        assert deserializedOnRemote.timestamp() == deserialized.timestamp();
        assert deserializedOnRemote.timestampOfLastDelete() == deserialized.timestampOfLastDelete();
    }

    @Test
    public void testUpdateDigest() throws Exception
    {
        Allocator allocator = HeapAllocator.instance;
        MessageDigest digest1 = MessageDigest.getInstance("md5");
        MessageDigest digest2 = MessageDigest.getInstance("md5");

        CounterContext.ContextState state = CounterContext.ContextState.allocate(4, 2, allocator);
        state.writeElement(NodeId.fromInt(1), 4L, 4L);
        state.writeElement(NodeId.fromInt(2), 4L, 4L, true);
        state.writeElement(NodeId.fromInt(3), 4L, 4L);
        state.writeElement(NodeId.fromInt(4), 4L, 4L, true);

        CounterColumn original = new CounterColumn(ByteBufferUtil.bytes("x"), state.context, 1L);
        CounterColumn cleared = new CounterColumn(ByteBufferUtil.bytes("x"), cc.clearAllDelta(state.context), 1L);

        original.updateDigest(digest1);
        cleared.updateDigest(digest2);

        assert Arrays.equals(digest1.digest(), digest2.digest());
    }
}
