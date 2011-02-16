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
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

public class CounterColumnTest extends SchemaLoader
{
    private static final CounterContext cc = new CounterContext();

    private static final int idLength;
    private static final int clockLength;
    private static final int countLength;

    private static final int stepLength;

    static
    {
        idLength      = 4; // size of int
        clockLength   = 8; // size of long
        countLength   = 8; // size of long

        stepLength    = idLength + clockLength + countLength;
    }

    @Test
    public void testCreate() throws UnknownHostException
    {
        AbstractCommutativeType type = CounterColumnType.instance;
        long delta = 3L;
        CounterUpdateColumn cuc = (CounterUpdateColumn)type.createColumn(
            ByteBufferUtil.bytes("x"),
            ByteBufferUtil.bytes(delta),
            1L);
        CounterColumn column = cuc.localCopy(Table.open("Keyspace5").getColumnFamilyStore("Counter1"));

        assert delta == column.total();
        assert Arrays.equals(FBUtilities.getLocalAddress().getAddress(), ArrayUtils.subarray(column.value().array(), 0, idLength));
        assert 1L == column.value().getLong(0*stepLength + idLength);
        assert delta == column.value().getLong(0*stepLength + idLength + clockLength);
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
        left  = new CounterColumn(ByteBufferUtil.bytes("x"), cc.create(FBUtilities.toByteArray(1), 2L, 3L), 1L, Long.MIN_VALUE);
        right = new CounterColumn(ByteBufferUtil.bytes("x"), cc.create(FBUtilities.toByteArray(1), 1L, 1L), 4L, 3L);

        assert left.reconcile(right) == right;

        // live last delete > live
        left  = new CounterColumn(ByteBufferUtil.bytes("x"), cc.create(FBUtilities.toByteArray(1), 2L, 3L), 6L, 5L);
        right = new CounterColumn(ByteBufferUtil.bytes("x"), cc.create(FBUtilities.toByteArray(1), 1L, 1L), 4L, 3L);

        assert left.reconcile(right) == left;

        // live + live
        left  = new CounterColumn(ByteBufferUtil.bytes("x"), cc.create(FBUtilities.toByteArray(1), 1L, 1L), 4L, Long.MIN_VALUE);
        right = new CounterColumn(ByteBufferUtil.bytes("x"), cc.create(FBUtilities.toByteArray(1), 2L, 3L), 1L, Long.MIN_VALUE);

        reconciled = left.reconcile(right);
        assert reconciled.name().equals(left.name());
        assert ((CounterColumn)reconciled).total() == 3L;
        assert reconciled.timestamp() == 4L;

        left  = reconciled;
        right = new CounterColumn(ByteBufferUtil.bytes("x"), cc.create(FBUtilities.toByteArray(2), 1L, 5L), 2L, Long.MIN_VALUE);

        reconciled = left.reconcile(right);
        assert reconciled.name().equals(left.name());
        assert ((CounterColumn)reconciled).total() == 8L;
        assert reconciled.timestamp() == 4L;

        left  = reconciled;
        right = new CounterColumn(ByteBufferUtil.bytes("x"), cc.create(FBUtilities.toByteArray(2), 2L, 2L), 6L, Long.MIN_VALUE);

        reconciled = left.reconcile(right);
        assert reconciled.name().equals(left.name());
        assert ((CounterColumn)reconciled).total() == 5L;
        assert reconciled.timestamp() == 6L;

        context = reconciled.value();
        assert 2 * stepLength == context.remaining();

        assert  1 == context.getInt(0*stepLength);
        assert 2L == context.getLong(0*stepLength + idLength);
        assert 3L == context.getLong(0*stepLength + idLength + clockLength);

        assert  2 == context.getInt(1*stepLength);
        assert 2L == context.getLong(1*stepLength + idLength);
        assert 2L == context.getLong(1*stepLength + idLength + clockLength);

        assert ((CounterColumn)reconciled).timestampOfLastDelete() == Long.MIN_VALUE;
    }

    @Test
    public void testDiff() throws UnknownHostException
    {
        ByteBuffer left;
        ByteBuffer right;

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
        left = ByteBuffer.wrap(Util.concatByteArrays(
            FBUtilities.toByteArray(3), FBUtilities.toByteArray(3L), FBUtilities.toByteArray(0L),
            FBUtilities.toByteArray(6), FBUtilities.toByteArray(2L), FBUtilities.toByteArray(0L),
            FBUtilities.toByteArray(9), FBUtilities.toByteArray(1L), FBUtilities.toByteArray(0L)
            ));
        //left = cc.update(left, InetAddress.getByAddress(FBUtilities.toByteArray(3)), 0L);
        right = ByteBufferUtil.clone(left);

        leftCol  = new CounterColumn(ByteBufferUtil.bytes("x"), left,  1L);
        rightCol = new CounterColumn(ByteBufferUtil.bytes("x"), right, 1L);
        assert null == leftCol.diff(rightCol);

        // greater than: left has superset of nodes (counts equal)
        left = ByteBuffer.wrap(Util.concatByteArrays(
            FBUtilities.toByteArray(3),  FBUtilities.toByteArray(3L), FBUtilities.toByteArray(0L),
            FBUtilities.toByteArray(6),  FBUtilities.toByteArray(2L), FBUtilities.toByteArray(0L),
            FBUtilities.toByteArray(9),  FBUtilities.toByteArray(1L), FBUtilities.toByteArray(0L),
            FBUtilities.toByteArray(12), FBUtilities.toByteArray(0L), FBUtilities.toByteArray(0L)
            ));
        right = ByteBuffer.wrap(Util.concatByteArrays(
            FBUtilities.toByteArray(3),  FBUtilities.toByteArray(3L), FBUtilities.toByteArray(0L),
            FBUtilities.toByteArray(6),  FBUtilities.toByteArray(2L), FBUtilities.toByteArray(0L),
            FBUtilities.toByteArray(9),  FBUtilities.toByteArray(1L), FBUtilities.toByteArray(0L)
            ));

        leftCol  = new CounterColumn(ByteBufferUtil.bytes("x"), left,  1L);
        rightCol = new CounterColumn(ByteBufferUtil.bytes("x"), right, 1L);
        assert null == leftCol.diff(rightCol);

        // less than: right has subset of nodes (counts equal)
        assert leftCol == rightCol.diff(leftCol);

        // disjoint: right and left have disjoint node sets
        left = ByteBuffer.wrap(Util.concatByteArrays(
            FBUtilities.toByteArray(3),  FBUtilities.toByteArray(1L), FBUtilities.toByteArray(0L),
            FBUtilities.toByteArray(4),  FBUtilities.toByteArray(1L), FBUtilities.toByteArray(0L),
            FBUtilities.toByteArray(9),  FBUtilities.toByteArray(1L), FBUtilities.toByteArray(0L)
            ));
        right = ByteBuffer.wrap(Util.concatByteArrays(
            FBUtilities.toByteArray(3),  FBUtilities.toByteArray(1L), FBUtilities.toByteArray(0L),
            FBUtilities.toByteArray(6),  FBUtilities.toByteArray(1L), FBUtilities.toByteArray(0L),
            FBUtilities.toByteArray(9),  FBUtilities.toByteArray(1L), FBUtilities.toByteArray(0L)
            ));

        leftCol  = new CounterColumn(ByteBufferUtil.bytes("x"), left,  1L);
        rightCol = new CounterColumn(ByteBufferUtil.bytes("x"), right, 1L);
        assert rightCol == leftCol.diff(rightCol);
        assert leftCol  == rightCol.diff(leftCol);
    }

    @Test
    public void testCleanNodeCounts() throws UnknownHostException
    {
        ByteBuffer context = ByteBuffer.wrap(Util.concatByteArrays(
            FBUtilities.toByteArray(1),  FBUtilities.toByteArray(1L), FBUtilities.toByteArray(1L),
            FBUtilities.toByteArray(2),  FBUtilities.toByteArray(2L), FBUtilities.toByteArray(2L),
            FBUtilities.toByteArray(4),  FBUtilities.toByteArray(3L), FBUtilities.toByteArray(3L),
            FBUtilities.toByteArray(8),  FBUtilities.toByteArray(4L), FBUtilities.toByteArray(4L)
            ));
        CounterColumn c = new CounterColumn(ByteBufferUtil.bytes("x"), context, 1L);

        CounterColumn d = c.cleanNodeCounts(InetAddress.getByAddress(FBUtilities.toByteArray(4)));

        assertEquals(7L, d.total());
    }

    @Test
    public void testSerializeDeserialize() throws IOException
    {
        ColumnFamily cf;

        ByteBuffer context = ByteBuffer.wrap(Util.concatByteArrays(
            FBUtilities.toByteArray(1),  FBUtilities.toByteArray(1L), FBUtilities.toByteArray(1L),
            FBUtilities.toByteArray(2),  FBUtilities.toByteArray(2L), FBUtilities.toByteArray(2L),
            FBUtilities.toByteArray(4),  FBUtilities.toByteArray(3L), FBUtilities.toByteArray(3L),
            FBUtilities.toByteArray(8),  FBUtilities.toByteArray(4L), FBUtilities.toByteArray(4L)
            ));
        CounterColumn original = new CounterColumn(ByteBufferUtil.bytes("x"), context, 1L);

        DataOutputBuffer bufOut = new DataOutputBuffer();
        Column.serializer().serialize(original, bufOut);

        ByteArrayInputStream bufIn = new ByteArrayInputStream(bufOut.getData(), 0, bufOut.getLength());
        CounterColumn deserialized = (CounterColumn)Column.serializer().deserialize(new DataInputStream(bufIn));

        assert original.equals(deserialized);
    }
}
