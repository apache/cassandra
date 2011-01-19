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

import org.apache.commons.lang.ArrayUtils;

import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.marshal.AbstractCommutativeType;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

public class CounterColumnTest
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
        CounterColumn column = (CounterColumn)type.createColumn(ByteBufferUtil.bytes("x"), ByteBufferUtil.bytes(delta), 1L);
        assert delta == column.value().getLong(column.value().arrayOffset());
        assert 0 == column.partitionedCounter().length;

        InetAddress node = InetAddress.getByAddress(FBUtilities.toByteArray(1));
        column.update(node);
        assert delta == column.value().getLong(column.value().arrayOffset());
        assert  1 == FBUtilities.byteArrayToInt( column.partitionedCounter(), 0*stepLength);
        assert 1L == FBUtilities.byteArrayToLong(column.partitionedCounter(), 0*stepLength + idLength);
        assert 3L == FBUtilities.byteArrayToLong(column.partitionedCounter(), 0*stepLength + idLength + clockLength);
    }

    @Test
    public void testUpdate() throws UnknownHostException
    {
        CounterColumn c = new CounterColumn(ByteBufferUtil.bytes("x"), ByteBufferUtil.bytes(0L), 0L);
        assert 0L == c.value().getLong(c.value().arrayOffset());

        assert c.partitionedCounter().length == 0 : "badly formatted initial context";

        c.value = ByteBufferUtil.bytes(1L);
        c.update(InetAddress.getByAddress(FBUtilities.toByteArray(1)));
        assert 1L == c.value().getLong(c.value().arrayOffset());

        assert c.partitionedCounter().length == stepLength;

        assert  1 == FBUtilities.byteArrayToInt( c.partitionedCounter(), 0*stepLength);
        assert 1L == FBUtilities.byteArrayToLong(c.partitionedCounter(), 0*stepLength + idLength);
        assert 1L == FBUtilities.byteArrayToLong(c.partitionedCounter(), 0*stepLength + idLength + clockLength);

        c.value = ByteBufferUtil.bytes(3L);
        c.update(InetAddress.getByAddress(FBUtilities.toByteArray(2)));

        c.value = ByteBufferUtil.bytes(2L);
        c.update(InetAddress.getByAddress(FBUtilities.toByteArray(2)));

        c.value = ByteBufferUtil.bytes(9L);
        c.update(InetAddress.getByAddress(FBUtilities.toByteArray(2)));

        assert 15L == c.value().getLong(c.value().arrayOffset());

        assert c.partitionedCounter().length == (2 * stepLength);

        assert  1 == FBUtilities.byteArrayToInt(c.partitionedCounter(),  0*stepLength);
        assert 1L == FBUtilities.byteArrayToLong(c.partitionedCounter(), 0*stepLength + idLength);
        assert 1L == FBUtilities.byteArrayToLong(c.partitionedCounter(), 0*stepLength + idLength + clockLength);

        assert   2 == FBUtilities.byteArrayToInt(c.partitionedCounter(),  1*stepLength);
        assert  3L == FBUtilities.byteArrayToLong(c.partitionedCounter(), 1*stepLength + idLength);
        assert 14L == FBUtilities.byteArrayToLong(c.partitionedCounter(), 1*stepLength + idLength + clockLength);
    }

    @Test
    public void testReconcile() throws UnknownHostException
    {
        IColumn left;
        IColumn right;
        IColumn reconciled;

        // tombstone + tombstone
        left  = new DeletedColumn(ByteBufferUtil.bytes("x"), 1, 1L);
        right = new DeletedColumn(ByteBufferUtil.bytes("x"), 2, 2L);

        assert left.reconcile(right).timestamp() == right.timestamp();
        assert right.reconcile(left).timestamp() == right.timestamp();

        // tombstone > live
        left  = new DeletedColumn(ByteBufferUtil.bytes("x"), 1, 2L);
        right = new CounterColumn(ByteBufferUtil.bytes("x"), ByteBufferUtil.bytes(0L), 1L);

        assert left.reconcile(right) == left;

        // tombstone < live last delete
        left  = new DeletedColumn(ByteBufferUtil.bytes("x"), 1, 1L);
        right = new CounterColumn(ByteBufferUtil.bytes("x"), ByteBufferUtil.bytes(0L), 4L, new byte[0], 2L);

        assert left.reconcile(right) == right;

        // tombstone == live last delete
        left  = new DeletedColumn(ByteBufferUtil.bytes("x"), 1, 2L);
        right = new CounterColumn(ByteBufferUtil.bytes("x"), ByteBufferUtil.bytes(0L), 4L, new byte[0], 2L);

        assert left.reconcile(right) == right;

        // tombstone > live last delete
        left  = new DeletedColumn(ByteBufferUtil.bytes("x"), 1, 4L);
        right = new CounterColumn(ByteBufferUtil.bytes("x"), ByteBufferUtil.bytes(0L), 9L, new byte[0], 1L);

        reconciled = left.reconcile(right);
        assert reconciled.name() == right.name();
        assert reconciled.value() == right.value();
        assert reconciled.timestamp() == right.timestamp();
        assert ((CounterColumn)reconciled).partitionedCounter() == ((CounterColumn)right).partitionedCounter();
        assert ((CounterColumn)reconciled).timestampOfLastDelete() == left.timestamp();

        // live < tombstone
        left  = new CounterColumn(ByteBufferUtil.bytes("x"), ByteBufferUtil.bytes(0L), 1L);
        right = new DeletedColumn(ByteBufferUtil.bytes("x"), 1, 2L);

        assert left.reconcile(right) == right;

        // live last delete > tombstone
        left  = new CounterColumn(ByteBufferUtil.bytes("x"), ByteBufferUtil.bytes(0L), 4L, new byte[0], 2L);
        right = new DeletedColumn(ByteBufferUtil.bytes("x"), 1, 1L);

        assert left.reconcile(right) == left;

        // live last delete == tombstone
        left  = new CounterColumn(ByteBufferUtil.bytes("x"), ByteBufferUtil.bytes(0L), 4L, new byte[0], 2L);
        right = new DeletedColumn(ByteBufferUtil.bytes("x"), 1, 2L);

        assert left.reconcile(right) == left;

        // live last delete < tombstone
        left  = new CounterColumn(ByteBufferUtil.bytes("x"), ByteBufferUtil.bytes(0L), 9L, new byte[0], 1L);
        right = new DeletedColumn(ByteBufferUtil.bytes("x"), 1, 4L);

        reconciled = left.reconcile(right);
        assert reconciled.name() == left.name();
        assert reconciled.value() == left.value();
        assert reconciled.timestamp() == left.timestamp();
        assert ((CounterColumn)reconciled).partitionedCounter() == ((CounterColumn)left).partitionedCounter();
        assert ((CounterColumn)reconciled).timestampOfLastDelete() == right.timestamp();

        // live + live
        byte[] context;

        context = new byte[0];
        context = cc.update(context, InetAddress.getByAddress(FBUtilities.toByteArray(1)), 1L);
        context = cc.update(context, InetAddress.getByAddress(FBUtilities.toByteArray(1)), 0L);
        context = cc.update(context, InetAddress.getByAddress(FBUtilities.toByteArray(1)), 1L);
        context = cc.update(context, InetAddress.getByAddress(FBUtilities.toByteArray(2)), 0L);
        context = cc.update(context, InetAddress.getByAddress(FBUtilities.toByteArray(2)), 5L);
        left  = new CounterColumn(ByteBufferUtil.bytes("x"), ByteBuffer.wrap(cc.total(context)), 9L, context, 1L);

        context = new byte[0];
        context = cc.update(context, InetAddress.getByAddress(FBUtilities.toByteArray(2)), 4L);
        context = cc.update(context, InetAddress.getByAddress(FBUtilities.toByteArray(3)), 2L);
        right = new CounterColumn(ByteBufferUtil.bytes("x"), ByteBuffer.wrap(cc.total(context)), 3L, context, 4L);

        reconciled = left.reconcile(right);

        assert reconciled.name() == left.name();
        assert 9L == reconciled.value().getLong(reconciled.value().position());
        assert reconciled.timestamp() == 9L;

        context = ((CounterColumn)reconciled).partitionedCounter();
        assert 3 * stepLength == context.length;

        assert  1 == FBUtilities.byteArrayToInt(context,  0*stepLength);
        assert 3L == FBUtilities.byteArrayToLong(context, 0*stepLength + idLength);
        assert 2L == FBUtilities.byteArrayToLong(context, 0*stepLength + idLength + clockLength);

        assert  2 == FBUtilities.byteArrayToInt(context,  1*stepLength);
        assert 2L == FBUtilities.byteArrayToLong(context, 1*stepLength + idLength);
        assert 5L == FBUtilities.byteArrayToLong(context, 1*stepLength + idLength + clockLength);

        assert  3 == FBUtilities.byteArrayToInt(context,  2*stepLength);
        assert 1L == FBUtilities.byteArrayToLong(context, 2*stepLength + idLength);
        assert 2L == FBUtilities.byteArrayToLong(context, 2*stepLength + idLength + clockLength);

        assert ((CounterColumn)reconciled).timestampOfLastDelete() == 4L;
    }

    @Test
    public void testDiff() throws UnknownHostException
    {
        byte[] left;
        byte[] right;

        CounterColumn leftCol;
        CounterColumn rightCol;

        // timestamp
        left    = new byte[0];
        leftCol = new CounterColumn(ByteBufferUtil.bytes("x"), ByteBuffer.wrap(cc.total(left)), 1L, left);

        right    = new byte[0];
        rightCol = new CounterColumn(ByteBufferUtil.bytes("x"), ByteBuffer.wrap(cc.total(right)), 2L, right);

        assert rightCol == leftCol.diff(rightCol);
        assert null     == rightCol.diff(leftCol);

        // timestampOfLastDelete
        left    = new byte[0];
        leftCol = new CounterColumn(ByteBufferUtil.bytes("x"), ByteBuffer.wrap(cc.total(left)), 1L, left, 1L);

        right    = new byte[0];
        rightCol = new CounterColumn(ByteBufferUtil.bytes("x"), ByteBuffer.wrap(cc.total(right)), 1L, right, 2L);

        assert rightCol == leftCol.diff(rightCol);
        assert null     == rightCol.diff(leftCol);

        // equality: equal nodes, all counts same
        left = Util.concatByteArrays(
            FBUtilities.toByteArray(3), FBUtilities.toByteArray(3L), FBUtilities.toByteArray(0L),
            FBUtilities.toByteArray(6), FBUtilities.toByteArray(2L), FBUtilities.toByteArray(0L),
            FBUtilities.toByteArray(9), FBUtilities.toByteArray(1L), FBUtilities.toByteArray(0L)
            );
        left = cc.update(left, InetAddress.getByAddress(FBUtilities.toByteArray(3)), 0L);
        right = ArrayUtils.clone(left);

        leftCol  = new CounterColumn(ByteBufferUtil.bytes("x"), ByteBuffer.wrap(cc.total(left)),  1L, left);
        rightCol = new CounterColumn(ByteBufferUtil.bytes("x"), ByteBuffer.wrap(cc.total(right)), 1L, right);
        assert null == leftCol.diff(rightCol);

        // greater than: left has superset of nodes (counts equal)
        left = Util.concatByteArrays(
            FBUtilities.toByteArray(3),  FBUtilities.toByteArray(3L), FBUtilities.toByteArray(0L),
            FBUtilities.toByteArray(6),  FBUtilities.toByteArray(2L), FBUtilities.toByteArray(0L),
            FBUtilities.toByteArray(9),  FBUtilities.toByteArray(1L), FBUtilities.toByteArray(0L),
            FBUtilities.toByteArray(12), FBUtilities.toByteArray(0L), FBUtilities.toByteArray(0L)
            );
        right = Util.concatByteArrays(
            FBUtilities.toByteArray(3),  FBUtilities.toByteArray(3L), FBUtilities.toByteArray(0L),
            FBUtilities.toByteArray(6),  FBUtilities.toByteArray(2L), FBUtilities.toByteArray(0L),
            FBUtilities.toByteArray(9),  FBUtilities.toByteArray(1L), FBUtilities.toByteArray(0L)
            );

        leftCol  = new CounterColumn(ByteBufferUtil.bytes("x"), ByteBuffer.wrap(cc.total(left)),  1L, left);
        rightCol = new CounterColumn(ByteBufferUtil.bytes("x"), ByteBuffer.wrap(cc.total(right)), 1L, right);
        assert null == leftCol.diff(rightCol);

        // less than: right has subset of nodes (counts equal)
        assert leftCol == rightCol.diff(leftCol);

        // disjoint: right and left have disjoint node sets
        left = Util.concatByteArrays(
            FBUtilities.toByteArray(3),  FBUtilities.toByteArray(1L), FBUtilities.toByteArray(0L),
            FBUtilities.toByteArray(4),  FBUtilities.toByteArray(1L), FBUtilities.toByteArray(0L),
            FBUtilities.toByteArray(9),  FBUtilities.toByteArray(1L), FBUtilities.toByteArray(0L)
            );
        right = Util.concatByteArrays(
            FBUtilities.toByteArray(3),  FBUtilities.toByteArray(1L), FBUtilities.toByteArray(0L),
            FBUtilities.toByteArray(6),  FBUtilities.toByteArray(1L), FBUtilities.toByteArray(0L),
            FBUtilities.toByteArray(9),  FBUtilities.toByteArray(1L), FBUtilities.toByteArray(0L)
            );

        leftCol  = new CounterColumn(ByteBufferUtil.bytes("x"), ByteBuffer.wrap(cc.total(left)),  1L, left);
        rightCol = new CounterColumn(ByteBufferUtil.bytes("x"), ByteBuffer.wrap(cc.total(right)), 1L, right);
        assert rightCol == leftCol.diff(rightCol);
        assert leftCol  == rightCol.diff(leftCol);
    }

    @Test
    public void testCleanNodeCounts() throws UnknownHostException
    {
        byte[] context = Util.concatByteArrays(
            FBUtilities.toByteArray(1),  FBUtilities.toByteArray(1L), FBUtilities.toByteArray(1L),
            FBUtilities.toByteArray(2),  FBUtilities.toByteArray(2L), FBUtilities.toByteArray(2L),
            FBUtilities.toByteArray(4),  FBUtilities.toByteArray(3L), FBUtilities.toByteArray(3L),
            FBUtilities.toByteArray(8),  FBUtilities.toByteArray(4L), FBUtilities.toByteArray(4L)
            );
        CounterColumn c = new CounterColumn(ByteBufferUtil.bytes("x"), ByteBuffer.wrap(cc.total(context)), 1L, context);

        CounterColumn d = c.cleanNodeCounts(InetAddress.getByAddress(FBUtilities.toByteArray(4)));

        assertEquals(7L, d.value().getLong(d.value().arrayOffset()));
    }

    @Test
    public void testSerializeDeserialize() throws IOException
    {
        ColumnFamily cf;

        byte[] context = Util.concatByteArrays(
            FBUtilities.toByteArray(1),  FBUtilities.toByteArray(1L), FBUtilities.toByteArray(1L),
            FBUtilities.toByteArray(2),  FBUtilities.toByteArray(2L), FBUtilities.toByteArray(2L),
            FBUtilities.toByteArray(4),  FBUtilities.toByteArray(3L), FBUtilities.toByteArray(3L),
            FBUtilities.toByteArray(8),  FBUtilities.toByteArray(4L), FBUtilities.toByteArray(4L)
            );
        CounterColumn original = new CounterColumn(ByteBufferUtil.bytes("x"), ByteBuffer.wrap(cc.total(context)), 1L, context);

        DataOutputBuffer bufOut = new DataOutputBuffer();
        Column.serializer().serialize(original, bufOut);

        ByteArrayInputStream bufIn = new ByteArrayInputStream(bufOut.getData(), 0, bufOut.getLength());
        CounterColumn deserialized = (CounterColumn)Column.serializer().deserialize(new DataInputStream(bufIn));

        assert original.equals(deserialized);
    }
}
