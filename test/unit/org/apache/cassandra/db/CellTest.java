package org.apache.cassandra.db;
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


import java.nio.ByteBuffer;

import org.junit.Test;

import junit.framework.Assert;
import org.apache.cassandra.Util;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.NativeAllocator;
import org.apache.cassandra.utils.memory.NativePool;

public class CellTest
{

    private static final OpOrder order = new OpOrder();
    private static NativeAllocator allocator = new NativePool(Integer.MAX_VALUE, Integer.MAX_VALUE, 1f, null).newAllocator();

    @Test
    public void testConflictingTypeEquality()
    {
        boolean[] tf = new boolean[]{ true, false };
        for (boolean lhs : tf)
        {
            for (boolean rhs : tf)
            {
                // don't test equality for both sides native, as this is based on CellName resolution
                if (lhs && rhs)
                    continue;
                Cell a = expiring("a", "a", 1, 1, lhs);
                Cell b = regular("a", "a", 1, rhs);
                Assert.assertNotSame(a, b);
                Assert.assertNotSame(b, a);
                a = deleted("a", 1, 1, lhs);
                b = regular("a", ByteBufferUtil.bytes(1), 1, rhs);
                Assert.assertNotSame(a, b);
                Assert.assertNotSame(b, a);
            }
        }
    }

    @Test
    public void testExpiringCellReconile()
    {
        // equal
        Assert.assertEquals(0, testExpiring("a", "a", 1, 1, null, null, null, null));

        // newer timestamp
        Assert.assertEquals(-1, testExpiring("a", "a", 2, 1, null, null, 1L, null));
        Assert.assertEquals(-1, testExpiring("a", "a", 2, 1, null, "b", 1L, 2));

        // newer TTL
        Assert.assertEquals(-1, testExpiring("a", "a", 1, 2, null, null, null, 1));
        Assert.assertEquals(1, testExpiring("a", "a", 1, 2, null, "b", null, 1));

        // newer value
        Assert.assertEquals(-1, testExpiring("a", "b", 2, 1, null, "a", null, null));
        Assert.assertEquals(-1, testExpiring("a", "b", 2, 1, null, "a", null, 2));
    }

    private int testExpiring(String n1, String v1, long t1, int et1, String n2, String v2, Long t2, Integer et2)
    {
        if (n2 == null)
            n2 = n1;
        if (v2 == null)
            v2 = v1;
        if (t2 == null)
            t2 = t1;
        if (et2 == null)
            et2 = et1;
        int result = testExpiring(n1, v1, t1, et1, false, n2, v2, t2, et2, false);
        Assert.assertEquals(result, testExpiring(n1, v1, t1, et1, false, n2, v2, t2, et2, true));
        Assert.assertEquals(result, testExpiring(n1, v1, t1, et1, true, n2, v2, t2, et2, false));
        Assert.assertEquals(result, testExpiring(n1, v1, t1, et1, true, n2, v2, t2, et2, true));
        return result;
    }

    private int testExpiring(String n1, String v1, long t1, int et1, boolean native1, String n2, String v2, long t2, int et2, boolean native2)
    {
        Cell c1 = expiring(n1, v1, t1, et1, native1);
        Cell c2 = expiring(n2, v2, t2, et2, native2);
        return reconcile(c1, c2);
    }

    int reconcile(Cell c1, Cell c2)
    {
        if (c1.reconcile(c2) == c1)
            return c2.reconcile(c1) == c1 ? -1 : 0;
        return c2.reconcile(c1) == c2 ? 1 : 0;
    }

    private Cell expiring(String name, String value, long timestamp, int expirationTime, boolean nativeCell)
    {
        ExpiringCell cell = new BufferExpiringCell(Util.cellname(name), ByteBufferUtil.bytes(value), timestamp, 1, expirationTime);
        if (nativeCell)
            cell = new NativeExpiringCell(allocator, order.getCurrent(), cell);
        return cell;
    }

    private Cell regular(String name, ByteBuffer value, long timestamp, boolean nativeCell)
    {
        Cell cell = new BufferCell(Util.cellname(name), value, timestamp);
        if (nativeCell)
            cell = new NativeCell(allocator, order.getCurrent(), cell);
        return cell;
    }
    private Cell regular(String name, String value, long timestamp, boolean nativeCell)
    {
        return regular(name, ByteBufferUtil.bytes(value), timestamp, nativeCell);
    }

    private Cell deleted(String name, int localDeletionTime, long timestamp, boolean nativeCell)
    {
        DeletedCell cell = new BufferDeletedCell(Util.cellname(name), localDeletionTime, timestamp);
        if (nativeCell)
            cell = new NativeDeletedCell(allocator, order.getCurrent(), cell);
        return cell;
    }
}
