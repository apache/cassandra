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
import java.util.*;

import org.apache.commons.lang.ArrayUtils;

import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.db.context.IContext.ContextRelationship;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Note: these tests assume IPv4 (4 bytes) is used for id.
 *       if IPv6 (16 bytes) is used, tests will fail (but the code will work).
 *       however, it might be pragmatic to modify the code to just use
 *       the IPv4 portion of the IPv6 address-space.
 */
public class CounterContextTest
{
    private static final CounterContext cc = new CounterContext();

    private static final InetAddress idAddress;
    private static final byte[] id;
    private static final int idLength;
    private static final int clockLength;
    private static final int countLength;

    private static final int stepLength;
    private static final int defaultEntries;

    static
    {
        idAddress      = FBUtilities.getLocalAddress();
        id             = idAddress.getAddress();
        idLength       = 4; // size of int
        clockLength    = 8; // size of long
        countLength    = 8; // size of long
        stepLength     = idLength + clockLength + countLength;

        defaultEntries = 10;
    }

    @Test
    public void testCreate()
    {
        ByteBuffer context = cc.create(4);
        assert context.remaining() == stepLength;
    }

    @Test
    public void testDiff()
    {
        ByteBuffer left = ByteBuffer.allocate(3 * stepLength);
        ByteBuffer right;

        // equality: equal nodes, all counts same
        cc.writeElementAtOffset(left, 0 * stepLength, FBUtilities.toByteArray(3), 3L, 0L);
        cc.writeElementAtOffset(left, 1 * stepLength, FBUtilities.toByteArray(6), 2L, 0L);
        cc.writeElementAtOffset(left, 2 * stepLength, FBUtilities.toByteArray(9), 1L, 0L);
        right = ByteBufferUtil.clone(left);

        assert ContextRelationship.EQUAL ==
            cc.diff(left, right);

        // greater than: left has superset of nodes (counts equal)
        left = ByteBuffer.allocate(4 * stepLength);
        cc.writeElementAtOffset(left, 0 * stepLength, FBUtilities.toByteArray(3),  3L, 0L);
        cc.writeElementAtOffset(left, 1 * stepLength, FBUtilities.toByteArray(6),  2L, 0L);
        cc.writeElementAtOffset(left, 2 * stepLength, FBUtilities.toByteArray(9),  1L, 0L);
        cc.writeElementAtOffset(left, 3 * stepLength, FBUtilities.toByteArray(12), 0L, 0L);

        right = ByteBuffer.allocate(3 * stepLength);
        cc.writeElementAtOffset(right, 0 * stepLength, FBUtilities.toByteArray(3), 3L, 0L);
        cc.writeElementAtOffset(right, 1 * stepLength, FBUtilities.toByteArray(6), 2L, 0L);
        cc.writeElementAtOffset(right, 2 * stepLength, FBUtilities.toByteArray(9), 1L, 0L);

        assert ContextRelationship.GREATER_THAN ==
            cc.diff(left, right);
        
        // less than: left has subset of nodes (counts equal)
        left = ByteBuffer.allocate(3 * stepLength);
        cc.writeElementAtOffset(left, 0 * stepLength, FBUtilities.toByteArray(3), 3L, 0L);
        cc.writeElementAtOffset(left, 1 * stepLength, FBUtilities.toByteArray(6), 2L, 0L);
        cc.writeElementAtOffset(left, 2 * stepLength, FBUtilities.toByteArray(9), 1L, 0L);

        right = ByteBuffer.allocate(4 * stepLength);
        cc.writeElementAtOffset(right, 0 * stepLength, FBUtilities.toByteArray(3),  3L, 0L);
        cc.writeElementAtOffset(right, 1 * stepLength, FBUtilities.toByteArray(6),  2L, 0L);
        cc.writeElementAtOffset(right, 2 * stepLength, FBUtilities.toByteArray(9),  1L, 0L);
        cc.writeElementAtOffset(right, 3 * stepLength, FBUtilities.toByteArray(12), 0L, 0L);

        assert ContextRelationship.LESS_THAN ==
            cc.diff(left, right);

        // greater than: equal nodes, but left has higher counts
        left = ByteBuffer.allocate(3 * stepLength);
        cc.writeElementAtOffset(left, 0 * stepLength, FBUtilities.toByteArray(3), 3L, 0L);
        cc.writeElementAtOffset(left, 1 * stepLength, FBUtilities.toByteArray(6), 2L, 0L);
        cc.writeElementAtOffset(left, 2 * stepLength, FBUtilities.toByteArray(9), 3L, 0L);

        right = ByteBuffer.allocate(3 * stepLength);
        cc.writeElementAtOffset(right, 0 * stepLength, FBUtilities.toByteArray(3), 3L, 0L);
        cc.writeElementAtOffset(right, 1 * stepLength, FBUtilities.toByteArray(6), 2L, 0L);
        cc.writeElementAtOffset(right, 2 * stepLength, FBUtilities.toByteArray(9), 1L, 0L);

        assert ContextRelationship.GREATER_THAN ==
            cc.diff(left, right);

        // less than: equal nodes, but right has higher counts
        left = ByteBuffer.allocate(3 * stepLength);
        cc.writeElementAtOffset(left, 0 * stepLength, FBUtilities.toByteArray(3), 3L, 0L);
        cc.writeElementAtOffset(left, 1 * stepLength, FBUtilities.toByteArray(6), 2L, 0L);
        cc.writeElementAtOffset(left, 2 * stepLength, FBUtilities.toByteArray(9), 3L, 0L);

        right = ByteBuffer.allocate(3 * stepLength);
        cc.writeElementAtOffset(right, 0 * stepLength, FBUtilities.toByteArray(3), 3L, 0L);
        cc.writeElementAtOffset(right, 1 * stepLength, FBUtilities.toByteArray(6), 9L, 0L);
        cc.writeElementAtOffset(right, 2 * stepLength, FBUtilities.toByteArray(9), 3L, 0L);

        assert ContextRelationship.LESS_THAN ==
            cc.diff(left, right);

        // disjoint: right and left have disjoint node sets
        left = ByteBuffer.allocate(3 * stepLength);
        cc.writeElementAtOffset(left, 0 * stepLength, FBUtilities.toByteArray(3), 1L, 0L);
        cc.writeElementAtOffset(left, 1 * stepLength, FBUtilities.toByteArray(4), 1L, 0L);
        cc.writeElementAtOffset(left, 2 * stepLength, FBUtilities.toByteArray(9), 1L, 0L);

        right = ByteBuffer.allocate(3 * stepLength);
        cc.writeElementAtOffset(right, 0 * stepLength, FBUtilities.toByteArray(3), 1L, 0L);
        cc.writeElementAtOffset(right, 1 * stepLength, FBUtilities.toByteArray(6), 1L, 0L);
        cc.writeElementAtOffset(right, 2 * stepLength, FBUtilities.toByteArray(9), 1L, 0L);

        assert ContextRelationship.DISJOINT ==
            cc.diff(left, right);

        left = ByteBuffer.allocate(3 * stepLength);
        cc.writeElementAtOffset(left, 0 * stepLength, FBUtilities.toByteArray(3), 1L, 0L);
        cc.writeElementAtOffset(left, 1 * stepLength, FBUtilities.toByteArray(4), 1L, 0L);
        cc.writeElementAtOffset(left, 2 * stepLength, FBUtilities.toByteArray(9), 1L, 0L);

        right = ByteBuffer.allocate(3 * stepLength);
        cc.writeElementAtOffset(right, 0 * stepLength, FBUtilities.toByteArray(2),  1L, 0L);
        cc.writeElementAtOffset(right, 1 * stepLength, FBUtilities.toByteArray(6),  1L, 0L);
        cc.writeElementAtOffset(right, 2 * stepLength, FBUtilities.toByteArray(12), 1L, 0L);

        assert ContextRelationship.DISJOINT ==
            cc.diff(left, right);

        // disjoint: equal nodes, but right and left have higher counts in differing nodes
        left = ByteBuffer.allocate(3 * stepLength);
        cc.writeElementAtOffset(left, 0 * stepLength, FBUtilities.toByteArray(3), 1L, 0L);
        cc.writeElementAtOffset(left, 1 * stepLength, FBUtilities.toByteArray(6), 3L, 0L);
        cc.writeElementAtOffset(left, 2 * stepLength, FBUtilities.toByteArray(9), 1L, 0L);

        right = ByteBuffer.allocate(3 * stepLength);
        cc.writeElementAtOffset(right, 0 * stepLength, FBUtilities.toByteArray(3), 1L, 0L);
        cc.writeElementAtOffset(right, 1 * stepLength, FBUtilities.toByteArray(6), 1L, 0L);
        cc.writeElementAtOffset(right, 2 * stepLength, FBUtilities.toByteArray(9), 5L, 0L);

        assert ContextRelationship.DISJOINT ==
            cc.diff(left, right);

        left = ByteBuffer.allocate(3 * stepLength);
        cc.writeElementAtOffset(left, 0 * stepLength, FBUtilities.toByteArray(3), 2L, 0L);
        cc.writeElementAtOffset(left, 1 * stepLength, FBUtilities.toByteArray(6), 3L, 0L);
        cc.writeElementAtOffset(left, 2 * stepLength, FBUtilities.toByteArray(9), 1L, 0L);

        right = ByteBuffer.allocate(3 * stepLength);
        cc.writeElementAtOffset(right, 0 * stepLength, FBUtilities.toByteArray(3), 1L, 0L);
        cc.writeElementAtOffset(right, 1 * stepLength, FBUtilities.toByteArray(6), 9L, 0L);
        cc.writeElementAtOffset(right, 2 * stepLength, FBUtilities.toByteArray(9), 5L, 0L);

        assert ContextRelationship.DISJOINT ==
            cc.diff(left, right);

        // disjoint: left has more nodes, but lower counts
        left = ByteBuffer.allocate(4 * stepLength);
        cc.writeElementAtOffset(left, 0 * stepLength, FBUtilities.toByteArray(3),  2L, 0L);
        cc.writeElementAtOffset(left, 1 * stepLength, FBUtilities.toByteArray(6),  3L, 0L);
        cc.writeElementAtOffset(left, 2 * stepLength, FBUtilities.toByteArray(9),  1L, 0L);
        cc.writeElementAtOffset(left, 3 * stepLength, FBUtilities.toByteArray(12), 1L, 0L);

        right = ByteBuffer.allocate(3 * stepLength);
        cc.writeElementAtOffset(right, 0 * stepLength, FBUtilities.toByteArray(3), 4L, 0L);
        cc.writeElementAtOffset(right, 1 * stepLength, FBUtilities.toByteArray(6), 9L, 0L);
        cc.writeElementAtOffset(right, 2 * stepLength, FBUtilities.toByteArray(9), 5L, 0L);

        assert ContextRelationship.DISJOINT ==
            cc.diff(left, right);
        
        // disjoint: left has less nodes, but higher counts
        left = ByteBuffer.allocate(3 * stepLength);
        cc.writeElementAtOffset(left, 0 * stepLength, FBUtilities.toByteArray(3), 5L, 0L);
        cc.writeElementAtOffset(left, 1 * stepLength, FBUtilities.toByteArray(6), 3L, 0L);
        cc.writeElementAtOffset(left, 2 * stepLength, FBUtilities.toByteArray(9), 2L, 0L);

        right = ByteBuffer.allocate(4 * stepLength);
        cc.writeElementAtOffset(right, 0 * stepLength, FBUtilities.toByteArray(3),  4L, 0L);
        cc.writeElementAtOffset(right, 1 * stepLength, FBUtilities.toByteArray(6),  3L, 0L);
        cc.writeElementAtOffset(right, 2 * stepLength, FBUtilities.toByteArray(9),  2L, 0L);
        cc.writeElementAtOffset(right, 3 * stepLength, FBUtilities.toByteArray(12), 1L, 0L);

        assert ContextRelationship.DISJOINT ==
            cc.diff(left, right);

        // disjoint: mixed nodes and counts
        left = ByteBuffer.allocate(3 * stepLength);
        cc.writeElementAtOffset(left, 0 * stepLength, FBUtilities.toByteArray(3), 5L, 0L);
        cc.writeElementAtOffset(left, 1 * stepLength, FBUtilities.toByteArray(6), 2L, 0L);
        cc.writeElementAtOffset(left, 2 * stepLength, FBUtilities.toByteArray(9), 2L, 0L);

        right = ByteBuffer.allocate(4 * stepLength);
        cc.writeElementAtOffset(right, 0 * stepLength, FBUtilities.toByteArray(3),  4L, 0L);
        cc.writeElementAtOffset(right, 1 * stepLength, FBUtilities.toByteArray(6),  3L, 0L);
        cc.writeElementAtOffset(right, 2 * stepLength, FBUtilities.toByteArray(9),  2L, 0L);
        cc.writeElementAtOffset(right, 3 * stepLength, FBUtilities.toByteArray(12), 1L, 0L);

        assert ContextRelationship.DISJOINT ==
            cc.diff(left, right);

        left = ByteBuffer.allocate(4 * stepLength);
        cc.writeElementAtOffset(left, 0 * stepLength, FBUtilities.toByteArray(3), 5L, 0L);
        cc.writeElementAtOffset(left, 1 * stepLength, FBUtilities.toByteArray(6), 2L, 0L);
        cc.writeElementAtOffset(left, 2 * stepLength, FBUtilities.toByteArray(7), 2L, 0L);
        cc.writeElementAtOffset(left, 3 * stepLength, FBUtilities.toByteArray(9), 2L, 0L);

        right = ByteBuffer.allocate(3 * stepLength);
        cc.writeElementAtOffset(right, 0 * stepLength, FBUtilities.toByteArray(3), 4L, 0L);
        cc.writeElementAtOffset(right, 1 * stepLength, FBUtilities.toByteArray(6), 3L, 0L);
        cc.writeElementAtOffset(right, 2 * stepLength, FBUtilities.toByteArray(9), 2L, 0L);

        assert ContextRelationship.DISJOINT ==
            cc.diff(left, right);
    }

    @Test
    public void testMerge()
    {
        // note: local counts aggregated; remote counts are reconciled (i.e. take max)
        ByteBuffer left = ByteBuffer.allocate(4 * stepLength);
        cc.writeElementAtOffset(left, 0 * stepLength, FBUtilities.toByteArray(1), 1L, 1L);
        cc.writeElementAtOffset(left, 1 * stepLength, FBUtilities.toByteArray(2), 2L, 2L);
        cc.writeElementAtOffset(left, 2 * stepLength, FBUtilities.toByteArray(4), 6L, 3L);
        cc.writeElementAtOffset(
            left,
            3 * stepLength,
            FBUtilities.getLocalAddress().getAddress(),
            7L,
            3L);

        ByteBuffer right = ByteBuffer.allocate(3 * stepLength);
        cc.writeElementAtOffset(right, 0 * stepLength, FBUtilities.toByteArray(4), 4L, 4L);
        cc.writeElementAtOffset(right, 1 * stepLength, FBUtilities.toByteArray(5), 5L, 5L);
        cc.writeElementAtOffset(
            right,
            2 * stepLength,
            FBUtilities.getLocalAddress().getAddress(),
            2L,
            9L);

        ByteBuffer merged = cc.merge(left, right);

        assertEquals(5 * stepLength, merged.remaining());
        // local node id's counts are aggregated
        assertEquals(0, ByteBufferUtil.compareSubArrays(
            ByteBuffer.wrap(FBUtilities.getLocalAddress().getAddress()),
            0,
            merged,
            4*stepLength,
            4));
        assertEquals(  9L, merged.getLong(4*stepLength + idLength));
        assertEquals(12L,  merged.getLong(4*stepLength + idLength + clockLength));

        // remote node id counts are reconciled (i.e. take max)
        assertEquals( 4,   merged.getInt( 2*stepLength));
        assertEquals( 6L,  merged.getLong(2*stepLength + idLength));
        assertEquals( 3L,  merged.getLong(2*stepLength + idLength + clockLength));

        assertEquals( 5,   merged.getInt( 3*stepLength));
        assertEquals( 5L,  merged.getLong(3*stepLength + idLength));
        assertEquals( 5L,  merged.getLong(3*stepLength + idLength + clockLength));

        assertEquals( 2,   merged.getInt( 1*stepLength));
        assertEquals( 2L,  merged.getLong(1*stepLength + idLength));
        assertEquals( 2L,  merged.getLong(1*stepLength + idLength + clockLength));

        assertEquals( 1,   merged.getInt( 0*stepLength));
        assertEquals( 1L,  merged.getLong(0*stepLength + idLength));
        assertEquals( 1L,  merged.getLong(0*stepLength + idLength + clockLength));
    }

    @Test
    public void testTotal()
    {
        ByteBuffer left = ByteBuffer.allocate(4 * stepLength);
        cc.writeElementAtOffset(left, 0 * stepLength, FBUtilities.toByteArray(1), 1L, 1L);
        cc.writeElementAtOffset(left, 1 * stepLength, FBUtilities.toByteArray(2), 2L, 2L);
        cc.writeElementAtOffset(left, 2 * stepLength, FBUtilities.toByteArray(4), 3L, 3L);
        cc.writeElementAtOffset(
            left,
            3 * stepLength,
            FBUtilities.getLocalAddress().getAddress(),
            3L,
            3L);

        ByteBuffer right = ByteBuffer.allocate(3 * stepLength);
        cc.writeElementAtOffset(right, 0 * stepLength, FBUtilities.toByteArray(4), 4L, 4L);
        cc.writeElementAtOffset(right, 1 * stepLength, FBUtilities.toByteArray(5), 5L, 5L);
        cc.writeElementAtOffset(
            right,
            2 * stepLength,
            FBUtilities.getLocalAddress().getAddress(),
            9L,
            9L);

        ByteBuffer merged = cc.merge(left, right);

        // 127.0.0.1: 12 (3+9)
        // 0.0.0.1:    1
        // 0.0.0.2:    2
        // 0.0.0.4:    4
        // 0.0.0.5:    5

        assertEquals(24L, cc.total(merged));
    }

    @Test
    public void testCleanNodeCounts() throws UnknownHostException
    {
        ByteBuffer bytes = ByteBuffer.allocate(4 * stepLength);
        cc.writeElementAtOffset(bytes, 0 * stepLength, FBUtilities.toByteArray(1), 1L, 1L);
        cc.writeElementAtOffset(bytes, 1 * stepLength, FBUtilities.toByteArray(2), 2L, 2L);
        cc.writeElementAtOffset(bytes, 2 * stepLength, FBUtilities.toByteArray(4), 3L, 3L);
        cc.writeElementAtOffset(bytes, 3 * stepLength, FBUtilities.toByteArray(8), 4L, 4L);

        assertEquals(4, bytes.getInt( 2*stepLength));
        assertEquals(3L, bytes.getLong(2*stepLength + idLength));

        bytes = cc.cleanNodeCounts(bytes, InetAddress.getByAddress(FBUtilities.toByteArray(4)));

        // node: 0.0.0.4 should be removed
        assertEquals(3 * stepLength, bytes.remaining());

        // other nodes should be unaffected
        assertEquals(1, bytes.getInt( 0*stepLength));
        assertEquals(1L, bytes.getLong(0*stepLength + idLength));

        assertEquals(2, bytes.getInt( 1*stepLength));
        assertEquals(2L, bytes.getLong(1*stepLength + idLength));

        assertEquals(8, bytes.getInt( 2*stepLength));
        assertEquals(4L, bytes.getLong(2*stepLength + idLength));
    }
}
