/*
* Licensed to the Apache Software Foundation (ASF) under one * or more contributor license agreements.  See the NOTICE file
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

import java.nio.ByteBuffer;

import org.junit.Test;

import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;
import static org.apache.cassandra.Util.getBytes;
import static org.apache.cassandra.Util.concatByteArrays;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

public class SuperColumnTest
{   
    private static final CounterContext cc = new CounterContext();

    @Test
    public void testMissingSubcolumn() {
    	SuperColumn sc = new SuperColumn(ByteBufferUtil.bytes("sc1"), LongType.instance);
    	sc.addColumn(new Column(getBytes(1), ByteBufferUtil.bytes("value"), 1));
    	assertNotNull(sc.getSubColumn(getBytes(1)));
    	assertNull(sc.getSubColumn(getBytes(2)));
    }

    @Test
    public void testAddColumnIncrementCounter()
    {
        byte[] context;

    	SuperColumn sc = new SuperColumn(ByteBufferUtil.bytes("sc1"), LongType.instance);

        context = concatByteArrays(
            FBUtilities.toByteArray(1), FBUtilities.toByteArray(7L), FBUtilities.toByteArray(0L),
            FBUtilities.toByteArray(2), FBUtilities.toByteArray(5L), FBUtilities.toByteArray(7L),
            FBUtilities.toByteArray(4), FBUtilities.toByteArray(2L), FBUtilities.toByteArray(9L),
            FBUtilities.getLocalAddress().getAddress(), FBUtilities.toByteArray(3L), FBUtilities.toByteArray(3L)
            );
        sc.addColumn(new CounterColumn(getBytes(1), ByteBuffer.wrap(context), 3L, 0L));
        context = concatByteArrays(
            FBUtilities.toByteArray(2), FBUtilities.toByteArray(3L), FBUtilities.toByteArray(4L),
            FBUtilities.toByteArray(4), FBUtilities.toByteArray(4L), FBUtilities.toByteArray(1L),
            FBUtilities.toByteArray(8), FBUtilities.toByteArray(9L), FBUtilities.toByteArray(0L),
            FBUtilities.getLocalAddress().getAddress(), FBUtilities.toByteArray(9L), FBUtilities.toByteArray(5L)
            );
        sc.addColumn(new CounterColumn(getBytes(1), ByteBuffer.wrap(context), 10L, 0L));

        context = concatByteArrays(
            FBUtilities.toByteArray(2), FBUtilities.toByteArray(1L), FBUtilities.toByteArray(0L),
            FBUtilities.toByteArray(3), FBUtilities.toByteArray(6L), FBUtilities.toByteArray(0L),
            FBUtilities.toByteArray(7), FBUtilities.toByteArray(3L), FBUtilities.toByteArray(0L)
            );
        sc.addColumn(new CounterColumn(getBytes(2), ByteBuffer.wrap(context), 9L, 0L));
                    
    	assertNotNull(sc.getSubColumn(getBytes(1)));
    	assertNull(sc.getSubColumn(getBytes(3)));

        // column: 1
    	byte[] c1 = concatByteArrays(
                FBUtilities.toByteArray(1), FBUtilities.toByteArray(7L), FBUtilities.toByteArray(0L),
                FBUtilities.toByteArray(2), FBUtilities.toByteArray(5L), FBUtilities.toByteArray(7L),
                FBUtilities.toByteArray(4), FBUtilities.toByteArray(4L), FBUtilities.toByteArray(1L),
                FBUtilities.toByteArray(8), FBUtilities.toByteArray(9L), FBUtilities.toByteArray(0L),
                FBUtilities.getLocalAddress().getAddress(), FBUtilities.toByteArray(12L), FBUtilities.toByteArray(8L)
                );
        assert 0 == ByteBufferUtil.compareSubArrays(
            ((CounterColumn)sc.getSubColumn(getBytes(1))).value(),
            0,
            ByteBuffer.wrap(c1),
            0,
            c1.length);

        // column: 2
        byte[] c2 = concatByteArrays(
                FBUtilities.toByteArray(2), FBUtilities.toByteArray(1L), FBUtilities.toByteArray(0L),
                FBUtilities.toByteArray(3), FBUtilities.toByteArray(6L), FBUtilities.toByteArray(0L),
                FBUtilities.toByteArray(7), FBUtilities.toByteArray(3L), FBUtilities.toByteArray(0L)
                );
        assert 0 == ByteBufferUtil.compareSubArrays(
            ((CounterColumn)sc.getSubColumn(getBytes(2))).value(),
            0,
            ByteBuffer.wrap(c2),
            0,
            c2.length);

    	assertNotNull(sc.getSubColumn(getBytes(1)));
    	assertNotNull(sc.getSubColumn(getBytes(2)));
    	assertNull(sc.getSubColumn(getBytes(3)));
    }
}
