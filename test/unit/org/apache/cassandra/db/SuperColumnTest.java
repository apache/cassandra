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
import static org.apache.cassandra.db.context.CounterContext.ContextState;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NodeId;

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
        ContextState state;

    	SuperColumn sc = new SuperColumn(ByteBufferUtil.bytes("sc1"), LongType.instance);

        state = ContextState.allocate(4, 1);
        state.writeElement(NodeId.fromInt(1), 7L, 0L);
        state.writeElement(NodeId.fromInt(2), 5L, 7L);
        state.writeElement(NodeId.fromInt(4), 2L, 9L);
        state.writeElement(NodeId.getLocalId(), 3L, 3L, true);
        sc.addColumn(new CounterColumn(getBytes(1), state.context, 3L, 0L));

        state = ContextState.allocate(4, 1);
        state.writeElement(NodeId.fromInt(2), 3L, 4L);
        state.writeElement(NodeId.fromInt(4), 4L, 1L);
        state.writeElement(NodeId.fromInt(8), 9L, 0L);
        state.writeElement(NodeId.getLocalId(), 9L, 5L, true);
        sc.addColumn(new CounterColumn(getBytes(1), state.context, 10L, 0L));

        state = ContextState.allocate(3, 0);
        state.writeElement(NodeId.fromInt(2), 1L, 0L);
        state.writeElement(NodeId.fromInt(3), 6L, 0L);
        state.writeElement(NodeId.fromInt(7), 3L, 0L);
        sc.addColumn(new CounterColumn(getBytes(2), state.context, 9L, 0L));
                    
    	assertNotNull(sc.getSubColumn(getBytes(1)));
    	assertNull(sc.getSubColumn(getBytes(3)));

        // column: 1
        ContextState c1 = ContextState.allocate(5, 1);
        c1.writeElement(NodeId.fromInt(1), 7L, 0L);
        c1.writeElement(NodeId.fromInt(2), 5L, 7L);
        c1.writeElement(NodeId.fromInt(4), 4L, 1L);
        c1.writeElement(NodeId.fromInt(8), 9L, 0L);
        c1.writeElement(NodeId.getLocalId(), 12L, 8L, true);
        assert 0 == ByteBufferUtil.compareSubArrays(
            ((CounterColumn)sc.getSubColumn(getBytes(1))).value(),
            0,
            c1.context,
            0,
            c1.context.remaining());

        // column: 2
        ContextState c2 = ContextState.allocate(3, 0);
        c2.writeElement(NodeId.fromInt(2), 1L, 0L);
        c2.writeElement(NodeId.fromInt(3), 6L, 0L);
        c2.writeElement(NodeId.fromInt(7), 3L, 0L);
        assert 0 == ByteBufferUtil.compareSubArrays(
            ((CounterColumn)sc.getSubColumn(getBytes(2))).value(),
            0,
            c2.context,
            0,
            c2.context.remaining());

    	assertNotNull(sc.getSubColumn(getBytes(1)));
    	assertNotNull(sc.getSubColumn(getBytes(2)));
    	assertNull(sc.getSubColumn(getBytes(3)));
    }
}
