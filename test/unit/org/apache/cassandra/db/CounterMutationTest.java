/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db;

import java.io.IOException;

import org.junit.Test;

import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.*;
import static org.apache.cassandra.db.context.CounterContext.ContextState;

public class CounterMutationTest extends CleanupHelper
{
    @Test
    public void testMergeOldShards() throws IOException
    {
        RowMutation rm;
        CounterMutation cm;

        rm = new RowMutation("Keyspace1", ByteBufferUtil.bytes("key1"));
        rm.addCounter(new QueryPath("Counter1", null, ByteBufferUtil.bytes("Column1")), 3);
        cm = new CounterMutation(rm, ConsistencyLevel.ONE);
        cm.apply();

        NodeId.renewLocalId(2L); // faking time of renewal for test
        NodeId id1 = NodeId.getLocalId();

        rm = new RowMutation("Keyspace1", ByteBufferUtil.bytes("key1"));
        rm.addCounter(new QueryPath("Counter1", null, ByteBufferUtil.bytes("Column1")), 4);
        cm = new CounterMutation(rm, ConsistencyLevel.ONE);
        cm.apply();

        NodeId.renewLocalId(4L); // faking time of renewal for test
        NodeId id2 = NodeId.getLocalId();

        rm = new RowMutation("Keyspace1", ByteBufferUtil.bytes("key1"));
        rm.addCounter(new QueryPath("Counter1", null, ByteBufferUtil.bytes("Column1")), 5);
        rm.addCounter(new QueryPath("Counter1", null, ByteBufferUtil.bytes("Column2")), 1);
        cm = new CounterMutation(rm, ConsistencyLevel.ONE);
        cm.apply();

        RowMutation reprm = cm.makeReplicationMutation();
        ColumnFamily cf = reprm.getColumnFamilies().iterator().next();
        CounterColumn.removeOldShards(cf, Integer.MAX_VALUE);
        IColumn c = cf.getColumn(ByteBufferUtil.bytes("Column1"));
        assert c != null;
        assert c instanceof CounterColumn;

        assert ((CounterColumn)c).total() == 12L;
        ContextState s = new ContextState(c.value());
        assert s.getNodeId().equals(id1);
        assert s.getCount() == 7;
        s.moveToNext();
        assert s.getNodeId().equals(id2);
        assert s.getCount() == 5;
    }
}

