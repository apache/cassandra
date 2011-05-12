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

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.apache.cassandra.Util;
import org.junit.Test;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.utils.ByteBufferUtil;

import org.apache.cassandra.Util;
import static org.apache.cassandra.Util.column;
import static org.apache.cassandra.db.TableTest.assertColumns;

public class RecoveryManagerTest extends CleanupHelper
{
    @Test
    public void testNothingToRecover() throws IOException {
        CommitLog.recover();
    }

    @Test
    public void testOne() throws IOException, ExecutionException, InterruptedException
    {
        Table table1 = Table.open("Keyspace1");
        Table table2 = Table.open("Keyspace2");

        RowMutation rm;
        DecoratedKey dk = Util.dk("keymulti");
        ColumnFamily cf;

        rm = new RowMutation("Keyspace1", dk.key);
        cf = ColumnFamily.create("Keyspace1", "Standard1");
        cf.addColumn(column("col1", "val1", 1L));
        rm.add(cf);
        rm.apply();

        rm = new RowMutation("Keyspace2", dk.key);
        cf = ColumnFamily.create("Keyspace2", "Standard3");
        cf.addColumn(column("col2", "val2", 1L));
        rm.add(cf);
        rm.apply();

        table1.getColumnFamilyStore("Standard1").clearUnsafe();
        table2.getColumnFamilyStore("Standard3").clearUnsafe();

        CommitLog.instance.resetUnsafe(); // disassociate segments from live CL
        CommitLog.recover();

        assertColumns(Util.getColumnFamily(table1, dk, "Standard1"), "col1");
        assertColumns(Util.getColumnFamily(table2, dk, "Standard3"), "col2");
    }

    @Test
    public void testRecoverCounter() throws IOException, ExecutionException, InterruptedException
    {
        Table table1 = Table.open("Keyspace1");

        RowMutation rm;
        DecoratedKey dk = Util.dk("key");
        ColumnFamily cf;

        for (int i = 0; i < 10; ++i)
        {
            rm = new RowMutation("Keyspace1", dk.key);
            cf = ColumnFamily.create("Keyspace1", "Counter1");
            cf.addColumn(new CounterColumn(ByteBufferUtil.bytes("col"), 1L, 1L));
            rm.add(cf);
            rm.apply();
        }

        table1.getColumnFamilyStore("Counter1").clearUnsafe();

        CommitLog.instance.resetUnsafe(); // disassociate segments from live CL
        CommitLog.recover();

        cf = Util.getColumnFamily(table1, dk, "Counter1");

        assert cf.getColumnCount() == 1;
        IColumn c = cf.getColumn(ByteBufferUtil.bytes("col"));

        assert c != null;
        assert ((CounterColumn)c).total() == 10L;
    }
}
