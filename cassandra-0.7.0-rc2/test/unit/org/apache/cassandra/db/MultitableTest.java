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


import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.apache.cassandra.Util;
import org.junit.Test;

import static org.apache.cassandra.db.TableTest.assertColumns;
import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.Util;
import static org.apache.cassandra.Util.column;

public class MultitableTest extends CleanupHelper
{
    @Test
    public void testSameCFs() throws IOException, ExecutionException, InterruptedException
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
        cf = ColumnFamily.create("Keyspace2", "Standard1");
        cf.addColumn(column("col2", "val2", 1L));
        rm.add(cf);
        rm.apply();

        table1.getColumnFamilyStore("Standard1").forceBlockingFlush();
        table2.getColumnFamilyStore("Standard1").forceBlockingFlush();

        assertColumns(Util.getColumnFamily(table1, dk, "Standard1"), "col1");
        assertColumns(Util.getColumnFamily(table2, dk, "Standard1"), "col2");
    }
}
