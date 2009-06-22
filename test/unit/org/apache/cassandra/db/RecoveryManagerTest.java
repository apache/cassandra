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

import org.junit.Test;

import org.apache.cassandra.CleanupHelper;
import static org.apache.cassandra.db.TableTest.assertColumns;

public class RecoveryManagerTest extends CleanupHelper
{
    @Test
    public void testNothing() throws IOException {
        // TODO nothing to recover
        RecoveryManager rm = RecoveryManager.instance();
        rm.doRecovery();  
    }

    @Test
    public void testSomething() throws IOException, ExecutionException, InterruptedException
    {
        Table table1 = Table.open("Table1");
        Table table2 = Table.open("Table2");

        RowMutation rm;
        ColumnFamily cf;

        rm = new RowMutation("Table1", "keymulti");
        cf = ColumnFamily.create("Table1", "Standard1");
        cf.addColumn(new Column("col1", "val1".getBytes(), 1L));
        rm.add(cf);
        rm.apply();

        rm = new RowMutation("Table2", "keymulti");
        cf = ColumnFamily.create("Table2", "Standard3");
        cf.addColumn(new Column("col2", "val2".getBytes(), 1L));
        rm.add(cf);
        rm.apply();

        table1.getColumnFamilyStore("Standard1").clearUnsafe();
        table2.getColumnFamilyStore("Standard3").clearUnsafe();

        RecoveryManager.doRecovery();

        assertColumns(table1.get("keymulti", "Standard1"), "col1");
        assertColumns(table2.get("keymulti", "Standard3"), "col2");
    }
}
