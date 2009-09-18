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
import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;

import org.junit.Test;

import org.apache.cassandra.CleanupHelper;
import static org.apache.cassandra.Util.column;

public class RecoveryManager2Test extends CleanupHelper
{
    @Test
    public void testWithFlush() throws IOException, ExecutionException, InterruptedException
    {
        Table table1 = Table.open("Keyspace1");
        Set<String> keys = new HashSet<String>();

        for (int i = 0; i < 100; i++)
        {
            String key = "key" + i;
            RowMutation rm = new RowMutation("Keyspace1", key);
            ColumnFamily cf = ColumnFamily.create("Keyspace1", "Standard1");
            cf.addColumn(column("col1", "val1", 1L));
            rm.add(cf);
            rm.apply();
            keys.add(key);
        }
        table1.getColumnFamilyStore("Standard1").forceBlockingFlush();

        table1.getColumnFamilyStore("Standard1").clearUnsafe();
        RecoveryManager.doRecovery();

        Set<String> foundKeys = new HashSet<String>(table1.getColumnFamilyStore("Standard1").getKeyRange("", "", 1000).keys);
        assert keys.equals(foundKeys);
    }
}
