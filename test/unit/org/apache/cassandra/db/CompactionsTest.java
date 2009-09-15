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
import java.util.concurrent.Future;
import java.util.Set;
import java.util.HashSet;

import org.junit.Test;

import org.apache.cassandra.io.SSTableReader;
import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.db.filter.QueryPath;
import static junit.framework.Assert.assertEquals;

public class CompactionsTest extends CleanupHelper
{
    @Test
    public void testCompactions() throws IOException, ExecutionException, InterruptedException
    {
        // this test does enough rows to force multiple block indexes to be used
        Table table = Table.open("Keyspace1");
        ColumnFamilyStore store = table.getColumnFamilyStore("Standard1");

        final int ROWS_PER_SSTABLE = 10;
        Set<String> inserted = new HashSet<String>();
        for (int j = 0; j < (SSTableReader.indexInterval() * 3) / ROWS_PER_SSTABLE; j++) {
            for (int i = 0; i < ROWS_PER_SSTABLE; i++) {
                String key = String.valueOf(i % 2);
                RowMutation rm = new RowMutation("Keyspace1", key);
                rm.add(new QueryPath("Standard1", null, String.valueOf(i / 2).getBytes()), new byte[0], j * ROWS_PER_SSTABLE + i);
                rm.apply();
                inserted.add(key);
            }
            store.forceBlockingFlush();
            assertEquals(table.getColumnFamilyStore("Standard1").getKeyRange("", "", 10000).keys.size(), inserted.size());
        }
        while (true)
        {
            Future<Integer> ft = CompactionManager.instance().submit(store);
            if (ft.get() == 0)
                break;
        }
        if (store.getSSTables().size() > 1)
        {
            store.doCompaction(2, store.getSSTables().size());
        }
        assertEquals(table.getColumnFamilyStore("Standard1").getKeyRange("", "", 10000).keys.size(), inserted.size());
    }
}
