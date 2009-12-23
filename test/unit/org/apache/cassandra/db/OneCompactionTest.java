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

import static junit.framework.Assert.assertEquals;
import org.apache.cassandra.db.filter.QueryPath;

public class OneCompactionTest
{
    private void testCompaction(String columnFamilyName, int insertsPerTable) throws IOException, ExecutionException, InterruptedException
    {
        CompactionManager.instance.disableAutoCompaction();

        Table table = Table.open("Keyspace1");
        ColumnFamilyStore store = table.getColumnFamilyStore(columnFamilyName);

        Set<String> inserted = new HashSet<String>();
        for (int j = 0; j < insertsPerTable; j++) {
            String key = String.valueOf(j);
            RowMutation rm = new RowMutation("Keyspace1", key);
            rm.add(new QueryPath(columnFamilyName, null, "0".getBytes()), new byte[0], j);
            rm.apply();
            inserted.add(key);
            store.forceBlockingFlush();
            assertEquals(inserted.size(), table.getColumnFamilyStore(columnFamilyName).getKeyRange("", "", 10000).keys.size());
        }
        CompactionManager.instance.submitMajor(store).get();
        assertEquals(1, store.getSSTables().size());
        assertEquals(table.getColumnFamilyStore(columnFamilyName).getKeyRange("", "", 10000).keys.size(), inserted.size());
    }

    @Test
    public void testCompaction1() throws IOException, ExecutionException, InterruptedException
    {
        testCompaction("Standard1", 1);
    }

    @Test
    public void testCompaction2() throws IOException, ExecutionException, InterruptedException
    {
        testCompaction("Standard2", 2);
    }
}
