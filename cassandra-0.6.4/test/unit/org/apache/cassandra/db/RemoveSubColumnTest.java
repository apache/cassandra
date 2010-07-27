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

import static junit.framework.Assert.assertNull;
import org.apache.cassandra.db.filter.IdentityQueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import static org.apache.cassandra.Util.addMutation;
import static org.apache.cassandra.Util.getBytes;
import org.apache.cassandra.CleanupHelper;

public class RemoveSubColumnTest extends CleanupHelper
{
    @Test
    public void testRemoveSubColumn() throws IOException, ExecutionException, InterruptedException
    {
        Table table = Table.open("Keyspace1");
        ColumnFamilyStore store = table.getColumnFamilyStore("Super1");
        RowMutation rm;

        // add data
        rm = new RowMutation("Keyspace1", "key1");
        addMutation(rm, "Super1", "SC1", 1, "asdf", 0);
        rm.apply();
        store.forceBlockingFlush();

        // remove
        rm = new RowMutation("Keyspace1", "key1");
        rm.delete(new QueryPath("Super1", "SC1".getBytes(), getBytes(1)), 1);
        rm.apply();

        ColumnFamily retrieved = store.getColumnFamily(new IdentityQueryFilter("key1", new QueryPath("Super1", "SC1".getBytes())));
        assert retrieved.getColumn("SC1".getBytes()).getSubColumn(getBytes(1)).isMarkedForDelete();
        assertNull(ColumnFamilyStore.removeDeleted(retrieved, Integer.MAX_VALUE));
    }
}