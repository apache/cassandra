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
import static org.junit.Assert.assertTrue;

import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.utils.ByteBufferUtil;


public class RemoveColumnTest extends SchemaLoader
{
    @Test
    public void testRemoveColumn() throws IOException, ExecutionException, InterruptedException
    {
        Table table = Table.open("Keyspace1");
        ColumnFamilyStore store = table.getColumnFamilyStore("Standard1");
        RowMutation rm;
        DecoratedKey dk = Util.dk("key1");

        // add data
        rm = new RowMutation("Keyspace1", dk.key);
        rm.add(new QueryPath("Standard1", null, ByteBufferUtil.bytes("Column1")), ByteBufferUtil.bytes("asdf"), 0);
        rm.apply();
        store.forceBlockingFlush();

        // remove
        rm = new RowMutation("Keyspace1", dk.key);
        rm.delete(new QueryPath("Standard1", null, ByteBufferUtil.bytes("Column1")), 1);
        rm.apply();

        ColumnFamily retrieved = store.getColumnFamily(QueryFilter.getNamesFilter(dk, new QueryPath("Standard1"), ByteBufferUtil.bytes("Column1")));
        assert retrieved.getColumn(ByteBufferUtil.bytes("Column1")).isMarkedForDelete();
        assertNull(Util.cloneAndRemoveDeleted(retrieved, Integer.MAX_VALUE));
        assertNull(Util.cloneAndRemoveDeleted(store.getColumnFamily(QueryFilter.getIdentityFilter(dk, new QueryPath("Standard1"))), Integer.MAX_VALUE));
    }

    @Test
    public void deletedColumnShouldAlwaysBeMarkedForDelete()
    {
        // Check for bug in #4307
        long timestamp = System.currentTimeMillis();
        int localDeletionTime = (int) (timestamp / 1000);
        Column c = DeletedColumn.create(localDeletionTime, timestamp, "dc1");
        assertTrue("DeletedColumn was not marked for delete", c.isMarkedForDelete());

        // Simulate a node that is 30 seconds behind
        c = DeletedColumn.create(localDeletionTime + 30, timestamp + 30000, "dc2");
        assertTrue("DeletedColumn was not marked for delete", c.isMarkedForDelete());

        // Simulate a node that is 30 ahead behind
        c = DeletedColumn.create(localDeletionTime - 30, timestamp - 30000, "dc3");
        assertTrue("DeletedColumn was not marked for delete", c.isMarkedForDelete());
    }

}
