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
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;

import org.junit.Test;

import static junit.framework.Assert.assertNull;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import static org.apache.cassandra.Util.getBytes;
import org.apache.cassandra.Util;
import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.utils.ByteBufferUtil;


public class RemoveSubColumnTest extends CleanupHelper
{
    @Test
    public void testRemoveSubColumn() throws IOException, ExecutionException, InterruptedException
    {
        Table table = Table.open("Keyspace1");
        ColumnFamilyStore store = table.getColumnFamilyStore("Super1");
        RowMutation rm;
        DecoratedKey dk = Util.dk("key1");

        // add data
        rm = new RowMutation("Keyspace1", dk.key);
        Util.addMutation(rm, "Super1", "SC1", 1, "asdf", 0);
        rm.apply();
        store.forceBlockingFlush();

        // remove
        rm = new RowMutation("Keyspace1", dk.key);
        rm.delete(new QueryPath("Super1", ByteBufferUtil.bytes("SC1"), getBytes(1L)), 1);
        rm.apply();

        ColumnFamily retrieved = store.getColumnFamily(QueryFilter.getIdentityFilter(dk, new QueryPath("Super1", ByteBufferUtil.bytes("SC1"))));
        assert retrieved.getColumn(ByteBufferUtil.bytes("SC1")).getSubColumn(getBytes(1L)).isMarkedForDelete();
        assertNull(Util.cloneAndRemoveDeleted(retrieved, Integer.MAX_VALUE));
    }
}
