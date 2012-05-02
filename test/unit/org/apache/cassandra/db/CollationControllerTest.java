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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.Test;

public class CollationControllerTest extends SchemaLoader
{
    @Test
    public void getTopLevelColumnsSkipsSSTablesModifiedBeforeRowDelete() 
            throws IOException, ExecutionException, InterruptedException
    {
        Table table = Table.open("Keyspace1");
        ColumnFamilyStore store = table.getColumnFamilyStore("Standard1");
        RowMutation rm;
        DecoratedKey dk = Util.dk("key1");
        QueryPath path = new QueryPath("Standard1", null, ByteBufferUtil.bytes("Column1"));
        
        // add data
        rm = new RowMutation("Keyspace1", dk.key);
        rm.add(path, ByteBufferUtil.bytes("asdf"), 0);
        rm.apply();
        store.forceBlockingFlush();
        
        // remove
        rm = new RowMutation("Keyspace1", dk.key);
        rm.delete(new QueryPath("Standard1"), 10);
        rm.apply();
        
        // add another mutation because sstable maxtimestamp isn't set
        // correctly during flush if the most recent mutation is a row delete
        rm = new RowMutation("Keyspace1", Util.dk("key2").key);
        rm.add(path, ByteBufferUtil.bytes("zxcv"), 20);
        rm.apply();
        
        store.forceBlockingFlush();

        // A NamesQueryFilter goes down one code path (through collectTimeOrderedData())
        QueryFilter filter = QueryFilter.getNamesFilter(dk, path, ByteBufferUtil.bytes("Column1"));
        CollationController controller = new CollationController(store, false, filter, Integer.MIN_VALUE);
        controller.getTopLevelColumns();
        assertEquals(1, controller.getSstablesIterated());
        
        // SliceQueryFilter goes down another path (through collectAllData())
        // Add another mutation, with a lower timestamp then force another flush 
        // so we can assert that we're not reading every sstable 
        rm = new RowMutation("Keyspace1", dk.key);
        rm.add(path, ByteBufferUtil.bytes("asdf"), 5);
        rm.apply();
        store.forceBlockingFlush();
        
        filter = QueryFilter.getIdentityFilter(dk, path);
        controller = new CollationController(store, false, filter, Integer.MIN_VALUE);
        controller.getTopLevelColumns();
        assertEquals(2, controller.getSstablesIterated());
    }
}
