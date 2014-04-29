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

import org.junit.Test;

import static org.junit.Assert.assertNull;
import org.apache.cassandra.db.filter.QueryFilter;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.utils.ByteBufferUtil;


public class RemoveColumnFamilyTest extends SchemaLoader
{
    @Test
    public void testRemoveColumnFamily()
    {
        Keyspace keyspace = Keyspace.open("Keyspace1");
        ColumnFamilyStore store = keyspace.getColumnFamilyStore("Standard1");
        Mutation rm;
        DecoratedKey dk = Util.dk("key1");

        // add data
        rm = new Mutation("Keyspace1", dk.getKey());
        rm.add("Standard1", Util.cellname("Column1"), ByteBufferUtil.bytes("asdf"), 0);
        rm.apply();

        // remove
        rm = new Mutation("Keyspace1", dk.getKey());
        rm.delete("Standard1", 1);
        rm.apply();

        ColumnFamily retrieved = store.getColumnFamily(QueryFilter.getIdentityFilter(dk, "Standard1", System.currentTimeMillis()));
        assert retrieved.isMarkedForDelete();
        assertNull(retrieved.getColumn(Util.cellname("Column1")));
        assertNull(Util.cloneAndRemoveDeleted(retrieved, Integer.MAX_VALUE));
    }
}
