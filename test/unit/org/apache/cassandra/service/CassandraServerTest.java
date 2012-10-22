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
package org.apache.cassandra.service;

import java.net.InetSocketAddress;

import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;

public class CassandraServerTest extends SchemaLoader
{
    /**
     * test get_count() to work correctly with 'count' settings around page size.
     * (CASSANDRA-4833)
     */
    @Test
    public void test_get_count() throws Exception
    {
        Schema.instance.clear(); // Schema are now written on disk and will be reloaded
        new EmbeddedCassandraService().start();
        ThriftSessionManager.instance.setCurrentSocket(new InetSocketAddress(9160));

        DecoratedKey key = Util.dk("testkey");
        for (int i = 0; i < 3050; i++)
        {
            RowMutation rm = new RowMutation("Keyspace1", key.key);
            rm.add(new QueryPath("Standard1", null, ByteBufferUtil.bytes(String.valueOf(i))),
                          ByteBufferUtil.EMPTY_BYTE_BUFFER,
                          System.currentTimeMillis());
            rm.apply();
        }

        CassandraServer server = new CassandraServer();
        server.set_keyspace("Keyspace1");

        // same as page size
        int count = server.get_count(key.key, new ColumnParent("Standard1"), predicateWithCount(1024), ConsistencyLevel.ONE);
        assert count == 1024 : "expected 1024 but was " + count;

        // 1 above page size
        count = server.get_count(key.key, new ColumnParent("Standard1"), predicateWithCount(1025), ConsistencyLevel.ONE);
        assert count == 1025 : "expected 1025 but was " + count;

        // above number of columns
        count = server.get_count(key.key, new ColumnParent("Standard1"), predicateWithCount(4000), ConsistencyLevel.ONE);
        assert count == 3050 : "expected 3050 but was " + count;

        // same as number of columns
        count = server.get_count(key.key, new ColumnParent("Standard1"), predicateWithCount(3050), ConsistencyLevel.ONE);
        assert count == 3050 : "expected 3050 but was " + count;

        // 1 above number of columns
        count = server.get_count(key.key, new ColumnParent("Standard1"), predicateWithCount(3051), ConsistencyLevel.ONE);
        assert count == 3050 : "expected 3050 but was " + count;
    }

    private SlicePredicate predicateWithCount(int count)
    {
        SliceRange range = new SliceRange();
        range.setStart("".getBytes());
        range.setFinish("".getBytes());
        range.setCount(count);
        SlicePredicate predicate = new SlicePredicate();
        predicate.setSlice_range(range);
        return predicate;
    }
}
