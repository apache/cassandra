package org.apache.cassandra.stress.util;
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


import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.thrift.TException;

public class SimpleThriftClient implements ThriftClient
{

    final Cassandra.Client client;
    public SimpleThriftClient(Cassandra.Client client)
    {
        this.client = client;
    }

    public void batch_mutate(Map<ByteBuffer, Map<String, List<Mutation>>> record, ConsistencyLevel consistencyLevel) throws TException
    {
        client.batch_mutate(record, consistencyLevel);
    }

    @Override
    public List<ColumnOrSuperColumn> get_slice(ByteBuffer key, ColumnParent column_parent, SlicePredicate predicate, ConsistencyLevel consistency_level) throws TException
    {
        return client.get_slice(key, column_parent, predicate, consistency_level);
    }

    @Override
    public List<KeySlice> get_indexed_slices(ColumnParent column_parent, IndexClause index_clause, SlicePredicate column_predicate, ConsistencyLevel consistency_level) throws TException
    {
        return client.get_indexed_slices(column_parent, index_clause, column_predicate, consistency_level);
    }

    @Override
    public List<KeySlice> get_range_slices(ColumnParent column_parent, SlicePredicate predicate, KeyRange range, ConsistencyLevel consistency_level) throws TException
    {
        return client.get_range_slices(column_parent, predicate, range, consistency_level);
    }

    @Override
    public Map<ByteBuffer, List<ColumnOrSuperColumn>> multiget_slice(List<ByteBuffer> keys, ColumnParent column_parent, SlicePredicate predicate, ConsistencyLevel consistency_level) throws TException
    {
        return client.multiget_slice(keys, column_parent, predicate, consistency_level);
    }

    @Override
    public void insert(ByteBuffer key, ColumnParent column_parent, Column column, ConsistencyLevel consistency_level) throws TException
    {
        client.insert(key, column_parent, column, consistency_level);
    }

    @Override
    public Integer prepare_cql3_query(String query, Compression compression) throws TException
    {
        return client.prepare_cql3_query(ByteBufferUtil.bytes(query), compression).itemId;
    }

    @Override
    public CqlResult execute_prepared_cql_query(int itemId, ByteBuffer key, List<ByteBuffer> values) throws TException
    {
        return client.execute_prepared_cql_query(itemId, values);
    }

    @Override
    public Integer prepare_cql_query(String query, Compression compression) throws InvalidRequestException, TException
    {
        return client.prepare_cql_query(ByteBufferUtil.bytes(query), compression).itemId;
    }

    @Override
    public CqlResult execute_cql3_query(String query, ByteBuffer key, Compression compression, ConsistencyLevel consistency) throws TException
    {
        return client.execute_cql3_query(ByteBufferUtil.bytes(query), compression, consistency);
    }

    @Override
    public CqlResult execute_prepared_cql3_query(int itemId, ByteBuffer key, List<ByteBuffer> values, ConsistencyLevel consistency) throws TException
    {
        return client.execute_prepared_cql3_query(itemId, values, consistency);
    }

    @Override
    public CqlResult execute_cql_query(String query, ByteBuffer key, Compression compression) throws TException
    {
        return client.execute_cql_query(ByteBufferUtil.bytes(query), compression);
    }
}
