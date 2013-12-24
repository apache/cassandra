package org.apache.cassandra.stress.util;

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
    public List<ColumnOrSuperColumn> get_slice(ByteBuffer key, ColumnParent column_parent, SlicePredicate predicate, ConsistencyLevel consistency_level) throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        return client.get_slice(key, column_parent, predicate, consistency_level);
    }

    @Override
    public List<KeySlice> get_indexed_slices(ColumnParent column_parent, IndexClause index_clause, SlicePredicate column_predicate, ConsistencyLevel consistency_level) throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        return client.get_indexed_slices(column_parent, index_clause, column_predicate, consistency_level);
    }

    @Override
    public List<KeySlice> get_range_slices(ColumnParent column_parent, SlicePredicate predicate, KeyRange range, ConsistencyLevel consistency_level) throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        return client.get_range_slices(column_parent, predicate, range, consistency_level);
    }

    @Override
    public Map<ByteBuffer, List<ColumnOrSuperColumn>> multiget_slice(List<ByteBuffer> keys, ColumnParent column_parent, SlicePredicate predicate, ConsistencyLevel consistency_level) throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        return client.multiget_slice(keys, column_parent, predicate, consistency_level);
    }

    @Override
    public void insert(ByteBuffer key, ColumnParent column_parent, Column column, ConsistencyLevel consistency_level) throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        client.insert(key, column_parent, column, consistency_level);
    }

    @Override
    public Integer prepare_cql3_query(String query, Compression compression) throws InvalidRequestException, TException
    {
        return client.prepare_cql3_query(ByteBufferUtil.bytes(query), compression).itemId;
    }

    @Override
    public CqlResult execute_prepared_cql_query(int itemId, ByteBuffer key, List<ByteBuffer> values) throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException
    {
        return client.execute_prepared_cql_query(itemId, values);
    }

    @Override
    public Integer prepare_cql_query(String query, Compression compression) throws InvalidRequestException, TException
    {
        return client.prepare_cql_query(ByteBufferUtil.bytes(query), compression).itemId;
    }

    @Override
    public CqlResult execute_cql3_query(String query, ByteBuffer key, Compression compression, ConsistencyLevel consistency) throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException
    {
        return client.execute_cql3_query(ByteBufferUtil.bytes(query), compression, consistency);
    }

    @Override
    public CqlResult execute_prepared_cql3_query(int itemId, ByteBuffer key, List<ByteBuffer> values, ConsistencyLevel consistency) throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException
    {
        return client.execute_prepared_cql3_query(itemId, values, consistency);
    }

    @Override
    public CqlResult execute_cql_query(String query, ByteBuffer key, Compression compression) throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException
    {
        return client.execute_cql_query(ByteBufferUtil.bytes(query), compression);
    }
}
