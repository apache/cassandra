package org.apache.cassandra.stress.util;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.*;
import org.apache.thrift.TException;

public interface ThriftClient
{

    public void batch_mutate(Map<ByteBuffer, Map<String, List<Mutation>>> record, ConsistencyLevel consistencyLevel) throws TException;

    List<ColumnOrSuperColumn> get_slice(ByteBuffer key, ColumnParent parent, SlicePredicate predicate, ConsistencyLevel consistencyLevel) throws InvalidRequestException, UnavailableException, TimedOutException, TException;

    void insert(ByteBuffer key, ColumnParent column_parent, Column column, ConsistencyLevel consistency_level) throws InvalidRequestException, UnavailableException, TimedOutException, TException;

    Map<ByteBuffer, List<ColumnOrSuperColumn>> multiget_slice(List<ByteBuffer> keys, ColumnParent column_parent, SlicePredicate predicate, ConsistencyLevel consistency_level) throws InvalidRequestException, UnavailableException, TimedOutException, TException;

    List<KeySlice> get_range_slices(ColumnParent column_parent, SlicePredicate predicate, KeyRange range, ConsistencyLevel consistency_level) throws InvalidRequestException, UnavailableException, TimedOutException, TException;

    List<KeySlice> get_indexed_slices(ColumnParent column_parent, IndexClause index_clause, SlicePredicate column_predicate, ConsistencyLevel consistency_level) throws InvalidRequestException, UnavailableException, TimedOutException, TException;

    Integer prepare_cql3_query(String query, Compression compression) throws InvalidRequestException, TException;

    CqlResult execute_prepared_cql3_query(int itemId, ByteBuffer key, List<ByteBuffer> values, ConsistencyLevel consistency) throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException;

    CqlResult execute_cql_query(String query, ByteBuffer key, Compression compression) throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException;

    CqlResult execute_cql3_query(String query, ByteBuffer key, Compression compression, ConsistencyLevel consistency) throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException;

    Integer prepare_cql_query(String query, Compression compression) throws InvalidRequestException, TException;

    CqlResult execute_prepared_cql_query(int itemId, ByteBuffer key, List<ByteBuffer> values) throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException;
}
