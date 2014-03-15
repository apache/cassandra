package io.teknek.arizona;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.cassandra.Util;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ArrayBackedSortedColumns;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.db.filter.NamesQueryFilter;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.thrift.AuthenticationException;
import org.apache.cassandra.thrift.AuthenticationRequest;
import org.apache.cassandra.thrift.AuthorizationException;
import org.apache.cassandra.thrift.CASResult;
import org.apache.cassandra.thrift.CassandraServer;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.CfSplit;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CounterColumn;
import org.apache.cassandra.thrift.CqlPreparedResult;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.IndexClause;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.MultiSliceRequest;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.ThriftClientState;
import org.apache.cassandra.thrift.ThriftSessionManager;
import org.apache.cassandra.thrift.ThriftValidation;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.TokenRange;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.thrift.TException;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;

import io.teknek.arizona.*;
import io.teknek.arizona.Arizona.Iface;
import io.teknek.arizona.transform.FunctionalTransform;
import io.teknek.arizona.transform.SimpleTransformer;
import io.teknek.arizona.transform.Transformer;

public class ArizonaServer implements Iface  {

  public ThriftClientState state()
  {
      return ThriftSessionManager.instance.currentSession();
  }
  
  @Override
  public void login(AuthenticationRequest auth_request) throws AuthenticationException,
          AuthorizationException, TException {
    
    
  }

  @Override
  public void set_keyspace(String keyspace) throws InvalidRequestException, TException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public ColumnOrSuperColumn get(ByteBuffer key, ColumnPath column_path,
          ConsistencyLevel consistency_level) throws InvalidRequestException, NotFoundException,
          UnavailableException, TimedOutException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<ColumnOrSuperColumn> get_slice(ByteBuffer key, ColumnParent column_parent,
          SlicePredicate predicate, ConsistencyLevel consistency_level)
          throws InvalidRequestException, UnavailableException, TimedOutException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int get_count(ByteBuffer key, ColumnParent column_parent, SlicePredicate predicate,
          ConsistencyLevel consistency_level) throws InvalidRequestException, UnavailableException,
          TimedOutException, TException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public Map<ByteBuffer, List<ColumnOrSuperColumn>> multiget_slice(List<ByteBuffer> keys,
          ColumnParent column_parent, SlicePredicate predicate, ConsistencyLevel consistency_level)
          throws InvalidRequestException, UnavailableException, TimedOutException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Map<ByteBuffer, Integer> multiget_count(List<ByteBuffer> keys, ColumnParent column_parent,
          SlicePredicate predicate, ConsistencyLevel consistency_level)
          throws InvalidRequestException, UnavailableException, TimedOutException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<KeySlice> get_range_slices(ColumnParent column_parent, SlicePredicate predicate,
          KeyRange range, ConsistencyLevel consistency_level) throws InvalidRequestException,
          UnavailableException, TimedOutException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<KeySlice> get_paged_slice(String column_family, KeyRange range,
          ByteBuffer start_column, ConsistencyLevel consistency_level)
          throws InvalidRequestException, UnavailableException, TimedOutException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<KeySlice> get_indexed_slices(ColumnParent column_parent, IndexClause index_clause,
          SlicePredicate column_predicate, ConsistencyLevel consistency_level)
          throws InvalidRequestException, UnavailableException, TimedOutException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void insert(ByteBuffer key, ColumnParent column_parent, Column column,
          ConsistencyLevel consistency_level) throws InvalidRequestException, UnavailableException,
          TimedOutException, TException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void add(ByteBuffer key, ColumnParent column_parent, CounterColumn column,
          ConsistencyLevel consistency_level) throws InvalidRequestException, UnavailableException,
          TimedOutException, TException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public CASResult cas(ByteBuffer key, String column_family, List<Column> expected,
          List<Column> updates, ConsistencyLevel serial_consistency_level,
          ConsistencyLevel commit_consistency_level) throws InvalidRequestException,
          UnavailableException, TimedOutException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void remove(ByteBuffer key, ColumnPath column_path, long timestamp,
          ConsistencyLevel consistency_level) throws InvalidRequestException, UnavailableException,
          TimedOutException, TException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void remove_counter(ByteBuffer key, ColumnPath path, ConsistencyLevel consistency_level)
          throws InvalidRequestException, UnavailableException, TimedOutException, TException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void batch_mutate(Map<ByteBuffer, Map<String, List<Mutation>>> mutation_map,
          ConsistencyLevel consistency_level) throws InvalidRequestException, UnavailableException,
          TimedOutException, TException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void atomic_batch_mutate(Map<ByteBuffer, Map<String, List<Mutation>>> mutation_map,
          ConsistencyLevel consistency_level) throws InvalidRequestException, UnavailableException,
          TimedOutException, TException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void truncate(String cfname) throws InvalidRequestException, UnavailableException,
          TimedOutException, TException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public List<ColumnOrSuperColumn> get_multi_slice(MultiSliceRequest request)
          throws InvalidRequestException, UnavailableException, TimedOutException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Map<String, List<String>> describe_schema_versions() throws InvalidRequestException,
          TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<KsDef> describe_keyspaces() throws InvalidRequestException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String describe_cluster_name() throws TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String describe_version() throws TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<TokenRange> describe_ring(String keyspace) throws InvalidRequestException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<TokenRange> describe_local_ring(String keyspace) throws InvalidRequestException,
          TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Map<String, String> describe_token_map() throws InvalidRequestException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String describe_partitioner() throws TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String describe_snitch() throws TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public KsDef describe_keyspace(String keyspace) throws NotFoundException,
          InvalidRequestException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<String> describe_splits(String cfName, String start_token, String end_token,
          int keys_per_split) throws InvalidRequestException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ByteBuffer trace_next_query() throws TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<CfSplit> describe_splits_ex(String cfName, String start_token, String end_token,
          int keys_per_split) throws InvalidRequestException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String system_add_column_family(CfDef cf_def) throws InvalidRequestException,
          SchemaDisagreementException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String system_drop_column_family(String column_family) throws InvalidRequestException,
          SchemaDisagreementException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String system_add_keyspace(KsDef ks_def) throws InvalidRequestException,
          SchemaDisagreementException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String system_drop_keyspace(String keyspace) throws InvalidRequestException,
          SchemaDisagreementException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String system_update_keyspace(KsDef ks_def) throws InvalidRequestException,
          SchemaDisagreementException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String system_update_column_family(CfDef cf_def) throws InvalidRequestException,
          SchemaDisagreementException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public CqlResult execute_cql_query(ByteBuffer query, Compression compression)
          throws InvalidRequestException, UnavailableException, TimedOutException,
          SchemaDisagreementException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public CqlResult execute_cql3_query(ByteBuffer query, Compression compression,
          ConsistencyLevel consistency) throws InvalidRequestException, UnavailableException,
          TimedOutException, SchemaDisagreementException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public CqlPreparedResult prepare_cql_query(ByteBuffer query, Compression compression)
          throws InvalidRequestException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public CqlPreparedResult prepare_cql3_query(ByteBuffer query, Compression compression)
          throws InvalidRequestException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public CqlResult execute_prepared_cql_query(int itemId, List<ByteBuffer> values)
          throws InvalidRequestException, UnavailableException, TimedOutException,
          SchemaDisagreementException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public CqlResult execute_prepared_cql3_query(int itemId, List<ByteBuffer> values,
          ConsistencyLevel consistency) throws InvalidRequestException, UnavailableException,
          TimedOutException, SchemaDisagreementException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void set_cql_version(String version) throws InvalidRequestException, TException {
    // TODO Auto-generated method stub
    
  }
      
  @Override
  public FunctionalTransformResponse funcional_transform(FunctionalTransformRequest request)
          throws InvalidRequestException, UnavailableException, TimedOutException, TException {
    System.out.println("what up");
    try
    {
        ThriftClientState cState = state();
        cState.hasColumnFamilyAccess(cState.getKeyspace(), request.column_family, Permission.MODIFY, Permission.SELECT);
        CFMetaData metadata = ThriftValidation.validateColumnFamily(cState.getKeyspace(), request.column_family, false);
        ThriftValidation.validateKey(metadata, request.key);
        CFMetaData cfm = Schema.instance.getCFMetaData(cState.getKeyspace(), request.column_family);
        FunctionalTransform ft = new FunctionalTransform(request.getPredicate(), null, cfm, new SimpleTransformer());
        
        ColumnFamily result = null; //= ArizonaProxy.functional_transform(keyspaceName, cfName, key, transform, consistencyForPaxos, consistencyForCommit)
        FunctionalTransformResponse ret = new FunctionalTransformResponse();
        ret.setSuccess(true);
        ret.setCurrent_value(CassandraServer.thriftifyColumnsAsColumns(result.getSortedColumns(), System.currentTimeMillis()));
        return ret;
    } catch (Exception ex){
      
    }
    return null;
  }

}
