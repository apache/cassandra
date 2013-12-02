package org.apache.cassandra.stress.util;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import org.apache.cassandra.stress.settings.StressSettings;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.IndexClause;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.thrift.TException;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class SmartThriftClient implements ThriftClient
{

    final String keyspace;
    final Metadata metadata;
    final StressSettings settings;
    final ConcurrentHashMap<Host, ConcurrentLinkedQueue<Client>> cache = new ConcurrentHashMap<>();

    final AtomicInteger queryIdCounter = new AtomicInteger();
    final ConcurrentHashMap<Integer, String> queryStrings = new ConcurrentHashMap<>();
    final ConcurrentHashMap<String, Integer> queryIds = new ConcurrentHashMap<>();

    public SmartThriftClient(StressSettings settings, String keyspace, Metadata metadata)
    {
        this.metadata = metadata;
        this.keyspace = keyspace;
        this.settings = settings;
    }

    private final AtomicInteger roundrobin = new AtomicInteger();

    private Integer getId(String query)
    {
        Integer r;
        if ((r = queryIds.get(query)) != null)
            return r;
        r = queryIdCounter.incrementAndGet();
        if (queryIds.putIfAbsent(query, r) == null)
            return r;
        queryStrings.put(r, query);
        return queryIds.get(query);
    }

    final class Client
    {
        final Cassandra.Client client;
        final Host host;
        final Map<Integer, Integer> queryMap = new HashMap<>();

        Client(Cassandra.Client client, Host host)
        {
            this.client = client;
            this.host = host;
        }

        Integer get(Integer id, boolean cql3) throws TException
        {
            Integer serverId = queryMap.get(id);
            if (serverId != null)
                return serverId;
            prepare(id, cql3);
            return queryMap.get(id);
        }

       void prepare(Integer id, boolean cql3) throws TException
       {
           String query;
           while ( null == (query = queryStrings.get(id)) ) ;
           if (cql3)
           {
               Integer serverId = client.prepare_cql3_query(ByteBufferUtil.bytes(query), Compression.NONE).itemId;
               queryMap.put(id, serverId);
           }
           else
           {
               Integer serverId = client.prepare_cql_query(ByteBufferUtil.bytes(query), Compression.NONE).itemId;
               queryMap.put(id, serverId);
           }
       }
    }

    private Client get(ByteBuffer pk)
    {
        Set<Host> hosts = metadata.getReplicas(keyspace, pk);
        int count = roundrobin.incrementAndGet() % hosts.size();
        if (count < 0)
            count = -count;
        Iterator<Host> iter = hosts.iterator();
        while (count > 0 && iter.hasNext())
            iter.next();
        Host host = iter.next();
        ConcurrentLinkedQueue<Client> q = cache.get(host);
        if (q == null)
        {
            cache.putIfAbsent(host, new ConcurrentLinkedQueue<Client>());
            q = cache.get(host);
        }
        Client tclient = q.poll();
        if (tclient != null)
            return tclient;
        return new Client(settings.getRawThriftClient(host.getAddress().getHostAddress()), host);
    }

    @Override
    public void batch_mutate(Map<ByteBuffer, Map<String, List<Mutation>>> record, ConsistencyLevel consistencyLevel) throws TException
    {
        for (Map.Entry<ByteBuffer, Map<String, List<Mutation>>> e : record.entrySet())
        {
            Client client = get(e.getKey());
            try
            {
                client.client.batch_mutate(Collections.singletonMap(e.getKey(), e.getValue()), consistencyLevel);
            } finally
            {
                cache.get(client.host).add(client);
            }
        }
    }

    @Override
    public List<ColumnOrSuperColumn> get_slice(ByteBuffer key, ColumnParent parent, SlicePredicate predicate, ConsistencyLevel consistencyLevel) throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        Client client = get(key);
        try
        {
            return client.client.get_slice(key, parent, predicate, consistencyLevel);
        } finally
        {
            cache.get(client.host).add(client);
        }
    }

    @Override
    public void insert(ByteBuffer key, ColumnParent column_parent, Column column, ConsistencyLevel consistency_level) throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        Client client = get(key);
        try
        {
            client.client.insert(key, column_parent, column, consistency_level);
        } finally
        {
            cache.get(client.host).add(client);
        }
    }

    @Override
    public CqlResult execute_cql_query(String query, ByteBuffer key, Compression compression) throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException
    {
        Client client = get(key);
        try
        {
            return client.client.execute_cql_query(ByteBufferUtil.bytes(query), compression);
        } finally
        {
            cache.get(client.host).add(client);
        }
    }

    @Override
    public CqlResult execute_cql3_query(String query, ByteBuffer key, Compression compression, ConsistencyLevel consistency) throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException
    {
        Client client = get(key);
        try
        {
            return client.client.execute_cql3_query(ByteBufferUtil.bytes(query), compression, consistency);
        } finally
        {
            cache.get(client.host).add(client);
        }
    }

    @Override
    public Integer prepare_cql3_query(String query, Compression compression) throws InvalidRequestException, TException
    {
        return getId(query);
    }

    @Override
    public CqlResult execute_prepared_cql3_query(int queryId, ByteBuffer key, List<ByteBuffer> values, ConsistencyLevel consistency) throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException
    {
        Client client = get(key);
        try
        {
            return client.client.execute_prepared_cql3_query(client.get(queryId, true), values, consistency);
        } finally
        {
            cache.get(client.host).add(client);
        }
    }

    @Override
    public Integer prepare_cql_query(String query, Compression compression) throws InvalidRequestException, TException
    {
        return getId(query);
    }

    @Override
    public CqlResult execute_prepared_cql_query(int queryId, ByteBuffer key, List<ByteBuffer> values) throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException
    {
        Client client = get(key);
        try
        {
            return client.client.execute_prepared_cql_query(client.get(queryId, true), values);
        } finally
        {
            cache.get(client.host).add(client);
        }
    }

    @Override
    public Map<ByteBuffer, List<ColumnOrSuperColumn>> multiget_slice(List<ByteBuffer> keys, ColumnParent column_parent, SlicePredicate predicate, ConsistencyLevel consistency_level) throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<KeySlice> get_range_slices(ColumnParent column_parent, SlicePredicate predicate, KeyRange range, ConsistencyLevel consistency_level) throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<KeySlice> get_indexed_slices(ColumnParent column_parent, IndexClause index_clause, SlicePredicate column_predicate, ConsistencyLevel consistency_level) throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        throw new UnsupportedOperationException();
    }

}
