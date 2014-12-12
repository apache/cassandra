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


import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Iterators;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import org.apache.cassandra.stress.settings.StressSettings;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.thrift.TException;

public class SmartThriftClient implements ThriftClient
{

    final String keyspace;
    final Metadata metadata;
    final StressSettings settings;
    final ConcurrentHashMap<InetAddress, ConcurrentLinkedQueue<Client>> cache = new ConcurrentHashMap<>();

    final AtomicInteger queryIdCounter = new AtomicInteger();
    final ConcurrentHashMap<Integer, String> queryStrings = new ConcurrentHashMap<>();
    final ConcurrentHashMap<String, Integer> queryIds = new ConcurrentHashMap<>();
    final Set<InetAddress> whiteset;
    final List<InetAddress> whitelist;

    public SmartThriftClient(StressSettings settings, String keyspace, Metadata metadata)
    {
        this.metadata = metadata;
        this.keyspace = keyspace;
        this.settings = settings;
        if (!settings.node.isWhiteList)
        {
            whiteset = null;
            whitelist = null;
        }
        else
        {
            whiteset = settings.node.resolveAllSpecified();
            whitelist = Arrays.asList(whiteset.toArray(new InetAddress[0]));
        }
    }

    private final AtomicInteger roundrobin = new AtomicInteger();

    private Integer getId(String query)
    {
        Integer r;
        if ((r = queryIds.get(query)) != null)
            return r;
        r = queryIdCounter.incrementAndGet();
        if (queryIds.putIfAbsent(query, r) == null)
        {
            queryStrings.put(r, query);
            return r;
        }
        return queryIds.get(query);
    }

    final class Client
    {
        final Cassandra.Client client;
        final InetAddress server;
        final Map<Integer, Integer> queryMap = new HashMap<>();

        Client(Cassandra.Client client, InetAddress server)
        {
            this.client = client;
            this.server = server;
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
        Set<Host> hosts = metadata.getReplicas(metadata.quote(keyspace), pk);
        InetAddress address = null;
        if (hosts.size() > 0)
        {
            int pos = roundrobin.incrementAndGet() % hosts.size();
            for (int i = 0 ; address == null && i < hosts.size() ; i++)
            {
                if (pos < 0)
                    pos = -pos;
                Host host = Iterators.get(hosts.iterator(), (pos + i) % hosts.size());
                if (whiteset == null || whiteset.contains(host.getAddress()))
                    address = host.getAddress();
            }
        }
        if (address == null)
            address = whitelist.get(ThreadLocalRandom.current().nextInt(whitelist.size()));
        ConcurrentLinkedQueue<Client> q = cache.get(address);
        if (q == null)
        {
            ConcurrentLinkedQueue<Client> newQ = new ConcurrentLinkedQueue<Client>();
            q = cache.putIfAbsent(address, newQ);
            if (q == null)
                q = newQ;
        }
        Client tclient = q.poll();
        if (tclient != null)
            return tclient;
        return new Client(settings.getRawThriftClient(address.getHostAddress()), address);
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
                cache.get(client.server).add(client);
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
            cache.get(client.server).add(client);
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
            cache.get(client.server).add(client);
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
            cache.get(client.server).add(client);
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
            cache.get(client.server).add(client);
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
            cache.get(client.server).add(client);
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
            cache.get(client.server).add(client);
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
