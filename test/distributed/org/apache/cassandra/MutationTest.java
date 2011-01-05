/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.IOException;
import java.io.Writer;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.ArrayList;

import org.apache.cassandra.thrift.*;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.utils.WrappedRunnable;
import  org.apache.thrift.TException;
import org.apache.cassandra.client.*;
import org.apache.cassandra.dht.RandomPartitioner;

import org.apache.cassandra.CassandraServiceController.Failure;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;

public class MutationTest extends TestBase
{
    @Test
    public void testInsert() throws Exception
    {
        List<InetAddress> hosts = controller.getHosts();
        final String keyspace = "TestInsert";
        addKeyspace(keyspace, 3);
        Cassandra.Client client = controller.createClient(hosts.get(0));
        client.set_keyspace(keyspace);

        ByteBuffer key = newKey();

        insert(client, key, "Standard1", "c1", "v1", 0, ConsistencyLevel.ONE);
        insert(client, key, "Standard1", "c2", "v2", 0, ConsistencyLevel.ONE);


        assertColumnEqual("c1", "v1", 0, getColumn(client, key, "Standard1", "c1", ConsistencyLevel.ONE));
        assertColumnEqual("c2", "v2", 0, getColumn(client, key, "Standard1", "c2", ConsistencyLevel.ONE));

        List<ColumnOrSuperColumn> coscs = get_slice(client, key, "Standard1", ConsistencyLevel.ONE);
        assertColumnEqual("c1", "v1", 0, coscs.get(0).column);
        assertColumnEqual("c2", "v2", 0, coscs.get(1).column);
    }

    @Test
    public void testWriteAllReadOne() throws Exception
    {
        List<InetAddress> hosts = controller.getHosts();
        Cassandra.Client client = controller.createClient(hosts.get(0));

        final String keyspace = "TestWriteAllReadOne";
        addKeyspace(keyspace, 3);
        client.set_keyspace(keyspace);

        ByteBuffer key = newKey();

        insert(client, key, "Standard1", "c1", "v1", 0, ConsistencyLevel.ALL);
        assertColumnEqual("c1", "v1", 0, getColumn(client, key, "Standard1", "c1", ConsistencyLevel.ONE));

        List<InetAddress> endpoints = endpointsForKey(hosts.get(0), key, keyspace);
        InetAddress coordinator = nonEndpointForKey(hosts.get(0), key, keyspace);
        Failure failure = controller.failHosts(endpoints.subList(1, endpoints.size()));

        Thread.sleep(10000); // let gossip catch up

        try {
            client = controller.createClient(coordinator);
            client.set_keyspace(keyspace);

            assertColumnEqual("c1", "v1", 0, getColumn(client, key, "Standard1", "c1", ConsistencyLevel.ONE));

            insert(client, key, "Standard1", "c3", "v3", 0, ConsistencyLevel.ALL);
            assert false;
        } catch (UnavailableException e) {
            // [this is good]
        } finally {
            failure.resolve();
            Thread.sleep(10000);
        }
    }

    @Test
    public void testWriteQuorumReadQuorum() throws Exception
    {
        List<InetAddress> hosts = controller.getHosts();
        Cassandra.Client client = controller.createClient(hosts.get(0));

        final String keyspace = "TestWriteQuorumReadQuorum";
        addKeyspace(keyspace, 3);
        client.set_keyspace(keyspace);

        ByteBuffer key = newKey();

        // with quorum-1 nodes up
        List<InetAddress> endpoints = endpointsForKey(hosts.get(0), key, keyspace);
        InetAddress coordinator = nonEndpointForKey(hosts.get(0), key, keyspace);
        Failure failure = controller.failHosts(endpoints.subList(1, endpoints.size())); //kill all but one nodes

        Thread.sleep(10000);
        client = controller.createClient(coordinator);
        client.set_keyspace(keyspace);
        try {
            insert(client, key, "Standard1", "c1", "v1", 0, ConsistencyLevel.QUORUM);
            assert false;
        } catch (UnavailableException e) {
            // [this is good]
        } finally {
            failure.resolve();
            Thread.sleep(10000);
        }

        // with all nodes up
        insert(client, key, "Standard1", "c2", "v2", 0, ConsistencyLevel.QUORUM);

        failure = controller.failHosts(endpoints.get(0));
        Thread.sleep(10000);
        try {
            getColumn(client, key, "Standard1", "c2", ConsistencyLevel.QUORUM);
        } finally {
            failure.resolve();
            Thread.sleep(10000);
        }
    }

    @Test
    public void testWriteOneReadAll() throws Exception
    {
        List<InetAddress> hosts = controller.getHosts();
        Cassandra.Client client = controller.createClient(hosts.get(0));

        final String keyspace = "TestWriteOneReadAll";
        addKeyspace(keyspace, 3);
        client.set_keyspace(keyspace);

        ByteBuffer key = newKey();

        List<InetAddress> endpoints = endpointsForKey(hosts.get(0), key, keyspace);
        InetAddress coordinator = nonEndpointForKey(hosts.get(0), key, keyspace);
        client = controller.createClient(coordinator);
        client.set_keyspace(keyspace);

        insert(client, key, "Standard1", "c1", "v1", 0, ConsistencyLevel.ONE);
        assertColumnEqual("c1", "v1", 0, getColumn(client, key, "Standard1", "c1", ConsistencyLevel.ALL));

        // with each of HH, read repair and proactive repair:
            // with one node up
            // write with one (success)
            // read with all (failure)
            // bring nodes up
            // repair
            // read with all (success)

        Failure failure = controller.failHosts(endpoints);
        Thread.sleep(10000);
        try {
            insert(client, key, "Standard1", "c2", "v2", 0, ConsistencyLevel.ONE);
            assert false;
        } catch (UnavailableException e) {
            // this is good
        } finally {
            failure.resolve();
        }
    }

    protected void insert(Cassandra.Client client, ByteBuffer key, String cf, String name, String value, long timestamp, ConsistencyLevel cl)
        throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        Column col = new Column(
             ByteBuffer.wrap(name.getBytes()),
             ByteBuffer.wrap(value.getBytes()),
             timestamp
             );
        client.insert(key, new ColumnParent(cf), col, cl);
    }

    protected Column getColumn(Cassandra.Client client, ByteBuffer key, String cf, String col, ConsistencyLevel cl)
        throws InvalidRequestException, UnavailableException, TimedOutException, TException, NotFoundException
    {
        ColumnPath cpath = new ColumnPath(cf);
        cpath.setColumn(col.getBytes());
        return client.get(key, cpath, cl).column;
    }

    protected List<ColumnOrSuperColumn> get_slice(Cassandra.Client client, ByteBuffer key, String cf, ConsistencyLevel cl)
      throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        SlicePredicate sp = new SlicePredicate();
        sp.setSlice_range(
            new SliceRange(
                ByteBuffer.wrap(new byte[0]),
                ByteBuffer.wrap(new byte[0]),
                false,
                1000
                )
            );
        return client.get_slice(key, new ColumnParent(cf), sp, cl);
    }

    protected void assertColumnEqual(String name, String value, long timestamp, Column col)
    {
        assertEquals(ByteBuffer.wrap(name.getBytes()), col.name);
        assertEquals(ByteBuffer.wrap(value.getBytes()), col.value);
        assertEquals(timestamp, col.timestamp);
    }

    protected List<InetAddress> endpointsForKey(InetAddress seed, ByteBuffer key, String keyspace)
        throws IOException
    {
        RingCache ring = new RingCache(keyspace, new RandomPartitioner(), seed.getHostAddress(), 9160);
        List<InetAddress> privateendpoints = ring.getEndpoint(key);
        List<InetAddress> endpoints = new ArrayList<InetAddress>();
        for (InetAddress endpoint : privateendpoints)
        {
            endpoints.add(controller.getPublicHost(endpoint));
        }
        return endpoints;
    }

    protected InetAddress nonEndpointForKey(InetAddress seed, ByteBuffer key, String keyspace)
        throws IOException
    {
        List<InetAddress> endpoints = endpointsForKey(seed, key, keyspace);
        for (InetAddress host : controller.getHosts())
        {
            if (!endpoints.contains(host))
            {
                return host;
            }
        }
        return null;
    }

    protected ByteBuffer newKey()
    {
        return ByteBuffer.wrap(String.format("test.key.%d", System.currentTimeMillis()).getBytes());
    }
}
