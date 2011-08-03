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

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.client.RingCache;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.WrappedRunnable;
import org.apache.thrift.TException;

import org.apache.cassandra.CassandraServiceController.Failure;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;

public class MutationTest extends TestBase
{
    private static final Logger logger = LoggerFactory.getLogger(MutationTest.class);

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

        // block until the column is available
        new Get(client, "Standard1", key).name("c1").value("v1").perform(ConsistencyLevel.ONE);
        new Get(client, "Standard1", key).name("c2").value("v2").perform(ConsistencyLevel.ONE);

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
        // should be instantly available
        assertColumnEqual("c1", "v1", 0, getColumn(client, key, "Standard1", "c1", ConsistencyLevel.ONE));

        List<InetAddress> endpoints = endpointsForKey(hosts.get(0), key, keyspace);
        InetAddress coordinator = nonEndpointForKey(hosts.get(0), key, keyspace);
        Failure failure = controller.failHosts(endpoints.subList(1, endpoints.size()));

        try {
            client = controller.createClient(coordinator);
            client.set_keyspace(keyspace);

            new Get(client, "Standard1", key).name("c1").value("v1")
                .perform(ConsistencyLevel.ONE);

            new Insert(client, "Standard1", key).name("c3").value("v3")
                .expecting(UnavailableException.class).perform(ConsistencyLevel.ALL);
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

        client = controller.createClient(coordinator);
        client.set_keyspace(keyspace);
        try {
            new Insert(client, "Standard1", key).name("c1").value("v1")
                .expecting(UnavailableException.class).perform(ConsistencyLevel.QUORUM);
        } finally {
            failure.resolve();
            Thread.sleep(10000);
        }

        // with all nodes up
        new Insert(client, "Standard1", key).name("c2").value("v2").perform(ConsistencyLevel.QUORUM);

        failure = controller.failHosts(endpoints.get(0));
        try {
            new Get(client, "Standard1", key).name("c2").value("v2").perform(ConsistencyLevel.QUORUM);
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
        try {
            new Insert(client, "Standard1", key).name("c2").value("v2")
                .expecting(UnavailableException.class).perform(ConsistencyLevel.ONE);
        } finally {
            failure.resolve();
        }
    }

    protected ByteBuffer newKey()
    {
        return ByteBufferUtil.bytes(String.format("test.key.%d", System.currentTimeMillis()));
    }
}
