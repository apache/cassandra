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
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.thrift.*;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.utils.WrappedRunnable;
import org.apache.thrift.TException;
import org.apache.cassandra.client.*;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.service.StorageService;

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

    protected class Get extends RetryingAction
    {
        public Get(Cassandra.Client client, String cf, ByteBuffer key)
        {
            super(client, cf, key);
        }

        public void tryPerformAction(ConsistencyLevel cl) throws Exception
        {
            assertColumnEqual(name, value, timestamp, getColumn(client, key, cf, name, cl));
        }
    }

    protected class Insert extends RetryingAction
    {
        public Insert(Cassandra.Client client, String cf, ByteBuffer key)
        {
            super(client, cf, key);
        }

        public void tryPerformAction(ConsistencyLevel cl) throws Exception
        {
            insert(client, key, cf, name, value, timestamp, cl);
        }
    }

    /** Performs an action repeatedly until timeout, success or failure. */
    protected abstract class RetryingAction
    {
        protected Cassandra.Client client;
        protected String cf;
        protected ByteBuffer key;
        protected String name;
        protected String value;
        protected long timestamp;

        private Set<Class<Exception>> expected = new HashSet<Class<Exception>>();
        private long timeout = StorageService.RING_DELAY;

        public RetryingAction(Cassandra.Client client, String cf, ByteBuffer key)
        {
            this.client = client;
            this.cf = cf;
            this.key = key;
            this.timestamp = 0;
        }

        public RetryingAction name(String name)
        {
            this.name = name; return this;
        }

        /** The value to expect for the return column, or null to expect the column to be missing. */
        public RetryingAction value(String value)
        {
            this.value = value; return this;
        }
        
        /** The total time to allow before failing. */
        public RetryingAction timeout(long timeout)
        {
            this.timeout = timeout; return this;
        }

        /** The expected timestamp of the returned column. */
        public RetryingAction timestamp(long timestamp)
        {
            this.timestamp = timestamp; return this;
        }

        /** The exception classes that indicate success. */
        public RetryingAction expecting(Class... tempExceptions)
        {
            this.expected.clear();
            for (Class exclass : tempExceptions)
                expected.add((Class<Exception>)exclass);
            return this;
        }

        public void perform(ConsistencyLevel cl) throws AssertionError
        {
            long deadline = System.currentTimeMillis() + timeout;
            int attempts = 0;
            String template = "%s for " + this + " after %d attempt(s) with %d ms to spare.";
            Exception e = null;
            while(deadline > System.currentTimeMillis())
            {
                try
                {
                    attempts++;
                    tryPerformAction(cl);
                    logger.info(String.format(template, "Succeeded", attempts, deadline - System.currentTimeMillis()));
                    return;
                }
                catch (Exception ex)
                {
                    e = ex;
                    if (!expected.contains(ex.getClass()))
                        continue;
                    logger.info(String.format(template, "Caught expected exception: " + e, attempts, deadline - System.currentTimeMillis()));
                    return;
                }
            }
            String err = String.format(template, "Caught unexpected: " + e, attempts, deadline - System.currentTimeMillis());
            logger.error(err);
            throw new AssertionError(err);
        }
        
        public String toString()
        {
            return this.getClass() + "(" + key + "," + name + ")";
        }

        protected abstract void tryPerformAction(ConsistencyLevel cl) throws Exception;
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
