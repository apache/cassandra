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

import java.io.*;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.thrift.TException;

import org.apache.cassandra.client.*;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.*;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;

public abstract class TestBase
{
    private static final Logger logger = LoggerFactory.getLogger(TestBase.class);

    protected static CassandraServiceController controller =
        CassandraServiceController.getInstance();

    static class KeyspaceCreation
    {
        private String name;
        private int rf;
        private CfDef cfdef;
        public KeyspaceCreation(String name)
        {
            this.name = name;
            cfdef = new CfDef(name, "Standard1");
            cfdef.setComparator_type("BytesType");
            cfdef.setKey_cache_size(10000);
            cfdef.setRow_cache_size(1000);
            cfdef.setRow_cache_save_period_in_seconds(0);
            cfdef.setKey_cache_save_period_in_seconds(3600);
            cfdef.setMemtable_throughput_in_mb(255);
            cfdef.setMemtable_operations_in_millions(0.29);
        }

        public KeyspaceCreation validator(String validator)
        {
            cfdef.setDefault_validation_class(validator);
            return this;
        }

        public KeyspaceCreation rf(int rf)
        {
            this.rf = rf;
            return this;
        }

        public void create() throws Exception
        {
            List<InetAddress> hosts = controller.getHosts();
            Cassandra.Client client = controller.createClient(hosts.get(0));
            Map<String,String> stratOptions = new HashMap<String,String>();
            stratOptions.put("replication_factor", "" + rf);
            client.system_add_keyspace(new KsDef(name,
                                                 "org.apache.cassandra.locator.SimpleStrategy",
                                                 Arrays.asList(cfdef))
                                               .setStrategy_options(stratOptions));

            // poll, until KS added
            for (InetAddress host : hosts)
            {
                try
                {
                    client = controller.createClient(host);
                    poll:
                    while (true)
                    {
                        List<KsDef> ksDefList = client.describe_keyspaces();
                        for (KsDef ks : ksDefList)
                        {
                            if (ks.name.equals(name))
                                break poll;
                        }

                        try
                        {
                            Thread.sleep(1000);
                        }
                        catch (InterruptedException e)
                        {
                            break poll;
                        }
                    }
                }
                catch (TException te)
                {
                    continue;
                }
            }
        }
    }

    protected static KeyspaceCreation keyspace(String name)
    {
        return new KeyspaceCreation(name);
    }

    protected static void addKeyspace(String name, int rf) throws Exception
    {
        keyspace(name).rf(rf).create();
    }

    @BeforeClass
    public static void setUp() throws Exception
    {
        controller.ensureClusterRunning();
    }

    protected ByteBuffer newKey()
    {
        return ByteBuffer.wrap(String.format("test.key.%d", System.currentTimeMillis()).getBytes());
    }

    protected void insert(Cassandra.Client client, ByteBuffer key, String cf, String name, String value, long timestamp, ConsistencyLevel cl)
        throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        Column col = new Column(ByteBuffer.wrap(name.getBytes()))
            .setValue(ByteBuffer.wrap(value.getBytes()))
            .setTimestamp(timestamp);
        client.insert(key, new ColumnParent(cf), col, cl);
    }

    protected Column getColumn(Cassandra.Client client, ByteBuffer key, String cf, String col, ConsistencyLevel cl)
        throws InvalidRequestException, UnavailableException, TimedOutException, TException, NotFoundException
    {
        ColumnPath cpath = new ColumnPath(cf);
        cpath.setColumn(col.getBytes());
        return client.get(key, cpath, cl).column;
    }

    protected class Get extends RetryingAction<String>
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

    protected class Insert extends RetryingAction<String>
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
    protected abstract class RetryingAction<T>
    {
        protected Cassandra.Client client;
        protected String cf;
        protected ByteBuffer key;
        protected String name;
        protected T value;
        protected long timestamp;

        private Set<Class<Exception>> expected = new HashSet<Class<Exception>>();
        private long timeout = StorageService.RING_DELAY * 2;

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

        /** A parameterized value for the action. */
        public RetryingAction value(T value)
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
            logger.error(err, e);
            throw new AssertionError(err);
        }
        
        public String toString()
        {
            return this.getClass().getSimpleName() + "(" + key + "," + name + ")";
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
}
