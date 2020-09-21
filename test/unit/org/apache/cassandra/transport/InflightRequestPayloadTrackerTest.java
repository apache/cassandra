/*
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

package org.apache.cassandra.transport;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.transport.messages.QueryMessage;

@RunWith(OrderedJUnit4ClassRunner.class)
public class InflightRequestPayloadTrackerTest extends CQLTester
{

    private static long LOW_LIMIT = 600L;
    private static long HIGH_LIMIT = 5000000000L;

    private static final QueryOptions V5_DEFAULT_OPTIONS = QueryOptions.create(
    QueryOptions.DEFAULT.getConsistency(),
    QueryOptions.DEFAULT.getValues(),
    QueryOptions.DEFAULT.skipMetadata(),
    QueryOptions.DEFAULT.getPageSize(),
    QueryOptions.DEFAULT.getPagingState(),
    QueryOptions.DEFAULT.getSerialConsistency(),
    ProtocolVersion.V5,
    KEYSPACE);

    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.setNativeTransportMaxConcurrentRequestsInBytesPerIp(600);
        DatabaseDescriptor.setNativeTransportMaxConcurrentRequestsInBytes(600);
        requireNetwork();
    }

    @AfterClass
    public static void tearDown()
    {
        DatabaseDescriptor.setNativeTransportMaxConcurrentRequestsInBytesPerIp(3000000000L);
        DatabaseDescriptor.setNativeTransportMaxConcurrentRequestsInBytes(HIGH_LIMIT);
    }

    @After
    public void dropCreatedTable()
    {
        try
        {
            QueryProcessor.executeOnceInternal("DROP TABLE " + KEYSPACE + ".atable");
        }
        catch (Throwable t)
        {
            // ignore
        }
    }

    private SimpleClient client() throws IOException
    {
        return new SimpleClient(nativeAddr.getHostAddress(),
                                nativePort,
                                ProtocolVersion.V5,
                                true,
                                new EncryptionOptions())
               .connect(false, false, true);
    }

    @Test
    public void testQueryExecutionWithThrowOnOverload() throws Throwable
    {
        try (SimpleClient client = client())
        {
            QueryMessage queryMessage = new QueryMessage("CREATE TABLE atable (pk1 int PRIMARY KEY, v text)",
                                                         V5_DEFAULT_OPTIONS);
            client.execute(queryMessage);
        }
    }

    @Test
    public void testQueryExecutionWithoutThrowOnOverload() throws Throwable
    {
        try (SimpleClient client = client())
        {
            client.connect(false, false, false);

            QueryMessage queryMessage = new QueryMessage("CREATE TABLE atable (pk int PRIMARY KEY, v text)",
                                                         V5_DEFAULT_OPTIONS);
            client.execute(queryMessage);
            queryMessage = new QueryMessage("SELECT * FROM atable",
                                            V5_DEFAULT_OPTIONS);
            client.execute(queryMessage);
        }
    }

    @Test
    public void testQueryUpdatesConcurrentMetricsUpdate() throws Throwable
    {
        try (SimpleClient client = client())
        {
            final QueryMessage create = new QueryMessage("CREATE TABLE atable (pk int PRIMARY KEY, v text)",
                                                         V5_DEFAULT_OPTIONS);
            client.execute(create);

            final QueryMessage queryMessage = new QueryMessage("SELECT * FROM atable",
                                                               V5_DEFAULT_OPTIONS);

            Assert.assertEquals(0L, Server.EndpointPayloadTracker.getCurrentGlobalUsage());
            AtomicBoolean running = new AtomicBoolean(true);
            // run query serially on repeat
            new Thread(() ->
                       {
                           while (running.get())
                           {
                               client.execute(queryMessage);
                           }
                       }).start();

            // checking metric may occur inbetween running of query, so check multiple times for up to 2 seconds
            long start = System.currentTimeMillis();
            while (running.get() && System.currentTimeMillis() - start < 2000)
            {
                if (Server.EndpointPayloadTracker.getCurrentGlobalUsage() > 0)
                {
                    running.set(false);
                }
            }

            // if this isnt false it never saw the usage go above zero
            Assert.assertFalse(running.get());

            // set to false to ensure stopping the background thread
            running.set(false);
        }
    }

    @Test
    public void testQueryExecutionWithoutThrowOnOverloadAndInflightLimitedExceeded() throws Throwable
    {
        try (SimpleClient client = new SimpleClient(nativeAddr.getHostAddress(),
                                                    nativePort,
                                                    ProtocolVersion.V5,
                                                    true,
                                                    new EncryptionOptions()))
        {
            client.connect(false, false, false);
            QueryMessage queryMessage = new QueryMessage("CREATE TABLE atable (pk int PRIMARY KEY, v text)",
                                                         V5_DEFAULT_OPTIONS);
            client.execute(queryMessage);

            queryMessage = new QueryMessage("INSERT INTO atable (pk, v) VALUES (1, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')",
                                            V5_DEFAULT_OPTIONS);
            client.execute(queryMessage);
        }
    }

    @Test
    public void testOverloadedExceptionForEndpointInflightLimit() throws Throwable
    {
        try (SimpleClient client = client())
        {
            QueryMessage queryMessage = new QueryMessage("CREATE TABLE atable (pk int PRIMARY KEY, v text)",
                                                         V5_DEFAULT_OPTIONS);
            client.execute(queryMessage);

            queryMessage = new QueryMessage("INSERT INTO atable (pk, v) VALUES (1, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')",
                                            V5_DEFAULT_OPTIONS);
            try
            {
                client.execute(queryMessage);
                Assert.fail();
            }
            catch (RuntimeException e)
            {
                Assert.assertTrue(e.getCause() instanceof OverloadedException);
            }
        }
    }

    @Test
    public void testOverloadedExceptionForOverallInflightLimit() throws Throwable
    {
        try (SimpleClient client = client())
        {
            QueryMessage queryMessage = new QueryMessage("CREATE TABLE atable (pk int PRIMARY KEY, v text)",
                                                         V5_DEFAULT_OPTIONS);
            client.execute(queryMessage);

            queryMessage = new QueryMessage("INSERT INTO atable (pk, v) VALUES (1, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')",
                                            V5_DEFAULT_OPTIONS);
            try
            {
                client.execute(queryMessage);
                Assert.fail();
            }
            catch (RuntimeException e)
            {
                Assert.assertTrue(e.getCause() instanceof OverloadedException);
            }
        }
    }

    @Test
    public void testChangingLimitsAtRuntime() throws Throwable
    {
        SimpleClient client = client();
        try
        {
            QueryMessage queryMessage = new QueryMessage(String.format("CREATE TABLE %s.atable (pk int PRIMARY KEY, v text)", KEYSPACE),
                                                         V5_DEFAULT_OPTIONS);
            client.execute(queryMessage);

            queryMessage = new QueryMessage(String.format("INSERT INTO %s.atable (pk, v) VALUES (1, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')", KEYSPACE),
                                            V5_DEFAULT_OPTIONS);
            try
            {
                client.execute(queryMessage);
                Assert.fail();
            }
            catch (RuntimeException e)
            {
                Assert.assertTrue(e.getCause() instanceof OverloadedException);
            }

            // change global limit, query will still fail because endpoint limit
            Server.EndpointPayloadTracker.setGlobalLimit(HIGH_LIMIT);
            Assert.assertEquals("new global limit not returned by EndpointPayloadTrackers", HIGH_LIMIT, Server.EndpointPayloadTracker.getGlobalLimit());
            Assert.assertEquals("new global limit not returned by DatabaseDescriptor", HIGH_LIMIT, DatabaseDescriptor.getNativeTransportMaxConcurrentRequestsInBytes());

            queryMessage = new QueryMessage(String.format("INSERT INTO %s.atable (pk, v) VALUES (1, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')", KEYSPACE),
                                            V5_DEFAULT_OPTIONS);
            try
            {
                client.execute(queryMessage);
                Assert.fail();
            }
            catch (RuntimeException e)
            {
                Assert.assertTrue(e.getCause() instanceof OverloadedException);
            }

            // change endpoint limit, query will still now succeed
            Server.EndpointPayloadTracker.setEndpointLimit(HIGH_LIMIT);
            Assert.assertEquals("new endpoint limit not returned by EndpointPayloadTrackers", HIGH_LIMIT, Server.EndpointPayloadTracker.getEndpointLimit());
            Assert.assertEquals("new endpoint limit not returned by DatabaseDescriptor", HIGH_LIMIT, DatabaseDescriptor.getNativeTransportMaxConcurrentRequestsInBytesPerIp());

            queryMessage = new QueryMessage(String.format("INSERT INTO %s.atable (pk, v) VALUES (1, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')", KEYSPACE),
                                            V5_DEFAULT_OPTIONS);
            client.execute(queryMessage);

            // ensure new clients also see the new raised limits
            client.close();
            client = new SimpleClient(nativeAddr.getHostAddress(),
                                       nativePort,
                                       ProtocolVersion.V5,
                                       true,
                                       new EncryptionOptions());
            client.connect(false, true, true);

            queryMessage = new QueryMessage(String.format("INSERT INTO %s.atable (pk, v) VALUES (1, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')", KEYSPACE),
                                            V5_DEFAULT_OPTIONS);
            client.execute(queryMessage);

            // lower the global limit and ensure the query fails again
            Server.EndpointPayloadTracker.setGlobalLimit(LOW_LIMIT);
            Assert.assertEquals("new global limit not returned by EndpointPayloadTrackers", LOW_LIMIT, Server.EndpointPayloadTracker.getGlobalLimit());
            Assert.assertEquals("new global limit not returned by DatabaseDescriptor", LOW_LIMIT, DatabaseDescriptor.getNativeTransportMaxConcurrentRequestsInBytes());

            queryMessage = new QueryMessage(String.format("INSERT INTO %s.atable (pk, v) VALUES (1, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')", KEYSPACE),
                                            V5_DEFAULT_OPTIONS);
            try
            {
                client.execute(queryMessage);
                Assert.fail();
            }
            catch (RuntimeException e)
            {
                Assert.assertTrue(e.getCause() instanceof OverloadedException);
            }

            // lower the endpoint limit and ensure existing clients also have requests that fail
            Server.EndpointPayloadTracker.setEndpointLimit(60);
            Assert.assertEquals("new endpoint limit not returned by EndpointPayloadTrackers", 60, Server.EndpointPayloadTracker.getEndpointLimit());
            Assert.assertEquals("new endpoint limit not returned by DatabaseDescriptor", 60, DatabaseDescriptor.getNativeTransportMaxConcurrentRequestsInBytesPerIp());

            queryMessage = new QueryMessage(String.format("CREATE TABLE %s.atable (pk int PRIMARY KEY, v text)", KEYSPACE),
                                            V5_DEFAULT_OPTIONS);
            try
            {
                client.execute(queryMessage);
                Assert.fail();
            }
            catch (RuntimeException e)
            {
                Assert.assertTrue(e.getCause() instanceof OverloadedException);
            }

            // ensure new clients also see the new lowered limit
            client.close();
            client = new SimpleClient(nativeAddr.getHostAddress(),
                                      nativePort,
                                      ProtocolVersion.V5,
                                      true,
                                      new EncryptionOptions());
            client.connect(false, true, true);

            queryMessage = new QueryMessage(String.format("CREATE TABLE %s.atable (pk int PRIMARY KEY, v text)", KEYSPACE),
                                            V5_DEFAULT_OPTIONS);
            try
            {
                client.execute(queryMessage);
                Assert.fail();
            }
            catch (RuntimeException e)
            {
                Assert.assertTrue(e.getCause() instanceof OverloadedException);
            }

            // put the test state back
            Server.EndpointPayloadTracker.setEndpointLimit(LOW_LIMIT);
            Assert.assertEquals("new endpoint limit not returned by EndpointPayloadTrackers", LOW_LIMIT, Server.EndpointPayloadTracker.getEndpointLimit());
            Assert.assertEquals("new endpoint limit not returned by DatabaseDescriptor", LOW_LIMIT, DatabaseDescriptor.getNativeTransportMaxConcurrentRequestsInBytesPerIp());

        }
        finally
        {
            client.close();
        }
    }
}
