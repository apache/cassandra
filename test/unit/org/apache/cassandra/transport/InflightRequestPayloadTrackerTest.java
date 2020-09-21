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
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
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
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.virtual.AbstractVirtualTable;
import org.apache.cassandra.db.virtual.SimpleDataSet;
import org.apache.cassandra.db.virtual.VirtualKeyspace;
import org.apache.cassandra.db.virtual.VirtualKeyspaceRegistry;
import org.apache.cassandra.db.virtual.VirtualTable;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.schema.TableMetadata;
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
            CyclicBarrier barrier = new CyclicBarrier(2);
            String table = createTableName();

            // reusing table name for keyspace name since cannot reuse KEYSPACE and want it to be unique
            TableMetadata tableMetadata =
                TableMetadata.builder(table, table)
                             .kind(TableMetadata.Kind.VIRTUAL)
                             .addPartitionKeyColumn("pk", UTF8Type.instance)
                             .addRegularColumn("v", Int32Type.instance)
                             .build();

            VirtualTable vt1 = new AbstractVirtualTable.SimpleTable(tableMetadata, () -> {
                try
                {
                    // sync up with main thread thats waiting for query to be in progress
                    barrier.await(30, TimeUnit.SECONDS);
                    // wait until metric has been checked
                    barrier.await(30, TimeUnit.SECONDS);
                }
                catch (Exception e)
                {
                    // ignore interuption and barrier exceptions
                }
                return new SimpleDataSet(tableMetadata);
            });
            VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(table, ImmutableList.of(vt1)));

            final QueryMessage queryMessage = new QueryMessage(String.format("SELECT * FROM %s.%s", table, table),
                                                               V5_DEFAULT_OPTIONS);

            Assert.assertEquals(0L, Server.EndpointPayloadTracker.getCurrentGlobalUsage());
            try
            {
                Thread tester = new Thread(() -> client.execute(queryMessage));
                tester.setDaemon(true); // so wont block exit if something fails
                tester.start();
                // block until query in progress
                barrier.await(30, TimeUnit.SECONDS);
                Assert.assertTrue(Server.EndpointPayloadTracker.getCurrentGlobalUsage() > 0);
            } finally
            {
                // notify query thread that metric has been checked. This will also throw TimeoutException if both
                // the query threads barriers are not reached
                barrier.await(30, TimeUnit.SECONDS);
            }
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
