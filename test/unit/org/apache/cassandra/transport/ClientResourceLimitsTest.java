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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import org.apache.cassandra.service.StorageService;
import org.junit.*;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.virtual.*;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.transport.messages.QueryMessage;
import org.apache.cassandra.utils.FBUtilities;
import org.awaitility.Awaitility;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ClientResourceLimitsTest extends CQLTester
{
    private static final long LOW_LIMIT = 600L;
    private static final long HIGH_LIMIT = 5000000000L;

    private static final QueryOptions V5_DEFAULT_OPTIONS = 
        QueryOptions.create(QueryOptions.DEFAULT.getConsistency(),
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
        DatabaseDescriptor.setNativeTransportReceiveQueueCapacityInBytes(1);
        DatabaseDescriptor.setNativeTransportMaxRequestDataInFlightPerIpInBytes(LOW_LIMIT);
        DatabaseDescriptor.setNativeTransportConcurrentRequestDataInFlightInBytes(LOW_LIMIT);

        requireNetwork();
    }

    @AfterClass
    public static void tearDown()
    {
        DatabaseDescriptor.setNativeTransportMaxRequestDataInFlightPerIpInBytes(3000000000L);
        DatabaseDescriptor.setNativeTransportConcurrentRequestDataInFlightInBytes(HIGH_LIMIT);
    }

    @Before
    public void setLimits()
    {
        ClientResourceLimits.setGlobalLimit(LOW_LIMIT);
        ClientResourceLimits.setEndpointLimit(LOW_LIMIT);
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

    private SimpleClient client(boolean throwOnOverload)
    {
        try
        {
            return SimpleClient.builder(nativeAddr.getHostAddress(), nativePort)
                               .protocolVersion(ProtocolVersion.V5)
                               .useBeta()
                               .build()
                               .connect(false, throwOnOverload);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Error initializing client", e);
        }
    }

    @SuppressWarnings("SameParameterValue")
    private SimpleClient client(boolean throwOnOverload, int largeMessageThreshold)
    {
        try
        {
            return SimpleClient.builder(nativeAddr.getHostAddress(), nativePort)
                               .protocolVersion(ProtocolVersion.V5)
                               .useBeta()
                               .largeMessageThreshold(largeMessageThreshold)
                               .build()
                               .connect(false, throwOnOverload);
        }
        catch (IOException e)
        {
           throw new RuntimeException("Error initializing client", e);
        }
    }

    @Test
    public void testQueryExecutionWithThrowOnOverload()
    {
        testQueryExecution(true);
    }

    @Test
    public void testQueryExecutionWithoutThrowOnOverload()
    {
        testQueryExecution(false);
    }

    private void testQueryExecution(boolean throwOnOverload)
    {
        try (SimpleClient client = client(throwOnOverload))
        {
            QueryMessage queryMessage = new QueryMessage("CREATE TABLE atable (pk int PRIMARY KEY, v text)",
                                                         V5_DEFAULT_OPTIONS);
            client.execute(queryMessage);
            queryMessage = new QueryMessage("SELECT * FROM atable", V5_DEFAULT_OPTIONS);
            client.execute(queryMessage);
        }
    }

    @Test
    public void testBackpressureOnGlobalLimitExceeded() throws Throwable
    {
        // Bump the per-endpoint limit to make sure we exhaust the global
        ClientResourceLimits.setEndpointLimit(HIGH_LIMIT);
        backPressureTest(() -> ClientResourceLimits.setGlobalLimit(HIGH_LIMIT),
                         (provider) -> provider.globalWaitQueue().signal());
    }

    @Test
    public void testBackPressureWhenEndpointLimitExceeded() throws Throwable
    {
        // Make sure we can only exceed the per-endpoint limit
        ClientResourceLimits.setGlobalLimit(HIGH_LIMIT);
        backPressureTest(() -> ClientResourceLimits.setEndpointLimit(HIGH_LIMIT),
                         (provider) -> provider.endpointWaitQueue().signal());
    }

    private void backPressureTest(Runnable limitLifter, Consumer<ClientResourceLimits.ResourceProvider> signaller) throws Throwable
    {
        final AtomicReference<Exception> error = new AtomicReference<>();
        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch complete = new CountDownLatch(1);
        
        try (SimpleClient client = client(false))
        {
            // The first query does not trigger backpressure/pause the connection:
            QueryMessage queryMessage = 
                    new QueryMessage("CREATE TABLE atable (pk int PRIMARY KEY, v text)", V5_DEFAULT_OPTIONS);
            Message.Response belowThresholdResponse = client.execute(queryMessage);
            assertEquals(0, getPausedConnectionsGauge().getValue().intValue());
            assertNoWarningContains(belowThresholdResponse, "bytes in flight");
            
            // A second query triggers backpressure but is allowed to complete...
            Message.Response aboveThresholdResponse = client.execute(queryMessage());
            assertEquals(1, getPausedConnectionsGauge().getValue().intValue());
            assertWarningsContain(aboveThresholdResponse, "bytes in flight");

            // ...and a third request is paused.
            final AtomicReference<Message.Response> response = new AtomicReference<>();
            
            Thread t = new Thread(() -> {
                try
                {
                    started.countDown();
                    response.set(client.execute(queryMessage()));
                    complete.countDown();
                }
                catch (Exception e)
                {
                    // if backpressure blocks the request execution for too long
                    // (10 seconds per SimpleClient), we'll catch a timeout exception
                    error.set(e);
                }
            });
            t.start();

            // verify the request hasn't completed
            assertFalse(complete.await(1, TimeUnit.SECONDS));

            // backpressure has been applied, if we increase the limits of the exhausted reserve and signal
            // the appropriate WaitQueue, it should be released and the client request will complete
            limitLifter.run();
            
            // We need a ResourceProvider to get access to the WaitQueue
            ClientResourceLimits.Allocator allocator = ClientResourceLimits.getAllocatorForEndpoint(FBUtilities.getJustLocalAddress());
            ClientResourceLimits.ResourceProvider queueHandle = new ClientResourceLimits.ResourceProvider.Default(allocator);
            signaller.accept(queueHandle);

            // SimpleClient has a 10 second timeout, so if we have to wait
            // longer than that assume that we're not going to receive a
            // reply. If all's well, the completion should happen immediately
            assertTrue(complete.await(SimpleClient.TIMEOUT_SECONDS + 1, TimeUnit.SECONDS));
            assertNull(error.get());
            assertEquals(0, getPausedConnectionsGauge().getValue().intValue());
            assertNoWarningContains(response.get(), "bytes in flight");
        }
    }

    @Test
    public void testOverloadedExceptionWhenGlobalLimitExceeded()
    {
        // Bump the per-endpoint limit to make sure we exhaust the global
        ClientResourceLimits.setEndpointLimit(HIGH_LIMIT);
        testOverloadedException(() -> client(true));
    }

    @Test
    public void testOverloadedExceptionWhenEndpointLimitExceeded()
    {
        // Make sure we can only exceed the per-endpoint limit
        ClientResourceLimits.setGlobalLimit(HIGH_LIMIT);
        testOverloadedException(() -> client(true));
    }

    @Test
    public void testOverloadedExceptionWhenGlobalLimitByMultiFrameMessage()
    {
        // Bump the per-endpoint limit to make sure we exhaust the global
        ClientResourceLimits.setEndpointLimit(HIGH_LIMIT);
        testOverloadedException(() -> client(true, Ints.checkedCast(LOW_LIMIT / 2)));
    }

    @Test
    public void testOverloadedExceptionWhenEndpointLimitByMultiFrameMessage()
    {
        // Make sure we can only exceed the per-endpoint limit
        ClientResourceLimits.setGlobalLimit(HIGH_LIMIT);
        testOverloadedException(() -> client(true, Ints.checkedCast(LOW_LIMIT / 2)));
    }

    private void testOverloadedException(Supplier<SimpleClient> clientSupplier)
    {
        try (SimpleClient client = clientSupplier.get())
        {
            QueryMessage queryMessage = new QueryMessage("CREATE TABLE atable (pk int PRIMARY KEY, v text)",
                                                         V5_DEFAULT_OPTIONS);
            client.execute(queryMessage);

            queryMessage = queryMessage();
            try
            {
                client.execute(queryMessage);
                fail();
            }
            catch (RuntimeException e)
            {
                assertTrue(e.getCause() instanceof OverloadedException);
            }
        }
    }

    private QueryMessage queryMessage()
    {
        StringBuilder query = new StringBuilder("INSERT INTO atable (pk, v) VALUES (1, '");
        for (int i=0; i < LOW_LIMIT * 2; i++)
            query.append('a');
        query.append("')");
        return new QueryMessage(query.toString(), V5_DEFAULT_OPTIONS);
    }

    @Test
    public void testQueryUpdatesConcurrentMetricsUpdate() throws Throwable
    {
        try (SimpleClient client = client(true))
        {
            // wait for the completion of the intial messages created by the client connection
            Awaitility.await()
                      .pollDelay(1, TimeUnit.SECONDS)
                      .atMost(30, TimeUnit.SECONDS)
                      .untilAsserted(() -> assertEquals(0, ClientResourceLimits.getCurrentGlobalUsage()));

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
            try
            {
                Thread tester = new Thread(() -> client.execute(queryMessage));
                tester.setDaemon(true); // so wont block exit if something fails
                tester.start();
                // block until query in progress
                barrier.await(30, TimeUnit.SECONDS);
                assertTrue(ClientResourceLimits.getCurrentGlobalUsage() > 0);
            }
            finally
            {
                // notify query thread that metric has been checked. This will also throw TimeoutException if both
                // the query threads barriers are not reached
                barrier.await(30, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    public void testChangingLimitsAtRuntime()
    {
        SimpleClient client = client(true);
        try
        {
            QueryMessage smallMessage = new QueryMessage(String.format("CREATE TABLE %s.atable (pk int PRIMARY KEY, v text)", KEYSPACE),
                                                         V5_DEFAULT_OPTIONS);
            client.execute(smallMessage);
            try
            {
                client.execute(queryMessage());
                fail();
            }
            catch (RuntimeException e)
            {
                assertTrue(e.getCause() instanceof OverloadedException);
            }

            // change global limit, query will still fail because endpoint limit
            ClientResourceLimits.setGlobalLimit(HIGH_LIMIT);
            Assert.assertEquals("new global limit not returned by EndpointPayloadTrackers", HIGH_LIMIT, ClientResourceLimits.getGlobalLimit());
            Assert.assertEquals("new global limit not returned by DatabaseDescriptor", HIGH_LIMIT, DatabaseDescriptor.getNativeTransportMaxRequestDataInFlightInBytes());

            try
            {
                client.execute(queryMessage());
                fail();
            }
            catch (RuntimeException e)
            {
                assertTrue(e.getCause() instanceof OverloadedException);
            }

            // change endpoint limit, query will now succeed
            ClientResourceLimits.setEndpointLimit(HIGH_LIMIT);
            Assert.assertEquals("new endpoint limit not returned by EndpointPayloadTrackers", HIGH_LIMIT, ClientResourceLimits.getEndpointLimit());
            Assert.assertEquals("new endpoint limit not returned by DatabaseDescriptor", HIGH_LIMIT, DatabaseDescriptor.getNativeTransportMaxRequestDataInFlightPerIpInBytes());
            client.execute(queryMessage());

            // ensure new clients also see the new raised limits
            client.close();
            client = client(true);
            client.execute(queryMessage());

            // lower the global limit and ensure the query fails again
            ClientResourceLimits.setGlobalLimit(LOW_LIMIT);
            Assert.assertEquals("new global limit not returned by EndpointPayloadTrackers", LOW_LIMIT, ClientResourceLimits.getGlobalLimit());
            Assert.assertEquals("new global limit not returned by DatabaseDescriptor", LOW_LIMIT, DatabaseDescriptor.getNativeTransportMaxRequestDataInFlightInBytes());
            try
            {
                client.execute(queryMessage());
                fail();
            }
            catch (RuntimeException e)
            {
                assertTrue(e.getCause() instanceof OverloadedException);
            }

            // lower the endpoint limit and ensure existing clients also have requests that fail
            ClientResourceLimits.setEndpointLimit(60);
            Assert.assertEquals("new endpoint limit not returned by EndpointPayloadTrackers", 60, ClientResourceLimits.getEndpointLimit());
            Assert.assertEquals("new endpoint limit not returned by DatabaseDescriptor", 60, DatabaseDescriptor.getNativeTransportMaxRequestDataInFlightPerIpInBytes());
            try
            {
                client.execute(smallMessage);
                fail();
            }
            catch (RuntimeException e)
            {
                assertTrue(e.getCause() instanceof OverloadedException);
            }

            // ensure new clients also see the new lowered limit
            client.close();
            client = client(true);
            try
            {
                client.execute(smallMessage);
                fail();
            }
            catch (RuntimeException e)
            {
                assertTrue(e.getCause() instanceof OverloadedException);
            }

            // put the test state back
            ClientResourceLimits.setEndpointLimit(LOW_LIMIT);
            Assert.assertEquals("new endpoint limit not returned by EndpointPayloadTrackers", LOW_LIMIT, ClientResourceLimits.getEndpointLimit());
            Assert.assertEquals("new endpoint limit not returned by DatabaseDescriptor", LOW_LIMIT, DatabaseDescriptor.getNativeTransportMaxRequestDataInFlightPerIpInBytes());
        }
        finally
        {
            client.close();
        }
    }

    @Test
    public void shouldChangeRequestsPerSecondAtRuntime()
    {
        StorageService.instance.setNativeTransportMaxRequestsPerSecond(100);
        assertEquals(100, ClientResourceLimits.getNativeTransportMaxRequestsPerSecond(), 0);
        assertEquals(100, ClientResourceLimits.GLOBAL_REQUEST_LIMITER.getRate(), 0);
        assertEquals(100, StorageService.instance.getNativeTransportMaxRequestsPerSecond());

        StorageService.instance.setNativeTransportMaxRequestsPerSecond(1000);
        assertEquals(1000, ClientResourceLimits.getNativeTransportMaxRequestsPerSecond(), 0);
        assertEquals(1000, ClientResourceLimits.GLOBAL_REQUEST_LIMITER.getRate(), 0);
        assertEquals(1000, StorageService.instance.getNativeTransportMaxRequestsPerSecond());

        StorageService.instance.setNativeTransportMaxRequestsPerSecond(500);
        assertEquals(500, ClientResourceLimits.getNativeTransportMaxRequestsPerSecond(), 0);
        assertEquals(500, ClientResourceLimits.GLOBAL_REQUEST_LIMITER.getRate(), 0);
        assertEquals(500, StorageService.instance.getNativeTransportMaxRequestsPerSecond());
    }
}
