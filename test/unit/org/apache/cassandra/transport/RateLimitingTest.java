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

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import com.codahale.metrics.Meter;
import com.google.common.base.Ticker;
import org.awaitility.Awaitility;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.transport.messages.QueryMessage;
import org.apache.cassandra.utils.Throwables;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static org.apache.cassandra.Util.spinAssertEquals;
import static org.apache.cassandra.transport.ProtocolVersion.V4;

@SuppressWarnings("UnstableApiUsage")
@RunWith(Parameterized.class)
public class RateLimitingTest extends CQLTester
{
    public static final String BACKPRESSURE_WARNING_SNIPPET = "Request breached global limit";
    
    private static final int LARGE_PAYLOAD_THRESHOLD_BYTES = 1000;
    private static final int OVERLOAD_PERMITS_PER_SECOND = 1;

    private static final long MAX_LONG_CONFIG_VALUE = Long.MAX_VALUE - 1;

    @Parameterized.Parameter
    public ProtocolVersion version;

    @Parameterized.Parameters(name="{0}")
    public static Collection<Object[]> versions()
    {
        return ProtocolVersion.SUPPORTED.stream()
                                        .map(v -> new Object[]{v})
                                        .collect(Collectors.toList());
    }

    private AtomicLong tick;
    private Ticker ticker;

    @BeforeClass
    public static void setup()
    {
        // If we don't exceed the queue capacity, we won't actually use the global/endpoint 
        // bytes-in-flight limits, and the assertions we make below around releasing them would be useless.
        DatabaseDescriptor.setNativeTransportReceiveQueueCapacityInBytes(1);

        requireNetwork();
    }

    @Before
    public void resetLimits()
    {
        // Reset to the original start time in case a test advances the clock.
        tick = new AtomicLong(ClientResourceLimits.GLOBAL_REQUEST_LIMITER.getStartedNanos());

        ticker = new Ticker()
        {
            @Override
            public long read()
            {
                return tick.get();
            }
        };

        ClientResourceLimits.setGlobalLimit(MAX_LONG_CONFIG_VALUE);
    }

    @Test
    public void shouldThrowOnOverloadSmallMessages() throws Exception
    {
        int payloadSize = LARGE_PAYLOAD_THRESHOLD_BYTES / 4;
        testOverload(payloadSize, true);
    }

    @Test
    public void shouldThrowOnOverloadLargeMessages() throws Exception
    {
        int payloadSize = LARGE_PAYLOAD_THRESHOLD_BYTES * 2;
        testOverload(payloadSize, true);
    }

    @Test
    public void shouldBackpressureSmallMessages() throws Exception
    {
        int payloadSize = LARGE_PAYLOAD_THRESHOLD_BYTES / 4;
        testOverload(payloadSize, false);
    }

    @Test
    public void shouldBackpressureLargeMessages() throws Exception
    {
        int payloadSize = LARGE_PAYLOAD_THRESHOLD_BYTES * 2;
        testOverload(payloadSize, false);
    }

    @Test
    public void shouldReleaseSmallMessageOnBytesInFlightOverload() throws Exception
    {
        testBytesInFlightOverload(LARGE_PAYLOAD_THRESHOLD_BYTES / 4);
    }

    @Test
    public void shouldReleaseLargeMessageOnBytesInFlightOverload() throws Exception
    {
        testBytesInFlightOverload(LARGE_PAYLOAD_THRESHOLD_BYTES * 2);
    }

    private void testBytesInFlightOverload(int payloadSize) throws Exception
    {
        try (SimpleClient client = client().connect(false, true))
        {
            StorageService.instance.setNativeTransportRateLimitingEnabled(false);
            QueryMessage queryMessage = new QueryMessage("CREATE TABLE IF NOT EXISTS " + KEYSPACE + ".atable (pk int PRIMARY KEY, v text)", queryOptions());
            client.execute(queryMessage);

            StorageService.instance.setNativeTransportRateLimitingEnabled(true);
            ClientResourceLimits.GLOBAL_REQUEST_LIMITER.setRate(OVERLOAD_PERMITS_PER_SECOND, ticker);
            ClientResourceLimits.setGlobalLimit(1);

            try
            {
                // The first query takes the one available permit, but should fail on the bytes in flight limit.
                client.execute(queryMessage(payloadSize));
            }
            catch (RuntimeException e)
            {
                assertTrue(Throwables.anyCauseMatches(e, cause -> cause instanceof OverloadedException));
            }
        }
        finally
        {
            // Sanity check bytes in flight limiter.
            Awaitility.await().untilAsserted(() -> assertEquals(0, ClientResourceLimits.getCurrentGlobalUsage()));
            StorageService.instance.setNativeTransportRateLimitingEnabled(false);
        }
    }

    private void testOverload(int payloadSize, boolean throwOnOverload) throws Exception
    {
        try (SimpleClient client = client().connect(false, throwOnOverload))
        {
            StorageService.instance.setNativeTransportRateLimitingEnabled(false);
            QueryMessage queryMessage = new QueryMessage("CREATE TABLE IF NOT EXISTS " + KEYSPACE + ".atable (pk int PRIMARY KEY, v text)", queryOptions());
            client.execute(queryMessage);

            StorageService.instance.setNativeTransportRateLimitingEnabled(true);
            ClientResourceLimits.GLOBAL_REQUEST_LIMITER.setRate(OVERLOAD_PERMITS_PER_SECOND, ticker);

            if (throwOnOverload)
                testThrowOnOverload(payloadSize, client);
            else
            {
                testBackpressureOnOverload(payloadSize, client);
            }   
        }
        finally
        {
            // Sanity the check bytes in flight limiter.
            Awaitility.await().untilAsserted(() -> assertEquals(0, ClientResourceLimits.getCurrentGlobalUsage()));
            StorageService.instance.setNativeTransportRateLimitingEnabled(false);
        }
    }

    private void testBackpressureOnOverload(int payloadSize, SimpleClient client) throws Exception
    {
        // The first query takes the one available permit.
        Message.Response firstResponse = client.execute(queryMessage(payloadSize));
        assertEquals(0, getPausedConnectionsGauge().getValue().intValue());
        assertNoWarningContains(firstResponse, BACKPRESSURE_WARNING_SNIPPET);
        
        // The second query activates backpressure.
        long overloadQueryStartTime = System.currentTimeMillis();
        Message.Response response = client.execute(queryMessage(payloadSize));

        // V3 does not support client warnings, but otherwise we should get one for this query.
        if (version.isGreaterOrEqualTo(V4))
            assertWarningsContain(response, BACKPRESSURE_WARNING_SNIPPET);

        AtomicReference<Throwable> error = new AtomicReference<>();
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch complete = new CountDownLatch(1);
        AtomicReference<Message.Response> pausedQueryResponse = new AtomicReference<>();
        
        Thread queryRunner = new Thread(() ->
        {
            try
            {
                started.countDown();
                pausedQueryResponse.set(client.execute(queryMessage(payloadSize)));
                complete.countDown();
            }
            catch (Throwable t)
            {
                error.set(t);
            }
        });

        // Advance the rater limiter so that this query will see an available permit. This also
        // means it should not produce a client warning, which we verify below.
        // (Note that we advance 2 intervals for the 2 prior queries.)
        tick.addAndGet(2 * ClientResourceLimits.GLOBAL_REQUEST_LIMITER.getIntervalNanos());
        
        queryRunner.start();

        // ...and the request should complete without error.
        assertTrue(complete.await(SimpleClient.TIMEOUT_SECONDS + 1, TimeUnit.SECONDS));
        assertNull(error.get());
        assertNoWarningContains(pausedQueryResponse.get(), BACKPRESSURE_WARNING_SNIPPET);

        // At least the number of milliseconds in the permit interval should already have elapsed 
        // since the start of the query that pauses the connection.
        double permitIntervalMillis = (double) TimeUnit.SECONDS.toMillis(1L) / OVERLOAD_PERMITS_PER_SECOND;
        long sinceQueryStarted = System.currentTimeMillis() - overloadQueryStartTime;
        long remainingMillis = ((long) permitIntervalMillis) - sinceQueryStarted;
        assertTrue("Query completed before connection unpause!", remainingMillis <= 0);
        
        spinAssertEquals("Timed out after waiting 5 seconds for paused connections metric to normalize.",
                         0, () -> getPausedConnectionsGauge().getValue(), 5, TimeUnit.SECONDS);
    }

    private void testThrowOnOverload(int payloadSize, SimpleClient client)
    {
        // The first query takes the one available permit...
        long dispatchedPrior = getRequestDispatchedMeter().getCount();
        client.execute(queryMessage(payloadSize));
        assertEquals(dispatchedPrior + 1, getRequestDispatchedMeter().getCount());
        
        try
        {   
            // ...and the second breaches the limit....
            client.execute(queryMessage(payloadSize));
        }
        catch (RuntimeException e)
        {
            assertTrue(Throwables.anyCauseMatches(e, cause -> cause instanceof OverloadedException));
        }

        // The last request attempt was rejected and therefore not dispatched.
        assertEquals(dispatchedPrior + 1, getRequestDispatchedMeter().getCount());

        // Advance the timeline and verify that we can take a permit again.
        // (Note that we don't take one when we throw on overload.)
        tick.addAndGet(ClientResourceLimits.GLOBAL_REQUEST_LIMITER.getIntervalNanos());
        client.execute(queryMessage(payloadSize));

        assertEquals(dispatchedPrior + 2, getRequestDispatchedMeter().getCount());
    }

    private QueryMessage queryMessage(int length)
    {
        StringBuilder query = new StringBuilder("INSERT INTO " + KEYSPACE + ".atable (pk, v) VALUES (1, '");
        
        for (int i = 0; i < length; i++)
        {
            query.append('a');
        }
        
        query.append("')");
        return new QueryMessage(query.toString(), queryOptions());
    }

    private SimpleClient client()
    {
        return SimpleClient.builder(nativeAddr.getHostAddress(), nativePort)
                           .protocolVersion(version)
                           .useBeta()
                           .largeMessageThreshold(LARGE_PAYLOAD_THRESHOLD_BYTES)
                           .build();
    }

    private QueryOptions queryOptions()
    {
        return QueryOptions.create(QueryOptions.DEFAULT.getConsistency(),
                                   QueryOptions.DEFAULT.getValues(),
                                   QueryOptions.DEFAULT.skipMetadata(),
                                   QueryOptions.DEFAULT.getPageSize(),
                                   QueryOptions.DEFAULT.getPagingState(),
                                   QueryOptions.DEFAULT.getSerialConsistency(),
                                   version,
                                   KEYSPACE);
    }

    protected static Meter getRequestDispatchedMeter()
    {
        String metricName = "org.apache.cassandra.metrics.Client.RequestDispatched";
        Map<String, Meter> metrics = CassandraMetricsRegistry.Metrics.getMeters((name, metric) -> name.equals(metricName));
        if (metrics.size() != 1)
            fail(String.format("Expected a single registered metric for request dispatched, found %s",metrics.size()));
        return metrics.get(metricName);
    }
}
