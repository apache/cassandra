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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import com.google.common.base.Ticker;
import org.apache.cassandra.service.StorageService;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.transport.messages.QueryMessage;
import org.apache.cassandra.utils.Throwables;

import static org.apache.cassandra.transport.ProtocolVersion.V4;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import static org.apache.cassandra.Util.spinAssertEquals;
import static org.apache.cassandra.transport.ProtocolVersion.V5;

@SuppressWarnings("UnstableApiUsage")
@RunWith(Parameterized.class)
public class RateLimitingTest extends CQLTester
{
    public static final String BACKPRESSURE_WARNING_SNIPPET = "Request breached global limit";
    
    private static final int LARGE_PAYLOAD_THRESHOLD_BYTES = 1000;
    private static final int DEFAULT_PERMITS_PER_SECOND = 1000;
    private static final double DEFAULT_BURST_SECONDS = 0.0;

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
        // The driver control connections would send queries that might interfere with the tests.
        requireNetworkWithoutDriver();
    }

    @Before
    public void resetLimits()
    {
        // Rate limiting is disabled by default.
        StorageService.instance.setNativeTransportRateLimitingEnabled(true);
        ClientResourceLimits.GLOBAL_REQUEST_LIMITER.reset(DEFAULT_PERMITS_PER_SECOND, DEFAULT_BURST_SECONDS, Ticker.systemTicker());

        tick = new AtomicLong(0);

        ticker = new Ticker()
        {
            @Override
            public long read()
            {
                return tick.get();
            }
        };
    }

    @Test
    public void shouldThrowOnOverloadSmallMessages() throws Exception
    {
        int payloadSize = LARGE_PAYLOAD_THRESHOLD_BYTES / 4;
        testOverload(payloadSize, null);
    }

    @Test
    public void shouldThrowOnOverloadLargeMessages() throws Exception
    {
        int payloadSize = LARGE_PAYLOAD_THRESHOLD_BYTES * 2;
        testOverload(payloadSize, null);
    }

    @Test
    public void shouldBackpressureSmallMessagesUnpauseOnTick() throws Exception
    {
        int payloadSize = LARGE_PAYLOAD_THRESHOLD_BYTES / 4;
        testOverload(payloadSize, this::advanceTickerToAtLeastNextPermit);
    }

    @Test
    public void shouldBackpressureSmallMessagesUnpauseOnDisable() throws Exception
    {
        int payloadSize = LARGE_PAYLOAD_THRESHOLD_BYTES / 4;
        testOverload(payloadSize, () -> StorageService.instance.setNativeTransportRateLimitingEnabled(false));
    }

    @Test
    public void shouldBackpressureLargeMessagesOnTick() throws Exception
    {
        int payloadSize = LARGE_PAYLOAD_THRESHOLD_BYTES * 2;
        testOverload(payloadSize, this::advanceTickerToAtLeastNextPermit);
    }

    @Test
    public void shouldBackpressureLargeMessagesOnDisable() throws Exception
    {
        int payloadSize = LARGE_PAYLOAD_THRESHOLD_BYTES * 2;
        testOverload(payloadSize, () -> StorageService.instance.setNativeTransportRateLimitingEnabled(false));
    }

    private void testOverload(int payloadSize, Runnable unpauser) throws Exception
    {
        try (SimpleClient client = client().connect(false, unpauser == null))
        {
            QueryMessage queryMessage = new QueryMessage("CREATE TABLE IF NOT EXISTS " + KEYSPACE + ".atable (pk int PRIMARY KEY, v text)", queryOptions());
            client.execute(queryMessage);
            
            double permitsPerSecond = 1000.0;
            double burstSeconds = 0.0;
            ClientResourceLimits.GLOBAL_REQUEST_LIMITER.reset(permitsPerSecond, burstSeconds, ticker);

            if (unpauser == null)
            {
                testThrowOnOverload(payloadSize, client);
            }
            else
            {
                testBackpressureOnOverload(payloadSize, client, unpauser);
            }   
        }
    }

    private void testBackpressureOnOverload(int payloadSize, SimpleClient client, Runnable unpauser) throws Exception
    {
        // The first query takes the one available permit...
        client.execute(queryMessage(payloadSize));
        assertEquals(0, getPausedConnectionsGauge().getValue().intValue());
        
        // For V5 and later, backpressure doesn't even allow the request that triggered it to complete.
        if (version.isSmallerThan(V5))
        {
            Message.Response response = client.execute(queryMessage(payloadSize));
            
            // V3 does not support client warnings.
            if (version.isGreaterOrEqualTo(V4)) 
            {
                assertWarningsContain(response, BACKPRESSURE_WARNING_SNIPPET);
            }
            
            assertEquals(1, getPausedConnectionsGauge().getValue().intValue());
        }

        AtomicReference<Throwable> error = new AtomicReference<>();
        CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch complete = new CountDownLatch(1);
        final AtomicReference<Message.Response> response = new AtomicReference<>();
        
        Thread queryRunner = new Thread(() -> 
        {
            try
            {
                started.countDown();
                response.set(client.execute(queryMessage(payloadSize)));
                complete.countDown();
            }
            catch (Throwable t)
            {
                error.set(t);
            }
        });
        queryRunner.start();
        
        // Make sure the thread has started but the query has not completed...
        started.await();
        assertFalse(complete.await(1, TimeUnit.SECONDS));

        if (version.isGreaterOrEqualTo(V5))
        {
            spinAssertEquals("Timed out after waiting 5 seconds for paused " +
                             "connections metric to increment due to backpressure!",
                             1, () -> getPausedConnectionsGauge().getValue(), 5, TimeUnit.SECONDS);
        }

        // Unpause the connection...
        unpauser.run();

        // ..and the request should complete without error.
        assertTrue(complete.await(SimpleClient.TIMEOUT_SECONDS + 1, TimeUnit.SECONDS));
        assertNull(error.get());
        
        if (version.isGreaterOrEqualTo(V5))
        {
            assertWarningsContain(response.get(), BACKPRESSURE_WARNING_SNIPPET);
        }
        
        assertEquals(0, getPausedConnectionsGauge().getValue().intValue());
    }

    private void testThrowOnOverload(int payloadSize, SimpleClient client)
    {
        try
        {
            // The first query takes the one available permit...
            client.execute(queryMessage(payloadSize));
            // ...and the second breaches the limit....
            client.execute(queryMessage(payloadSize));
        }
        catch (RuntimeException e)
        {
            assertTrue(Throwables.anyCauseMatches(e, cause -> cause instanceof OverloadedException));
        }
    }

    private void advanceTickerToAtLeastNextPermit()
    {
        long waitUntilNextPermit = ClientResourceLimits.GLOBAL_REQUEST_LIMITER.waitTimeMicros();
        tick.addAndGet(TimeUnit.MICROSECONDS.toNanos(waitUntilNextPermit));
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
}
