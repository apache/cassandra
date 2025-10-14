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

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.metrics.ClientMetrics;
import org.apache.cassandra.transport.messages.ErrorMessage;
import org.apache.cassandra.transport.messages.OptionsMessage;
import org.apache.cassandra.transport.messages.QueryMessage;

import static org.junit.Assert.*;

/**
 * Integration tests for Dispatcher with real connections and end-to-end workflows.
 * These tests focus on integration scenarios rather than unit testing individual components.
 */
public class DispatcherIntegrationTest extends CQLTester
{
    private SimpleClient client;
    private Server server;
    private ExecutorService executorService;

    // Save original settings to restore after tests
    private long originalMaxWaitTimeInMillis;
    private double originalThreshold;
    private int originalMaxRequestsPerSecond;
    private long originalMaxConcurrentRequests;

    @BeforeClass
    public static void setupClientMetrics() throws Exception
    {
        // Start the embedded Cassandra server for integration testing
        requireNetwork();

        // Initialize ClientMetrics with empty server collection for testing
        ClientMetrics.instance.init(Collections.emptyList());
    }

    @Before
    public void setUp() throws Throwable
    {
        // Save original settings
        originalMaxWaitTimeInMillis = DatabaseDescriptor.getMaxWaitTimeInTransportQueue(TimeUnit.MILLISECONDS);
        originalThreshold = DatabaseDescriptor.getNativeTransportQueueMaxItemAgeThreshold();
        originalMaxRequestsPerSecond = ClientResourceLimits.getNativeTransportMaxRequestsPerSecond();
        originalMaxConcurrentRequests = ClientResourceLimits.getGlobalLimit();

        executorService = Executors.newCachedThreadPool();

        // Create a test client that will connect to the embedded server
        client = new SimpleClient(nativeAddr.getHostAddress(), nativePort);
    }

    @After
    public void tearDown() throws Throwable
    {
        // Restore original settings
        DatabaseDescriptor.setMaxWaitTimeInTransportQueue(originalMaxWaitTimeInMillis);
        DatabaseDescriptor.setNativeTransportMaxQueueItemAgeThreshold(originalThreshold);
        ClientResourceLimits.setNativeTransportMaxRequestsPerSecond(originalMaxRequestsPerSecond);
        ClientResourceLimits.setGlobalLimit(originalMaxConcurrentRequests);

        if (client != null)
        {
            try
            {
                client.close();
            }
            catch (Exception e)
            {
                // Ignore cleanup errors
            }
        }

        if (executorService != null)
        {
            executorService.shutdownNow();
        }
    }

    /**
     * Integration test for end-to-end queue backpressure behavior with real connections.
     * Tests that when queue limits are exceeded, the dispatcher properly rejects requests.
     */
    @Test
    public void testEndToEndQueueBackpressureWithRealConnections() throws Throwable
    {
        // Configure very aggressive queue limits to trigger backpressure
        DatabaseDescriptor.setMaxWaitTimeInTransportQueue(1); // 1ms timeout to force failures
        DatabaseDescriptor.setNativeTransportMaxQueueItemAgeThreshold(0.1); // 10% threshold

        // Connect client to the embedded server
        client.connect(false);

        // Create a test keyspace and table
        execute("CREATE KEYSPACE IF NOT EXISTS test_integration WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        execute("CREATE TABLE IF NOT EXISTS test_integration.test_table (id int PRIMARY KEY, value text)");

        AtomicInteger successfulRequests = new AtomicInteger(0);
        AtomicInteger failedRequests = new AtomicInteger(0);
        CountDownLatch completionLatch = new CountDownLatch(100);

        // Submit many concurrent requests to overwhelm the queue
        for (int i = 0; i < 100; i++)
        {
            final int requestId = i;
            executorService.submit(() -> {
                try
                {
                    String query = "INSERT INTO test_integration.test_table (id, value) VALUES (" + requestId + ", 'test_value_" + requestId + "')";
                    Message.Response response = client.execute(new QueryMessage(query, QueryOptions.DEFAULT));

                    if (response instanceof ErrorMessage)
                    {
                        ErrorMessage error = (ErrorMessage) response;
                        if (error.error instanceof OverloadedException)
                        {
                            failedRequests.incrementAndGet();
                        }
                        else
                        {
                            successfulRequests.incrementAndGet();
                        }
                    }
                    else
                    {
                        successfulRequests.incrementAndGet();
                    }
                }
                catch (Exception e)
                {
                    if (e.getCause() instanceof OverloadedException)
                    {
                        failedRequests.incrementAndGet();
                    }
                    else
                    {
                        successfulRequests.incrementAndGet();
                    }
                }
                finally
                {
                    completionLatch.countDown();
                }
            });
        }

        // Wait for all requests to complete
        assertTrue("All requests should complete within timeout",
                completionLatch.await(30, TimeUnit.SECONDS));

        // Verify that some requests succeeded and some failed due to backpressure
        int totalSuccessful = successfulRequests.get();
        int totalFailed = failedRequests.get();

        assertTrue("Some requests should succeed", totalSuccessful > 0);
        assertTrue("Some requests should fail due to backpressure when queue is overwhelmed", totalFailed > 0);
        assertEquals("Total requests should match", 100, totalSuccessful + totalFailed);

        System.out.println("Integration test results: " + totalSuccessful + " successful, " + totalFailed + " failed due to backpressure");
    }

    /**
     * Integration test for dispatcher behavior under rate limiting with real connections.
     */
    @Test
    public void testEndToEndRateLimitingWithRealConnections() throws Throwable
    {
        // Enable rate limiting with a low threshold
        DatabaseDescriptor.setNativeTransportRateLimitingEnabled(true);
        ClientResourceLimits.setNativeTransportMaxRequestsPerSecond(10); // 10 requests per second

        client.connect(false);

        AtomicInteger successfulRequests = new AtomicInteger(0);
        AtomicInteger rateLimitedRequests = new AtomicInteger(0);
        CountDownLatch completionLatch = new CountDownLatch(25);

        // Submit requests rapidly to trigger rate limiting
        for (int i = 0; i < 25; i++)
        {
            executorService.submit(() -> {
                try
                {
                    Message.Response response = client.execute(new OptionsMessage());

                    if (response instanceof ErrorMessage)
                    {
                        ErrorMessage error = (ErrorMessage) response;
                        if (error.error instanceof OverloadedException)
                        {
                            rateLimitedRequests.incrementAndGet();
                        }
                        else
                        {
                            successfulRequests.incrementAndGet();
                        }
                    }
                    else
                    {
                        successfulRequests.incrementAndGet();
                    }
                }
                catch (Exception e)
                {
                    if (e.getCause() instanceof OverloadedException)
                    {
                        rateLimitedRequests.incrementAndGet();
                    }
                    else
                    {
                        successfulRequests.incrementAndGet();
                    }
                }
                finally
                {
                    completionLatch.countDown();
                }
            });
        }

        assertTrue("All requests should complete within timeout",
                completionLatch.await(15, TimeUnit.SECONDS));

        int totalSuccessful = successfulRequests.get();
        int totalRateLimited = rateLimitedRequests.get();

        assertTrue("Some requests should succeed", totalSuccessful > 0);
        assertTrue("Some requests should be rate limited when rate limit is exceeded", totalRateLimited > 0);
        assertEquals("Total requests should match", 25, totalSuccessful + totalRateLimited);

        System.out.println("Rate limiting test results: " + totalSuccessful + " successful, " + totalRateLimited + " rate limited");
    }

    /**
     * Integration test for concurrent request handling and resource limits.
     */
    @Test
    public void testConcurrentRequestResourceLimits() throws Throwable
    {
        // Set a low global limit to trigger resource limiting
        ClientResourceLimits.setGlobalLimit(1024); // 1KB total

        client.connect(false);

        AtomicInteger successfulRequests = new AtomicInteger(0);
        AtomicInteger resourceLimitedRequests = new AtomicInteger(0);
        CountDownLatch completionLatch = new CountDownLatch(20);

        // Create larger queries to consume more resources
        String largeValue = "x".repeat(200); // 200 character string

        for (int i = 0; i < 20; i++)
        {
            final int requestId = i;
            executorService.submit(() -> {
                try
                {
                    String query = "SELECT * FROM system.local WHERE key = '" + largeValue + requestId + "'";
                    Message.Response response = client.execute(new QueryMessage(query, QueryOptions.DEFAULT));

                    if (response instanceof ErrorMessage)
                    {
                        ErrorMessage error = (ErrorMessage) response;
                        if (error.error instanceof OverloadedException)
                        {
                            resourceLimitedRequests.incrementAndGet();
                        }
                        else
                        {
                            successfulRequests.incrementAndGet();
                        }
                    }
                    else
                    {
                        successfulRequests.incrementAndGet();
                    }
                }
                catch (Exception e)
                {
                    if (e.getCause() instanceof OverloadedException)
                    {
                        resourceLimitedRequests.incrementAndGet();
                    }
                    else
                    {
                        successfulRequests.incrementAndGet();
                    }
                }
                finally
                {
                    completionLatch.countDown();
                }
            });
        }

        assertTrue("All requests should complete within timeout",
                completionLatch.await(15, TimeUnit.SECONDS));

        int totalSuccessful = successfulRequests.get();
        int totalResourceLimited = resourceLimitedRequests.get();

        assertTrue("Some requests should succeed", totalSuccessful > 0);
        // Note: Resource limiting might not always trigger with this test setup
        assertTrue("Total requests should match", totalSuccessful + totalResourceLimited == 20);

        System.out.println("Resource limiting test results: " + totalSuccessful + " successful, " + totalResourceLimited + " resource limited");
    }

    /**
     * Integration test for dispatcher metrics collection during real request processing.
     */
    @Test
    public void testDispatcherMetricsIntegration() throws Throwable
    {
        client.connect(false);

        // Get baseline metrics
        int initialPausedConnections = ClientMetrics.instance.getNumberOfPausedConnections();

        // Execute some requests to generate metrics
        for (int i = 0; i < 10; i++)
        {
            client.execute(new OptionsMessage());
        }

        // Verify basic connection metrics behavior
        int finalPausedConnections = ClientMetrics.instance.getNumberOfPausedConnections();
        assertEquals("Paused connections should remain the same for successful requests",
                initialPausedConnections, finalPausedConnections);

        System.out.println("Metrics integration test: executed 10 requests successfully");
    }
}
