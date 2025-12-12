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

package org.apache.cassandra.service;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Predicates;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.exceptions.WriteFailureException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.ReplicaPlans;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaCollection;
import org.apache.cassandra.locator.ReplicaUtils;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.metrics.ClientMetrics;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;

import static java.util.concurrent.TimeUnit.DAYS;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Integration tests for counter write metrics tracking.
 */
public class CounterWriteMetricsIntegrationTest {
    static Keyspace ks;
    static ColumnFamilyStore cfs;
    static EndpointsForToken targets;
    static EndpointsForToken pending;

    private static Replica full(String name) {
        try {
            return ReplicaUtils.full(InetAddressAndPort.getByName(name));
        } catch (UnknownHostException e) {
            throw new AssertionError(e);
        }
    }

    @BeforeClass
    public static void setUpClass() throws Throwable {
        SchemaLoader.loadSchema();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);

        // Initialize ClientMetrics with empty servers list to ensure counter write metrics are available
        ClientMetrics.instance.init(Collections.emptyList());

        // Register peers with expected DC for NetworkTopologyStrategy.
        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        metadata.clearUnsafe();
        metadata.updateHostId(UUID.randomUUID(), InetAddressAndPort.getByName("127.1.0.255"));
        metadata.updateHostId(UUID.randomUUID(), InetAddressAndPort.getByName("127.2.0.255"));

        DatabaseDescriptor.setEndpointSnitch(new IEndpointSnitch() {
            public String getRack(InetAddressAndPort endpoint) {
                return null;
            }

            public String getDatacenter(InetAddressAndPort endpoint) {
                byte[] address = endpoint.getAddress().getAddress();
                if (address[1] == 1)
                    return "datacenter1";
                else
                    return "datacenter2";
            }

            public <C extends ReplicaCollection<? extends C>> C sortedByProximity(InetAddressAndPort address, C replicas) {
                return replicas;
            }

            public int compareEndpoints(InetAddressAndPort target, Replica a1, Replica a2) {
                return 0;
            }

            public void gossiperStarting() {
            }

            public boolean isWorthMergingForRangeQuery(ReplicaCollection<?> merged, ReplicaCollection<?> l1, ReplicaCollection<?> l2) {
                return false;
            }
        });
        DatabaseDescriptor.setBroadcastAddress(InetAddress.getByName("127.1.0.1"));
        SchemaLoader.createKeyspace("Foo", KeyspaceParams.nts("datacenter1", 3, "datacenter2", 3), SchemaLoader.standardCFMD("Foo", "Bar"));
        ks = Keyspace.open("Foo");
        cfs = ks.getColumnFamilyStore("Bar");
        targets = EndpointsForToken.of(DatabaseDescriptor.getPartitioner().getToken(ByteBufferUtil.bytes(0)),
                full("127.1.0.255"), full("127.1.0.254"), full("127.1.0.253"),
                full("127.2.0.255"), full("127.2.0.254"), full("127.2.0.253"));
        pending = EndpointsForToken.empty(DatabaseDescriptor.getPartitioner().getToken(ByteBufferUtil.bytes(0)));
    }

    @Before
    public void resetCounters() {
        // Reset configuration state
        DatabaseDescriptor.setTrackCounterWriteMetricsEnabled(false);
    }

    // ============================================================================
    // SECTION 1: Metrics existence Tests
    // ============================================================================

    /**
     * Test that ClientMetrics instance has all counter write metric meters.
     */
    @Test
    public void clientMetricsHasAllCounterWriteMeters() {
        ClientMetrics metrics = ClientMetrics.instance;

        assertNotNull("counterWriteCoordinatorWaitForReplicasTimeouts should exist",
                metrics.counterWriteCoordinatorWaitForReplicasTimeouts);
        assertNotNull("counterWriteCoordinatorWaitForLeaderTimeouts should exist",
                metrics.counterWriteCoordinatorWaitForLeaderTimeouts);
        assertNotNull("counterWriteLeaderWaitForReplicasTimeouts should exist",
                metrics.counterWriteLeaderWaitForReplicasTimeouts);

        assertNotNull("counterWriteCoordinatorWaitForReplicasFailures should exist",
                metrics.counterWriteCoordinatorWaitForReplicasFailures);
        assertNotNull("counterWriteCoordinatorWaitForLeaderFailures should exist",
                metrics.counterWriteCoordinatorWaitForLeaderFailures);
        assertNotNull("counterWriteLeaderWaitForReplicasFailures should exist",
                metrics.counterWriteLeaderWaitForReplicasFailures);
    }

    // ============================================================================
    // SECTION 2: Path Setting & Tracking
    // ============================================================================

    /**
     * Test that counter write path can be set on WriteResponseHandler and persists.
     */
    @Test
    public void counterWritePathCanBeSetOnWriteResponseHandler() {
        AbstractWriteResponseHandler handler = createWriteResponseHandler(ConsistencyLevel.QUORUM, ConsistencyLevel.QUORUM, WriteType.COUNTER);

        // Initially should be NONE
        assertEquals("Initial path should be NONE", CounterWritePath.NONE, handler.counterWritePath);

        // Set to COORDINATOR_WAIT_FOR_REPLICAS
        handler.setCounterWritePath(CounterWritePath.COORDINATOR_WAIT_FOR_REPLICAS);
        assertEquals("Path should be set correctly", CounterWritePath.COORDINATOR_WAIT_FOR_REPLICAS, handler.counterWritePath);

        // Set to LEADER_WAIT_FOR_REPLICAS (test path mutation)
        handler.setCounterWritePath(CounterWritePath.LEADER_WAIT_FOR_REPLICAS);
        assertEquals("Path should be updated on second call", CounterWritePath.LEADER_WAIT_FOR_REPLICAS, handler.counterWritePath);
    }

    // ============================================================================
    // SECTION 3: Handler Support
    // ============================================================================

    /**
     * Test that DatacenterSyncWriteResponseHandler inherits counter write path tracking.
     */
    @Test
    public void datacenterSyncWriteResponseHandlerSupportsCounterWritePath() {
        AbstractWriteResponseHandler handler = createWriteResponseHandler(ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.EACH_QUORUM, WriteType.COUNTER);

        handler.setCounterWritePath(CounterWritePath.LEADER_WAIT_FOR_REPLICAS);
        assertEquals("DatacenterSyncWriteResponseHandler should support counter write path",
                CounterWritePath.LEADER_WAIT_FOR_REPLICAS, handler.counterWritePath);
    }

    /**
     * Test that BatchlogResponseHandler can be created and used with counter write tracking.
     */
    @Test
    public void batchlogResponseHandlerSupportsCounterWriteTracking() {
        AbstractWriteResponseHandler handler = createWriteResponseHandler(ConsistencyLevel.ONE, ConsistencyLevel.ONE, WriteType.COUNTER);
        assertNotNull("Handler should be created", handler);

        // Verify it has counter write path field
        handler.setCounterWritePath(CounterWritePath.COORDINATOR_WAIT_FOR_LEADER);
        assertEquals("BatchlogResponseHandler should support counter write path",
                CounterWritePath.COORDINATOR_WAIT_FOR_LEADER, handler.counterWritePath);
    }

    // ============================================================================
    // SECTION 4: Individual Path Timeout Metrics
    // ============================================================================

    /**
     * Comprehensive test covering path metrics with all combinations:
     * - WriteType: COUNTER and SIMPLE
     * - Tracking: enabled and disabled
     * - Scenarios: timeout, failures, and successful acks
     */
    @Test
    public void counterWriteMetricsWithAllCombinations() {
        // Test 1: COORDINATOR_WAIT_FOR_REPLICAS + Tracking ENABLED + Timeout
        DatabaseDescriptor.setTrackCounterWriteMetricsEnabled(true);
        long beforeCoordTimeout = ClientMetrics.instance.counterWriteCoordinatorWaitForReplicasTimeouts.getCount();
        
        WriteResponseHandler handler1 = (WriteResponseHandler) createWriteResponseHandler(ConsistencyLevel.QUORUM, ConsistencyLevel.QUORUM, WriteType.COUNTER);
        handler1.setCounterWritePath(CounterWritePath.COORDINATOR_WAIT_FOR_REPLICAS);
        try {
            handler1.get();
        } catch (WriteTimeoutException e) {
            // Expected
        }
        long afterCoordTimeout = ClientMetrics.instance.counterWriteCoordinatorWaitForReplicasTimeouts.getCount();
        assertEquals("COORDINATOR_WAIT_FOR_REPLICAS + Timeout: metric should increment", beforeCoordTimeout + 1, afterCoordTimeout);

        // Test 2: COORDINATOR_WAIT_FOR_REPLICAS + Tracking ENABLED + Failures
        long beforeCoordFailure = ClientMetrics.instance.counterWriteCoordinatorWaitForReplicasFailures.getCount();
        
        WriteResponseHandler handler2 = (WriteResponseHandler) createWriteResponseHandler(ConsistencyLevel.QUORUM, ConsistencyLevel.QUORUM, WriteType.COUNTER);
        handler2.setCounterWritePath(CounterWritePath.COORDINATOR_WAIT_FOR_REPLICAS);
        handler2.onFailure(targets.get(0).endpoint(), RequestFailureReason.TIMEOUT);
        handler2.onFailure(targets.get(1).endpoint(), RequestFailureReason.TIMEOUT);
        handler2.onFailure(targets.get(2).endpoint(), RequestFailureReason.TIMEOUT);
        try {
            handler2.get();
        } catch (WriteFailureException e) {
            // Expected
        }
        long afterCoordFailure = ClientMetrics.instance.counterWriteCoordinatorWaitForReplicasFailures.getCount();
        assertEquals("COORDINATOR_WAIT_FOR_REPLICAS + Failure: metric should increment", beforeCoordFailure + 1, afterCoordFailure);

        // Test 3: LEADER_WAIT_FOR_REPLICAS + Tracking ENABLED + Timeout
        long beforeLeaderTimeout = ClientMetrics.instance.counterWriteLeaderWaitForReplicasTimeouts.getCount();
        
        WriteResponseHandler handler3 = (WriteResponseHandler) createWriteResponseHandler(ConsistencyLevel.QUORUM, ConsistencyLevel.QUORUM, WriteType.COUNTER);
        handler3.setCounterWritePath(CounterWritePath.LEADER_WAIT_FOR_REPLICAS);
        handler3.trackSuccessfulAck(targets.get(0).endpoint());
        handler3.trackSuccessfulAck(targets.get(1).endpoint());
        try {
            handler3.get();
        } catch (WriteTimeoutException e) {
            // Expected
        }
        long afterLeaderTimeout = ClientMetrics.instance.counterWriteLeaderWaitForReplicasTimeouts.getCount();
        assertEquals("LEADER_WAIT_FOR_REPLICAS + Timeout: metric should increment", beforeLeaderTimeout + 1, afterLeaderTimeout);

        // Test 4: LEADER_WAIT_FOR_REPLICAS + Tracking ENABLED + Failure
        long beforeLeaderFailure = ClientMetrics.instance.counterWriteLeaderWaitForReplicasFailures.getCount();
        
        WriteResponseHandler handler4 = (WriteResponseHandler) createWriteResponseHandler(ConsistencyLevel.QUORUM, ConsistencyLevel.QUORUM, WriteType.COUNTER);
        handler4.setCounterWritePath(CounterWritePath.LEADER_WAIT_FOR_REPLICAS);
        handler4.onFailure(targets.get(0).endpoint(), RequestFailureReason.TIMEOUT);
        handler4.onFailure(targets.get(1).endpoint(), RequestFailureReason.TIMEOUT);
        handler4.onFailure(targets.get(2).endpoint(), RequestFailureReason.TIMEOUT);
        try {
            handler4.get();
        } catch (WriteFailureException e) {
            // Expected
        }
        long afterLeaderFailure = ClientMetrics.instance.counterWriteLeaderWaitForReplicasFailures.getCount();
        assertEquals("LEADER_WAIT_FOR_REPLICAS + Failure: metric should increment", beforeLeaderFailure + 1, afterLeaderFailure);

        // Test 5: COORDINATOR_WAIT_FOR_LEADER + Tracking ENABLED + Timeout
        long beforeCoordLeaderTimeout = ClientMetrics.instance.counterWriteCoordinatorWaitForLeaderTimeouts.getCount();
        
        WriteResponseHandler handler5 = (WriteResponseHandler) createWriteResponseHandler(ConsistencyLevel.QUORUM, ConsistencyLevel.QUORUM, WriteType.COUNTER);
        handler5.setCounterWritePath(CounterWritePath.COORDINATOR_WAIT_FOR_LEADER);
        try {
            handler5.get();
        } catch (WriteTimeoutException e) {
            // Expected
        }
        long afterCoordLeaderTimeout = ClientMetrics.instance.counterWriteCoordinatorWaitForLeaderTimeouts.getCount();
        assertEquals("COORDINATOR_WAIT_FOR_LEADER + Timeout: metric should increment", beforeCoordLeaderTimeout + 1, afterCoordLeaderTimeout);

        // Test 6: COORDINATOR_WAIT_FOR_LEADER + Tracking ENABLED + Failure
        long beforeCoordLeaderFailure = ClientMetrics.instance.counterWriteCoordinatorWaitForLeaderFailures.getCount();
        
        WriteResponseHandler handler6 = (WriteResponseHandler) createWriteResponseHandler(ConsistencyLevel.QUORUM, ConsistencyLevel.QUORUM, WriteType.COUNTER);
        handler6.setCounterWritePath(CounterWritePath.COORDINATOR_WAIT_FOR_LEADER);
        handler6.onFailure(targets.get(0).endpoint(), RequestFailureReason.TIMEOUT);
        handler6.onFailure(targets.get(1).endpoint(), RequestFailureReason.TIMEOUT);
        handler6.onFailure(targets.get(2).endpoint(), RequestFailureReason.TIMEOUT);
        try {
            handler6.get();
        } catch (WriteFailureException e) {
            // Expected
        }
        long afterCoordLeaderFailure = ClientMetrics.instance.counterWriteCoordinatorWaitForLeaderFailures.getCount();
        assertEquals("COORDINATOR_WAIT_FOR_LEADER + Failure: metric should increment", beforeCoordLeaderFailure + 1, afterCoordLeaderFailure);

        // Test 7: SIMPLE + Tracking ENABLED + Timeout (should NOT increment counter metrics)
        long beforeSimpleTimeout = ClientMetrics.instance.counterWriteCoordinatorWaitForLeaderTimeouts.getCount();
        
        WriteResponseHandler handler7 = (WriteResponseHandler) createWriteResponseHandler(ConsistencyLevel.QUORUM, ConsistencyLevel.QUORUM, WriteType.SIMPLE);
        try {
            handler7.get();
        } catch (WriteTimeoutException e) {
            // Expected
        }
        long afterSimpleTimeout = ClientMetrics.instance.counterWriteCoordinatorWaitForLeaderTimeouts.getCount();
        assertEquals("SIMPLE+Tracking: counter metrics should NOT increment", beforeSimpleTimeout, afterSimpleTimeout);

        // Test 8: COUNTER + Tracking DISABLED + Timeout (should NOT record metrics)
        DatabaseDescriptor.setTrackCounterWriteMetricsEnabled(false);
        long beforeDisabledTimeout = ClientMetrics.instance.counterWriteCoordinatorWaitForReplicasTimeouts.getCount();
        
        WriteResponseHandler handler8 = (WriteResponseHandler) createWriteResponseHandler(ConsistencyLevel.QUORUM, ConsistencyLevel.QUORUM, WriteType.COUNTER);
        handler8.setCounterWritePath(CounterWritePath.COORDINATOR_WAIT_FOR_REPLICAS);
        try {
            handler8.get();
        } catch (WriteTimeoutException e) {
            // Expected
        }
        long afterDisabledTimeout = ClientMetrics.instance.counterWriteCoordinatorWaitForReplicasTimeouts.getCount();
        assertEquals("COUNTER+Disabled: metrics should NOT record", beforeDisabledTimeout, afterDisabledTimeout);

        // Test 9: COUNTER + Tracking DISABLED + Failures (should NOT record metrics)
        long beforeDisabledFailure = ClientMetrics.instance.counterWriteCoordinatorWaitForReplicasFailures.getCount();
        
        WriteResponseHandler handler9 = (WriteResponseHandler) createWriteResponseHandler(ConsistencyLevel.QUORUM, ConsistencyLevel.QUORUM, WriteType.COUNTER);
        handler9.setCounterWritePath(CounterWritePath.COORDINATOR_WAIT_FOR_REPLICAS);
        handler9.onFailure(targets.get(0).endpoint(), RequestFailureReason.TIMEOUT);
        handler9.onFailure(targets.get(1).endpoint(), RequestFailureReason.TIMEOUT);
        handler9.onFailure(targets.get(2).endpoint(), RequestFailureReason.TIMEOUT);
        try {
            handler9.get();
        } catch (WriteFailureException e) {
            // Expected
        }
        long afterDisabledFailure = ClientMetrics.instance.counterWriteCoordinatorWaitForReplicasFailures.getCount();
        assertEquals("COUNTER+Disabled+Failure: metrics should NOT record", beforeDisabledFailure, afterDisabledFailure);

        // Test 10: SIMPLE + Tracking DISABLED + Timeout (should NOT record counter metrics)
        long beforeSimpleDisabled = ClientMetrics.instance.counterWriteLeaderWaitForReplicasTimeouts.getCount();
        
        WriteResponseHandler handler10 = (WriteResponseHandler) createWriteResponseHandler(ConsistencyLevel.QUORUM, ConsistencyLevel.QUORUM, WriteType.SIMPLE);
        try {
            handler10.get();
        } catch (WriteTimeoutException e) {
            // Expected
        }
        long afterSimpleDisabled = ClientMetrics.instance.counterWriteLeaderWaitForReplicasTimeouts.getCount();
        assertEquals("SIMPLE+Disabled: counter metrics should NOT increment", beforeSimpleDisabled, afterSimpleDisabled);

        // Test 11: COUNTER + NONE path + Timeout (default case in switch - should not record any metrics)
        DatabaseDescriptor.setTrackCounterWriteMetricsEnabled(true);
        long beforeNoneTimeout = ClientMetrics.instance.counterWriteCoordinatorWaitForReplicasTimeouts.getCount();
        
        WriteResponseHandler handler11 = (WriteResponseHandler) createWriteResponseHandler(ConsistencyLevel.QUORUM, ConsistencyLevel.QUORUM, WriteType.COUNTER);
        // Handler initialized with NONE path (default) - do NOT call setCounterWritePath
        assertEquals("Handler should have NONE path by default", CounterWritePath.NONE, handler11.counterWritePath);
        try {
            handler11.get();
        } catch (WriteTimeoutException e) {
            // Expected
        }
        long afterNoneTimeout = ClientMetrics.instance.counterWriteCoordinatorWaitForReplicasTimeouts.getCount();
        assertEquals("NONE path: no metrics should be recorded", beforeNoneTimeout, afterNoneTimeout);

        // Test 12: COUNTER + NONE path + Failure (default case in switch - should not record any metrics)
        long beforeNoneFailure = ClientMetrics.instance.counterWriteCoordinatorWaitForReplicasFailures.getCount();
        
        WriteResponseHandler handler12 = (WriteResponseHandler) createWriteResponseHandler(ConsistencyLevel.QUORUM, ConsistencyLevel.QUORUM, WriteType.COUNTER);
        // Handler initialized with NONE path (default)
        assertEquals("Handler should have NONE path by default", CounterWritePath.NONE, handler12.counterWritePath);
        handler12.onFailure(targets.get(0).endpoint(), RequestFailureReason.TIMEOUT);
        handler12.onFailure(targets.get(1).endpoint(), RequestFailureReason.TIMEOUT);
        handler12.onFailure(targets.get(2).endpoint(), RequestFailureReason.TIMEOUT);
        try {
            handler12.get();
        } catch (WriteFailureException e) {
            // Expected
        }
        long afterNoneFailure = ClientMetrics.instance.counterWriteCoordinatorWaitForReplicasFailures.getCount();
        assertEquals("NONE path: no failure metrics should be recorded", beforeNoneFailure, afterNoneFailure);
    }

    // ============================================================================
    // SECTION 5: Non-Counter Write
    // ============================================================================

    /**
     * Test WriteResponseHandler.trackCounterWriteAck() with m == null (local response).
     * Verifies that onResponse(null) calls trackSuccessfulAck with local broadcast address.
     * 
     * Verifies the actual values:
     * - acked=1 (one local response from FBUtilities.getBroadcastAddressAndPort())
     * - ackedEndpoints contains the local broadcast address
     */
    @Test
    public void trackCounterWriteAckWithNullMessageAndNoSpamLoggerVerification() {
        DatabaseDescriptor.setTrackCounterWriteMetricsEnabled(true);

        // Create log appender to capture messages
        TestLogAppender testAppender = new TestLogAppender();
        ch.qos.logback.classic.Logger logger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(AbstractWriteResponseHandler.class);
        logger.addAppender(testAppender);
        testAppender.start();

        try {
            WriteResponseHandler handler = (WriteResponseHandler) createWriteResponseHandler(ConsistencyLevel.QUORUM, ConsistencyLevel.QUORUM, WriteType.COUNTER);
            handler.setCounterWritePath(CounterWritePath.COORDINATOR_WAIT_FOR_REPLICAS);

            // Call onResponse with null (local response) - exercises line 92: if (m == null)
            handler.onResponse(null);

            try {
                handler.get();
                fail("Should have thrown WriteTimeoutException");
            } catch (WriteTimeoutException e) {
                // Expected - timeout will trigger logging
            }

            // Verify NoSpamLogger captured the message with local response tracked
            assertFalse("Log message should have been captured", testAppender.capturedMessages.isEmpty());
            String logMessage = testAppender.capturedMessages.get(0);
            
            // Assert the log contains expected fields
            assertTrue("Log should contain 'Counter write TIMEOUT'", logMessage.contains("Counter write TIMEOUT"));
            assertTrue("Log should contain 'COORDINATOR_WAIT_FOR_REPLICAS'", logMessage.contains("COORDINATOR_WAIT_FOR_REPLICAS"));
            
            // Verify acked=1 (one local response)
            assertTrue("Log should contain 'acked=1' (one local response from null message)", logMessage.contains("acked=1"));
            
            // Verify ackedEndpoints contains the local broadcast address
            assertTrue("Log should contain 'ackedEndpoints='", logMessage.contains("ackedEndpoints="));
            // The local broadcast address is 127.0.0.1 (from FBUtilities.getBroadcastAddressAndPort())
            assertTrue("Log should contain local broadcast address in ackedEndpoints", logMessage.contains("127.0.0.1"));

        } finally {
            testAppender.stop();
            logger.detachAppender(testAppender);
        }
    }

    /**
     * Test WriteResponseHandler.trackCounterWriteAck() with m.from() != null (remote response).
     * Verifies that onResponse with a message from an endpoint tracks that endpoint and logs correctly.
     */
    @Test
    public void trackCounterWriteAckWithRemoteMessageAndNoSpamLoggerVerification() {
        DatabaseDescriptor.setTrackCounterWriteMetricsEnabled(true);

        // Create log appender to capture messages
        TestLogAppender testAppender = new TestLogAppender();
        ch.qos.logback.classic.Logger logger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(AbstractWriteResponseHandler.class);
        logger.addAppender(testAppender);
        testAppender.start();

        try {
            WriteResponseHandler handler = (WriteResponseHandler) createWriteResponseHandler(ConsistencyLevel.QUORUM, ConsistencyLevel.QUORUM, WriteType.COUNTER);
            handler.setCounterWritePath(CounterWritePath.LEADER_WAIT_FOR_REPLICAS);

            // Call onResponse() with messages from remote endpoints
            // This properly decrements the responses counter and tracks successful acks via trackCounterWriteAck
            Message<NoPayload> msg1 = Message.builder(Verb.ECHO_REQ, NoPayload.noPayload)
                    .from(targets.get(0).endpoint())
                    .build();
            Message<NoPayload> msg2 = Message.builder(Verb.ECHO_REQ, NoPayload.noPayload)
                    .from(targets.get(1).endpoint())
                    .build();
            Message<NoPayload> msg3 = Message.builder(Verb.ECHO_REQ, NoPayload.noPayload)
                    .from(targets.get(2).endpoint())
                    .build();

            handler.onResponse(msg1);
            handler.onResponse(msg2);
            handler.onResponse(msg3);

            try {
                handler.get();
                fail("Should have thrown WriteTimeoutException");
            } catch (WriteTimeoutException e) {
                // Expected - timeout will trigger logging
            }

            // Verify NoSpamLogger captured the message with remote endpoints tracked
            assertFalse("Log message should have been captured", testAppender.capturedMessages.isEmpty());
            String logMessage = testAppender.capturedMessages.get(0);
            
            // Assert the log contains expected fields
            assertTrue("Log should contain 'Counter write TIMEOUT'", logMessage.contains("Counter write TIMEOUT"));
            assertTrue("Log should contain 'LEADER_WAIT_FOR_REPLICAS'", logMessage.contains("LEADER_WAIT_FOR_REPLICAS"));
            assertTrue("Log should contain 'ackedEndpoints='", logMessage.contains("ackedEndpoints="));
            
            // Verify acked count reflects the three responses received
            // ackCount() = blockFor() - responses
            // responses starts at blockFor=4, then decremented 3 times: 4 - 1 = 3 acks
            assertTrue("Log should contain 'acked=3'", logMessage.contains("acked=3"));
            
            // Verify acked endpoints are logged (from m.from() != null path)
            // The log shows: ackedEndpoints=[/127.1.0.253:7012, /127.1.0.255:7012, /127.1.0.254:7012]
            // which are targets[2], targets[0], targets[1] (order may differ)
            String ackedEndpointsSection = logMessage.substring(logMessage.indexOf("ackedEndpoints="));
            assertTrue("ackedEndpoints should contain targets[0] endpoint", 
                ackedEndpointsSection.contains(targets.get(0).endpoint().getAddress().getHostAddress()));
            assertTrue("ackedEndpoints should contain targets[1] endpoint", 
                ackedEndpointsSection.contains(targets.get(1).endpoint().getAddress().getHostAddress()));
            assertTrue("ackedEndpoints should contain targets[2] endpoint", 
                ackedEndpointsSection.contains(targets.get(2).endpoint().getAddress().getHostAddress()));

        } finally {
            testAppender.stop();
            logger.detachAppender(testAppender);
        }
    }

    /**
     * Custom log appender to capture log messages for assertion in tests.
     */
    private static class TestLogAppender extends AppenderBase<ILoggingEvent> {
        private final java.util.List<String> capturedMessages = new java.util.ArrayList<>();

        @Override
        protected void append(ILoggingEvent event) {
            capturedMessages.add(event.getFormattedMessage());
        }
    }

    private static AbstractWriteResponseHandler createWriteResponseHandler(ConsistencyLevel cl, ConsistencyLevel ideal, WriteType writeType) {
        return ks.getReplicationStrategy().getWriteResponseHandler(ReplicaPlans.forWrite(ks, cl, targets, pending, Predicates.alwaysTrue(), ReplicaPlans.writeAll),
                null, writeType, null, Dispatcher.RequestTime.forImmediateExecution(), ideal);
    }

    private static AbstractWriteResponseHandler createCounterWriteResponseHandler(ConsistencyLevel cl, ConsistencyLevel ideal) {
        return createWriteResponseHandler(cl, ideal, WriteType.COUNTER);
    }
}
