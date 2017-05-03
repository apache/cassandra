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
package org.apache.cassandra.net;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.MessagingService.Verb;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The tests check whether Queue expiration in the OutboundTcpConnection behaves properly for droppable and
 * non-droppable messages.
 */
public class OutboundTcpConnectionTest
{
    AtomicInteger messageId = new AtomicInteger(0);

    final static Verb VERB_DROPPABLE = Verb.MUTATION; // Droppable, 2s timeout
    final static Verb VERB_NONDROPPABLE = Verb.GOSSIP_DIGEST_ACK; // Not droppable
    
    final static long NANOS_FOR_TIMEOUT;

    static
    {
        DatabaseDescriptor.daemonInitialization();
        NANOS_FOR_TIMEOUT = TimeUnit.MILLISECONDS.toNanos(VERB_DROPPABLE.getTimeout()*2);
    }
    
    /**
     * Verifies our assumptions whether a Verb can be dropped or not. The tests make use of droppabilty, and
     * may produce wrong test results if their droppabilty is changed. 
     */
    @BeforeClass
    public static void assertDroppability()
    {
        if (!MessagingService.DROPPABLE_VERBS.contains(VERB_DROPPABLE))
            throw new AssertionError("Expected " + VERB_DROPPABLE + " to be droppable");
        if (MessagingService.DROPPABLE_VERBS.contains(VERB_NONDROPPABLE))
            throw new AssertionError("Expected " + VERB_NONDROPPABLE + " not to be droppable");
    }

    /**
     * Tests that non-droppable messages are never expired
     */
    @Test
    public void testNondroppable() throws UnknownHostException
    {
        OutboundTcpConnection otc = getOutboundTcpConnectionForLocalhost();
        long nanoTimeBeforeEnqueue = System.nanoTime();

        assertFalse("Fresh OutboundTcpConnection contains expired messages",
                otc.backlogContainsExpiredMessages(nanoTimeBeforeEnqueue));

        fillToPurgeSize(otc, VERB_NONDROPPABLE);
        fillToPurgeSize(otc, VERB_NONDROPPABLE);
        otc.expireMessages(expirationTimeNanos());

        assertFalse("OutboundTcpConnection with non-droppable verbs should not expire",
                otc.backlogContainsExpiredMessages(expirationTimeNanos()));
    }

    /**
     * Tests that droppable messages will be dropped after they expire, but not before.
     * 
     * @throws UnknownHostException
     */
    @Test
    public void testDroppable() throws UnknownHostException
    {
        OutboundTcpConnection otc = getOutboundTcpConnectionForLocalhost();
        long nanoTimeBeforeEnqueue = System.nanoTime();

        initialFill(otc, VERB_DROPPABLE);
        assertFalse("OutboundTcpConnection with droppable verbs should not expire immediately",
                otc.backlogContainsExpiredMessages(nanoTimeBeforeEnqueue));

        otc.expireMessages(nanoTimeBeforeEnqueue);
        assertFalse("OutboundTcpConnection with droppable verbs should not expire with enqueue-time expiration",
                otc.backlogContainsExpiredMessages(nanoTimeBeforeEnqueue));

        // Lets presume, expiration time have passed => At that time there shall be expired messages in the Queue
        long nanoTimeWhenExpired = expirationTimeNanos();
        assertTrue("OutboundTcpConnection with droppable verbs should have expired",
                otc.backlogContainsExpiredMessages(nanoTimeWhenExpired));

        // Using the same timestamp, lets expire them and check whether they have gone
        otc.expireMessages(nanoTimeWhenExpired);
        assertFalse("OutboundTcpConnection should not have expired entries",
                otc.backlogContainsExpiredMessages(nanoTimeWhenExpired));

        // Actually the previous test can be done in a harder way: As expireMessages() has run, we cannot have
        // ANY expired values, thus lets test also against nanoTimeBeforeEnqueue
        assertFalse("OutboundTcpConnection should not have any expired entries",
                otc.backlogContainsExpiredMessages(nanoTimeBeforeEnqueue));

    }

    /**
     * Fills the given OutboundTcpConnection with (1 + BACKLOG_PURGE_SIZE), elements. The first
     * BACKLOG_PURGE_SIZE elements are non-droppable, the last one is a message with the given Verb and can be
     * droppable or non-droppable.
     */
    private void initialFill(OutboundTcpConnection otc, Verb verb)
    {
        assertFalse("Fresh OutboundTcpConnection contains expired messages",
                otc.backlogContainsExpiredMessages(System.nanoTime()));

        fillToPurgeSize(otc, VERB_NONDROPPABLE);
        MessageOut<?> messageDroppable10s = new MessageOut<>(verb);
        otc.enqueue(messageDroppable10s, nextMessageId());
        otc.expireMessages(System.nanoTime());
    }

    /**
     * Returns a nano timestamp in the far future, when expiration should have been performed for VERB_DROPPABLE.
     * The offset is chosen as 2 times of the expiration time of VERB_DROPPABLE.
     * 
     * @return The future nano timestamp
     */
    private long expirationTimeNanos()
    {
        return System.nanoTime() + NANOS_FOR_TIMEOUT;
    }

    private int nextMessageId()
    {
        return messageId.incrementAndGet();
    }

    /**
     * Adds BACKLOG_PURGE_SIZE messages to the queue. Hint: At BACKLOG_PURGE_SIZE expiration starts to work.
     * 
     * @param otc
     *            The OutboundTcpConnection
     * @param verb
     *            The verb that defines the message type
     */
    private void fillToPurgeSize(OutboundTcpConnection otc, Verb verb)
    {
        for (int i = 0; i < OutboundTcpConnection.BACKLOG_PURGE_SIZE; i++)
        {
            otc.enqueue(new MessageOut<>(verb), nextMessageId());
        }
    }

    private OutboundTcpConnection getOutboundTcpConnectionForLocalhost() throws UnknownHostException
    {
        InetAddress lo = InetAddress.getByName("127.0.0.1");
        OutboundTcpConnectionPool otcPool = new OutboundTcpConnectionPool(lo, null);
        OutboundTcpConnection otc = new OutboundTcpConnection(otcPool, "lo-OutboundTcpConnectionTest");
        return otc;
    }
}
