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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.runner.RunWith;

import com.datastax.driver.core.Session;
import io.netty.channel.Channel;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.transport.messages.QueryMessage;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(BMUnitRunner.class)
public class PreV5HandlersTest extends CQLTester
{
    private static final ProtocolVersion VERSION = ProtocolVersion.V4;

    // state shared with the Byteman rules below, which run on the request worker and the channel's event loop
    private static volatile String heldQuery;
    private static volatile Channel serverChannel;
    private static volatile Object writtenResponse;
    private static final CountDownLatch processing = new CountDownLatch(1);
    private static final CountDownLatch proceed = new CountDownLatch(1);
    private static final CountDownLatch written = new CountDownLatch(1);

    /**
     * Regression test for CASSANDRA-21766 to ensure that a pre-V5 client can disconnect while a request is in flight
     * without leaking the response. The query is held on the request worker before it is executed until the client
     * has disconnected and the server has torn the connection's pipeline down, then released, so the Flusher always
     * writes the response to a dead channel. Whatever the Flusher wrote must not hold an unreleased buffer.
     */
    @Test
    @BMRules(rules = {
    @BMRule(name = "Hold the request until the connection is gone",
            targetClass = "org.apache.cassandra.transport.Dispatcher",
            targetMethod = "processRequest(io.netty.channel.Channel, org.apache.cassandra.transport.Message$Request, " +
                           "org.apache.cassandra.transport.ClientResourceLimits$Overload, " +
                           "org.apache.cassandra.transport.Dispatcher$RequestTime)",
            targetLocation = "AT ENTRY",
            action = "org.apache.cassandra.transport.PreV5HandlersTest.beforeProcess($1, $2)"),
    @BMRule(name = "Capture what the Flusher wrote",
            targetClass = "org.apache.cassandra.transport.Flusher",
            targetMethod = "processUnframedResponse",
            targetLocation = "AT EXIT",
            action = "org.apache.cassandra.transport.PreV5HandlersTest.afterWrite($1)") })
    public void testPreV5ClientDisconnectWithRequestsInFlight() throws Throwable
    {
        requireNetwork();
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        execute("INSERT INTO %s (k, v) VALUES (0, 0)");
        heldQuery = String.format("SELECT * FROM %s.%s WHERE k = 0", KEYSPACE, currentTable());

        try
        {
            Session session = sessionNet(VERSION).getCluster().connect();
            session.executeAsync(heldQuery);
            assertTrue("query should reach the request worker", processing.await(5, TimeUnit.SECONDS));

            session.closeAsync().force().get(10, TimeUnit.SECONDS);
            assertTrue("server should see the disconnect", serverChannel.closeFuture().await(5, TimeUnit.SECONDS));
            // deregistration, and with it the pipeline teardown, runs as a separate task after close completes
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (serverChannel.pipeline().firstContext() != null && System.nanoTime() < deadline)
                Thread.sleep(10);
            assertNull("server pipeline should have been destroyed", serverChannel.pipeline().first());
        }
        finally
        {
            proceed.countDown();
        }

        assertTrue("Flusher should write the response", written.await(5, TimeUnit.SECONDS));
        assertNotNull(writtenResponse);
        // Before CASSANDRA-21508 the Flusher writes a Message.Response, which holds no buffer until the (now gone)
        // pipeline encodes it. Since then it writes an Envelope whose body was allocated on the request worker.
        if (writtenResponse instanceof Envelope)
            assertEquals("response envelope body leaked", 0, ((Envelope) writtenResponse).body.refCnt());
    }

    public static void beforeProcess(Channel channel, Message.Request request) throws InterruptedException
    {
        if (!(request instanceof QueryMessage) || !((QueryMessage) request).query.equals(heldQuery))
            return;
        serverChannel = channel;
        processing.countDown();
        proceed.await(5, TimeUnit.SECONDS);
    }

    public static void afterWrite(Flusher.FlushItem<?> item)
    {
        if (item.channel != serverChannel || proceed.getCount() > 0)
            return;
        writtenResponse = item.responseEnvelope;
        written.countDown();
    }
}
