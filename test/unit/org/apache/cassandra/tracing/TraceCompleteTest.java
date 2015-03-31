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
package org.apache.cassandra.tracing;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.transport.Event;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolException;
import org.apache.cassandra.transport.Server;
import org.apache.cassandra.transport.SimpleClient;
import org.apache.cassandra.transport.messages.QueryMessage;
import org.apache.cassandra.transport.messages.RegisterMessage;

public class TraceCompleteTest extends CQLTester
{
    @Test
    public void testTraceComplete() throws Throwable
    {
        sessionNet(3);

        SimpleClient clientA = new SimpleClient(nativeAddr.getHostAddress(), nativePort);
        clientA.connect(false);
        try
        {
            SimpleClient.SimpleEventHandler eventHandlerA = new SimpleClient.SimpleEventHandler();
            clientA.setEventHandler(eventHandlerA);

            SimpleClient clientB = new SimpleClient(nativeAddr.getHostAddress(), nativePort);
            clientB.connect(false);
            try
            {
                SimpleClient.SimpleEventHandler eventHandlerB = new SimpleClient.SimpleEventHandler();
                clientB.setEventHandler(eventHandlerB);

                Message.Response resp = clientA.execute(new RegisterMessage(Collections.singletonList(Event.Type.TRACE_COMPLETE)));
                Assert.assertSame(Message.Type.READY, resp.type);

                createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");

                QueryMessage query = new QueryMessage("SELECT * FROM " + KEYSPACE + '.' + currentTable(), QueryOptions.DEFAULT);
                query.setTracingRequested();
                resp = clientA.execute(query);

                Event event = eventHandlerA.queue.poll(250, TimeUnit.MILLISECONDS);
                Assert.assertNotNull(event);

                // assert that only the connection that started the trace receives the trace-complete event
                Assert.assertNull(eventHandlerB.queue.poll(100, TimeUnit.MILLISECONDS));

                Assert.assertSame(Event.Type.TRACE_COMPLETE, event.type);
                Assert.assertEquals(resp.getTracingId(), ((Event.TraceComplete) event).traceSessionId);
            }
            finally
            {
                clientB.close();
            }
        }
        finally
        {
            clientA.close();
        }
    }

    @Test
    public void testTraceCompleteVersion3() throws Throwable
    {
        sessionNet(3);

        SimpleClient clientA = new SimpleClient(nativeAddr.getHostAddress(), nativePort, Server.VERSION_3);
        clientA.connect(false);
        try
        {
            SimpleClient.SimpleEventHandler eventHandlerA = new SimpleClient.SimpleEventHandler();
            clientA.setEventHandler(eventHandlerA);

            try
            {
                clientA.execute(new RegisterMessage(Collections.singletonList(Event.Type.TRACE_COMPLETE)));
                Assert.fail();
            }
            catch (RuntimeException e)
            {
                Assert.assertTrue(e.getCause() instanceof ProtocolException); // that's what we want
            }

            createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");

            QueryMessage query = new QueryMessage("SELECT * FROM " + KEYSPACE + '.' + currentTable(), QueryOptions.DEFAULT);
            query.setTracingRequested();
            clientA.execute(query);

            Event event = eventHandlerA.queue.poll(250, TimeUnit.MILLISECONDS);
            Assert.assertNull(event);
        }
        finally
        {
            clientA.close();
        }
    }

    @Test
    public void testTraceCompleteNotRegistered() throws Throwable
    {
        sessionNet(3);

        SimpleClient clientA = new SimpleClient(nativeAddr.getHostAddress(), nativePort);
        clientA.connect(false);
        try
        {
            SimpleClient.SimpleEventHandler eventHandlerA = new SimpleClient.SimpleEventHandler();
            clientA.setEventHandler(eventHandlerA);

            createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");

            // check that we do NOT receive a trace-complete event, since we didn't register for that

            // with setTracingRequested()
            QueryMessage query = new QueryMessage("SELECT * FROM " + KEYSPACE + '.' + currentTable(), QueryOptions.DEFAULT);
            query.setTracingRequested();
            clientA.execute(query);
            // and without setTracingRequested()
            query = new QueryMessage("SELECT * FROM " + KEYSPACE + '.' + currentTable(), QueryOptions.DEFAULT);
            clientA.execute(query);

            Event event = eventHandlerA.queue.poll(250, TimeUnit.MILLISECONDS);
            Assert.assertNull(event);
        }
        finally
        {
            clientA.close();
        }
    }

    @Test
    public void testTraceCompleteWithProbability() throws Throwable
    {
        sessionNet(3);

        double traceProbability = StorageService.instance.getTraceProbability();
        // check for trace-probability in QueryState.traceNextQuery() is x<y, not x<=y
        StorageService.instance.setTraceProbability(1.1d);

        SimpleClient clientA = new SimpleClient(nativeAddr.getHostAddress(), nativePort);
        clientA.connect(false);
        try
        {
            SimpleClient.SimpleEventHandler eventHandlerA = new SimpleClient.SimpleEventHandler();
            clientA.setEventHandler(eventHandlerA);

            SimpleClient clientB = new SimpleClient(nativeAddr.getHostAddress(), nativePort);
            clientB.connect(false);
            try
            {
                SimpleClient.SimpleEventHandler eventHandlerB = new SimpleClient.SimpleEventHandler();
                clientB.setEventHandler(eventHandlerB);

                Message.Response resp = clientA.execute(new RegisterMessage(Collections.singletonList(Event.Type.TRACE_COMPLETE)));
                Assert.assertSame(Message.Type.READY, resp.type);

                createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");

                QueryMessage query = new QueryMessage("SELECT * FROM " + KEYSPACE + '.' + currentTable(), QueryOptions.DEFAULT);
                clientA.execute(query);

                Event event = eventHandlerA.queue.poll(2000, TimeUnit.MILLISECONDS);
                Assert.assertNull(event);

                Assert.assertNull(eventHandlerB.queue.poll(100, TimeUnit.MILLISECONDS));
            }
            finally
            {
                clientB.close();
            }
        }
        finally
        {
            StorageService.instance.setTraceProbability(traceProbability);
            clientA.close();
        }
    }
}
