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

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.util.concurrent.Uninterruptibles;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.metrics.ClientMetrics;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.QueryMessage;
import org.apache.cassandra.utils.Clock;

import io.netty.channel.Channel;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MessageManagementDispatcherTest
{
    private static ManagementTestDispatcher dispatch;
    private static int maxManagementThreadsBeforeTests;

    @BeforeClass
    public static void init() throws Exception
    {
        DatabaseDescriptor.daemonInitialization();
        ClientMetrics.instance.init(null);
        maxManagementThreadsBeforeTests = DatabaseDescriptor.getNativeTransportManagementMaxThreads();
        dispatch = new ManagementTestDispatcher();
    }

    @AfterClass
    public static void restoreManagementSize()
    {
        DatabaseDescriptor.setNativeTransportManagementMaxThreads(maxManagementThreadsBeforeTests);
    }

    @Test
    public void testManagementExecutorRouting() throws Exception
    {
        long startRequests = completedRequests();
        long startAuth = completedAuth();

        DatabaseDescriptor.setNativeTransportManagementMaxThreads(1);

        long managementTasks = tryRequest(this::completedManagement, queryMessage("INVOKE COMMAND status;"));
        assertEquals("Management request should be routed to management executor",
                     1, managementTasks);
        assertEquals("No auth requests should be processed", startAuth, completedAuth());
        assertEquals("Regular requests should not increase", startRequests, completedRequests());
    }

    @Test
    public void testManagementExecutorIsolation() throws Exception
    {
        long startManagement = completedManagement();

        DatabaseDescriptor.setNativeTransportManagementMaxThreads(1);

        // Test that regular (non-management) requests don't use management executor
        for (Message.Type type : Message.Type.values())
        {
            if (type.direction != Message.Direction.REQUEST)
                continue;

            long requests = tryRequest(() -> Message.Type.CREDENTIALS == type || Message.Type.AUTH_RESPONSE == type
                                             ? completedAuth()
                                             : completedRequests(),
                                       createRegularRequest(type));

            assertEquals("No management tasks should be processed", startManagement, completedManagement());
            assertEquals(format("Request should be processed for type: %s", type), 1, requests);
        }
    }

    @Test
    public void testManagementConnectionAllMessageTypes() throws Exception
    {
        DatabaseDescriptor.setNativeTransportManagementMaxThreads(1);

        for (Message.Type type : Message.Type.values())
        {
            if (type.direction != Message.Direction.REQUEST)
                continue;

            Message.Request request = createManagementRequest(type);
            long managementTasks = tryRequest(() -> Message.Type.CREDENTIALS == type || Message.Type.AUTH_RESPONSE == type
                                                    ? completedAuth()
                                                    : completedManagement(),
                                              request);
            assertEquals(format("Management %s request should route to management executor", type),
                         1, managementTasks);
        }
    }

    @Test
    public void testNonServerConnectionNotRoutedToManagement() throws Exception
    {
        DatabaseDescriptor.setNativeTransportManagementMaxThreads(1);

        // Create a connection that is not a ServerConnection
        Connection nonServerConnection = connectionMock();

        Message.Request request = new Message.Request(Message.Type.QUERY)
        {
            @Override
            public Connection connection()
            {
                return nonServerConnection;
            }

            @Override
            public Response execute(QueryState queryState, Dispatcher.RequestTime requestTime, boolean traceRequest)
            {
                return null;
            }
        };

        long startManagement = completedManagement();
        long regularTasks = tryRequest(this::completedRequests, request);

        assertEquals("Non-server connection should use regular executor", 1, regularTasks);
        assertEquals("Non-server connection should not use management executor",
                            startManagement, completedManagement());
    }

    @Test
    public void testCommandStatementAllowed()
    {
        assertTrue("INVOKE COMMAND statements should be allowed",
                   Dispatcher.isManagementRequestAllowed(queryMessage("INVOKE COMMAND status;")));
    }

    @Test
    public void testCommandWithParamsAllowed()
    {
        assertTrue("INVOKE COMMAND with params should be allowed",
                   Dispatcher.isManagementRequestAllowed(
                   queryMessage("INVOKE COMMAND forcecompact WITH \"keyspace\" = 'ks' AND \"table\" = 'tbl';")));
    }

    @Test
    public void testSelectSystemLocalAllowed()
    {
        assertTrue("SELECT from system.local should be allowed",
                   Dispatcher.isManagementRequestAllowed(
                   queryMessage("SELECT * FROM system.local WHERE key = 'local';")));
    }

    @Test
    public void testSelectSystemPeersAllowed()
    {
        assertTrue("SELECT from system.peers should be allowed",
                   Dispatcher.isManagementRequestAllowed(
                   queryMessage("SELECT * FROM system.peers;")));
    }

    @Test
    public void testSelectSystemSchemaKeyspacesAllowed()
    {
        assertTrue("SELECT from system_schema.keyspaces should be allowed",
                   Dispatcher.isManagementRequestAllowed(
                   queryMessage("SELECT * FROM system_schema.keyspaces;")));
    }

    @Test
    public void testSelectSystemSchemaTablesAllowed()
    {
        assertTrue("SELECT from system_schema.tables should be allowed",
                   Dispatcher.isManagementRequestAllowed(
                   queryMessage("SELECT * FROM system_schema.tables;")));
    }

    @Test
    public void testSelectSystemSchemaColumnsAllowed()
    {
        assertTrue("SELECT from system_schema.columns should be allowed",
                   Dispatcher.isManagementRequestAllowed(
                   queryMessage("SELECT * FROM system_schema.columns;")));
    }

    @Test
    public void testSelectUserKeyspaceRejected()
    {
        assertFalse("SELECT from user keyspace should be rejected",
                    Dispatcher.isManagementRequestAllowed(
                    queryMessage("SELECT * FROM my_keyspace.my_table;")));
    }

    @Test
    public void testSelectSystemAuthRejected()
    {
        assertFalse("SELECT from system_auth.roles should be rejected",
                    Dispatcher.isManagementRequestAllowed(
                    queryMessage("SELECT * FROM system_auth.roles;")));
        assertFalse("SELECT from system_auth.role_permissions should be rejected",
                    Dispatcher.isManagementRequestAllowed(
                    queryMessage("SELECT * FROM system_auth.role_permissions;")));
    }

    @Test
    public void testUseSystemAuthRejected()
    {
        assertFalse("USE system_auth should be rejected",
                    Dispatcher.isManagementRequestAllowed(queryMessage("USE system_auth;")));
    }

    @Test
    public void testSelectSystemDistributedAllowed()
    {
        assertTrue("SELECT from system_distributed should be allowed",
                   Dispatcher.isManagementRequestAllowed(
                   queryMessage("SELECT * FROM system_distributed.repair_history;")));
    }

    @Test
    public void testInsertRejected()
    {
        assertFalse("INSERT should be rejected",
                    Dispatcher.isManagementRequestAllowed(queryMessage(
                    "INSERT INTO system.local (key) VALUES ('test');")));
    }

    @Test
    public void testCreateTableRejected()
    {
        assertFalse("DDL should be rejected",
                    Dispatcher.isManagementRequestAllowed(queryMessage(
                    "CREATE TABLE system.foo (k text PRIMARY KEY);")));
    }

    @Test
    public void testUnqualifiedSelectRejected()
    {
        assertFalse("Unqualified SELECT should be rejected",
                    Dispatcher.isManagementRequestAllowed(queryMessage("SELECT * FROM local;")));
    }

    @Test
    public void testInvalidSyntaxRejected()
    {
        assertFalse("Invalid syntax should be rejected",
                    Dispatcher.isManagementRequestAllowed(queryMessage("NOT VALID CQL AT ALL;")));
    }

    @Test
    public void testProtocolMessagesAllowed()
    {
        Message.Request startup = createManagementRequest(Message.Type.STARTUP);
        assertTrue("STARTUP should be allowed", Dispatcher.isManagementRequestAllowed(startup));

        Message.Request options = createManagementRequest(Message.Type.OPTIONS);
        assertTrue("OPTIONS should be allowed", Dispatcher.isManagementRequestAllowed(options));

        Message.Request register = createManagementRequest(Message.Type.REGISTER);
        assertTrue("REGISTER should be allowed", Dispatcher.isManagementRequestAllowed(register));
    }

    @Test
    public void testPrepareRejected()
    {
        Message.Request prepare = createManagementRequest(Message.Type.PREPARE);
        assertFalse("PREPARE should be rejected", Dispatcher.isManagementRequestAllowed(prepare));
    }

    @Test
    public void testBatchRejected()
    {
        Message.Request batch = createManagementRequest(Message.Type.BATCH);
        assertFalse("BATCH should be rejected", Dispatcher.isManagementRequestAllowed(batch));
    }

    @Test
    public void testExecuteRejected()
    {
        Message.Request execute = createManagementRequest(Message.Type.EXECUTE);
        assertFalse("EXECUTE should be rejected", Dispatcher.isManagementRequestAllowed(execute));
    }

    @Test
    public void testUseSystemKeyspaceAllowed()
    {
        assertTrue("USE system should be allowed",
                   Dispatcher.isManagementRequestAllowed(queryMessage("USE system;")));
    }

    @Test
    public void testUseSystemSchemaAllowed()
    {
        assertTrue("USE system_schema should be allowed",
                   Dispatcher.isManagementRequestAllowed(queryMessage("USE system_schema;")));
    }

    @Test
    public void testUseVirtualKeyspaceAllowed()
    {
        assertTrue("USE system_views should be allowed",
                   Dispatcher.isManagementRequestAllowed(queryMessage("USE system_views;")));
    }

    @Test
    public void testUseUserKeyspaceRejected()
    {
        assertFalse("USE user keyspace should be rejected",
                    Dispatcher.isManagementRequestAllowed(queryMessage("USE my_keyspace;")));
    }

    @Test
    public void testIsDoneTracksManagementExecutor() throws Exception
    {
        Dispatcher managementDispatcher = new ManagementTestDispatcher(true);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Dispatcher.managementExecutor.submit(() -> {
            started.countDown();
            Uninterruptibles.awaitUninterruptibly(release);
        });
        try
        {
            assertTrue(started.await(10, TimeUnit.SECONDS));
            assertFalse("An in-flight management task should block the management drain",
                        managementDispatcher.isDone());
            awaitTrue("The regular dispatcher drain should ignore the management executor",
                      dispatch::isDone);
        }
        finally
        {
            release.countDown();
        }
        awaitTrue("The management drain should complete once its task finishes",
                  managementDispatcher::isDone);
    }

    @Test
    public void testIsDoneIgnoresRequestExecutorBacklog() throws Exception
    {
        Dispatcher managementDispatcher = new ManagementTestDispatcher(true);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Dispatcher.requestExecutor.submit(() -> {
            started.countDown();
            Uninterruptibles.awaitUninterruptibly(release);
        });
        try
        {
            assertTrue(started.await(10, TimeUnit.SECONDS));
            assertFalse("An in-flight regular task should block the regular drain",
                        dispatch.isDone());
            assertTrue("The management drain should ignore the regular request executor",
                       managementDispatcher.isDone());
        }
        finally
        {
            release.countDown();
        }
        awaitTrue("The regular drain should complete once its task finishes",
                  dispatch::isDone);
    }

    /**
     * A management command may itself initiate the server stop (e.g. INVOKE COMMAND stopdaemon), in which
     * case the drain-wait runs on the very thread executing the command and must not wait for its own task.
     */
    @Test
    public void testIsDoneExcludesStopInitiatingManagementTask() throws Exception
    {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean isDoneInsideTask = new AtomicBoolean();

        Dispatcher managementDispatcher = new ManagementTestDispatcher(true)
        {
            @Override
            <P> void processRequest(Channel channel,
                                    Message.Request request,
                                    FlushItemConverter<P> forFlusher,
                                    P param,
                                    ClientResourceLimits.Overload backpressure,
                                    RequestTime requestTime)
            {
                isDoneInsideTask.set(isDone());
                entered.countDown();
                Uninterruptibles.awaitUninterruptibly(release);
            }
        };

        Message.Request request = createManagementRequest(Message.Type.STARTUP);
        managementDispatcher.dispatch(request.connection().channel(), request, (param, channel, req, response) -> null,
                                      null, ClientResourceLimits.Overload.NONE);
        try
        {
            assertTrue(entered.await(10, TimeUnit.SECONDS));
            assertTrue("The drain must not wait for the management task that initiated it",
                       isDoneInsideTask.get());
            assertFalse("Other threads should still see the in-flight management task",
                        managementDispatcher.isDone());
        }
        finally
        {
            release.countDown();
        }
        awaitTrue("The management drain should complete once its task finishes",
                  managementDispatcher::isDone);
    }

    /**
     * Queue-time backpressure must be keyed to the executor serving the connection: a backlog on the
     * management pool should trigger backpressure for management connections only, and must stay
     * invisible to regular client connections (and vice versa).
     */
    @Test
    public void testHasQueueCapacityTracksOwnExecutor() throws Exception
    {
        Dispatcher managementDispatcher = new ManagementTestDispatcher(true);
        double thresholdBefore = DatabaseDescriptor.getRawConfig().native_transport_queue_max_item_age_threshold;
        DatabaseDescriptor.getRawConfig().native_transport_queue_max_item_age_threshold = 1e-9;

        int poolSize = Dispatcher.managementExecutor.getMaximumPoolSize();
        CountDownLatch saturated = new CountDownLatch(poolSize);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch processed = new CountDownLatch(1);
        try
        {
            assertTrue("Both drains should report capacity while idle", managementDispatcher.hasQueueCapacity());
            assertTrue(dispatch.hasQueueCapacity());

            for (int i = 0; i < poolSize; i++)
            {
                Dispatcher.managementExecutor.submit(() -> {
                    saturated.countDown();
                    Uninterruptibles.awaitUninterruptibly(release);
                });
            }
            assertTrue(saturated.await(10, TimeUnit.SECONDS));

            Dispatcher queuedProcessor = new ManagementTestDispatcher(true)
            {
                @Override
                <P> void processRequest(Channel channel,
                                        Message.Request request,
                                        FlushItemConverter<P> forFlusher,
                                        P param,
                                        ClientResourceLimits.Overload backpressure,
                                        RequestTime requestTime)
                {
                    processed.countDown();
                }
            };
            Message.Request request = createManagementRequest(Message.Type.STARTUP);
            queuedProcessor.dispatch(request.connection().channel(), request, (param, channel, req, response) -> null,
                                     null, ClientResourceLimits.Overload.NONE);

            awaitTrue("A queued management request should trigger management backpressure",
                      () -> !managementDispatcher.hasQueueCapacity());
            assertTrue("A management backlog should be invisible to the regular drain",
                       dispatch.hasQueueCapacity());
        }
        finally
        {
            release.countDown();
            DatabaseDescriptor.getRawConfig().native_transport_queue_max_item_age_threshold = thresholdBefore;
        }
        assertTrue(processed.await(10, TimeUnit.SECONDS));
        awaitTrue("The management pool should drain once its tasks finish",
                  managementDispatcher::isDone);
    }

    private static void awaitTrue(String message, Callable<Boolean> condition) throws Exception
    {
        long timeout = Clock.Global.currentTimeMillis();
        while (!condition.call() && Clock.Global.currentTimeMillis() - timeout < 10_000)
            Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
        assertTrue(message, condition.call());
    }

    private long completedRequests()
    {
        return Dispatcher.requestExecutor.getCompletedTaskCount();
    }

    private long completedAuth()
    {
        return Dispatcher.authExecutor.getCompletedTaskCount();
    }

    private long completedManagement()
    {
        return Dispatcher.managementExecutor.getCompletedTaskCount();
    }

    private long tryRequest(Callable<Long> check, Message.Request request) throws Exception
    {
        long start = check.call();
        dispatch.dispatch(request.connection().channel(), request, (param, channel, req, response) -> null,
                          null, ClientResourceLimits.Overload.NONE);

        long timeout = Clock.Global.currentTimeMillis();
        while (start == check.call() && Clock.Global.currentTimeMillis() - timeout < 1000)
            Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
        return check.call() - start;
    }

    private static ServerConnection managementConnectionMock()
    {
        Connection.Tracker tracker = Mockito.mock(Connection.Tracker.class);
        Mockito.when(tracker.isRunning()).thenAnswer(invocation -> true);

        Channel channel = Mockito.mock(Channel.class);
        ServerConnection connection = Mockito.mock(ServerConnection.class);
        Mockito.when(connection.getTracker()).thenAnswer(invocation -> tracker);
        Mockito.when(connection.isManagementConnection()).thenReturn(true);
        Mockito.when(connection.getVersion()).thenReturn(ProtocolVersion.CURRENT);
        Mockito.when(connection.channel()).thenReturn(channel);

        return connection;
    }

    private static Connection connectionMock()
    {
        Connection.Tracker tracker = Mockito.mock(Connection.Tracker.class);
        Mockito.when(tracker.isRunning()).thenAnswer(invocation -> true);
        Connection c = Mockito.mock(Connection.class);
        Mockito.when(c.getTracker()).thenAnswer(invocation -> tracker);
        return c;
    }

    private static Message.Request createManagementRequest(Message.Type type)
    {
        return createRequest(type, managementConnectionMock());
    }

    private static Message.Request createRegularRequest(Message.Type type)
    {
        return createRequest(type, connectionMock());
    }

    private static Message.Request createRequest(Message.Type type, Connection conn)
    {
        return new Message.Request(type)
        {
            @Override
            public Connection connection()
            {
                return conn;
            }

            @Override
            public Response execute(QueryState queryState, Dispatcher.RequestTime requestTime, boolean traceRequest)
            {
                return null;
            }
        };
    }

    private static QueryMessage queryMessage(String cql)
    {
        QueryMessage msg = new QueryMessage(cql, QueryOptions.DEFAULT)
        {
            @Override
            public Connection connection()
            {
                return managementConnectionMock();
            }
        };
        msg.setSource(new Envelope(Envelope.Header.dummy(1, msg.type), null));
        return msg;
    }

    public static class ManagementTestDispatcher extends Dispatcher
    {
        public ManagementTestDispatcher()
        {
            this(false);
        }

        public ManagementTestDispatcher(boolean isManagementDispatcher)
        {
            super(false, isManagementDispatcher);
        }

        @Override
        <P> void processRequest(Channel channel,
                                Message.Request request,
                                FlushItemConverter<P> forFlusher,
                                P param,
                                ClientResourceLimits.Overload backpressure,
                                RequestTime requestTime)
        {
            // noop - just for testing routing
        }
    }
}