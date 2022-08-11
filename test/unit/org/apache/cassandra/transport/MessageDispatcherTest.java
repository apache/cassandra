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
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.channel.Channel;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.metrics.ClientMetrics;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.AuthResponse;

public class MessageDispatcherTest
{
    static final Message.Request AUTH_RESPONSE_REQUEST = new AuthResponse(new byte[0])
    {
        public Response execute(QueryState queryState, long queryStartNanoTime, boolean traceRequest)
        {
            return null;
        }
    };

    private static AuthTestDispatcher dispatch;
    private static int maxAuthThreadsBeforeTests;

    @BeforeClass
    public static void init() throws Exception
    {
        DatabaseDescriptor.daemonInitialization();
        ClientMetrics.instance.init(Collections.emptyList());
        maxAuthThreadsBeforeTests = DatabaseDescriptor.getNativeTransportMaxAuthThreads();
        dispatch = new AuthTestDispatcher();
    }

    @AfterClass
    public static void restoreAuthSize()
    {
        DatabaseDescriptor.setNativeTransportMaxAuthThreads(maxAuthThreadsBeforeTests);
    }

    @Test
    public void testAuthRateLimiter() throws Exception
    {
        long startRequests = completedRequests();

        DatabaseDescriptor.setNativeTransportMaxAuthThreads(1);
        long auths = tryAuth(this::completedAuth);
        Assert.assertEquals(auths, 1);

        DatabaseDescriptor.setNativeTransportMaxAuthThreads(100);
        auths = tryAuth(this::completedAuth);
        Assert.assertEquals(auths, 1);

        // Make sure no tasks executed on the regular pool
        Assert.assertEquals(startRequests, completedRequests());
    }

    @Test
    public void testAuthRateLimiterNotUsed() throws Exception
    {
        DatabaseDescriptor.setNativeTransportMaxAuthThreads(1);
        for (Message.Type type : Message.Type.values())
        {
            if (type == Message.Type.AUTH_RESPONSE || type == Message.Type.CREDENTIALS || type.direction != Message.Direction.REQUEST)
                continue;

            long auths = completedAuth();
            long requests = tryAuth(this::completedRequests, new Message.Request(type)
            {
                public Response execute(QueryState queryState, long queryStartNanoTime, boolean traceRequest)
                {
                    return null;
                }
            });
            Assert.assertEquals(requests, 1);
            Assert.assertEquals(completedAuth() - auths, 0);
        }
    }

    @Test
    public void testAuthRateLimiterDisabled() throws Exception
    {
        long startAuthRequests = completedAuth();

        DatabaseDescriptor.setNativeTransportMaxAuthThreads(0);
        long requests = tryAuth(this::completedRequests);
        Assert.assertEquals(requests, 1);

        DatabaseDescriptor.setNativeTransportMaxAuthThreads(-1);
        requests = tryAuth(this::completedRequests);
        Assert.assertEquals(requests, 1);

        DatabaseDescriptor.setNativeTransportMaxAuthThreads(-1000);
        requests = tryAuth(this::completedRequests);
        Assert.assertEquals(requests, 1);

        // Make sure no tasks executed on the auth pool
        Assert.assertEquals(startAuthRequests, completedAuth());
    }

    private long completedRequests()
    {
        return Dispatcher.requestExecutor.getCompletedTaskCount();
    }

    private long completedAuth()
    {
        return Dispatcher.authExecutor.getCompletedTaskCount();
    }

    public long tryAuth(Callable<Long> check) throws Exception
    {
        return tryAuth(check, AUTH_RESPONSE_REQUEST);
    }

    @SuppressWarnings("UnstableApiUsage")
    public long tryAuth(Callable<Long> check, Message.Request request) throws Exception
    {
        long start = check.call();
        dispatch.dispatch(null, request, (channel,req,response) -> null, ClientResourceLimits.Overload.NONE);

        // While this is timeout based, we should be *well below* a full second on any of this processing in any sane environment.
        long timeout = System.currentTimeMillis();
        while(start == check.call() && System.currentTimeMillis() - timeout < 1000)
        {
            Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
        }
        return check.call() - start;
    }

    public static class AuthTestDispatcher extends Dispatcher
    {
        public AuthTestDispatcher()
        {
            super(false);
        }

        @Override
        void processRequest(Channel channel,
                            Message.Request request,
                            FlushItemConverter forFlusher,
                            ClientResourceLimits.Overload backpressure,
                            long approxStartTimeNanos)
        {
            // noop
        }
    }
}
