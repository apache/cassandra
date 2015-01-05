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

import java.util.List;
import java.util.concurrent.TimeUnit;

import io.netty.util.concurrent.AbstractEventExecutor;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.Future;
import org.apache.cassandra.concurrent.TracingAwareExecutorService;
import org.apache.cassandra.config.DatabaseDescriptor;

import static org.apache.cassandra.concurrent.SharedExecutorPool.SHARED;

public class RequestThreadPoolExecutor extends AbstractEventExecutor
{
    private final static int MAX_QUEUED_REQUESTS = 128;
    private final static String THREAD_FACTORY_ID = "Native-Transport-Requests";
    private final TracingAwareExecutorService wrapped = SHARED.newExecutor(DatabaseDescriptor.getNativeTransportMaxThreads(),
                                                                           MAX_QUEUED_REQUESTS,
                                                                           "transport",
                                                                           THREAD_FACTORY_ID);

    public boolean isShuttingDown()
    {
        return wrapped.isShutdown();
    }

    public Future<?> shutdownGracefully(long l, long l2, TimeUnit timeUnit)
    {
        throw new IllegalStateException();
    }

    public Future<?> terminationFuture()
    {
        throw new IllegalStateException();
    }

    @Override
    public void shutdown()
    {
        wrapped.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow()
    {
        return wrapped.shutdownNow();
    }

    public boolean isShutdown()
    {
        return wrapped.isShutdown();
    }

    public boolean isTerminated()
    {
        return wrapped.isTerminated();
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
    {
        return wrapped.awaitTermination(timeout, unit);
    }

    public EventExecutorGroup parent()
    {
        return null;
    }

    public boolean inEventLoop(Thread thread)
    {
        return false;
    }

    public void execute(Runnable command)
    {
        wrapped.execute(command);
    }
}
