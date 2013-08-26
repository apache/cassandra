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

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.jboss.netty.handler.execution.MemoryAwareThreadPoolExecutor;
import org.jboss.netty.util.ObjectSizeEstimator;

import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.DatabaseDescriptor;

public class RequestThreadPoolExecutor extends MemoryAwareThreadPoolExecutor
{
    private final static int CORE_THREAD_TIMEOUT_SEC = 30;
    // Number of request we accept to queue before blocking. We could allow this to be configured...
    private final static int MAX_QUEUED_REQUESTS = 128;

    public RequestThreadPoolExecutor()
    {
        super(DatabaseDescriptor.getNativeTransportMaxThreads(),
              0, // We don't use the per-channel limit, only the global one
              MAX_QUEUED_REQUESTS,
              CORE_THREAD_TIMEOUT_SEC, TimeUnit.SECONDS,
              sizeEstimator(),
              new NamedThreadFactory("Native-Transport-Requests"));
    }

    /*
     * In theory, the ObjectSizeEstimator should estimate the actual size of a
     * request, and MemoryAwareThreadPoolExecutor sets a memory limit on how
     * much memory we allow for request before blocking.
     *
     * However, the memory size used by a CQL query is not very intersting and
     * by no mean reflect the memory size it's execution will use (the interesting part).
     * Furthermore, we're mainly interested in limiting the number of unhandled requests that
     * piles up to implement some back-pressure, and for that, there is no real need to do
     * fancy esimation of request size. So we use a trivial estimator that just count the
     * number of request.
     *
     * We could get more fancy later ...
     */
    private static ObjectSizeEstimator sizeEstimator()
    {
        return new ObjectSizeEstimator()
        {
            public int estimateSize(Object o)
            {
                return 1;
            }
        };
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t)
    {
        super.afterExecute(r, t);
        DebuggableThreadPoolExecutor.logExceptionsAfterExecute(r, t);
    }
}
