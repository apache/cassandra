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
package org.apache.cassandra.cql3.functions;

import org.apache.cassandra.concurrent.ThreadPoolExecutorJMXAdapter;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.concurrent.ThreadPoolExecutorBase;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.cassandra.config.CassandraRelevantProperties.UDF_EXECUTOR_THREAD_KEEPALIVE_MS;
import static org.apache.cassandra.utils.FBUtilities.getAvailableProcessors;
import static org.apache.cassandra.utils.concurrent.BlockingQueues.newBlockingQueue;

/**
 * Executor service which exposes stats via JMX, but which doesn't reference internal classes
 * as these are forbidden by the UDF execution sandbox.
 *
 * TODO: see if we can port to ExecutorPlus to avoid duplication
 */
final class UDFExecutorService extends ThreadPoolExecutorBase
{
    private static final int KEEPALIVE = UDF_EXECUTOR_THREAD_KEEPALIVE_MS.getInt();

    public UDFExecutorService(NamedThreadFactory threadFactory, String jmxPath)
    {
        super(getAvailableProcessors(), KEEPALIVE, MILLISECONDS, newBlockingQueue(), threadFactory);
        ThreadPoolExecutorJMXAdapter.register(jmxPath, this);
    }

    public int getCoreThreads()
    {
        return getCorePoolSize();
    }

    public void setCoreThreads(int newCorePoolSize)
    {
        setCorePoolSize(newCorePoolSize);
    }

    public int getMaximumThreads()
    {
        return getMaximumPoolSize();
    }

    public void setMaximumThreads(int maxPoolSize)
    {
        setMaximumPoolSize(maxPoolSize);
    }
}
