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

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Executor service which exposes stats via JMX, but which doesn't reference
 * internal classes in its beforeExecute & afterExecute methods as these are
 * forbidden by the UDF execution sandbox
 */
final class UDFExecutorService extends JMXEnabledThreadPoolExecutor
{
    private static int KEEPALIVE = Integer.getInteger("cassandra.udf_executor_thread_keepalive_ms", 30000);

    UDFExecutorService(NamedThreadFactory threadFactory, String jmxPath)
    {
        super(FBUtilities.getAvailableProcessors(),
              KEEPALIVE,
              TimeUnit.MILLISECONDS,
              new LinkedBlockingQueue<>(),
              threadFactory,
              jmxPath);
    }

    protected void afterExecute(Runnable r, Throwable t)
    {
    }

    protected void beforeExecute(Thread t, Runnable r)
    {
    }
}
