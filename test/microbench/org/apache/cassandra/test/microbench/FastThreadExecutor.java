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

package org.apache.cassandra.test.microbench;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import io.netty.util.concurrent.FastThreadLocalThread;

/**
 * Created to test perf of FastThreadLocal
 *
 * Used in MutationBench via:
 * jvmArgsAppend = {"-Djmh.executor=CUSTOM", "-Djmh.executor.class=org.apache.cassandra.test.microbench.FastThreadExecutor"}
 */
public class FastThreadExecutor extends AbstractExecutorService
{
    final FastThreadLocalThread thread;
    final LinkedBlockingQueue<Runnable> queue = new LinkedBlockingQueue<>();
    final CountDownLatch shutdown = new CountDownLatch(1);

    public FastThreadExecutor(int size, String name)
    {
        assert size == 1;

        thread = new FastThreadLocalThread(() -> {
            Runnable work = null;
            try
            {
                while ((work = queue.take()) != null)
                    work.run();
            }
            catch (InterruptedException e)
            {
                shutdown.countDown();
            }
        });

        thread.setName(name + "-1");
        thread.setDaemon(true);

        thread.start();
    }


    public void shutdown()
    {
        thread.interrupt();
    }

    public List<Runnable> shutdownNow()
    {
        thread.interrupt();
        return Collections.emptyList();
    }

    public boolean isShutdown()
    {
        return shutdown.getCount() == 0;
    }

    public boolean isTerminated()
    {
        return shutdown.getCount() == 0;
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
    {
        return shutdown.await(timeout, unit);
    }

    public void execute(Runnable command)
    {
        while(!queue.add(command));
    }
}
