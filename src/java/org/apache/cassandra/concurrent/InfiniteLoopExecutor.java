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

package org.apache.cassandra.concurrent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class InfiniteLoopExecutor
{
    private static final Logger logger = LoggerFactory.getLogger(InfiniteLoopExecutor.class);

    public interface InterruptibleRunnable
    {
        void run() throws InterruptedException;
    }

    private final Thread thread;
    private final InterruptibleRunnable runnable;
    private volatile boolean isShutdown = false;

    public InfiniteLoopExecutor(String name, InterruptibleRunnable runnable)
    {
        this.runnable = runnable;
        this.thread = new Thread(this::loop, name);
        this.thread.setDaemon(true);
    }

    private void loop()
    {
        while (!isShutdown)
        {
            try
            {
                runnable.run();
            }
            catch (InterruptedException ie)
            {
                if (isShutdown)
                    return;
                logger.error("Interrupted while executing {}, but not shutdown; continuing with loop", runnable, ie);
            }
            catch (Throwable t)
            {
                logger.error("Exception thrown by runnable, continuing with loop", t);
            }
        }
    }

    public InfiniteLoopExecutor start()
    {
        thread.start();
        return this;
    }

    public void shutdown()
    {
        isShutdown = true;
        thread.interrupt();
    }

    public void awaitTermination(long time, TimeUnit unit) throws InterruptedException
    {
        thread.join(unit.toMillis(time));
    }
}
