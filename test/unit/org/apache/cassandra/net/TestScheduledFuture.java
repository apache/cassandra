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

package org.apache.cassandra.net;

import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class TestScheduledFuture implements ScheduledFuture<Object>
{
    private boolean cancelled = false;

    public long getDelay(TimeUnit unit)
    {
        return 0;
    }

    public int compareTo(Delayed o)
    {
        return 0;
    }

    public boolean cancel(boolean mayInterruptIfRunning)
    {
        cancelled = true;
        return false;
    }

    public boolean isCancelled()
    {
        return cancelled;
    }

    public boolean isDone()
    {
        return false;
    }

    public Object get() throws InterruptedException, ExecutionException
    {
        return null;
    }

    public Object get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException
    {
        return null;
    }
}
