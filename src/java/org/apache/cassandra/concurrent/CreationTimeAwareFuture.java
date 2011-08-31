/**
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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Decorates {@link FutureTask}
 * </p>
 * This Future implementation makes the future.get(long timeout, TimeUnit unit)
 * wait the amount of time specified in the .get(...) call based on the object creation
 * by keeping an internal timestamp of when this object was constructed
 *
 * @param <V>
 */
public class CreationTimeAwareFuture<V> implements Future<V> 
{

    private long creationTime = System.currentTimeMillis();

    private Future<V> future;

    public CreationTimeAwareFuture(Future<V> future) 
    {
        this.future = future;
        creationTime = System.currentTimeMillis();
    }

    public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException 
    {
        timeout = unit.toMillis(timeout);
        long overallTimeout = timeout - (System.currentTimeMillis() - creationTime);
        return future.get(overallTimeout, TimeUnit.MILLISECONDS);
    }

    public boolean cancel(boolean mayInterruptIfRunning) 
    {
        return future.cancel(mayInterruptIfRunning);
    }

    public boolean isCancelled()
    {
        return future.isCancelled();
    }

    public boolean isDone()
    {
        return future.isDone();
    }

    public V get() throws InterruptedException, ExecutionException
    {
       return future.get();
    }

}
