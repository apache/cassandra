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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractFuture;

/**
 * A callback specialized for returning a value from a single target; that is, this is for messages
 * that we only send to one recipient.
 */
public class AsyncOneResponse<T> extends AbstractFuture<T> implements IAsyncCallback<T>
{
    private final long start = System.nanoTime();

    public void response(MessageIn<T> response)
    {
        set(response.payload);
    }

    public boolean isLatencyForSnitch()
    {
        return false;
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws TimeoutException
    {
        long adjustedTimeout = unit.toNanos(timeout) - (System.nanoTime() - start);
        if (adjustedTimeout <= 0)
        {
            throw new TimeoutException("Operation timed out.");
        }
        try
        {
            return super.get(adjustedTimeout, TimeUnit.NANOSECONDS);
        }
        catch (InterruptedException | ExecutionException e)
        {
            throw new AssertionError(e);
        }
    }

    @VisibleForTesting
    public static <T> AsyncOneResponse<T> immediate(T value)
    {
        AsyncOneResponse<T> response = new AsyncOneResponse<>();
        response.set(value);
        return response;
    }
}
