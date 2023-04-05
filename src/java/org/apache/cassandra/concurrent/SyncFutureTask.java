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

import java.util.concurrent.Callable;

import org.apache.cassandra.utils.Closeable;
import org.apache.cassandra.utils.WithResources;
import org.apache.cassandra.utils.concurrent.RunnableFuture;
import org.apache.cassandra.utils.concurrent.SyncFuture;

public class SyncFutureTask<T> extends SyncFuture<T> implements RunnableFuture<T>
{
    final Callable<T> call;

    public SyncFutureTask(Callable<T> call)
    {
        this.call = call;
    }

    public SyncFutureTask(WithResources withResources, Callable<T> call)
    {
        this.call = new Callable<T>()
        {
            @Override
            public T call() throws Exception
            {
                try (Closeable close = withResources.get())
                {
                    return call.call();
                }
            }

            @Override
            public String toString()
            {
                return call.toString();
            }
        };
    }

    public void run()
    {
        try
        {
            if (!setUncancellable())
            {
                if (isCancelled()) return;
                else throw new IllegalStateException();
            }

            if (!trySuccess(call.call()))
                throw new IllegalStateException();
        }
        catch (Throwable t)
        {
            tryFailure(t);
            ExecutionFailure.handle(t);
        }
    }

    @Override
    public String description()
    {
        return call.toString();
    }
}
