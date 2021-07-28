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

package org.apache.cassandra.hints;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;

public class HintsCleanupExecutor
{
    private final ExecutorService executor;

    HintsCleanupExecutor()
    {
        // Keep the executor as single threaded.
        // Otherwise, update the {@link #cleanup(HintsStore)} to prevent re-submisison of the same store.
        executor = DebuggableThreadPoolExecutor.createWithFixedPoolSize(getClass().getSimpleName(), 1);
    }

    void shutdownBlocking()
    {
        executor.shutdown();
        try
        {
            executor.awaitTermination(1, TimeUnit.MINUTES);
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }
    }

    Future<?> cleanup(HintsStore hintsStore)
    {
        return executor.submit(() -> hintsStore.deleteExpiredHints(System.currentTimeMillis()));
    }
}
