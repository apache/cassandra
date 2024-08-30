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

package accord.utils.async;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.cassandra.utils.concurrent.AsyncPromise;

public class TimeoutUtils
{
    public interface FailingRunnable
    {
        void run() throws Throwable;
    }

    public static void runBlocking(Duration timeout, String threadName, FailingRunnable fn) throws ExecutionException, InterruptedException, TimeoutException
    {
        // MAINTENANCE: Once the accord branch merges to trunk this can be dropped and will be AsyncChain again, but since this is forked into C* (that doesn't have AsyncChain) need to use Futures
//        AsyncResult.Settable<?> promise = AsyncResults.settable();
        AsyncPromise<?> promise = new AsyncPromise<>();
        Thread t = new Thread(() -> {
            try
            {
                fn.run();
                promise.setSuccess(null);
            }
            catch (Throwable e)
            {
                promise.setFailure(e);
            }
        });
        t.setName(threadName);
        t.setDaemon(true);
        t.start();
        try
        {
//            AsyncChains.getBlocking(promise, timeout.toNanos(), TimeUnit.NANOSECONDS);
            promise.get(timeout.toNanos(), TimeUnit.NANOSECONDS);
        }
        catch (InterruptedException e)
        {
            t.interrupt();
            throw e;
        }
        catch (TimeoutException e)
        {
            t.interrupt();
            throw e;
        }
    }
}
