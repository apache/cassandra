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

package org.apache.cassandra.utils.concurrent;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.apache.cassandra.utils.Intercept;
import org.apache.cassandra.utils.Shared;

import static org.apache.cassandra.utils.Shared.Scope.SIMULATION;

@Shared(scope = SIMULATION)
public interface CountDownLatch extends Awaitable
{
    /**
     * Count down by 1, signalling waiters if we have reached zero
     */
    void decrement();

    /**
     * @return the current count
     */
    int count();

    /**
     * Factory method used to capture and redirect instantiations for simulation
     */
    @Intercept
    static CountDownLatch newCountDownLatch(int count)
    {
        return new Async(count);
    }

    static class Async extends AsyncAwaitable implements CountDownLatch
    {
        private static final AtomicIntegerFieldUpdater<CountDownLatch.Async> countUpdater = AtomicIntegerFieldUpdater.newUpdater(CountDownLatch.Async.class, "count");
        private volatile int count;

        // WARNING: if extending this class, consider simulator interactions
        protected Async(int count)
        {
            this.count = count;
            if (count == 0)
                signal();
        }

        public void decrement()
        {
            if (countUpdater.decrementAndGet(this) == 0)
                signal();
        }

        public int count()
        {
            return count;
        }

        @Override
        protected boolean isSignalled()
        {
            return count <= 0;
        }
    }

    static final class Sync extends SyncAwaitable implements CountDownLatch
    {
        private int count;

        public Sync(int count)
        {
            this.count = count;
        }

        public synchronized void decrement()
        {
            if (count > 0 && --count == 0)
                notifyAll();
        }

        public synchronized int count()
        {
            return count;
        }

        /**
         * not synchronized as only intended for internal usage by externally synchronized methods
         */

        @Override
        protected boolean isSignalled()
        {
            return count <= 0;
        }
    }
}
