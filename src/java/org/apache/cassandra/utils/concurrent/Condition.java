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

import org.apache.cassandra.utils.Intercept;
import org.apache.cassandra.utils.Shared;

import static org.apache.cassandra.utils.Shared.Scope.SIMULATION;

/**
 * Simpler API than java.util.concurrent.Condition; would be nice to extend it, but also nice
 * to share API with Future, for which Netty's API is incompatible with java.util.concurrent.Condition
 *
 * {@link Awaitable} for explicit external signals.
 */
@Shared(scope = SIMULATION)
public interface Condition extends Awaitable
{
    /**
     * Returns true once signalled. Unidirectional; once true, will never again be false.
     */
    boolean isSignalled();

    /**
     * Signal the condition as met, and wake all waiting threads.
     */
    void signal();

    /**
     * Signal the condition as met, and wake all waiting threads.
     */
    default void signalAll() { signal(); }

    /**
     * Factory method used to capture and redirect instantiations for simulation
     */
    @Intercept
    static Condition newOneTimeCondition()
    {
        return new Async();
    }

    /**
     * An asynchronous {@link Condition}. Typically lower overhead than {@link Sync}.
     */
    public static class Async extends AsyncAwaitable implements Condition
    {
        private volatile boolean signaled = false;

        // WARNING: if extending this class, consider simulator interactions
        protected Async() {}

        public boolean isSignalled()
        {
            return signaled;
        }

        public void signal()
        {
            signaled = true;
            super.signal();
        }
    }

    /**
     * A {@link Condition} based on its object monitor.
     * WARNING: lengthy operations performed while holding the lock may prevent timely notification of waiting threads
     * that a deadline has passed.
     */
    public static class Sync extends SyncAwaitable implements Condition
    {
        private boolean signaled = false;

        // this can be instantiated directly, as we intercept monitors directly with byte weaving
        public Sync() {}

        public synchronized boolean isSignalled()
        {
            return signaled;
        }

        public synchronized void signal()
        {
            signaled = true;
            notifyAll();
        }
    }
}
