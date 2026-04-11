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

package org.apache.cassandra.utils;

import org.apache.cassandra.concurrent.ExecutorPlus;

import static org.apache.cassandra.utils.Shared.Scope.SIMULATION;

/**
 * A generic interface for encapsulating a Runnable task with related work before and after execution,
 * using the built-in try-with-resources functionality offered by {@link Closeable}.
 *
 * See {@link ExecutorPlus#execute(WithResources, Runnable)}
 */
@Shared(scope = SIMULATION)
public interface WithResources
{
    static class None implements WithResources
    {
        static final None INSTANCE = new None();
        private None() {}
        @Override
        public Closeable get()
        {
            return () -> {};
        }

        @Override
        public boolean isNoOp()
        {
            return true;
        }
    }

    /**
     * Instantiate any necessary resources
     * @return an object that closes any instantiated resources
     */
    public Closeable get();

    /**
     * A convenience method to avoid unnecessary work.
     * @return true iff this object performs no work when {@link #get()} is invoked, nor when {@link Closeable#close()}
     *         is invoked on the object it returns.
     */
    default public boolean isNoOp() { return false; }
    default public WithResources and(WithResources withResources)
    {
        return and(this, withResources);
    }
    static WithResources none() { return None.INSTANCE; }

    public static WithResources and(WithResources first, WithResources second)
    {
        if (second.isNoOp()) return first;
        if (first.isNoOp()) return second;
        return () -> {
            Closeable a = first.get();
            try
            {
                Closeable b = second.get();
                return () -> {
                    try { a.close(); }
                    finally { b.close(); }
                };
            }
            catch (Throwable t)
            {
                try { a.close(); } catch (Throwable t2) { t.addSuppressed(t2); }
                throw t;
            }
        };
    }
}
