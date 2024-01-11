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


import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * An implementation similar to Guava's Suppliers.memoizeWithExpiration(Supplier)
 * but allowing for memoization to be skipped.
 *
 * See CASSANDRA-16148
 */
public class ExpiringMemoizingSupplier<T> implements Supplier<T>
{
    final Supplier<ReturnValue<T>> delegate;
    final long durationNanos;
    transient volatile T value;
    // The special value 0 means "not yet initialized".
    transient volatile long expirationNanos;

    public static <T> Supplier<T> memoizeWithExpiration(Supplier<ReturnValue<T>> delegate, long duration, TimeUnit unit)
    {
        return new ExpiringMemoizingSupplier<>(delegate, duration, unit);
    }

    ExpiringMemoizingSupplier(Supplier<ReturnValue<T>> delegate, long duration, TimeUnit unit) {
        this.delegate = Preconditions.checkNotNull(delegate);
        this.durationNanos = unit.toNanos(duration);
        Preconditions.checkArgument(duration > 0);
    }

    @Override
    public T get() {
        // Another variant of Double Checked Locking.
        //
        // We use two volatile reads.  We could reduce this to one by
        // putting our fields into a holder class, but (at least on x86)
        // the extra memory consumption and indirection are more
        // expensive than the extra volatile reads.
        long nanos = this.expirationNanos;
        long now = nanoTime();
        if (nanos == 0L || now - nanos >= 0L) {
            synchronized(this) {
                if (nanos == this.expirationNanos) {
                    ReturnValue<T> t = this.delegate.get();
                    if (t.canMemoize())
                        this.value = t.value();
                    else
                        return t.value();

                    this.expirationNanos = now + this.durationNanos;
                    return t.value();
                }
            }
        }
        return this.value;
    }

    @VisibleForTesting
    public synchronized void expire()
    {
        this.expirationNanos = 0;
    }

    @Override
    public String toString() {
        // This is a little strange if the unit the user provided was not NANOS,
        // but we don't want to store the unit just for toString
        return "Suppliers.memoizeWithExpiration(" + delegate + ", " + durationNanos + ", NANOS)";
    }

    private static final long serialVersionUID = 0;

    public static abstract class ReturnValue<T>
    {
        protected final T value;

        ReturnValue(T value){
            this.value = value;
        }

        public abstract boolean canMemoize();

        public T value()
        {
            return value;
        }
    }

    public static class Memoized<T> extends ReturnValue<T>
    {
        public Memoized(T value)
        {
            super(value);
        }

        public boolean canMemoize()
        {
            return true;
        }
    }

    public static class NotMemoized<T> extends ReturnValue<T>
    {
        public NotMemoized(T value)
        {
            super(value);
        }

        public boolean canMemoize()
        {
            return false;
        }
    }
}
