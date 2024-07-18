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

package org.apache.cassandra.service.accord;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;

import accord.api.Key;
import accord.impl.SafeTimestampsForKey;
import accord.impl.TimestampsForKey;
import accord.primitives.Timestamp;

public class AccordSafeTimestampsForKey extends SafeTimestampsForKey implements AccordSafeState<Key, TimestampsForKey>
{
    private boolean invalidated;
    private final AccordCachingState<Key, TimestampsForKey> global;
    private TimestampsForKey original;
    private TimestampsForKey current;

    public AccordSafeTimestampsForKey(AccordCachingState<Key, TimestampsForKey> global)
    {
        super((Key) global.key());
        this.global = global;
        this.original = null;
        this.current = null;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AccordSafeTimestampsForKey that = (AccordSafeTimestampsForKey) o;
        return Objects.equals(original, that.original) && Objects.equals(current, that.current);
    }

    @Override
    public int hashCode()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString()
    {
        return "AccordSafeTimestampsForKey{" +
               "invalidated=" + invalidated +
               ", global=" + global +
               ", original=" + original +
               ", current=" + current +
               '}';
    }

    @Override
    public AccordCachingState<Key, TimestampsForKey> global()
    {
        checkNotInvalidated();
        return global;
    }

    @Override
    public TimestampsForKey current()
    {
        checkNotInvalidated();
        return current;
    }

    @Override
    @VisibleForTesting
    public void set(TimestampsForKey cfk)
    {
        checkNotInvalidated();
        this.current = cfk;
    }

    public TimestampsForKey original()
    {
        checkNotInvalidated();
        return original;
    }

    @Override
    public void preExecute()
    {
        checkNotInvalidated();
        original = global.get();
        current = original;
    }

    @Override
    public void invalidate()
    {
        invalidated = true;
    }

    @Override
    public boolean invalidated()
    {
        return invalidated;
    }

    public long lastExecutedMicros()
    {
        return current().lastExecutedHlc();
    }

    public static long timestampMicrosFor(TimestampsForKey timestamps, Timestamp executeAt, boolean isForWriteTxn)
    {
        return timestamps.hlcFor(executeAt, isForWriteTxn);
    }

    public static int nowInSecondsFor(TimestampsForKey timestamps, Timestamp executeAt, boolean isForWriteTxn)
    {
        timestamps.validateExecuteAtTime(executeAt, isForWriteTxn);
        // we use the executeAt time instead of the monotonic database timestamp to prevent uneven
        // ttl expiration in extreme cases, ie 1M+ writes/second to a key causing timestamps to overflow
        // into the next second on some keys and not others.
        return Math.toIntExact(TimeUnit.MICROSECONDS.toSeconds(timestamps.lastExecutedTimestamp().hlc()));
    }
}
