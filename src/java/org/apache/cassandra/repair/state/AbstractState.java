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
package org.apache.cassandra.repair.state;

import java.util.EnumMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Throwables;

import org.apache.cassandra.utils.Clock;

public abstract class AbstractState<T extends Enum<T>, I> implements State<T, I>
{
    public static final int INIT = -1;
    public static final int COMPLETE = -2;

    private final long creationTimeMillis = Clock.Global.currentTimeMillis(); // used to convert from nanos to millis
    private final long creationTimeNanos = Clock.Global.nanoTime();

    public final I id;
    private final Class<T> klass;
    protected final long[] stateTimesNanos;
    protected int currentState = INIT;
    protected volatile long lastUpdatedAtNs;
    private final AtomicReference<Result> result = new AtomicReference<>(null);

    public AbstractState(I id, Class<T> klass)
    {
        this.id = id;
        this.klass = klass;
        this.stateTimesNanos = new long[klass.getEnumConstants().length];
    }

    @Override
    public I getId()
    {
        return id;
    }

    @Override
    public T getStatus()
    {
        int current = currentState;
        if (current < 0) // init or complete
            return null;
        return klass.getEnumConstants()[current];
    }

    public int getCurrentState()
    {
        return currentState;
    }

    @Override
    public long getInitializedAtMillis()
    {
        return nanosToMillis(creationTimeNanos);
    }

    @Override
    public long getInitializedAtNanos()
    {
        return creationTimeNanos;
    }

    @Override
    public long getLastUpdatedAtMillis()
    {
        return nanosToMillis(lastUpdatedAtNs);
    }

    @Override
    public long getLastUpdatedAtNanos()
    {
        return lastUpdatedAtNs;
    }

    @Override
    public EnumMap<T, Long> getStateTimesMillis()
    {
        long[] millis = getStateTimesMillisArray();
        EnumMap<T, Long> map = new EnumMap<>(klass);
        for (int i = 0; i < millis.length; i++)
        {
            long ms = millis[i];
            if (ms != 0)
                map.put(klass.getEnumConstants()[i], ms);
        }
        return map;
    }

    private long[] getStateTimesMillisArray()
    {
        long[] millis = new long[stateTimesNanos.length];
        for (int i = 0; i < millis.length; i++)
        {
            long value = stateTimesNanos[i];
            if (value != 0)
                millis[i] = nanosToMillis(value);
        }
        return millis;
    }

    @Override
    public Result getResult()
    {
        return result.get();
    }

    protected void updateState(T state)
    {
        int currentState = this.currentState;
        if (currentState >= state.ordinal())
            throw new IllegalStateException("State went backwards; current=" + klass.getEnumConstants()[currentState] + ", desired=" + state);
        long now = Clock.Global.nanoTime();
        stateTimesNanos[this.currentState = state.ordinal()] = now;
        lastUpdatedAtNs = now;
    }

    protected boolean tryResult(Result result)
    {
        if (!this.result.compareAndSet(null, result))
            return false;
        currentState = COMPLETE;
        lastUpdatedAtNs = Clock.Global.nanoTime();
        return true;
    }

    protected long nanosToMillis(long nanos)
    {
        // nanos - creationTimeNanos = delta since init
        return creationTimeMillis + TimeUnit.NANOSECONDS.toMillis(nanos - creationTimeNanos);
    }

    protected class BaseSkipPhase extends BasePhase
    {
        public void skip(String msg)
        {
            tryResult(Result.skip(msg));
        }
    }

    protected class BasePhase
    {
        public void success()
        {
            tryResult(Result.success());
        }

        public void success(String msg)
        {
            tryResult(Result.success(msg));
        }

        public void fail(Throwable e)
        {
            fail(e == null ? null : Throwables.getStackTraceAsString(e));
        }

        public void fail(String failureCause)
        {
            tryResult(Result.fail(failureCause));
        }
    }
}
