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

import org.apache.cassandra.utils.Clock;

public abstract class AbstractState<T extends Enum<T>, I> extends AbstractCompletable<I> implements State<T, I>
{
    public static final int INIT = -1;
    public static final int COMPLETE = -2;

    private final Class<T> klass;
    protected final long[] stateTimesNanos;
    protected int currentState = INIT;

    public AbstractState(I id, Class<T> klass)
    {
        super(id);
        this.klass = klass;
        this.stateTimesNanos = new long[klass.getEnumConstants().length];
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

    @Override
    protected void onComplete()
    {
        currentState = COMPLETE;
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

    protected void updateState(T state)
    {
        int currentState = this.currentState;
        if (currentState >= state.ordinal())
            throw new IllegalStateException("State went backwards; current=" + klass.getEnumConstants()[currentState] + ", desired=" + state);
        long now = Clock.Global.nanoTime();
        stateTimesNanos[this.currentState = state.ordinal()] = now;
        lastUpdatedAtNs = now;
    }
}
