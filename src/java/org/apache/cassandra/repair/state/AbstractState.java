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
    protected enum UpdateType
    {
        NO_CHANGE, ACCEPTED,
        LARGER_STATE_SEEN, ALREADY_COMPLETED;

        protected boolean isRejected()
        {
            switch (this)
            {
                case NO_CHANGE:
                case ACCEPTED:
                    return false;
                case LARGER_STATE_SEEN:
                case ALREADY_COMPLETED:
                    return true;
                default:
                    throw new IllegalStateException("Unknown type: " + this);
            }
        }
    }

    public static final int INIT = -1;
    public static final int COMPLETE = -2;

    private final Class<T> klass;
    protected final long[] stateTimesNanos;
    protected int currentState = INIT;

    public AbstractState(Clock clock, I id, Class<T> klass)
    {
        super(clock, id);
        this.klass = klass;
        this.stateTimesNanos = new long[klass.getEnumConstants().length];
    }

    @Override
    public boolean isAccepted()
    {
        return currentState == INIT ? false : true;
    }

    @Override
    public T getStatus()
    {
        int current = currentState;
        if (current < 0) // init or complete
            return null;
        return klass.getEnumConstants()[current];
    }

    public String status()
    {
        T state = getStatus();
        Result result = getResult();
        if (result != null)
            return result.kind.name();
        if (state == null)
            return "init";
        return state.name();
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "{" +
               "id=" + id +
               ", status=" + status() +
               ", lastUpdatedAtNs=" + lastUpdatedAtNs +
               '}';
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
        if (maybeUpdateState(state).isRejected())
            throw new IllegalStateException("State went backwards; current=" + klass.getEnumConstants()[currentState] + ", desired=" + state);
    }

    protected UpdateType maybeUpdateState(T state)
    {
        int currentState = this.currentState;
        if (currentState == COMPLETE)
            return UpdateType.ALREADY_COMPLETED;
        if (currentState == state.ordinal())
            return UpdateType.NO_CHANGE;
        if (currentState > state.ordinal())
            return UpdateType.LARGER_STATE_SEEN;
        long now = clock.nanoTime();
        stateTimesNanos[this.currentState = state.ordinal()] = now;
        lastUpdatedAtNs = now;
        return UpdateType.ACCEPTED;
    }
}
