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

package org.apache.cassandra.service.paxos;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.utils.Throwables;

import static org.apache.cassandra.service.paxos.AbstractPaxosRepair.Result.Outcome;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

public abstract class AbstractPaxosRepair
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractPaxosRepair.class);

    public static class State {}

    public interface StateUpdater<S, I, T extends Throwable>
    {
        State apply(S in, I param) throws T;
    }

    public interface Listener
    {
        void onComplete(AbstractPaxosRepair repair, Result result);
    }

    abstract class ConsumerState<T> extends State implements Consumer<T>
    {
        public void accept(T input)
        {
            updateState(this, input, ConsumerState::execute);
        }

        abstract State execute(T input) throws Throwable;
    }

    public static class Result extends State
    {
        enum Outcome { DONE, CANCELLED, FAILURE }

        final Outcome outcome;

        public Result(Outcome outcome)
        {
            this.outcome = outcome;
        }

        public String toString()
        {
            return outcome.toString();
        }

        public boolean wasSuccessful()
        {
            return outcome == Outcome.DONE;
        }
    }

    static boolean isResult(State state)
    {
        return state instanceof Result;
    }

    public static final Result DONE = new Result(Outcome.DONE);

    public static final Result CANCELLED = new Result(Outcome.CANCELLED);

    public static final class Failure extends Result
    {
        public final Throwable failure;

        Failure(Throwable failure)
        {
            super(Outcome.FAILURE);
            this.failure = failure;
        }

        public String toString()
        {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            if (failure != null)
                failure.printStackTrace(pw);
            return outcome.toString() + ": " + sw;
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Failure failure1 = (Failure) o;
            return Objects.equals(failure, failure1.failure);
        }

        public int hashCode()
        {
            return Objects.hash(failure);
        }
    }

    private final DecoratedKey partitionKey;
    private final Ballot incompleteBallot;
    private List<Listener> listeners = null;
    private volatile State state;
    private volatile long startedNanos = Long.MIN_VALUE;

    public AbstractPaxosRepair(DecoratedKey partitionKey, Ballot incompleteBallot)
    {
        this.partitionKey = partitionKey;
        this.incompleteBallot = incompleteBallot;
    }

    public State state()
    {
        return state;
    }

    public long startedNanos()
    {
        return startedNanos;
    }

    public boolean isStarted()
    {
        return startedNanos != Long.MIN_VALUE;
    }

    public boolean isComplete()
    {
        return isResult(state);
    }

    public Ballot incompleteBallot()
    {
        return incompleteBallot;
    }

    /**
     * add a listener to this repair, or if the repair has already completed, call the listener with the result
     */
    public AbstractPaxosRepair addListener(Listener listener)
    {
        Result result = null;
        synchronized (this)
        {
            if (isResult(state))
            {
                result = (Result) state;
            }
            else
            {
                if (listeners == null)
                    listeners = new ArrayList<>();

                listeners.add(listener);
            }
        }

        if (result != null)
            listener.onComplete(this, result);

        return this;
    }

    public AbstractPaxosRepair addListener(Consumer<Result> listener)
    {
        return addListener(((repair, result) -> listener.accept(result)));
    }

    public final DecoratedKey partitionKey()
    {
        return partitionKey;
    }

    public State restart(State state) { return restart(state, Long.MIN_VALUE); }
    public abstract State restart(State state, long waitUntil);

    public final synchronized AbstractPaxosRepair start()
    {
        updateState(null, null, (state, i2) -> {
            Preconditions.checkState(!isStarted());
            startedNanos = Math.max(Long.MIN_VALUE + 1, nanoTime());
            return restart(state);
        });
        return this;
    }

    public final void cancel()
    {
        set(CANCELLED);
    }

    public final void cancelUnexceptionally()
    {
        try
        {
            cancel();
        }
        catch (Throwable t)
        {
            logger.error("Exception cancelling paxos repair", t);
        }
    }

    public final synchronized Result await() throws InterruptedException
    {
        while (!isResult(state))
            wait();
        return (Result) state;
    }

    protected void set(Result result)
    {
        updateState(state(), null, (i1, i2) -> result);
    }

    protected <S extends State, I, T extends Throwable> void updateState(S expect, I param, StateUpdater<S, I, T> transform)
    {
        Result result = null;
        List<Listener> listeners = null;
        synchronized (this)
        {
            State next;
            try
            {
                if (state != expect)
                    return;

                state = next = transform.apply(expect, param);
            }
            catch (Throwable t)
            {
                state = next = new Failure(t);
            }

            if (isResult(next))
            {
                notifyAll();
                result = (Result) next;
                listeners = this.listeners;
                this.listeners = null;
            }
        }

        if (result != null && listeners != null)
        {
            Throwable t = null;
            for (int i=0, mi=listeners.size(); i<mi; i++)
            {
                try
                {
                    listeners.get(i).onComplete(this, result);
                }
                catch (Throwable throwable)
                {
                    t = Throwables.merge(t, throwable);
                }
            }
            Throwables.maybeFail(t);
        }
    }
}
