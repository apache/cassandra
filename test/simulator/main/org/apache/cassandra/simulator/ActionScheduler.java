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

package org.apache.cassandra.simulator;

import java.util.function.Consumer;

import org.apache.cassandra.simulator.Action.Modifiers;

import static org.apache.cassandra.simulator.Action.Modifier.ALWAYS_DELAY;
import static org.apache.cassandra.simulator.Action.Modifier.RELIABLE;
import static org.apache.cassandra.simulator.Action.Modifiers.NONE;
import static org.apache.cassandra.simulator.Action.Modifiers.NO_TIMEOUTS;

public abstract class ActionScheduler implements Consumer<Action>, ActionSchedulers
{
    public enum Kind { RANDOM_WALK, UNIFORM }
    enum Delay { NO, YES }

    public final boolean drop(Action action)
    {
        return !action.is(RELIABLE) && decideIfDrop();
    }

    private Delay delay(Action action)
    {
        Modifiers modifiers = action.self();
        if (modifiers.is(ALWAYS_DELAY) || decideIfDelay())
            return Delay.YES;
        else
            return Delay.NO;
    }

    public double priority(Action action)
    {
        return priority(delay(action), action);
    }

    protected double priority(Delay delay, Action action)
    {
        switch (delay)
        {
            case YES: return delayed(action);
            // TODO (now): optionally allocate a sequential id, to further reduce chaos between not-delayed actions
            case NO: return Double.NEGATIVE_INFINITY;
        }
        return delayed(action);
    }

    public void accept(Action action)
    {
        attachTo(action);
    }

    protected void attachTo(Action action)
    {
        action.setScheduler(this);
        if (!decideIfPermitTimeouts())
            action.add(NO_TIMEOUTS, NONE);
    }

    protected ActionScheduler next()
    {
        return this;
    }

    public void attachTo(ActionList actions)
    {
        actions.forEach(next());
    }

    protected abstract double delayed(Action action);
    protected abstract boolean decideIfDrop();
    protected abstract boolean decideIfDelay();
    protected abstract boolean decideIfPermitTimeouts();
}
