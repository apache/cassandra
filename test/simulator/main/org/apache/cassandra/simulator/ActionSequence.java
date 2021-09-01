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

import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.cassandra.simulator.OrderOn.StrictSequential;

public class ActionSequence extends ActionList
{
    private static final ActionSequence EMPTY = new ActionSequence(OrderOn.NONE, new Action[0]);
    public static ActionSequence empty() { return EMPTY; }
    public static ActionSequence of(Action action) { return new ActionSequence(action); }
    public static ActionSequence strictSequential(List<Action> actions) { return strictSequential(actions.toArray(new Action[0])); }
    public static ActionSequence strictSequential(Action ... actions) { return new ActionSequence(StrictSequential::new, actions); }
    public static ActionSequence strictSequential(Object on, Action ... actions) { return new ActionSequence(new StrictSequential(on), actions); }
    public static ActionSequence unordered(Action ... actions) { return new ActionSequence(OrderOn.NONE, actions); }
    public static ActionSequence unordered(List<Action> actions) { return new ActionSequence(OrderOn.NONE, actions.toArray(new Action[0])); }

    final OrderOn orderOn;

    private ActionSequence(Action action)
    {
        this(new Action[] { action });
    }

    private ActionSequence(Action[] actions)
    {
        this(OrderOn.NONE, actions);
    }

    ActionSequence(OrderOn orderOn, Action[] actions)
    {
        super(actions);
        this.orderOn = orderOn;
    }

    ActionSequence(Function<ActionSequence, OrderOn> orderOn, Action[] actions)
    {
        super(actions);
        this.orderOn = orderOn.apply(this);
    }

    public ActionSequence transform(Function<Action, Action> apply)
    {
        return new ActionSequence(orderOn, stream().map(apply).toArray(Action[]::new));
    }

    public ActionSequence filter(Predicate<Action> apply)
    {
        return new ActionSequence(orderOn, stream().filter(apply).toArray(Action[]::new));
    }

    public ActionSequence andThen(ActionList andThen)
    {
        return super.andThen(andThen).ordered(orderOn);
    }
}

