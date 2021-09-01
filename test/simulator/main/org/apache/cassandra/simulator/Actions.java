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

import java.util.function.Supplier;

import org.apache.cassandra.simulator.Action.Modifiers;

import static org.apache.cassandra.simulator.Action.Modifiers.INFINITE_STREAM;
import static org.apache.cassandra.simulator.Action.Modifiers.INFINITE_STREAM_ITEM;
import static org.apache.cassandra.simulator.Action.Modifiers.NONE;
import static org.apache.cassandra.simulator.Action.Modifiers.RELIABLE;
import static org.apache.cassandra.simulator.Action.Modifiers.STREAM;
import static org.apache.cassandra.simulator.Action.Modifiers.STREAM_ITEM;
import static org.apache.cassandra.simulator.Action.Modifiers.STRICT;
import static org.apache.cassandra.utils.LazyToString.lazy;

public class Actions
{
    public static class LambdaAction extends Action
    {
        private Supplier<ActionList> perform;

        public LambdaAction(Object description, Supplier<ActionList> perform)
        {
            this(description, Modifiers.NONE, perform);
        }

        public LambdaAction(Object description, Modifiers self, Supplier<ActionList> perform)
        {
            this(description, self, Modifiers.NONE, perform);
        }

        public LambdaAction(Object description, Modifiers self, Modifiers children, Supplier<ActionList> perform)
        {
            this(description, OrderOn.NONE, self, children, perform);
        }

        public LambdaAction(Object description, OrderOn orderOn, Modifiers self, Modifiers children, Supplier<ActionList> perform)
        {
            super(description, orderOn, self, children);
            this.perform = perform;
        }

        protected ActionList performSimple()
        {
            ActionList result = perform.get();
            perform = null;
            return result;
        }
    }

    /**
     * Should always be performed eventually.
     */
    public static class ReliableAction extends LambdaAction
    {
        public ReliableAction(Object description, Supplier<ActionList> perform, boolean transitive)
        {
            this(description, RELIABLE, transitive ? RELIABLE : NONE, perform);
        }

        public ReliableAction(Object description, Modifiers self, Modifiers children, Supplier<ActionList> perform)
        {
            this(description, OrderOn.NONE, self, children, perform);
        }

        public ReliableAction(Object description, OrderOn orderOn, Modifiers self, Modifiers children, Supplier<ActionList> perform)
        {
            super(description, orderOn, self, children, perform);
            assert !is(Modifier.DROP);
            assert children.is(Modifier.RELIABLE);
        }

        public static ReliableAction transitively(Object description, Supplier<ActionList> action)
        {
            return new ReliableAction(description, action, true);
        }
    }

    /**
     * Should always be performed in strict order, i.e. all of this action's child actions should complete before
     * the next action scheduled by the same actor is invoked.
     */
    public static class StrictAction extends LambdaAction
    {
        public StrictAction(Object description, Supplier<ActionList> perform, boolean transitive)
        {
            super(description, STRICT, transitive ? STRICT : NONE, perform);
        }

        public static StrictAction of(Object description, Supplier<ActionList> action)
        {
            return new StrictAction(description, action, false);
        }
    }

    public static Action of(Object description, Supplier<ActionList> action)
    {
        return new LambdaAction(description, action);
    }

    public static Action of(Modifiers self, Modifiers children, Object description, Supplier<ActionList> action)
    {
        return new LambdaAction(description, self, children, action);
    }

    public static Action of(OrderOn orderOn, Modifiers self, Modifiers children, Object description, Supplier<ActionList> action)
    {
        return new LambdaAction(description, orderOn, self, children, action);
    }

    public static Action empty(String message)
    {
        return of(message, ActionList::empty);
    }

    public static Action empty(Modifiers modifiers, Object message)
    {
        return of(modifiers, NONE, message, ActionList::empty);
    }

    public static Action stream(int concurrency, Supplier<Action> actions) { return stream(new OrderOn.Strict(actions, concurrency), actions); }
    public static Action stream(OrderOn on, Supplier<Action> actions) { return of(OrderOn.NONE, STREAM, NONE, on, () -> ActionList.of(streamNextSupplier(STREAM, STREAM_ITEM, on, 0, on, actions))); }
    public static Action infiniteStream(int concurrency, Supplier<Action> actions) { return infiniteStream(new OrderOn.Strict(actions, concurrency), actions); }
    public static Action infiniteStream(OrderOn on, Supplier<Action> actions) { return of(OrderOn.NONE, INFINITE_STREAM, NONE, on, () -> ActionList.of(streamNextSupplier(INFINITE_STREAM, INFINITE_STREAM_ITEM, on, 0, on, actions))); }
    private static ActionList next(Modifiers modifiers, Object description, int sequence, OrderOn on, Supplier<Action> actions)
    {
        Action next = actions.get();
        if (next == null)
            return ActionList.empty();
        return ActionList.of(next, streamNextSupplier(modifiers, modifiers, description, sequence + 1, on, actions));
    }

    private static Action streamNextSupplier(Modifiers modifiers, Modifiers nextModifiers, Object description, int sequence, OrderOn on, Supplier<Action> actions)
    {
        return Actions.of(on, modifiers, NONE,
                          lazy(() -> description + " " + sequence), () -> next(nextModifiers, description, sequence, on, actions));
    }


}
