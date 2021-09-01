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

import static org.apache.cassandra.simulator.Action.Modifiers.NONE;
import static org.apache.cassandra.simulator.Action.Modifiers.RELIABLE;
import static org.apache.cassandra.simulator.Action.Modifiers.STRICT;

public class Actions
{
    public static abstract class SimpleAction extends Action
    {
        public SimpleAction(Object description, Modifiers self, Modifiers transitive)
        {
            super(description, self, transitive);
        }

        public SimpleAction(Object description, OrderOn orderOn, Modifiers self, Modifiers transitive)
        {
            super(description, orderOn, self, transitive);
        }

        protected ActionList perform(boolean perform)
        {
            return performed(perform ? performInternal() : dropInternal(), true, true);
        }

        protected ActionList dropInternal() { return ActionList.empty(); }
        protected abstract ActionList performInternal();
    }

    public static class LambdaAction extends SimpleAction
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

        protected ActionList performInternal()
        {
            ActionList result = perform.get();
            perform = null;
            return result;
        }

        protected ActionList dropInternal()
        {
            perform = null;
            return ActionList.empty();
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
            assert is(Modifier.RELIABLE);
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
    public static class StrictAction extends ReliableAction
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

    public static Action of(Action.Modifiers self, Action.Modifiers children, Object description, Supplier<ActionList> action)
    {
        return new LambdaAction(description, self, children, action);
    }

    public static Action of(OrderOn orderOn, Action.Modifiers self, Action.Modifiers children, Object description, Supplier<ActionList> action)
    {
        return new LambdaAction(description, orderOn, self, children, action);
    }

    public static Action empty(String message)
    {
        return of(message, ActionList::empty);
    }

    public static Action empty(Action.Modifiers modifiers, Object message)
    {
        return of(modifiers, NONE, message, ActionList::empty);
    }
}
