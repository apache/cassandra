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
import java.util.function.Consumer;

public interface ActionListener
{
    enum Before { EXECUTE, DROP, INVALIDATE }

    /**
     * Immediately before the action is first executed
     * @param action the action we are about to perform
     * @param before if the action is to be performed (rather than dropped)
     */
    default void before(Action action, Before before) {}

    /**
     * Immediately after the action is first executed (or dropped)
     * @param consequences the actions that result from the execution
     */
    default void consequences(ActionList consequences) {}

    /**
     * If an ActionThread, after termination; otherwise immediately after invoked
     * @param finished the action that has finished
     */
    default void after(Action finished) {}

    /**
     * After the action and all its consequent terminate (excluding the initiation of an infinite loop execution)
     * @param finished the action that has finished
     */
    default void transitivelyAfter(Action finished) {}

    static ActionListener runAfter(Consumer<Action> after)
    {
        return new ActionListener()
        {
            @Override
            public void after(Action performed)
            {
                after.accept(performed);
            }
        };
    }

    static ActionListener runAfterAndTransitivelyAfter(Consumer<Action> after)
    {
        return new ActionListener()
        {
            @Override
            public void after(Action performed)
            {
                after.accept(performed);
            }

            @Override
            public void transitivelyAfter(Action performed)
            {
                after.accept(performed);
            }
        };
    }

    static ActionListener runAfterTransitiveClosure(Consumer<Action> transitivelyAfter)
    {
        return new ActionListener()
        {
            @Override
            public void transitivelyAfter(Action performed)
            {
                transitivelyAfter.accept(performed);
            }
        };
    }

    static ActionListener recursive(ActionListener runOnAll)
    {
        return new WrappedRecursiveActionListener(runOnAll);
    }

    public interface SelfAddingActionListener extends ActionListener, Consumer<Action>
    {
        @Override
        default public void accept(Action action)
        {
            action.register(this);
        }
    }

    public static class RecursiveActionListener implements SelfAddingActionListener
    {
        @Override
        public void consequences(ActionList consequences)
        {
            consequences.forEach(this);
        }
    }

    public static class WrappedRecursiveActionListener extends Wrapped implements SelfAddingActionListener
    {
        public WrappedRecursiveActionListener(ActionListener wrap)
        {
            super(wrap);
        }

        @Override
        public void consequences(ActionList consequences)
        {
            consequences.forEach(this);
            super.consequences(consequences);
        }
    }

    public static class Wrapped implements ActionListener
    {
        final ActionListener wrap;

        public Wrapped(ActionListener wrap)
        {
            this.wrap = wrap;
        }

        @Override
        public void before(Action action, Before before)
        {
            wrap.before(action, before);
        }

        @Override
        public void consequences(ActionList consequences)
        {
            wrap.consequences(consequences);
        }

        @Override
        public void after(Action finished)
        {
            wrap.after(finished);
        }

        @Override
        public void transitivelyAfter(Action finished)
        {
            wrap.transitivelyAfter(finished);
        }
    }

    public static class Combined implements ActionListener
    {
        final List<ActionListener> combined;

        public Combined(List<ActionListener> combined)
        {
            this.combined = combined;
        }

        @Override
        public void before(Action action, Before before)
        {
            combined.forEach(listener -> listener.before(action, before));
        }

        @Override
        public void consequences(ActionList consequences)
        {
            combined.forEach(listener -> listener.consequences(consequences));
        }

        @Override
        public void after(Action finished)
        {
            combined.forEach(listener -> listener.after(finished));
        }

        @Override
        public void transitivelyAfter(Action finished)
        {
            combined.forEach(listener -> listener.transitivelyAfter(finished));
        }
    }
}

