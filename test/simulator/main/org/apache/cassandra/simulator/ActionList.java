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

import java.util.AbstractCollection;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Iterators;

import org.apache.cassandra.simulator.OrderOn.StrictSequential;
import org.apache.cassandra.utils.Throwables;

import static java.util.Arrays.copyOf;

public class ActionList extends AbstractCollection<Action>
{
    private static final ActionList EMPTY = new ActionList(new Action[0]);
    public static ActionList empty() { return EMPTY; }
    public static ActionList of(Action action) { return new ActionList(new Action[] { action }); }
    public static ActionList of(Stream<Action> action) { return new ActionList(action.toArray(Action[]::new)); }
    public static ActionList of(Collection<Action> actions) { return actions.isEmpty() ? EMPTY : new ActionList(actions.toArray(new Action[0])); }
    public static ActionList of(Action ... actions) { return new ActionList(actions); }

    private final Action[] actions;

    ActionList(Action[] actions)
    {
        this.actions = actions;
    }

    public int size()
    {
        return actions.length;
    }

    public boolean isEmpty()
    {
        return 0 == actions.length;
    }

    public Action get(int i)
    {
        return actions[i];
    }

    public Iterator<Action> iterator()
    {
        return Iterators.forArray(actions);
    }

    public Stream<Action> stream()
    {
        return Stream.of(actions);
    }

    public ActionList transform(Function<Action, Action> apply)
    {
        return ActionList.of(stream().map(apply));
    }

    public ActionList filter(Predicate<Action> apply)
    {
        return ActionList.of(stream().filter(apply));
    }

    public boolean anyMatch(Predicate<Action> test)
    {
        for (int i = 0 ; i < actions.length ; ++i)
            if (test.test(actions[i])) return true;
        return false;
    }

    public ActionList andThen(Action andThen)
    {
        return andThen(ActionList.of(andThen));
    }

    public ActionList andThen(ActionList andThen)
    {
        Action[] result = copyOf(actions, size() + andThen.size());
        System.arraycopy(andThen.actions, 0, result, size(), andThen.size());
        return new ActionList(result);
    }

    public ActionList setStrictlySequential()
    {
        return setStrictlySequentialOn(this);
    }

    public ActionList setStrictlySequentialOn(Object on)
    {
        if (isEmpty()) return this;
        StrictSequential orderOn = new StrictSequential(on);
        forEach(a -> a.orderOn(orderOn));
        return this;
    }

    public Throwable safeForEach(Consumer<Action> forEach)
    {
        Throwable result = null;
        for (Action action : actions)
        {
            try
            {
                forEach.accept(action);
            }
            catch (Throwable t)
            {
                result = Throwables.merge(result, t);
            }
        }
        return result;
    }

    public String toString()
    {
        return Arrays.toString(actions);
    }

    public String toReconcileString()
    {
        return Arrays.stream(actions).map(Action::toReconcileString).collect(Collectors.joining(",", "[", "]"));
    }
}

