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
import java.util.function.LongSupplier;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.simulator.ActionSchedule.Work;
import org.apache.cassandra.simulator.systems.SimulatedTime;
import org.apache.cassandra.utils.CloseableIterator;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.cassandra.simulator.ActionSchedule.Mode.FINITE;
import static org.apache.cassandra.simulator.ActionSchedule.Mode.UNLIMITED;

public class ActionPlan
{
    /**
     * Actions to perform (reliably, and in strict order) before starting the proper simulation
     */
    public final ActionList pre;

    /**
     * List of action sequences, each representing the actions planned by a given actor, that will
     * be performed in the provided sequence but otherwise randomly interleaved with the other action sequences.
     * These planned actions may initiate other actions, that will all complete before the next planned action
     * for that action sequence is started.
     */
    public final List<ActionList> interleave;

    /**
     * Actions to perform (reliably, and in strict order) after finishing the proper simulation.
     *
     * This is only run if the simulation was successful, so no cleanup should be performed here unless optional.
     */
    public final ActionList post;

    public ActionPlan(ActionList pre, List<ActionList> interleave, ActionList post)
    {
        this.pre = pre;
        this.interleave = interleave;
        this.post = post;
    }

    public CloseableIterator<?> iterator(ActionSchedule.Mode mode, long runForNanos, LongSupplier schedulerJitter, SimulatedTime time, RunnableActionScheduler runnableScheduler, FutureActionScheduler futureScheduler)
    {
        return new ActionSchedule(time, futureScheduler, schedulerJitter, runnableScheduler,
                                  new Work(UNLIMITED, singletonList(pre.setStrictlySequential())),
                                  new Work(mode, runForNanos, interleave),
                                  new Work(FINITE, singletonList(post.setStrictlySequential())));
    }

    public static ActionPlan interleave(List<ActionList> interleave)
    {
        return new ActionPlan(ActionList.empty(), interleave, ActionList.empty());
    }

    public static ActionPlan setUpTearDown(ActionList pre, ActionList post)
    {
        return new ActionPlan(pre, emptyList(), post);
    }

    public ActionPlan encapsulate(ActionPlan that)
    {
        return new ActionPlan(
                this.pre.andThen(that.pre),
                ImmutableList.<ActionList>builder().addAll(this.interleave).addAll(that.interleave).build(),
                that.post.andThen(this.post));
    }
}

