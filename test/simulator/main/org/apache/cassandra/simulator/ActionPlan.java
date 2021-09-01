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
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;

import org.apache.cassandra.simulator.systems.SimulatedTime;

import static java.util.Collections.singletonList;

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
    public final List<ActionSequence> interleave;

    /**
     * Actions to perform (reliably, and in strict order) after finishing the proper simulation.
     *
     * This is only run if the simulation was successful, so no cleanup should be performed here unless optional.
     */
    public final ActionList post;

    public ActionPlan(ActionList pre, List<ActionSequence> interleave, ActionList post)
    {
        this.pre = pre;
        this.interleave = interleave;
        this.post = post;
    }

    public Iterable<?> iterable(SimulatedTime time, ActionSchedulers preAndPostSchedulers, ActionSchedulers mainSchedulers)
    {
        return () -> Iterators.concat(
                new ActionSchedule(time, preAndPostSchedulers, singletonList(pre.strictlySequential())),
                new ActionSchedule(time, mainSchedulers, interleave),
                new ActionSchedule(time, preAndPostSchedulers, singletonList(post.strictlySequential()))
        );
    }

    public ActionPlan encapsulate(ActionPlan that)
    {
        return new ActionPlan(
                this.pre.andThen(that.pre),
                ImmutableList.<ActionSequence>builder().addAll(this.interleave).addAll(that.interleave).build(),
                that.post.andThen(this.post));
    }
}

