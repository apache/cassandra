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

package org.apache.cassandra.simulator.systems;

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor.SerializableRunnable;
import org.apache.cassandra.simulator.Action;
import org.apache.cassandra.simulator.Action.Modifiers;
import org.apache.cassandra.simulator.Debug;
import org.apache.cassandra.simulator.FutureActionScheduler;
import org.apache.cassandra.simulator.RandomSource;
import org.apache.cassandra.simulator.cluster.Topology;
import org.apache.cassandra.simulator.cluster.TopologyListener;

import static org.apache.cassandra.simulator.Action.Modifiers.NONE;
import static org.apache.cassandra.simulator.Action.Modifiers.RELIABLE;

public class SimulatedSystems
{
    public final RandomSource random;
    public final SimulatedTime time;
    public final SimulatedMessageDelivery delivery;
    public final SimulatedExecution execution;
    public final SimulatedBallots ballots;
    public final SimulatedFailureDetector failureDetector;
    public final SimulatedSnitch snitch;
    public final FutureActionScheduler futureScheduler;
    public final Debug debug;
    public final Failures failures;
    private final List<TopologyListener> topologyListeners; // TODO (cleanup): this is a mutable set of listeners but shared between instances

    public SimulatedSystems(SimulatedSystems copy)
    {
        this(copy.random, copy.time, copy.delivery, copy.execution, copy.ballots, copy.failureDetector, copy.snitch, copy.futureScheduler, copy.debug, copy.failures, copy.topologyListeners);
    }

    public SimulatedSystems(RandomSource random, SimulatedTime time, SimulatedMessageDelivery delivery, SimulatedExecution execution, SimulatedBallots ballots, SimulatedFailureDetector failureDetector, SimulatedSnitch snitch, FutureActionScheduler futureScheduler, Debug debug, Failures failures)
    {
        this(random, time, delivery, execution, ballots, failureDetector, snitch, futureScheduler, debug, failures, new ArrayList<>());
    }

    private SimulatedSystems(RandomSource random, SimulatedTime time, SimulatedMessageDelivery delivery, SimulatedExecution execution, SimulatedBallots ballots, SimulatedFailureDetector failureDetector, SimulatedSnitch snitch, FutureActionScheduler futureScheduler, Debug debug, Failures failures, List<TopologyListener> topologyListeners)
    {
        this.random = random;
        this.time = time;
        this.delivery = delivery;
        this.execution = execution;
        this.ballots = ballots;
        this.failureDetector = failureDetector;
        this.snitch = snitch;
        this.futureScheduler = futureScheduler;
        this.debug = debug;
        this.failures = failures;
        this.topologyListeners = topologyListeners;
    }

    public Action run(Object description, IInvokableInstance on, SerializableRunnable run)
    {
        return new SimulatedActionTask(description, NONE, NONE, this, on, run);
    }

    public Action transitivelyReliable(Object description, IInvokableInstance on, SerializableRunnable run)
    {
        return new SimulatedActionTask(description, NONE, RELIABLE, this, on, run);
    }

    /**
     * Useful for non-serializable operations we still need to intercept the execution of
     */
    public Action invoke(Object description, Modifiers self, Modifiers children, InterceptedExecution invoke)
    {
        return new SimulatedActionTask(description, self, children, null, this, invoke);
    }

    public void announce(Topology topology)
    {
        for (int i = 0; i < topologyListeners.size() ; ++i)
            topologyListeners.get(i).onChange(topology);
    }

    public void register(TopologyListener listener)
    {
        topologyListeners.add(listener);
    }
}
