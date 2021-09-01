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

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import javax.annotation.Nullable;

import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IMessage;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.net.RequestCallbacks;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.simulator.Action;
import org.apache.cassandra.simulator.ActionList;
import org.apache.cassandra.simulator.Actions;
import org.apache.cassandra.simulator.OrderOn;
import org.apache.cassandra.simulator.systems.InterceptedExecution.InterceptedRunnableExecution;
import org.apache.cassandra.simulator.systems.InterceptedWait.TriggerListener;
import org.apache.cassandra.utils.LazyToString;
import org.apache.cassandra.utils.Shared;

import static org.apache.cassandra.net.MessagingService.instance;
import static org.apache.cassandra.simulator.Action.Modifiers.NONE;
import static org.apache.cassandra.simulator.Action.Modifiers.RELIABLE;
import static org.apache.cassandra.simulator.Action.Modifiers.SCHEDULED;
import static org.apache.cassandra.simulator.Action.Modifiers.START_INFINITE_LOOP;
import static org.apache.cassandra.simulator.Action.Modifiers.TIMEOUT;
import static org.apache.cassandra.simulator.Action.Modifiers.WAKE_UP_THREAD;
import static org.apache.cassandra.simulator.Action.Modifiers.WAKE_UP_THREAD_LATER;
import static org.apache.cassandra.simulator.systems.SimulatedAction.Kind.TASK;
import static org.apache.cassandra.simulator.Debug.Info.LOG;
import static org.apache.cassandra.simulator.systems.InterceptedWait.Kind.UNBOUNDED_WAIT;
import static org.apache.cassandra.utils.LazyToString.lazy;
import static org.apache.cassandra.utils.Shared.Scope.SIMULATION;

/**
 * This class is the nexus of simulation, as this is where we translate intercepted system events
 * into Action events.
 */
public abstract class SimulatedAction extends Action implements InterceptorOfConsequences
{
    @Shared(scope = SIMULATION)
    public enum Kind
    {
        TASK(RELIABLE, NONE, WAKE_UP_THREAD),
        SCHEDULED_TASK(SCHEDULED, NONE, WAKE_UP_THREAD),
        SCHEDULED_TIMEOUT(TIMEOUT, NONE, WAKE_UP_THREAD),
        DAEMON(Modifiers.DAEMON, NONE, WAKE_UP_THREAD),
        THREAD(RELIABLE, NONE, WAKE_UP_THREAD),
        INFINITE_LOOP(START_INFINITE_LOOP, NONE, WAKE_UP_THREAD);

        public final Modifiers self, transitive, signal;
        Kind(Modifiers self, Modifiers transitive, Modifiers signal)
        {
            this.self = self;
            this.transitive = transitive;
            this.signal = signal;
        }

        public boolean isTask()
        {
            return this == TASK || this == SCHEDULED_TASK;
        }
    }

    class Signal extends Action implements TriggerListener
    {
        final boolean isTimeout;
        final InterceptedWait wakeup;
        boolean signalling;

        // note that we do not inherit from the parent thread's self, as anything relevantly heritable by continuations is likely already transitive
        protected Signal(Object description, Modifiers self, boolean isTimeout, InterceptedWait wakeup)
        {
            super(description, self.inheritIfContinuation(SimulatedAction.this.self()), NONE);
            this.isTimeout = isTimeout;
            this.wakeup = wakeup;
            assert !wakeup.isTriggered();
            wakeup.addListener(this);
        }

        // provide the parameters to send to SimulatedAction.this
        @Override
        protected ActionList performed(ActionList consequences, boolean isStart, boolean isFinish)
        {
            assert !isStart;
            ActionList restored = super.performed(ActionList.empty(), true, true);
            consequences = SimulatedAction.this.performed(consequences, false, isFinish);
            if (!restored.isEmpty()) consequences = consequences.andThen(restored);
            return consequences;
        }

        @Override
        protected ActionList perform(boolean perform)
        {
            if (!perform)
                return performed(ActionList.empty(), false, false);

            assert !wakeup.isTriggered();
            assert !isFinished();
            assert !realThreadHasTerminated;

            signalling = true;
            return performed(simulate(() -> wakeup.triggerAndAwaitDone(SimulatedAction.this, isTimeout)),
                             false, realThreadHasTerminated);
        }

        @Override
        public void onTrigger(InterceptedWait triggered)
        {
            if (!signalling)
                invalidate();
        }
    }

    protected final SimulatedAction.Kind kind;
    protected final SimulatedSystems simulated;
    protected Map<Verb, Modifiers> verbModifiers;

    private InterceptibleThread realThread; // unset until first simulation

    private List<Action> consequences; // valid only for one round of simulation, until paused
    private @Nullable InterceptedWait pausedOn;
    private boolean realThreadHasTerminated;

    public SimulatedAction(Object description, Modifiers self, Modifiers transitive, SimulatedSystems simulated)
    {
        this(description, OrderOn.NONE, self, transitive, simulated);
    }

    public SimulatedAction(Object description, OrderOn orderOn, Modifiers self, Modifiers transitive, SimulatedSystems simulated)
    {
        this(description, TASK, orderOn, self, transitive, Collections.emptyMap(), simulated);
    }

    public SimulatedAction(Object description, Kind kind, OrderOn orderOn, Modifiers self, Modifiers transitive, Map<Verb, Modifiers> verbModifiers, SimulatedSystems simulated)
    {
        super(description, orderOn, self, transitive);
        this.kind = kind;
        this.simulated = simulated;
        this.verbModifiers = verbModifiers;
    }

    @Override
    public void interceptMessage(IInvokableInstance from, IInvokableInstance to, IMessage message)
    {
        consequences.add(applyToMessage(from, to, message));
    }

    @Override
    public void interceptWakeup(InterceptedWait wakeup, InterceptorOfConsequences waitWasInterceptedBy)
    {
        SimulatedAction action = (SimulatedAction) waitWasInterceptedBy;
        action.applyToWakeup(consequences, wakeup);
    }

    @Override
    public void interceptExecution(InterceptedExecution invoke, InterceptingExecutor orderOn)
    {
        consequences.add(applyToExecution(invoke, orderOn));
    }

    @Override
    public void interceptWait(@Nullable InterceptedWait wakeupWith)
    {
        pausedOn = wakeupWith;
    }

    @Override
    public void interceptTermination()
    {
        realThreadHasTerminated = true;
    }

    private ActionList simulate(Runnable simulate)
    {
        try
        {
            simulate.run();

            InterceptedWait wakeUpWith = pausedOn;
            if (wakeUpWith != null && wakeUpWith.kind() != UNBOUNDED_WAIT)
            {
                applyToWait(consequences, wakeUpWith);
            }
            else if (wakeUpWith != null && kind.isTask() && !isFinished())
            {
                if (simulated.debug.isOn(LOG))
                    consequences.add(Actions.empty(Modifiers.INFO, lazy(() -> "Waiting[" + wakeUpWith + "] " + realThread)));
            }

            return ActionList.of(consequences);
        }
        finally
        {
            this.consequences = null;
            this.pausedOn = null;
        }
    }

    protected abstract InterceptedExecution task();

    protected ActionList perform(boolean perform)
    {
        if (!perform)
            return performed(ActionList.empty(), true, true);

        return performed(simulate(() -> task().invokeAndAwaitPause(this)),
                         true, realThreadHasTerminated);
    }

    @Override
    public void beforeInvocation(InterceptibleThread realThread)
    {
        this.consequences = new ArrayList<>();
        this.realThread = realThread;
    }

    void applyToWait(List<Action> out, InterceptedWait wakeupWith)
    {
        switch (wakeupWith.kind())
        {
            case TIMED_WAIT: applyToSignal(out, Modifiers.TIMEOUT, "Timeout", wakeupWith, true); break;
            case NEMESIS: applyToSignal(out, WAKE_UP_THREAD_LATER, "Nemesis", wakeupWith, false); break;
            default : applyToSignal(out, WAKE_UP_THREAD, "Continue", wakeupWith, false); break;
        }
    }

    void applyToWakeup(List<Action> out, InterceptedWait wakeup)
    {
        applyToSignal(out, kind.signal, "Wakeup", wakeup, false);
    }

    void applyToSignal(List<Action> out, Modifiers self, String kind, InterceptedWait wakeup, boolean isTimeout)
    {
        applyToSignal(out, lazy(() -> kind + '[' + wakeup + "] " + realThread), self, isTimeout, wakeup);
    }

    void applyToSignal(List<Action> out, LazyToString id, Modifiers self, boolean isTimeout, InterceptedWait wakeup)
    {
        if (isTimeout && !self.is(Modifier.TIMEOUT))
            throw new IllegalStateException();

        out.add(new Signal(id, self, isTimeout, wakeup));
    }

    Action applyToMessage(IInvokableInstance from, IInvokableInstance to, IMessage message)
    {
        InterceptedExecution.InterceptedTaskExecution task = new InterceptedRunnableExecution(
            (InterceptingExecutor) to.executorFor(message.verb()), () -> to.receiveMessageWithInvokingThread(message)
        );

        Verb verb = Verb.fromId(message.verb());
        Modifiers self = verbModifiers.getOrDefault(verb, NONE);

        return applyTo(lazy(() -> String.format("%s(%d) from %s to %s", Verb.fromId(message.verb()), message.id(), message.from(), to.broadcastAddress())),
                       TASK, task.executor, self, task, () -> {
                        InetSocketAddress failedOn = (verb.isResponse() ? from : to).broadcastAddress();
                        IInvokableInstance notify = verb.isResponse() ? to : from;
                        InterceptedExecution.InterceptedTaskExecution failTask = new InterceptedRunnableExecution(
                        (InterceptingExecutor) notify.executorFor(verb.id),
                            () -> notify.unsafeApplyOnThisThread(
                                (socketAddress, id, decision) -> {
                                    InetAddressAndPort address = InetAddressAndPort.getByAddress(socketAddress);
                                    RequestCallbacks.CallbackInfo callback = instance().callbacks.remove(id, address);
                                    if (callback != null && callback.callback.invokeOnFailure())
                                    {
                                        RequestCallback<?> invokeOn = (RequestCallback<?>) callback.callback;
                                        // TODO (future): randomise this with biased distribution
                                        RequestFailureReason reason = decision ? RequestFailureReason.UNKNOWN : RequestFailureReason.TIMEOUT;
                                        invokeOn.onFailure(address, reason);
                                    }
                                    return null;
                            }, failedOn, message.id(), simulated.random.decide(0.5f))
                        );
            return ActionList.of(applyTo((lazy(() -> String.format("Report Timeout of %s(%d) from %s to %s", Verb.fromId(message.verb()), message.id(), failedOn, notify.broadcastAddress()))),
                                         TASK, failTask.executor, RELIABLE, failTask
            ));
        });
    }

    Action applyToExecution(InterceptedExecution invoke, OrderOn orderedOn)
    {
        return applyTo(lazy(() -> String.format("Invoke %s", invoke)),
                       invoke.kind(), orderedOn, invoke.kind().self,
                       invoke);
    }

    Action applyTo(Object description, Kind kind, OrderOn orderOn, Modifiers self, InterceptedExecution task)
    {
        return new SimulatedActionTask(description, kind, orderOn, self, NONE, verbModifiers, simulated, task);
    }

    Action applyTo(Object description, Kind kind, OrderOn orderOn, Modifiers self, InterceptedExecution task, Supplier<ActionList> drop)
    {
        return new SimulatedActionTaskDroppable(description, kind, orderOn, self, NONE, verbModifiers, simulated, task, drop);
    }

    @SuppressWarnings({"SameParameterValue", "UnusedReturnValue"})
    protected SimulatedAction setMessageModifiers(Verb verb, Modifiers self, Modifiers responses)
    {
        if (verbModifiers.isEmpty())
            verbModifiers = new EnumMap<>(Verb.class);
        verbModifiers.put(verb, self);
        verbModifiers.put(verb.responseVerb, responses);
        return this;
    }

    public Object description()
    {
        return realThread == null ? super.description() : super.description() + " on " + realThread;
    }
}
