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
import java.util.concurrent.Executor;

import javax.annotation.Nullable;
import org.apache.cassandra.concurrent.ImmediateExecutor;
import com.google.common.base.Preconditions;

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
import org.apache.cassandra.simulator.FutureActionScheduler.Deliver;
import org.apache.cassandra.simulator.OrderOn;
import org.apache.cassandra.simulator.systems.InterceptedExecution.InterceptedRunnableExecution;
import org.apache.cassandra.simulator.systems.InterceptedWait.Trigger;
import org.apache.cassandra.simulator.systems.InterceptedWait.TriggerListener;
import org.apache.cassandra.utils.LazyToString;
import org.apache.cassandra.utils.Shared;

import static org.apache.cassandra.net.MessagingService.instance;
import static org.apache.cassandra.simulator.Action.Modifiers.DROP;
import static org.apache.cassandra.simulator.Action.Modifiers.NONE;
import static org.apache.cassandra.simulator.Action.Modifiers.PSEUDO_ORPHAN;
import static org.apache.cassandra.simulator.Action.Modifiers.START_DAEMON_TASK;
import static org.apache.cassandra.simulator.Action.Modifiers.START_SCHEDULED_TASK;
import static org.apache.cassandra.simulator.Action.Modifiers.START_INFINITE_LOOP;
import static org.apache.cassandra.simulator.Action.Modifiers.START_TASK;
import static org.apache.cassandra.simulator.Action.Modifiers.START_THREAD;
import static org.apache.cassandra.simulator.Action.Modifiers.START_TIMEOUT_TASK;
import static org.apache.cassandra.simulator.Action.Modifiers.WAKE_UP_THREAD;
import static org.apache.cassandra.simulator.FutureActionScheduler.Deliver.DELIVER;
import static org.apache.cassandra.simulator.FutureActionScheduler.Deliver.DELIVER_AND_TIMEOUT;
import static org.apache.cassandra.simulator.FutureActionScheduler.Deliver.FAILURE;
import static org.apache.cassandra.simulator.systems.InterceptedWait.Trigger.SIGNAL;
import static org.apache.cassandra.simulator.systems.InterceptedWait.Trigger.TIMEOUT;
import static org.apache.cassandra.simulator.systems.SimulatedAction.Kind.MESSAGE;
import static org.apache.cassandra.simulator.systems.SimulatedAction.Kind.REDUNDANT_MESSAGE_TIMEOUT;
import static org.apache.cassandra.simulator.systems.SimulatedAction.Kind.SCHEDULED_TIMEOUT;
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
        MESSAGE(NONE, NONE, WAKE_UP_THREAD),
        REDUNDANT_MESSAGE_TIMEOUT(PSEUDO_ORPHAN, NONE, WAKE_UP_THREAD),
        TASK(START_TASK, NONE, WAKE_UP_THREAD),
        SCHEDULED_TASK(START_SCHEDULED_TASK, NONE, WAKE_UP_THREAD),
        SCHEDULED_TIMEOUT(START_TIMEOUT_TASK, NONE, WAKE_UP_THREAD),
        SCHEDULED_DAEMON(START_DAEMON_TASK, NONE, WAKE_UP_THREAD),
        THREAD(START_THREAD, NONE, WAKE_UP_THREAD),
        INFINITE_LOOP(START_INFINITE_LOOP, NONE, WAKE_UP_THREAD);

        public final Modifiers self, transitive, signal;
        Kind(Modifiers self, Modifiers transitive, Modifiers signal)
        {
            this.self = self;
            this.transitive = transitive;
            this.signal = signal;
        }

        public boolean logWakeups()
        {
            return this == MESSAGE || this == TASK || this == SCHEDULED_TASK;
        }
    }

    class Signal extends Action implements TriggerListener
    {
        final InterceptedWait wakeup;
        final Trigger trigger;
        boolean signalling;

        // note that we do not inherit from the parent thread's self, as anything relevantly heritable by continuations is likely already transitive
        protected Signal(Object description, Modifiers self, InterceptedWait wakeup, Trigger trigger, long deadlineNanos)
        {
            super(description, self.inheritIfContinuation(SimulatedAction.this.self()), NONE);
            this.wakeup = wakeup;
            this.trigger = trigger;
            assert deadlineNanos < 0 || trigger == TIMEOUT;
            if (deadlineNanos >= 0)
                setDeadline(simulated.time, deadlineNanos);
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
        protected ActionList performAndRegister()
        {
            assert !wakeup.isTriggered();
            assert !isFinished();

            if (SimulatedAction.this.isFinished())
                return super.performed(ActionList.empty(), true, true);
            assert !realThreadHasTerminated;

            signalling = true;
            return performed(performSimple(), false, realThreadHasTerminated);
        }

        @Override
        protected ActionList performSimple()
        {
            return simulate(() -> wakeup.triggerAndAwaitDone(SimulatedAction.this, trigger));
        }

        @Override
        public void onTrigger(InterceptedWait triggered)
        {
            if (!signalling)
                cancel();
        }
    }

    protected final SimulatedAction.Kind kind;
    protected final SimulatedSystems simulated;
    protected final Verb forVerb;
    protected Map<Verb, Modifiers> verbModifiers;

    private InterceptibleThread realThread; // unset until first simulation

    private List<Action> consequences; // valid only for one round of simulation, until paused
    private @Nullable InterceptedWait pausedOn;
    private boolean realThreadHasTerminated;

    public SimulatedAction(Object description, Modifiers self, Modifiers transitive, Verb forVerb, SimulatedSystems simulated)
    {
        this(description, OrderOn.NONE, self, transitive, forVerb, simulated);
    }

    public SimulatedAction(Object description, OrderOn orderOn, Modifiers self, Modifiers transitive, Verb forVerb, SimulatedSystems simulated)
    {
        this(description, TASK, orderOn, self, transitive, Collections.emptyMap(), forVerb, simulated);
    }

    public SimulatedAction(Object description, Kind kind, OrderOn orderOn, Modifiers self, Modifiers transitive, Map<Verb, Modifiers> verbModifiers, Verb forVerb, SimulatedSystems simulated)
    {
        super(description, orderOn, self, transitive);
        Preconditions.checkNotNull(kind);
        Preconditions.checkNotNull(verbModifiers);
        Preconditions.checkNotNull(simulated);
        this.kind = kind;
        this.simulated = simulated;
        this.verbModifiers = verbModifiers;
        this.forVerb = forVerb;
    }

    @Override
    public void interceptMessage(IInvokableInstance from, IInvokableInstance to, IMessage message)
    {
        if (!to.isShutdown())
            consequences.addAll(applyToMessage(from, to, message));
    }

    @Override
    public void interceptWakeup(InterceptedWait wakeup, Trigger trigger, InterceptorOfConsequences waitWasInterceptedBy)
    {
        SimulatedAction action = (SimulatedAction) waitWasInterceptedBy;
        action.applyToWakeup(consequences, wakeup, trigger);
    }

    @Override
    public void interceptExecution(InterceptedExecution invoke, OrderOn orderOn)
    {
        if (invoke.kind() == SCHEDULED_TIMEOUT && transitive().is(Modifier.RELIABLE) && transitive().is(Modifier.NO_THREAD_TIMEOUTS))
            invoke.cancel();
        else
            consequences.add(applyToExecution(invoke, orderOn));
    }

    @Override
    public void interceptWait(@Nullable InterceptedWait wakeupWith)
    {
        pausedOn = wakeupWith;
    }

    @Override
    public void interceptTermination(boolean isThreadTermination)
    {
        realThreadHasTerminated = true;
    }

    private ActionList simulate(Runnable simulate)
    {
        try
        {
            try
            {
                simulate.run();
            }
            catch (Throwable t)
            {
                consequences.forEach(Action::invalidate);
                throw t;
            }

            InterceptedWait wakeUpWith = pausedOn;
            if (wakeUpWith != null && wakeUpWith.kind() != UNBOUNDED_WAIT)
            {
                applyToWait(consequences, wakeUpWith);
            }
            else if (wakeUpWith != null && kind.logWakeups() && !isFinished())
            {
                if (simulated.debug.isOn(LOG))
                    consequences.add(Actions.empty(Modifiers.INFO, lazy(() -> "Waiting[" + wakeUpWith + "] " + realThread)));
            }

            for (int i = consequences.size() - 1; i >= 0 ; --i)
            {
                // a scheduled future might be cancelled by the same action that creates it
                if (consequences.get(i).isCancelled())
                    consequences.remove(i);
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

    protected ActionList performAndRegister()
    {
        return performed(performSimple(), true, realThreadHasTerminated);
    }

    @Override
    protected ActionList performSimple()
    {
        return simulate(() -> task().invokeAndAwaitPause(this));
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
            case WAIT_UNTIL:
                applyToSignal(out, START_TIMEOUT_TASK, "Timeout", wakeupWith, TIMEOUT, wakeupWith.waitTime());
                break;
            case NEMESIS:
                applyToSignal(out, WAKE_UP_THREAD, "Nemesis", wakeupWith, SIGNAL, -1L);
                break;
            default :
                applyToSignal(out, WAKE_UP_THREAD, "Continue", wakeupWith, SIGNAL, -1L);
                break;
        }
    }

    void applyToWakeup(List<Action> out, InterceptedWait wakeup, Trigger trigger)
    {
        applyToSignal(out, kind.signal, "Wakeup", wakeup, trigger, -1);
    }

    void applyToSignal(List<Action> out, Modifiers self, String kind, InterceptedWait wakeup, Trigger trigger, long deadlineNanos)
    {
        applyToSignal(out, lazy(() -> kind + wakeup + ' ' + realThread), self, wakeup, trigger, deadlineNanos);
    }

    void applyToSignal(List<Action> out, LazyToString id, Modifiers self, InterceptedWait wakeup, Trigger trigger, long deadlineNanos)
    {
        if (deadlineNanos >= 0 && !self.is(Modifier.THREAD_TIMEOUT))
            throw new IllegalStateException();

        out.add(new Signal(id, self, wakeup, trigger, deadlineNanos));
    }

    List<Action> applyToMessage(IInvokableInstance from, IInvokableInstance to, IMessage message)
    {
        Executor executor = to.executorFor(message.verb());
        if (executor instanceof ImmediateExecutor)
            executor = to.executor();

        Verb verb = Verb.fromId(message.verb());
        Modifiers self = verbModifiers.getOrDefault(verb, NONE);

        int fromNum = from.config().num();
        int toNum = to.config().num();

        long expiresAtNanos = simulated.time.get(fromNum).localToGlobalNanos(message.expiresAtNanos());
        boolean isReliable = is(Modifier.RELIABLE) || self.is(Modifier.RELIABLE);
        Deliver deliver = isReliable ? DELIVER : simulated.futureScheduler.shouldDeliver(fromNum, toNum);

        List<Action> actions = new ArrayList<>(deliver == DELIVER_AND_TIMEOUT ? 2 : 1);
        switch (deliver)
        {
            default: throw new AssertionError();
            case DELIVER:
            case DELIVER_AND_TIMEOUT:
            {
                InterceptedExecution.InterceptedTaskExecution task = new InterceptedRunnableExecution(
                    (InterceptingExecutor) executor, () -> to.receiveMessageWithInvokingThread(message)
                );
                Object description = lazy(() -> String.format("%s(%d) from %s to %s", verb, message.id(), message.from(), to.broadcastAddress()));
                OrderOn orderOn = task.executor.orderAppliesAfterScheduling();
                Action action = applyTo(description, MESSAGE, orderOn, self, verb, task);
                long deadlineNanos = simulated.futureScheduler.messageDeadlineNanos(fromNum, toNum);
                if (deliver == DELIVER && deadlineNanos >= expiresAtNanos)
                {
                    if (isReliable) deadlineNanos = verb.isResponse() ? expiresAtNanos : expiresAtNanos / 2;
                    else deliver = DELIVER_AND_TIMEOUT;
                }
                action.setDeadline(simulated.time, deadlineNanos);
                actions.add(action);
                if (deliver == DELIVER)
                    break;
            }
            case FAILURE:
            case TIMEOUT:
            {
                InetSocketAddress failedOn;
                IInvokableInstance notify;
                if (verb.isResponse())
                {
                    failedOn = from.broadcastAddress();
                    notify = to;
                }
                else
                {
                    failedOn = to.broadcastAddress();
                    notify = from;
                }
                boolean isTimeout = deliver != FAILURE;
                InterceptedExecution.InterceptedTaskExecution failTask = new InterceptedRunnableExecution(
                    (InterceptingExecutor) notify.executorFor(verb.id),
                    () -> notify.unsafeApplyOnThisThread((socketAddress, id, innerIsTimeout) -> {
                        InetAddressAndPort address = InetAddressAndPort.getByAddress(socketAddress);
                        RequestCallbacks.CallbackInfo callback = instance().callbacks.remove(id, address);
                        if (callback != null)
                        {
                            RequestCallback<?> invokeOn = (RequestCallback<?>) callback.callback;
                            RequestFailureReason reason = innerIsTimeout ? RequestFailureReason.TIMEOUT : RequestFailureReason.UNKNOWN;
                            invokeOn.onFailure(address, reason);
                        }
                        return null;
                    }, failedOn, message.id(), isTimeout)
                );

                Object description = (lazy(() -> String.format("Report Timeout of %s(%d) from %s to %s", Verb.fromId(message.verb()), message.id(), failedOn, notify.broadcastAddress())));
                OrderOn orderOn = failTask.executor.orderAppliesAfterScheduling();
                self = DROP.with(self);
                Kind kind = deliver == DELIVER_AND_TIMEOUT ? REDUNDANT_MESSAGE_TIMEOUT : MESSAGE;
                Action action = applyTo(description, kind, orderOn, self, failTask);
                switch (deliver)
                {
                    default: throw new AssertionError();
                    case FAILURE:
                        long deadlineNanos = simulated.futureScheduler.messageFailureNanos(toNum, fromNum);
                        if (deadlineNanos < expiresAtNanos)
                        {
                            action.setDeadline(simulated.time, deadlineNanos);
                            break;
                        }
                    case DELIVER_AND_TIMEOUT:
                    case TIMEOUT:
                        long expirationIntervalNanos = from.unsafeCallOnThisThread(RequestCallbacks::defaultExpirationInterval);
                        action.setDeadline(simulated.time, simulated.futureScheduler.messageTimeoutNanos(expiresAtNanos, expirationIntervalNanos));
                        break;
                }
                actions.add(action);
            }
        }
        return actions;
    }

    Action applyToExecution(InterceptedExecution invoke, OrderOn orderOn)
    {
        Action result =  applyTo(lazy(() -> String.format("Invoke %s", invoke)),
                                 invoke.kind(), orderOn, invoke.kind().self, invoke);
        switch (invoke.kind())
        {
            case SCHEDULED_DAEMON:
            case SCHEDULED_TASK:
            case SCHEDULED_TIMEOUT:
                result.setDeadline(simulated.time, invoke.deadlineNanos());
        }

        return result;
    }

    Action applyTo(Object description, Kind kind, OrderOn orderOn, Modifiers self, InterceptedExecution task)
    {
        return new SimulatedActionTask(description, kind, orderOn, self, NONE, verbModifiers, forVerb, simulated, task);
    }

    Action applyTo(Object description, Kind kind, OrderOn orderOn, Modifiers self, Verb verb, InterceptedExecution task)
    {
        return new SimulatedActionTask(description, kind, orderOn, self, NONE, verbModifiers, verb, simulated, task);
    }

    @SuppressWarnings({"SameParameterValue", "UnusedReturnValue"})
    protected SimulatedAction setMessageModifiers(Verb verb, Modifiers self, Modifiers responses)
    {
        if (verbModifiers.isEmpty())
            verbModifiers = new EnumMap<>(Verb.class);
        verbModifiers.put(verb, self);
        if (verb.responseVerb != null)
            verbModifiers.put(verb.responseVerb, responses);
        return this;
    }

    public Object description()
    {
        return realThread == null ? super.description() : super.description() + " on " + realThread;
    }
}
