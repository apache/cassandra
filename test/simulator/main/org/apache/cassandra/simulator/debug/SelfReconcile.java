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

package org.apache.cassandra.simulator.debug;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ExecutorFactory;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IMessage;
import org.apache.cassandra.simulator.ClusterSimulation;
import org.apache.cassandra.simulator.OrderOn;
import org.apache.cassandra.simulator.RandomSource;
import org.apache.cassandra.simulator.SimulationRunner.RecordOption;
import org.apache.cassandra.simulator.systems.InterceptedExecution;
import org.apache.cassandra.simulator.systems.InterceptedWait;
import org.apache.cassandra.simulator.systems.InterceptedWait.CaptureSites;
import org.apache.cassandra.simulator.systems.InterceptibleThread;
import org.apache.cassandra.simulator.systems.InterceptorOfConsequences;
import org.apache.cassandra.simulator.systems.SimulatedTime;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;
import org.apache.cassandra.utils.memory.HeapPool;

import static org.apache.cassandra.simulator.SimulationRunner.RecordOption.NONE;
import static org.apache.cassandra.simulator.SimulationRunner.RecordOption.WITH_CALLSITES;
import static org.apache.cassandra.simulator.SimulatorUtils.failWithOOM;
import static org.apache.cassandra.simulator.debug.Reconcile.NORMALISE_LAMBDA;
import static org.apache.cassandra.simulator.debug.Reconcile.NORMALISE_THREAD;

/**
 * Simulator runs should be deterministic, so run two in parallel and see if they produce the same event stream
 */
public class SelfReconcile
{
    private static final Logger logger = LoggerFactory.getLogger(SelfReconcile.class);
    static final Pattern NORMALISE_RECONCILE_THREAD = Pattern.compile("(Thread\\[Reconcile:)[0-9]+,[0-9],Reconcile(_[0-9]+)?]");

    static class InterceptReconciler implements InterceptorOfConsequences, Supplier<RandomSource>, SimulatedTime.Listener
    {
        final List<Object> events = new ArrayList<>();
        final boolean withRngCallsites;
        int counter = 0;
        boolean verifyUninterceptedRng;
        boolean closed;

        InterceptReconciler(boolean withRngCallsites)
        {
            this.withRngCallsites = withRngCallsites;
        }

        @Override
        public void beforeInvocation(InterceptibleThread realThread)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public synchronized void interceptMessage(IInvokableInstance from, IInvokableInstance to, IMessage message)
        {
            verify("Send " + message.verb() + " from " + from.config().num() + " to " + to.config().num());
        }

        @Override
        public synchronized void interceptWakeup(InterceptedWait wakeup, InterceptedWait.Trigger trigger, InterceptorOfConsequences waitWasInterceptedBy)
        {
            verify(normalise("Wakeup " + wakeup.waiting() + wakeup));
        }

        @Override
        public synchronized void interceptExecution(InterceptedExecution invoke, OrderOn orderOn)
        {
            verify("Execute " + normalise(invoke.toString()) + " on " + orderOn);
        }

        @Override
        public synchronized void interceptWait(InterceptedWait wakeupWith)
        {
            verify(normalise("Wait " + wakeupWith.waiting() + wakeupWith));
        }

        @Override
        public void interceptTermination(boolean isThreadTermination)
        {
            // Cannot verify this, as thread may terminate after triggering follow-on events
        }

        void interceptAllocation(long amount, String table)
        {
            verify("Allocate " + amount + " for " + table + " on " + Thread.currentThread());
        }

        synchronized void verify(Object event)
        {
            if (closed)
                return;

            events.add(event);

            if (events.size() == 1)
            {
                int cur = counter;
                while (cur == counter)
                {
                    try
                    {
                        wait();
                    }
                    catch (InterruptedException e)
                    {
                        throw new UncheckedInterruptedException(e);
                    }
                }
            }
            else
            {
                if (events.size() != 2)
                    throw new IllegalStateException();

                try
                {
                    Object event0 = events.get(0);
                    Object event1 = events.get(1);
                    if (event0 instanceof Pair)
                        event0 = ((Pair<?, ?>) event0).left;
                    if (event1 instanceof Pair)
                        event1 = ((Pair<?, ?>) event1).left;
                    String e0 = normalise(event0.toString());
                    String e1 = normalise(event1.toString());
                    if (!e0.equals(e1))
                        throw failWithOOM();
                }
                finally
                {
                    events.clear();
                    ++counter;
                    notifyAll();
                }
            }
        }

        public RandomSource get()
        {
            return new RandomSource.Abstract()
            {
                final RandomSource wrapped = new Default();

                @Override
                public float uniformFloat()
                {
                    return verify("uniformFloat:", wrapped.uniformFloat());
                }

                @Override
                public double uniformDouble()
                {
                    return verify("uniformDouble:", wrapped.uniformDouble());
                }

                @Override
                public void reset(long seed)
                {
                    wrapped.reset(seed);
                    verify("reset(" + seed + ')', "");
                }

                @Override
                public long reset()
                {
                    return verify("reset:", wrapped.reset());
                }

                @Override
                public int uniform(int min, int max)
                {
                    return verify("uniform(" + min + ',' + max + "):", wrapped.uniform(min, max));
                }

                @Override
                public long uniform(long min, long max)
                {
                    return verify("uniform(" + min + ',' + max + "):", wrapped.uniform(min, max));
                }

                private <T> T verify(String event, T result)
                {
                    Thread thread = Thread.currentThread();
                    if (!(thread instanceof InterceptibleThread) || !((InterceptibleThread) thread).isIntercepting())
                    {
                        if (!verifyUninterceptedRng)
                            return result;
                    }
                    InterceptReconciler.this.verify(withRngCallsites ? event + result + ' ' + Thread.currentThread() + ' '
                                                                       + new CaptureSites(Thread.currentThread())
                                                                         .toString(ste -> !ste.getClassName().startsWith(SelfReconcile.class.getName()))
                                                                     : event + result);
                    return result;
                }
            };
        }

        void close()
        {
            closed = true;
        }

        @Override
        public void accept(String kind, long value)
        {
            verify(Thread.currentThread() + ":" + kind + ':' + value);
        }
    }

    public static void reconcileWithSelf(long seed, RecordOption withRng, RecordOption withTime, boolean withAllocations, ClusterSimulation.Builder<?> builder)
    {
        logger.error("Seed 0x{}", Long.toHexString(seed));

        InterceptReconciler reconciler = new InterceptReconciler(withRng == WITH_CALLSITES);
        if (withRng != NONE) builder.random(reconciler);
        if (withTime != NONE) builder.timeListener(reconciler);

        HeapPool.Logged.Listener memoryListener = withAllocations ? reconciler::interceptAllocation : null;
        ExecutorService executor = ExecutorFactory.Global.executorFactory().pooled("Reconcile", 2);

        try (ClusterSimulation<?> cluster1 = builder.unique(0).memoryListener(memoryListener).create(seed);
             ClusterSimulation<?> cluster2 = builder.unique(1).memoryListener(memoryListener).create(seed))
        {
            try
            {

                InterceptibleThread.setDebugInterceptor(reconciler);
                reconciler.verifyUninterceptedRng = true;

                Future<?> f1 = executor.submit(() -> {
                    try (CloseableIterator<?> iter = cluster1.simulation.iterator())
                    {
                        while (iter.hasNext())
                        {
                            Object o = iter.next();
                            reconciler.verify(Pair.create(normalise(o.toString()), o));
                        }
                    }
                    reconciler.verify("done");
                });
                Future<?> f2 = executor.submit(() -> {
                    try (CloseableIterator<?> iter = cluster2.simulation.iterator())
                    {
                        while (iter.hasNext())
                        {
                            Object o = iter.next();
                            reconciler.verify(Pair.create(normalise(o.toString()), o));
                        }
                    }
                    reconciler.verify("done");
                });
                f1.get();
                f2.get();
            }
            finally
            {
                reconciler.close();
            }
        }
        catch (Throwable t)
        {
            t.printStackTrace();
            throw new RuntimeException("Failed on seed " + Long.toHexString(seed), t);
        }
    }

    private static String normalise(String input)
    {
        return NORMALISE_RECONCILE_THREAD.matcher(
            NORMALISE_THREAD.matcher(
                NORMALISE_LAMBDA.matcher(input).replaceAll("")
            ).replaceAll("$1$2]")
        ).replaceAll("$1]");
    }

}
