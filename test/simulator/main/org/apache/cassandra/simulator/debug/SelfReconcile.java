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
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IMessage;
import org.apache.cassandra.simulator.ClusterSimulation;
import org.apache.cassandra.simulator.RandomSource;
import org.apache.cassandra.simulator.systems.InterceptedExecution;
import org.apache.cassandra.simulator.systems.InterceptedWait;
import org.apache.cassandra.simulator.systems.InterceptedWait.CaptureSites;
import org.apache.cassandra.simulator.systems.InterceptingExecutor;
import org.apache.cassandra.simulator.systems.InterceptibleThread;
import org.apache.cassandra.simulator.systems.InterceptorOfConsequences;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static org.apache.cassandra.simulator.SimulatorUtils.failWithOOM;
import static org.apache.cassandra.simulator.debug.Reconcile.NORMALISE_LAMBDA;
import static org.apache.cassandra.simulator.debug.Reconcile.NORMALISE_THREAD;
import static org.apache.cassandra.simulator.debug.Reconcile.failWithHeapDump;

/**
 * Simulator runs should be deterministic, so run two in parallel and see if they produce the same event stream
 */
public class SelfReconcile
{
    private static final Logger logger = LoggerFactory.getLogger(SelfReconcile.class);

    static class InterceptReconciler implements InterceptorOfConsequences, Supplier<RandomSource>
    {
        final List<Object> events = new ArrayList<>();
        final boolean withRngCallsites;
        int counter = 0;
        boolean failOnUninterceptedRng;

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
        public synchronized void interceptWakeup(InterceptedWait wakeup, InterceptorOfConsequences waitWasInterceptedBy)
        {
            verify(normalise("Wakeup " + wakeup.waiting() + wakeup));
        }

        @Override
        public synchronized void interceptExecution(InterceptedExecution invoke, InterceptingExecutor executor)
        {
            verify("Execute " + normalise(invoke.toString()) + " on " + executor);
        }

        @Override
        public synchronized void interceptWait(InterceptedWait wakeupWith)
        {
            verify(normalise("Wait " + wakeupWith.waiting() + wakeupWith));
        }

        @Override
        public synchronized void interceptTermination()
        {
            verify("Terminate " + Thread.currentThread());
        }

        synchronized void verify(String event)
        {
            events.add(event);

            try
            {
                int cur = counter;
                while (events.size() == 1 && cur == counter)
                    wait();

                if (events.size() == 0 || cur != counter)
                    return;

                if (events.size() != 2)
                    throw new IllegalStateException();

                if (!events.get(0).toString().equals(events.get(1)))
                    failWithOOM();

                ++counter;
                events.clear();
            }
            catch (InterruptedException e)
            {
                throw new UncheckedInterruptedException(e);
            }
            finally
            {
                notifyAll();
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
                        if (failOnUninterceptedRng)
                            failWithOOM();
                        return result;
                    }
                    InterceptReconciler.this.verify(withRngCallsites ? event + result + ' ' + new CaptureSites(Thread.currentThread(), false) : event);
                    return result;
                }
            };
        }
    }

    public static void reconcileWithSelf(long seed, boolean withRng, boolean withRngCallSites, ClusterSimulation.Builder<?> builder)
    {
        logger.error("Seed 0x{}", Long.toHexString(seed));

        InterceptReconciler reconciler = new InterceptReconciler(withRngCallSites);
        if (withRng) builder.random(reconciler);

        Executor executor = Executors.newFixedThreadPool(2);
        try (ClusterSimulation<?> cluster1 = builder.unique(0).create(seed);
             ClusterSimulation<?> cluster2 = builder.unique(1).create(seed))
        {
            InterceptibleThread.setDebugInterceptor(reconciler);
            Iterable<?> iter1 = cluster1.simulation.iterable();
            Iterable<?> iter2 = cluster2.simulation.iterable();
            reconciler.failOnUninterceptedRng = true;

            final Object done = new Object();
            SynchronousQueue<Object> check1 = new SynchronousQueue<>();
            SynchronousQueue<Object> check2 = new SynchronousQueue<>();
            executor.execute(() -> {
                for (Object o : iter1)
                {
                    try
                    {
                        check1.put(Pair.create(o, o.toString()));
                    }
                    catch (InterruptedException e)
                    {
                        throw new UncheckedInterruptedException(e);
                    }
                }
            });
            executor.execute(() -> {
                for (Object o : iter2)
                {
                    try
                    {
                        check2.put(Pair.create(o, o.toString()));
                    }
                    catch (InterruptedException e)
                    {
                        throw new UncheckedInterruptedException(e);
                    }
                }
            });

            Object next1, next2 = new Object();
            while (done != (next1 = check1.take()) && done != (next2 = check2.take()))
            {
                String normalised1 = normalise(((Pair)next1).right.toString());
                String normalised2 = normalise(((Pair)next2).right.toString());
                if (!normalised1.equals(normalised2))
                    failWithHeapDump(-1, normalised1, normalised2);
            }
            if (next1 != next2)
                failWithHeapDump(-1, next1, next2);
        }
        catch (Throwable t)
        {
            t.printStackTrace();
            throw new RuntimeException("Failed on seed " + Long.toHexString(seed), t);
        }
    }

    private static String normalise(String input)
    {
        return NORMALISE_THREAD.matcher(
            NORMALISE_LAMBDA.matcher(input).replaceAll("")
        ).replaceAll("$1$2]");
    }


}
