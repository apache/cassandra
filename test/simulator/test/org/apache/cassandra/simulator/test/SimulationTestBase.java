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

package org.apache.cassandra.simulator.test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.function.Predicate;

import com.google.common.collect.Iterators;

import org.apache.cassandra.concurrent.ExecutorFactory;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.impl.AbstractCluster;
import org.apache.cassandra.distributed.impl.IsolatedExecutor;
import org.apache.cassandra.distributed.shared.InstanceClassLoader;
import org.apache.cassandra.simulator.Action;
import org.apache.cassandra.simulator.ActionList;
import org.apache.cassandra.simulator.ActionPlan;
import org.apache.cassandra.simulator.ActionSchedule;
import org.apache.cassandra.simulator.ActionSchedule.Work;
import org.apache.cassandra.simulator.FutureActionScheduler;
import org.apache.cassandra.simulator.RunnableActionScheduler;
import org.apache.cassandra.simulator.ClusterSimulation;
import org.apache.cassandra.simulator.Debug;
import org.apache.cassandra.simulator.RandomSource;
import org.apache.cassandra.simulator.Simulation;
import org.apache.cassandra.simulator.SimulationRunner;
import org.apache.cassandra.simulator.asm.InterceptClasses;
import org.apache.cassandra.simulator.asm.NemesisFieldSelectors;
import org.apache.cassandra.simulator.systems.Failures;
import org.apache.cassandra.simulator.systems.InterceptibleThread;
import org.apache.cassandra.simulator.systems.InterceptingExecutorFactory;
import org.apache.cassandra.simulator.systems.InterceptingGlobalMethods;
import org.apache.cassandra.simulator.systems.InterceptorOfGlobalMethods;
import org.apache.cassandra.simulator.systems.SimulatedExecution;
import org.apache.cassandra.simulator.systems.SimulatedQuery;
import org.apache.cassandra.simulator.systems.SimulatedSystems;
import org.apache.cassandra.simulator.systems.SimulatedTime;
import org.apache.cassandra.simulator.utils.LongRange;
import org.apache.cassandra.utils.CloseableIterator;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.simulator.ActionSchedule.Mode.STREAM_LIMITED;
import static org.apache.cassandra.simulator.ActionSchedule.Mode.UNLIMITED;
import static org.apache.cassandra.simulator.ClusterSimulation.ISOLATE;
import static org.apache.cassandra.simulator.ClusterSimulation.SHARE;
import static org.apache.cassandra.simulator.SimulatorUtils.failWithOOM;
import static org.apache.cassandra.simulator.systems.InterceptedWait.CaptureSites.Capture.NONE;
import static org.apache.cassandra.simulator.utils.KindOfSequence.UNIFORM;
import static org.apache.cassandra.utils.Shared.Scope.ANY;
import static org.apache.cassandra.utils.Shared.Scope.SIMULATION;

public class SimulationTestBase
{
    static abstract class DTestClusterSimulation implements Simulation
    {
        final SimulatedSystems simulated;
        final RunnableActionScheduler scheduler;
        final Cluster cluster;

        public DTestClusterSimulation(SimulatedSystems simulated, RunnableActionScheduler scheduler, Cluster cluster)
        {
            this.simulated = simulated;
            this.scheduler = scheduler;
            this.cluster = cluster;
        }

        public Action executeQuery(int node, String query, ConsistencyLevel cl, Object... bindings)
        {
            return new SimulatedQuery(String.format("Execute query: %s %s %s", query, cl, Arrays.toString(bindings)),
                                      simulated,
                                      cluster.get(node),
                                      query,
                                      cl,
                                      null,
                                      bindings);
        }

        public Action schemaChange(int node, String query)
        {
            return new SimulatedQuery(String.format("Schema change: %s", query),
                                      simulated,
                                      cluster.get(node),
                                      query,
                                      org.apache.cassandra.distributed.api.ConsistencyLevel.ALL,
                                      null);
        }

        protected abstract ActionList initialize();

        protected abstract ActionList execute();

        public CloseableIterator<?> iterator()
        {
            return new ActionPlan(ActionList.of(initialize()),
                                  Collections.singletonList(execute()),
                                  ActionList.empty())
                   .iterator(STREAM_LIMITED, -1, () -> 0L, simulated.time, scheduler, simulated.futureScheduler);
        }

        public void run()
        {
            try (CloseableIterator<?> iter = iterator())
            {
                while (iter.hasNext())
                    iter.next();
            }
        }

        public void close() throws Exception
        {

        }
    }

    public static void simulate(Function<DTestClusterSimulation, ActionList> init,
                                Function<DTestClusterSimulation, ActionList> test,
                                Consumer<ClusterSimulation.Builder<DTestClusterSimulation>> configure) throws IOException
    {
        SimulationRunner.beforeAll();
        long seed = System.currentTimeMillis();
        RandomSource random = new RandomSource.Default();
        random.reset(seed);
        class Factory extends ClusterSimulation.Builder<DTestClusterSimulation>
        {
            public ClusterSimulation<DTestClusterSimulation> create(long seed) throws IOException
            {
                return new ClusterSimulation<>(random, seed, 1, this,
                                               (c) -> {},
                                               (simulated, scheduler, cluster, options) -> new DTestClusterSimulation(simulated, scheduler, cluster) {

                                                   protected ActionList initialize()
                                                   {
                                                       return init.apply(this);
                                                   }

                                                   protected ActionList execute()
                                                   {
                                                       return test.apply(this);
                                                   }
                                               });
            }
        }

        Factory factory = new Factory();
        configure.accept(factory);
        try (ClusterSimulation<?> cluster = factory.create(seed))
        {
            try
            {
                cluster.simulation.run();
            }
            catch (Throwable t)
            {
                throw new AssertionError(String.format("Failed on seed %s", Long.toHexString(seed)),
                                         t);
            }
        }
    }

    public static void simulate(IIsolatedExecutor.SerializableRunnable run,
                                IIsolatedExecutor.SerializableRunnable check)
    {
        simulate(new IIsolatedExecutor.SerializableRunnable[]{run},
                 check);
    }

    public static void simulate(IIsolatedExecutor.SerializableRunnable[] runnables,
                                IIsolatedExecutor.SerializableRunnable check)
    {
        Failures failures = new Failures();
        RandomSource random = new RandomSource.Default();
        long seed = System.currentTimeMillis();
        System.out.println("Using seed: " + seed);
        random.reset(seed);
        SimulatedTime time = new SimulatedTime(1, random, 1577836800000L /*Jan 1st UTC*/, new LongRange(1, 100, MILLISECONDS, NANOSECONDS),
                                               UNIFORM, UNIFORM.period(new LongRange(10L, 60L, SECONDS, NANOSECONDS), random), (i1, i2) -> {});
        SimulatedExecution execution = new SimulatedExecution();

        Predicate<String> sharedClassPredicate = AbstractCluster.getSharedClassPredicate(ISOLATE, SHARE, ANY, SIMULATION);
        InstanceClassLoader classLoader = new InstanceClassLoader(1, 1, AbstractCluster.CURRENT_VERSION.classpath,
                                                                  Thread.currentThread().getContextClassLoader(),
                                                                  sharedClassPredicate,
                                                                  new InterceptClasses(() -> 1.0f, () -> 1.0f, NemesisFieldSelectors.get(), ClassLoader.getSystemClassLoader(), sharedClassPredicate.negate())::apply);

        ThreadGroup tg = new ThreadGroup("test");
        InterceptorOfGlobalMethods interceptorOfGlobalMethods = new InterceptingGlobalMethods(NONE, null, failures, random);
        InterceptingExecutorFactory factory = execution.factory(interceptorOfGlobalMethods, classLoader, tg);

        time.setup(1, classLoader);
        IsolatedExecutor.transferAdhoc((IIsolatedExecutor.SerializableConsumer<ExecutorFactory>) ExecutorFactory.Global::unsafeSet, classLoader)
                        .accept(factory);

        IsolatedExecutor.transferAdhoc((IIsolatedExecutor.SerializableBiConsumer<InterceptorOfGlobalMethods, IntSupplier>) InterceptorOfGlobalMethods.Global::unsafeSet, classLoader)
                        .accept(interceptorOfGlobalMethods, () -> {
                            if (InterceptibleThread.isDeterministic())
                                throw failWithOOM();
                            return random.uniform(Integer.MIN_VALUE, Integer.MAX_VALUE);
                        });

        SimulatedSystems simulated = new SimulatedSystems(random, time, null, execution, null, null, null, new FutureActionScheduler()
        {
            @Override
            public Deliver shouldDeliver(int from, int to)
            {
                return Deliver.DELIVER;
            }

            @Override
            public long messageDeadlineNanos(int from, int to)
            {
                return 0;
            }

            @Override
            public long messageTimeoutNanos(long expiresAfterNanos, long expirationIntervalNanos)
            {
                return 0;
            }

            @Override
            public long messageFailureNanos(int from, int to)
            {
                return 0;
            }

            @Override
            public long schedulerDelayNanos()
            {
                return 0;
            }
        }, new Debug(), failures);

        RunnableActionScheduler runnableScheduler = new RunnableActionScheduler.RandomUniform(random);

        Action entrypoint = new Action("entrypoint", Action.Modifiers.NONE, Action.Modifiers.NONE)
        {
            protected ActionList performSimple()
            {
                Action[] actions = new Action[runnables.length];
                for (int i = 0; i < runnables.length; i++)
                    actions[i] = toAction(runnables[i], classLoader, factory, simulated);

                return ActionList.of(actions);
            }
        };


        ActionSchedule testSchedule = new ActionSchedule(simulated.time, simulated.futureScheduler, () -> 0, runnableScheduler, new Work(UNLIMITED, Collections.singletonList(ActionList.of(entrypoint))));
        Iterators.advance(testSchedule, Integer.MAX_VALUE);
        ActionSchedule checkSchedule = new ActionSchedule(simulated.time, simulated.futureScheduler, () -> 0, runnableScheduler, new Work(UNLIMITED, Collections.singletonList(ActionList.of(toAction(check, classLoader, factory, simulated)))));
        Iterators.advance(checkSchedule, Integer.MAX_VALUE);
    }

    public static Action toAction(IIsolatedExecutor.SerializableRunnable r, ClassLoader classLoader, InterceptingExecutorFactory factory, SimulatedSystems simulated)
    {
        Runnable runnable = IsolatedExecutor.transferAdhoc(r, classLoader);
        return simulated.invoke("action", Action.Modifiers.NONE, Action.Modifiers.NONE,
                                factory.startParked("begin", runnable));
    }

    public static <T> T[] arr(T... arr)
    {
        return arr;
    }
}