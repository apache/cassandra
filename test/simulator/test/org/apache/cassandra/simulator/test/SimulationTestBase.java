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
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.function.Predicate;

import javax.annotation.Nullable;

import com.google.common.collect.Iterators;
import org.junit.BeforeClass;

import org.apache.cassandra.concurrent.ExecutorFactory;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.api.IMessage;
import org.apache.cassandra.distributed.impl.AbstractCluster;
import org.apache.cassandra.distributed.impl.IsolatedExecutor;
import org.apache.cassandra.distributed.shared.InstanceClassLoader;
import org.apache.cassandra.simulator.AbstractSimulation;
import org.apache.cassandra.simulator.Action;
import org.apache.cassandra.simulator.ActionList;
import org.apache.cassandra.simulator.ActionPlan;
import org.apache.cassandra.simulator.ActionSchedule;
import org.apache.cassandra.simulator.ActionSchedule.Work;
import org.apache.cassandra.simulator.ClusterSimulation;
import org.apache.cassandra.simulator.Debug;
import org.apache.cassandra.simulator.FutureActionScheduler;
import org.apache.cassandra.simulator.RandomSource;
import org.apache.cassandra.simulator.RunnableActionScheduler;
import org.apache.cassandra.simulator.Simulation;
import org.apache.cassandra.simulator.SimulationException;
import org.apache.cassandra.simulator.SimulationRunner;
import org.apache.cassandra.simulator.asm.InterceptClasses;
import org.apache.cassandra.simulator.asm.NemesisFieldSelectors;
import org.apache.cassandra.simulator.cluster.ClusterActions;
import org.apache.cassandra.simulator.systems.Failures;
import org.apache.cassandra.simulator.systems.InterceptedWait;
import org.apache.cassandra.simulator.systems.InterceptibleThread;
import org.apache.cassandra.simulator.systems.InterceptingExecutorFactory;
import org.apache.cassandra.simulator.systems.InterceptingGlobalMethods;
import org.apache.cassandra.simulator.systems.InterceptorOfGlobalMethods;
import org.apache.cassandra.simulator.systems.SimulatedExecution;
import org.apache.cassandra.simulator.systems.SimulatedQuery;
import org.apache.cassandra.simulator.systems.SimulatedSystems;
import org.apache.cassandra.simulator.systems.SimulatedTime;
import org.apache.cassandra.simulator.utils.LongRange;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.CloseableIterator;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.config.CassandraRelevantProperties.CLOCK_GLOBAL;
import static org.apache.cassandra.config.CassandraRelevantProperties.CLOCK_MONOTONIC_APPROX;
import static org.apache.cassandra.config.CassandraRelevantProperties.CLOCK_MONOTONIC_PRECISE;
import static org.apache.cassandra.simulator.ActionSchedule.Mode.TIME_LIMITED;
import static org.apache.cassandra.simulator.ActionSchedule.Mode.UNLIMITED;
import static org.apache.cassandra.simulator.ClusterSimulation.ISOLATE;
import static org.apache.cassandra.simulator.ClusterSimulation.SHARE;
import static org.apache.cassandra.simulator.SimulatorUtils.failWithOOM;
import static org.apache.cassandra.simulator.cluster.ClusterActions.InitialConfiguration.initializeAll;
import static org.apache.cassandra.simulator.utils.KindOfSequence.UNIFORM;
import static org.apache.cassandra.utils.Shared.Scope.ANY;
import static org.apache.cassandra.utils.Shared.Scope.SIMULATION;

public class SimulationTestBase
{
    @BeforeClass
    public static void beforeAll()
    {
        // Disallow time on the bootstrap classloader
        for (CassandraRelevantProperties property : Arrays.asList(CLOCK_GLOBAL, CLOCK_MONOTONIC_APPROX, CLOCK_MONOTONIC_PRECISE))
            property.setString("org.apache.cassandra.simulator.systems.SimulatedTime$Delegating");
        try { Clock.Global.nanoTime(); } catch (IllegalStateException e) {} // make sure static initializer gets called
    }

    // Don't use loggers before invoking simulator it messes up initialization order
//    private static final Logger logger = LoggerFactory.getLogger(Logger.class);


    static abstract class SimpleSimulation extends AbstractSimulation
    {
        protected SimpleSimulation(SimulatedSystems simulated, RunnableActionScheduler scheduler, Cluster cluster)
        {
            super(simulated, scheduler, cluster);
        }

        protected SimpleSimulation(SimulatedSystems simulated, RunnableActionScheduler scheduler, Cluster cluster, ClusterActions.Options options)
        {
            super(simulated, scheduler, cluster, options);
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
            return schemaChange(node, query, null);
        }

        public Action schemaChange(int node, String query, @Nullable Predicate<Throwable> onFailure)
        {
            return new SimulatedQuery(String.format("Schema change: %s", query),
                                      simulated,
                                      cluster.get(node),
                                      query,
                                      org.apache.cassandra.distributed.api.ConsistencyLevel.ALL,
                                      null,
                                      onFailure);
        }

        protected ActionList initialize()
        {
            return ActionList.of(clusterActions.initializeCluster(initializeAll(cluster.size())));
        }
        protected ActionList teardown()
        {
            return ActionList.of();
        }
        protected abstract ActionList execute();

        protected ActionSchedule.Mode mode()
        {
            return TIME_LIMITED;
        }

        protected long runForNanos()
        {
            return MINUTES.toNanos(10);
        }

        @Override
        public CloseableIterator<?> iterator()
        {
            return ActionPlan.setUpTearDown(initialize(),
                                            teardown())
                             .encapsulate(ActionPlan.interleave(Collections.singletonList(execute())))
                             .iterator(mode(), runForNanos(), () -> 0L, simulated.time, scheduler, simulated.futureScheduler);
        }
    }

    static abstract class BasicSimulationBuilder<S extends Simulation> extends ClusterSimulation.Builder<S>
    {
        abstract S create(SimulatedSystems simulated, RunnableActionScheduler scheduler, Cluster cluster, ClusterActions.Options options);

        protected void updateConfig(IInstanceConfig config)
        {

        }

        public ClusterSimulation<S> create(long seed) throws IOException
        {
            RandomSource random = new RandomSource.Default();
            random.reset(seed);

            return new ClusterSimulation<>(random, seed, 1, this,
                                           this::updateConfig,
                                           this::create);
        }
    }

    static class DTestClusterSimulationBuilder extends ClusterSimulation.Builder<SimpleSimulation>
    {
        protected final Function<SimpleSimulation, ActionList> init;
        protected final Function<SimpleSimulation, ActionList> test;
        protected final Function<SimpleSimulation, ActionList> teardown;
        protected final BiConsumer<RandomSource, IInstanceConfig> configUpdater;

        DTestClusterSimulationBuilder(Function<SimpleSimulation, ActionList> init,
                                      Function<SimpleSimulation, ActionList> test,
                                      Function<SimpleSimulation, ActionList> teardown,
                                      BiConsumer<RandomSource, IInstanceConfig> configUpdater)
        {
            this.init = init;
            this.test = test;
            this.teardown = teardown;
            this.configUpdater = configUpdater;
        }

        public ClusterSimulation<SimpleSimulation> create(long seed) throws IOException
        {
            RandomSource random = new RandomSource.Default();
            random.reset(seed);

            return new ClusterSimulation<>(random, seed, 1, this,
                                           c -> configUpdater.accept(random, c),
                                           (simulated, scheduler, cluster, options) -> new SimpleSimulation(simulated, scheduler, cluster)
                                           {
                                               protected ActionList initialize()
                                               {
                                                   return init.apply(this);
                                               }

                                               protected ActionList teardown()
                                               {
                                                   return teardown.apply(this);
                                               }

                                               protected ActionList execute()
                                               {
                                                   return test.apply(this);
                                               }
                                           });
        }
    }

    static void simulate(Function<SimpleSimulation, ActionList> init,
                         Function<SimpleSimulation, ActionList> test,
                         Function<SimpleSimulation, ActionList> teardown,
                         Consumer<ClusterSimulation.Builder<SimpleSimulation>> configure) throws IOException
    {
        simulate(init, test, teardown, configure, (i1, i2) -> {});
    }

    @SuppressWarnings("unused")
    static void simulate(long seed,
                         Function<SimpleSimulation, ActionList> init,
                         Function<SimpleSimulation, ActionList> test,
                         Function<SimpleSimulation, ActionList> teardown,
                         Consumer<ClusterSimulation.Builder<SimpleSimulation>> configure) throws IOException
    {
        simulate(seed, init, test, teardown, configure, (i1, i2) -> {});
    }

    static void simulate(Function<SimpleSimulation, ActionList> init,
                         Function<SimpleSimulation, ActionList> test,
                         Function<SimpleSimulation, ActionList> teardown,
                         Consumer<ClusterSimulation.Builder<SimpleSimulation>> configure,
                         BiConsumer<RandomSource, IInstanceConfig> configUpdater) throws IOException
    {
        simulate(new DTestClusterSimulationBuilder(init, test, teardown, configUpdater),
                 configure);
    }

    @SuppressWarnings("unused")
    static void simulate(long seed,
                         Function<SimpleSimulation, ActionList> init,
                         Function<SimpleSimulation, ActionList> test,
                         Function<SimpleSimulation, ActionList> teardown,
                         Consumer<ClusterSimulation.Builder<SimpleSimulation>> configure,
                         BiConsumer<RandomSource, IInstanceConfig> configUpdater) throws IOException
    {
        simulate(() -> seed, new DTestClusterSimulationBuilder(init, test, teardown, configUpdater),
                configure);
    }

    public static <T extends Simulation> void simulate(ClusterSimulation.Builder<T> factory,
                                                       Consumer<ClusterSimulation.Builder<T>> configure) throws IOException
    {
        simulate(System::currentTimeMillis, factory, configure);
    }

    public static <T extends Simulation> void simulate(long seed, ClusterSimulation.Builder<T> factory) throws IOException
    {
        simulate(() -> seed, factory, i ->{});
    }

    public static <T extends Simulation> void simulate(ClusterSimulation.SimulationFactory<T> factory) throws IOException
    {
        simulate(System.currentTimeMillis(), factory);
    }

    public static <T extends Simulation> void simulate(long seed, ClusterSimulation.SimulationFactory<T> factory) throws IOException
    {
        simulate(seed, factory, b -> {});
    }

    public static <T extends Simulation> void simulate(ClusterSimulation.SimulationFactory<T> factory, Consumer<ClusterSimulation.Builder<T>> configure) throws IOException
    {
        simulate(System.currentTimeMillis(), factory, configure);
    }

    public static <T extends Simulation> void simulate(long seed, ClusterSimulation.SimulationFactory<T> factory, Consumer<ClusterSimulation.Builder<T>> configure) throws IOException
    {
        BasicSimulationBuilder<T> builder = new BasicSimulationBuilder<>()
        {
            @Override
            T create(SimulatedSystems simulated, RunnableActionScheduler scheduler, Cluster cluster, ClusterActions.Options options)
            {
                return factory.create(simulated, scheduler, cluster, options);
            }
        };
        simulate(() -> seed, builder, configure);
    }

    public static <T extends Simulation> void simulate(LongSupplier seedGen,
                                                       ClusterSimulation.Builder<T> factory,
                                                       Consumer<ClusterSimulation.Builder<T>> configure) throws IOException
    {
        SimulationRunner.beforeAll();
        long seed = seedGen.getAsLong();
        // Development seed:
        //long seed = 1687184561194L;
        System.out.printf("Simulation seed: %dL%n", seed);
        configure.accept(factory);
        try (ClusterSimulation<?> cluster = factory.create(seed))
        {
            try (Simulation simulation = cluster.simulation())
            {
                simulation.run();
            }
            catch (Throwable t)
            {
                throw new SimulationException(seed, t);
            }
        }
        catch (Throwable t)
        {
            if (t instanceof SimulationException)
                throw t;
            throw new SimulationException(seed, t);
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
        simulate(runnables, check, System.currentTimeMillis());
    }

    public static void simulate(IIsolatedExecutor.SerializableRunnable[] runnables,
                                IIsolatedExecutor.SerializableRunnable check,
                                long seed)
    {
        Failures failures = new HarrySimulatorTest.HaltOnError();
        RandomSource random = new RandomSource.Default();
        System.out.println("Using seed: " + seed);
        random.reset(seed);
        SimulatedTime time = new SimulatedTime(1, random, 1577836800000L /*Jan 1st UTC*/, new LongRange(1, 100, MILLISECONDS, NANOSECONDS),
                                               UNIFORM, UNIFORM.period(new LongRange(10L, 60L, SECONDS, NANOSECONDS), random), (i1, i2) -> {});
        SimulatedExecution execution = new SimulatedExecution();

        Predicate<String> sharedClassPredicate = AbstractCluster.getSharedClassPredicate(ISOLATE, SHARE, ANY, SIMULATION);
        InstanceClassLoader classLoader = new InstanceClassLoader(1, 1, AbstractCluster.CURRENT_VERSION.classpath,
                                                                  Thread.currentThread().getContextClassLoader(),
                                                                  sharedClassPredicate,
                                                                  new InterceptClasses((x) -> () -> 1.0f, (x) -> () -> 1.0f,
                                                                                       NemesisFieldSelectors.get(),
                                                                                       ClassLoader.getSystemClassLoader(),
                                                                                       sharedClassPredicate.negate())::apply);

        ThreadGroup tg = new ThreadGroup("test");
        InterceptedWait.CaptureSites.Capture capture = new InterceptedWait.CaptureSites.Capture(false, false, false);
        InterceptorOfGlobalMethods interceptorOfGlobalMethods = IsolatedExecutor.transferAdhoc((IIsolatedExecutor.SerializableQuadFunction<InterceptedWait.CaptureSites.Capture, LongConsumer, Consumer<Throwable>, RandomSource, InterceptorOfGlobalMethods>) InterceptingGlobalMethods::new, classLoader)
                                                                                .apply(capture, (ignore) -> {}, failures, random);

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
            public DeliverResult shouldDeliver(int from, int to, IInvokableInstance invoker, IMessage message)
            {
                return DELIVER_UNPROTECTED_RESULT;
            }

            @Override
            public long messageDeadlineNanos(int from, int to, boolean protectedMessage)
            {
                return 0;
            }

            @Override
            public long messageTimeoutNanos(long expiresAfterNanos, long expirationIntervalNanos, boolean protectedMessage)
            {
                return 0;
            }

            @Override
            public long messageFailureNanos(int from, int to, boolean protectedMessage)
            {
                return 0;
            }

            @Override
            public long schedulerDelayNanos()
            {
                return 0;
            }
        }, Collections.emptyMap(), new Debug(), failures);

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
        if (failures.hasFailure())
        {
            AssertionError error = new AssertionError(String.format("Unexpected errors for seed %d", seed));
            for (Throwable t : failures.get())
                error.addSuppressed(t);
            throw error;
        }

        ActionSchedule checkSchedule = new ActionSchedule(simulated.time, simulated.futureScheduler, () -> 0, runnableScheduler, new Work(UNLIMITED, Collections.singletonList(ActionList.of(toAction(check, classLoader, factory, simulated)))));
        Iterators.advance(checkSchedule, Integer.MAX_VALUE);
        if (failures.hasFailure())
        {
            AssertionError error = new AssertionError(String.format("Unexpected errors for seed %d", seed));
            for (Throwable t : failures.get())
                error.addSuppressed(t);
            throw error;
        }
    }

    public static Action toAction(IIsolatedExecutor.SerializableRunnable r, ClassLoader classLoader, InterceptingExecutorFactory factory, SimulatedSystems simulated)
    {
        Runnable runnable = IsolatedExecutor.transferAdhoc(r, classLoader);
        return simulated.invoke("action", Action.Modifiers.NONE, Action.Modifiers.NONE,
                                factory.startParked("begin", runnable));
    }

    @SafeVarargs
    public static <T> T[] arr(T... arr)
    {
        return arr;
    }

    public static int[] arr(int... arr)
    {
        return arr;
    }
}