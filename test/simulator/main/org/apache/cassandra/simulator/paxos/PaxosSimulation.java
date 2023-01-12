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

package org.apache.cassandra.simulator.paxos;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;
import java.util.stream.Stream;
import javax.annotation.Nullable;

import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ExecutorFactory;
import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.service.paxos.BallotGenerator;
import org.apache.cassandra.simulator.ActionList;
import org.apache.cassandra.simulator.ActionPlan;
import org.apache.cassandra.simulator.ActionSchedule;
import org.apache.cassandra.simulator.RunnableActionScheduler;
import org.apache.cassandra.simulator.Simulation;
import org.apache.cassandra.simulator.cluster.ClusterActionListener;
import org.apache.cassandra.simulator.systems.InterceptorOfGlobalMethods;
import org.apache.cassandra.simulator.systems.SimulatedActionCallable;
import org.apache.cassandra.simulator.systems.SimulatedSystems;
import org.apache.cassandra.utils.AssertionUtils;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.concurrent.Threads;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static org.apache.cassandra.simulator.Action.Modifiers.DISPLAY_ORIGIN;
import static org.apache.cassandra.simulator.Action.Modifiers.NONE;
import static org.apache.cassandra.simulator.SimulatorUtils.failWithOOM;
import static org.apache.cassandra.simulator.paxos.HistoryChecker.causedBy;
import static org.apache.cassandra.utils.AssertionUtils.anyOf;
import static org.apache.cassandra.utils.AssertionUtils.hasCause;
import static org.apache.cassandra.utils.AssertionUtils.isThrowableInstanceof;

public abstract class PaxosSimulation implements Simulation, ClusterActionListener
{
    private static final Logger logger = LoggerFactory.getLogger(PaxosSimulation.class);

    private static String createDescription(int[] primaryKeys, int id, String idString)
    {
        return primaryKeys.length == 1 ? Integer.toString(primaryKeys[0]) : Arrays.toString(primaryKeys) + "/" + id + ": " + idString;
    }

    protected Class<? extends Throwable>[] expectedExceptions()
    {
        return (Class<? extends Throwable>[]) new Class<?>[] { RequestExecutionException.class };
    }

    abstract class Operation extends SimulatedActionCallable<SimpleQueryResult> implements BiConsumer<SimpleQueryResult, Throwable>
    {
        final int[] primaryKeys;
        final int id;
        int start;

        public Operation(int[] primaryKeys, int id, IInvokableInstance instance,
                         String idString, IIsolatedExecutor.SerializableCallable<SimpleQueryResult> query)
        {
            super(createDescription(primaryKeys, id, idString), DISPLAY_ORIGIN, NONE, PaxosSimulation.this.simulated, instance, query);
            this.primaryKeys = primaryKeys;
            this.id = id;
        }

        public ActionList performAndRegister()
        {
            start = logicalClock.incrementAndGet();
            return super.performAndRegister();
        }

        @Override
        public void accept(SimpleQueryResult success, Throwable failure)
        {
            if (failure != null && !expectedException(failure))
            {
                if (!simulated.failures.hasFailure() || !(failure instanceof UncheckedInterruptedException))
                    logger.error("Unexpected exception", failure);
                simulated.failures.accept(failure);
                return;
            }
            else if (failure != null)
            {
                logger.trace("{}", failure.getMessage());
            }
            verify(new Observation(id, success, start, logicalClock.incrementAndGet()));
        }

        protected boolean expectedException(Throwable failure)
        {
            // due to class loaders can't use instanceOf directly
            return hasCause(anyOf(Stream.of(expectedExceptions()).map(AssertionUtils::isThrowableInstanceof))).matches(failure);
        }
        abstract void verify(Observation outcome);
    }

    final Cluster cluster;
    final SimulatedSystems simulated;
    final RunnableActionScheduler runnableScheduler;
    final AtomicInteger logicalClock = new AtomicInteger(1);
    final ActionSchedule.Mode mode;
    final long runForNanos;
    final LongSupplier jitter;

    public PaxosSimulation(ActionSchedule.Mode mode, SimulatedSystems simulated, Cluster cluster, RunnableActionScheduler runnableScheduler, long runForNanos, LongSupplier jitter)
    {
        this.cluster = cluster;
        this.simulated = simulated;
        this.runnableScheduler = runnableScheduler;
        this.runForNanos = runForNanos;
        this.mode = mode;
        this.jitter = jitter;
    }

    protected abstract ActionPlan plan();

    public void run()
    {
        AtomicReference<CloseableIterator<?>> onFailedShutdown = new AtomicReference<>();
        AtomicInteger shutdown = new AtomicInteger();

        AtomicLong counter = new AtomicLong();
        ScheduledExecutorPlus livenessChecker = null;
        ScheduledFuture<?> liveness = null;
        if (CassandraRelevantProperties.TEST_SIMULATOR_LIVENESS_CHECK.getBoolean())
        {
            livenessChecker = ExecutorFactory.Global.executorFactory().scheduled("SimulationLiveness");
            liveness = livenessChecker.scheduleWithFixedDelay(new Runnable()
            {
                long prev = 0;
                @Override
                public void run()
                {
                    Thread.currentThread().setUncaughtExceptionHandler((th, ex) -> {
                        logger.error("Unexpected exception on {}", th, ex);
                    });
                    if (shutdown.get() > 0)
                    {
                        int attempts = shutdown.getAndIncrement();
                        if (attempts > 2 || onFailedShutdown.get() == null)
                        {
                            logger.error("Failed to exit despite best efforts, dumping threads and forcing shutdown");
                            for (Map.Entry<Thread, StackTraceElement[]> stes : Thread.getAllStackTraces().entrySet())
                            {
                                logger.error("{}", stes.getKey());
                                logger.error("{}", Threads.prettyPrint(stes.getValue(), false, "\n"));
                            }

                            System.exit(1);
                        }
                        else if (attempts > 1)
                        {
                            logger.error("Failed to exit cleanly, force closing simulation");
                            onFailedShutdown.get().close();
                        }
                    }
                    else
                    {
                        long cur = counter.get();
                        if (cur == prev)
                        {
                            logger.error("Simulation appears to have stalled; terminating. To disable set -Dcassandra.test.simulator.livenesscheck=false");
                            shutdown.set(1);
                            throw failWithOOM();
                        }
                        prev = cur;
                    }
                }
            }, 5L, 5L, TimeUnit.MINUTES);
        }

        try (CloseableIterator<?> iter = iterator())
        {
            onFailedShutdown.set(iter);
            while (iter.hasNext())
            {
                if (shutdown.get() > 0)
                    throw failWithOOM();

                iter.next();
                counter.incrementAndGet();
            }
        }

        // only cancel if successfully shutdown; otherwise we may have a shutdown liveness issue, and should kill process
        if (liveness != null)
            liveness.cancel(true);
        if (livenessChecker != null)
            livenessChecker.shutdownNow();
    }

    public CloseableIterator<?> iterator()
    {
        CloseableIterator<?> iterator = plan().iterator(mode, runForNanos, jitter, simulated.time, runnableScheduler, simulated.futureScheduler);
        return new CloseableIterator<Object>()
        {
            @Override
            public boolean hasNext()
            {
                return !isDone() && iterator.hasNext();
            }

            @Override
            public Object next()
            {
                try
                {
                    return iterator.next();
                }
                catch (Throwable t)
                {
                    throw failWith(t);
                }
            }

            @Override
            public void close()
            {
                iterator.close();
            }
        };
    }

    boolean isDone()
    {
        if (!simulated.failures.hasFailure())
            return false;

        throw logAndThrow();
    }

    RuntimeException failWith(Throwable t)
    {
        simulated.failures.onFailure(t);
        throw logAndThrow();
    }

    abstract void log(@Nullable Integer primaryKey);

    private RuntimeException logAndThrow()
    {
        Integer causedByPrimaryKey = null;
        Throwable causedByThrowable = null;
        for (Throwable t : simulated.failures.get())
        {
            if (null != (causedByPrimaryKey = causedBy(t)))
            {
                causedByThrowable = t;
                break;
            }
        }

        log(causedByPrimaryKey);
        if (causedByPrimaryKey != null) throw Throwables.propagate(causedByThrowable);
        else throw Throwables.propagate(simulated.failures.get().get(0));
    }

    public void close()
    {
        // stop intercepting message delivery
        cluster.setMessageSink(null);
        cluster.forEach(i -> {
            if (!i.isShutdown())
            {
                i.unsafeRunOnThisThread(() -> BallotGenerator.Global.unsafeSet(new BallotGenerator.Default()));
                i.unsafeRunOnThisThread(InterceptorOfGlobalMethods.Global::unsafeReset);
            }
        });
    }
}
