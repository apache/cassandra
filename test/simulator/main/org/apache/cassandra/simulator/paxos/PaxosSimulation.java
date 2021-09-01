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

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import javax.annotation.Nullable;

import com.google.common.base.Throwables;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.service.paxos.BallotGenerator;
import org.apache.cassandra.simulator.ActionList;
import org.apache.cassandra.simulator.ActionPlan;
import org.apache.cassandra.simulator.ActionSchedulerSequential;
import org.apache.cassandra.simulator.ActionSchedulers;
import org.apache.cassandra.simulator.Simulation;
import org.apache.cassandra.simulator.cluster.ClusterActionListener;
import org.apache.cassandra.simulator.systems.InterceptorOfGlobalMethods;
import org.apache.cassandra.simulator.systems.SimulatedQuery;
import org.apache.cassandra.simulator.systems.SimulatedSystems;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static org.apache.cassandra.simulator.Action.Modifiers.NONE;
import static org.apache.cassandra.simulator.Action.Modifiers.RELIABLE_NO_TIMEOUTS;
import static org.apache.cassandra.simulator.paxos.HistoryChecker.causedBy;

public abstract class PaxosSimulation implements Simulation, ClusterActionListener
{
    private static final Logger logger = LoggerFactory.getLogger(PaxosSimulation.class);

    abstract class Operation extends SimulatedQuery implements BiConsumer<Object[][], Throwable>
    {
        final int primaryKey;
        final int id;
        int start;

        public Operation(int primaryKey, int id, IInvokableInstance instance,
                         String idString, String query, ConsistencyLevel commitConsistency, ConsistencyLevel serialConistency, Object... params)
        {
            super(primaryKey + "/" + id + ": " + idString, RELIABLE_NO_TIMEOUTS, NONE, PaxosSimulation.this.simulated, instance, query, commitConsistency, serialConistency, params);
            this.primaryKey = primaryKey;
            this.id = id;
        }

        public ActionList perform(boolean perform)
        {
            start = logicalClock.incrementAndGet();
            return super.perform(perform);
        }

        @Override
        public void accept(Object[][] success, Throwable failure)
        {
            if (failure != null && !(failure instanceof RequestExecutionException))
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

        abstract void verify(Observation outcome);
    }

    final Cluster cluster;
    final SimulatedSystems simulated;
    final ActionSchedulers schedulers;
    final AtomicInteger logicalClock = new AtomicInteger(1);

    public PaxosSimulation(SimulatedSystems simulated, Cluster cluster, ActionSchedulers schedulers)
    {
        this.cluster = cluster;
        this.simulated = simulated;
        this.schedulers = schedulers;
    }

    protected abstract ActionPlan plan();

    public void run()
    {
        //noinspection StatementWithEmptyBody
        for (Object ignore : iterable()) {}
    }

    public Iterable<?> iterable()
    {
        Iterator<?> iterator = plan().iterable(simulated.time, new ActionSchedulerSequential(), schedulers).iterator();
        return (Iterable<Object>) () -> new Iterator<Object>()
        {
            public boolean hasNext()
            {
                return !isDone() && iterator.hasNext();
            }

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
        cluster.forEach(i -> i.unsafeRunOnThisThread(() -> BallotGenerator.Global.unsafeSet(new BallotGenerator.Default())));
        cluster.forEach(i -> i.unsafeRunOnThisThread(InterceptorOfGlobalMethods.Global::unsafeReset));
    }
}
