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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.harry.core.Run;
import org.apache.cassandra.harry.operations.CompiledStatement;
import org.apache.cassandra.harry.tracker.DataTracker;
import org.apache.cassandra.harry.visitors.MutatingRowVisitor;
import org.apache.cassandra.harry.visitors.VisitExecutor;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.impl.Query;
import org.apache.cassandra.simulator.Action;
import org.apache.cassandra.simulator.systems.InterceptedExecution;
import org.apache.cassandra.simulator.systems.InterceptingExecutor;
import org.apache.cassandra.simulator.systems.SimulatedActionCallable;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

/**
 * Point of integration between Harry and Simulator. Creates series of stictly-ordered tasks that constitute a visit
 * of a single LTS.
 */
public class SimulatedVisitExectuor extends VisitExecutor
{
    private static final Logger logger = LoggerFactory.getLogger(SimulatedVisitExectuor.class);

    private Action action = null;
    private final List<Long> lts = new ArrayList<>();
    private final List<String> statements = new ArrayList<>();
    private final List<Object> bindings = new ArrayList<>();
    private final MutatingRowVisitor rowVisitor;
    private final DataTracker tracker;
    private final HarrySimulatorTest.HarrySimulation simulation;
    private final ConsistencyLevel cl;

    public SimulatedVisitExectuor(HarrySimulatorTest.HarrySimulation simulation,
                                  Run run,
                                  ConsistencyLevel cl)
    {
        this.rowVisitor = new MutatingRowVisitor(run);
        this.simulation = simulation;
        this.tracker = run.tracker;
        this.cl = cl;
    }

    public Action build()
    {
        String query = String.join(" ", statements);

        if (statements.size() > 1)
            query = String.format("BEGIN BATCH\n%s\nAPPLY BATCH;", query);

        Object[] bindingsArray = new Object[bindings.size()];
        bindings.toArray(bindingsArray);

        action = new SimulatedActionCallable<Object[][]>("Batch",
                                                         Action.Modifiers.RELIABLE_NO_TIMEOUTS,
                                                         Action.Modifiers.RELIABLE_NO_TIMEOUTS,
                                                         simulation.simulated,
                                                         simulation.cluster.get((int) ((lts.get(0) % simulation.cluster.size()) + 1)),
                                                         new RetryingQuery(query, cl, bindingsArray))
        {
            private final List<Long> localLts = new ArrayList<>(SimulatedVisitExectuor.this.lts);

            @Override
            protected InterceptedExecution.InterceptedTaskExecution task()
            {
                return new InterceptedExecution.InterceptedTaskExecution((InterceptingExecutor) on.executor())
                {
                    public void run()
                    {
                        for (Long l : localLts)
                            tracker.beginModification(l);    
                        
                        // we'll be invoked on the node's executor, but we need to ensure the task is loaded on its classloader
                        try { accept(on.unsafeCallOnThisThread(execute), null); }
                        catch (Throwable t) { accept(null, t); }
                        finally { execute = null; }
                    }
                };
            }

            @Override
            public void accept(Object[][] result, Throwable failure)
            {
                if (failure != null)
                    simulated.failures.accept(failure);
                else
                    for (Long l : localLts)
                        tracker.endModification(l);
            }
        };

        statements.clear();
        bindings.clear();
        lts.clear();

        Action current = action;
        action = null;
        return current;
    }

    @Override
    protected void beforeLts(long lts, long pd)
    {
        this.lts.add(lts);
    }

    @Override
    protected void afterLts(long lts, long pd)
    {
    }

    @Override
    protected void operation(Operation operation)
    {
        CompiledStatement statement = rowVisitor.perform(operation);
        statements.add(statement.cql());
        Collections.addAll(bindings, statement.bindings());
    }

    public void shutdown() throws InterruptedException
    {
    }

    private static class RetryingQuery extends Query
    {
        public RetryingQuery(String query, ConsistencyLevel cl, Object[] boundValues)
        {
            super(query, -1, cl, null, boundValues);
        }

        @Override
        public Object[][] call()
        {
            while (true)
            {
                try
                {
                    return super.call();
                }
                catch (UncheckedInterruptedException e)
                {
                    throw new RuntimeException(e);
                }
                catch (Throwable t)
                {
                    logger.error("Caught error while executing query. Will ignore and retry: " + t.getMessage());
                }
            }
        }
    }
}