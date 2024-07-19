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

package org.apache.cassandra.harry.visitors;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ExecutorFactory;
import org.apache.cassandra.concurrent.InfiniteLoopExecutor;
import org.apache.cassandra.concurrent.Interruptible;
import org.apache.cassandra.harry.core.Configuration;
import org.apache.cassandra.harry.core.MetricReporter;
import org.apache.cassandra.harry.core.Run;
import org.apache.cassandra.harry.ddl.SchemaSpec;
import org.apache.cassandra.harry.model.Model;
import org.apache.cassandra.harry.model.OpSelectors;
import org.apache.cassandra.harry.sut.SystemUnderTest;
import org.apache.cassandra.harry.operations.Query;
import org.apache.cassandra.harry.tracker.DataTracker;
import org.apache.cassandra.harry.runner.Runner;
import org.apache.cassandra.utils.concurrent.WaitQueue;

import static org.apache.cassandra.concurrent.InfiniteLoopExecutor.Daemon.NON_DAEMON;
import static org.apache.cassandra.concurrent.InfiniteLoopExecutor.Interrupts.UNSYNCHRONIZED;
import static org.apache.cassandra.concurrent.InfiniteLoopExecutor.SimulatorSafe.SAFE;

/**
 * Concurrently validates all partitions that were visited during this run.
 */
public class AllPartitionsValidator implements Visitor
{
    private static final Logger logger = LoggerFactory.getLogger(AllPartitionsValidator.class);

    protected final Model model;
    protected final SchemaSpec schema;
    protected final QueryLogger queryLogger;
    protected final OpSelectors.Clock clock;
    protected final OpSelectors.PdSelector pdSelector;
    protected final MetricReporter metricReporter;
    protected final SystemUnderTest sut;
    protected final DataTracker tracker;

    protected final int concurrency;

    public static Configuration.VisitorConfiguration factoryForTests(int concurrency,
                                                                     Model.ModelFactory modelFactory)
    {
        return (r) -> new AllPartitionsValidator(r, concurrency, modelFactory, QueryLogger.NO_OP);
    }

    public AllPartitionsValidator(Run run,
                                  int concurrency,
                                  Model.ModelFactory modelFactory)
    {
        this(run, concurrency, modelFactory, QueryLogger.NO_OP);
    }

    public AllPartitionsValidator(Run run,
                                  int concurrency,
                                  Model.ModelFactory modelFactory,
                                  QueryLogger logger)
    {
        this.metricReporter = run.metricReporter;
        this.model = modelFactory.make(run);
        this.schema = run.schemaSpec;
        this.clock = run.clock;
        this.sut = run.sut;
        this.pdSelector = run.pdSelector;
        this.concurrency = concurrency;
        this.tracker = run.tracker;
        this.queryLogger = logger;
    }

    protected void validateAllPartitions() throws Throwable
    {
        List<Interruptible> threads = new ArrayList<>();
        WaitQueue queue = WaitQueue.newWaitQueue();
        WaitQueue.Signal interrupt = queue.register();
        List<Throwable> errors = new CopyOnWriteArrayList<>();

        final long maxPosition = pdSelector.maxPosition(tracker.maxStarted());

        AtomicLong currentPosition = new AtomicLong();
        for (int i = 0; i < concurrency; i++)
        {
            Interruptible thread = ExecutorFactory.Global.executorFactory().infiniteLoop(String.format("AllPartitionsValidator-%d", i + 1),
                                                                                         Runner.wrapInterrupt((state) -> {
                                                                                             if (state == Interruptible.State.NORMAL)
                                                                                             {
                                                                                                 metricReporter.validatePartition();
                                                                                                 long pos = currentPosition.getAndIncrement();
                                                                                                 if (pos < maxPosition)
                                                                                                 {
                                                                                                     for (boolean reverse : new boolean[]{ true, false })
                                                                                                     {
                                                                                                         Query query = Query.selectAllColumns(schema, pdSelector.pd(pdSelector.minLtsAt(pos), schema), reverse);
                                                                                                         model.validate(query);
                                                                                                         queryLogger.logSelectQuery((int)pos, query);
                                                                                                     }
                                                                                                 }
                                                                                                 else
                                                                                                 {
                                                                                                     interrupt.signalAll();
                                                                                                 }
                                                                                             }
                                                                                         }, interrupt::signal, errors::add), SAFE, NON_DAEMON, UNSYNCHRONIZED);
            threads.add(thread);
        }

        interrupt.awaitUninterruptibly();

        for (Interruptible thread : threads)
        {
            ((InfiniteLoopExecutor)thread).shutdown(false);
            Assert.assertTrue(thread.awaitTermination(1, TimeUnit.MINUTES));
        }

        if (!errors.isEmpty())
            Runner.mergeAndThrow(errors);
    }

    public void visit()
    {
        try
        {
            validateAllPartitions();
        }
        catch (Throwable e)
        {
            throw new RuntimeException(e);
        }
    }
}