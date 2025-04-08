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

package org.apache.cassandra.harry.stress;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.HdrHistogram.Histogram;
import org.apache.cassandra.concurrent.ExecutorFactory;
import org.apache.cassandra.concurrent.InfiniteLoopExecutor;
import org.apache.cassandra.concurrent.Interruptible;
import org.apache.cassandra.harry.execution.CQLVisitExecutor;
import org.apache.cassandra.harry.execution.CompiledStatement;
import org.apache.cassandra.harry.execution.DataTracker;
import org.apache.cassandra.harry.execution.InJvmDTestVisitExecutor;
import org.apache.cassandra.harry.execution.QueryBuildingVisitExecutor;
import org.apache.cassandra.harry.execution.ResultSetRow;
import org.apache.cassandra.harry.model.Model;
import org.apache.cassandra.harry.model.QuiescentChecker;
import org.apache.cassandra.harry.op.Operations;
import org.apache.cassandra.harry.op.Visit;
import org.apache.cassandra.utils.Clock;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.concurrent.InfiniteLoopExecutor.SimulatorSafe.SAFE;

public class StressWorker implements Interruptible
{
    public static class WorkerMetrics
    {
        final Histogram reads, writes;
        final AtomicInteger failedReads = new AtomicInteger(), failedWrites = new AtomicInteger();

        public WorkerMetrics(Histogram reads, Histogram writes)
        {
            this.reads = reads;
            this.writes = writes;
        }
    }
    private final LinkedBlockingQueue<Visit> tasks;
    private final int capacity;

    private final CQLVisitExecutor executor;
    private final Interruptible loop;
    private final AtomicReference<WorkerMetrics> metrics = new AtomicReference<>();

    public StressWorker(int idx,
                        Model.PartialReplay replay,
                        DataTracker tracker,
                        ActivePartition.Partitions partitionFactory,
                        Supplier<BiFunction<CompiledStatement, Runnable, Object[][]>> sutFactory,
                        Consumer<Throwable> onException,
                        int queueCapacity)
    {
        resetMetrics();
        Function<CompiledStatement, Object[][]> sut = new Function<>()
        {
            final BiFunction<CompiledStatement, Runnable, Object[][]> delegate = sutFactory.get();

            @Override
            public Object[][] apply(CompiledStatement compiledStatement)
            {
                long start = Clock.Global.nanoTime();
                return delegate.apply(compiledStatement, () -> {
                    long end = Clock.Global.nanoTime();
                    WorkerMetrics metrics1 = StressWorker.this.metrics.get();
                    (compiledStatement.validating ? metrics1.reads : metrics1.writes).recordValue(end - start);
                });
            }
        };

        this.tasks = new LinkedBlockingQueue<>(queueCapacity);
        this.capacity = queueCapacity;
        this.executor = new CQLVisitExecutor(partitionFactory.schema,
                                             tracker,
                                             new QuiescentChecker(partitionFactory, replay),
                                             new QueryBuildingVisitExecutor(partitionFactory.schema, QueryBuildingVisitExecutor.WrapQueries.EMPTY, partitionFactory))
        {
            @Override
            protected List<ResultSetRow> executeWithResult(Visit visit, CompiledStatement statement)
            {
                Object[][] result = sut.apply(statement);
                if (result == null)
                    return new ArrayList<>();
                return InJvmDTestVisitExecutor.rowsToResultSet(schema, partitionFactory,
                                                               (Operations.SelectStatement) visit.operations[0], result);
            }

            @Override
            protected void executeWithoutResult(Visit visit, CompiledStatement statement)
            {
                sut.apply(statement);
            }
        };
        this.loop = executorFactory().infiniteLoop("visit-executor" + idx, state -> {
            try
            {
                switch (state)
                {
                    case NORMAL:
                        Visit visit = tasks.take();
                        try
                        {
                            executor.execute(visit);
                        }
                        catch (Throwable e)
                        {
                            if (e.getClass().toString().contains("InterruptedException"))
                                return;
                            WorkerMetrics ms = metrics.get();
                            (visit.validating ? ms.failedReads : ms.failedWrites).incrementAndGet();
                            onException.accept(e);
                        }
                        break;
                    case INTERRUPTED:
                    case SHUTTING_DOWN:
                        break;
                }
            }
            catch (Throwable e)
            {
                onException.accept(e);
            }
        }, SAFE, ExecutorFactory.SystemThreadTag.DAEMON, InfiniteLoopExecutor.Interrupts.UNSYNCHRONIZED);
    }

    public int getFreeSlots()
    {
        return capacity - tasks.size();
    }

    public WorkerMetrics resetMetrics()
    {
        return metrics.getAndSet(new WorkerMetrics(new Histogram(3), new Histogram(3)));
    }

    public boolean offer(Visit visit)
    {
        return tasks.offer(visit);
    }

    @Override
    public void interrupt()
    {
        loop.interrupt();
    }

    @Override
    public boolean isTerminated()
    {
        return loop.isTerminated();
    }

    @Override
    public void shutdown()
    {
        loop.shutdown();
    }

    @Override
    public Object shutdownNow()
    {
        return loop.shutdownNow();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit units) throws InterruptedException
    {
        return loop.awaitTermination(timeout, units);
    }
}